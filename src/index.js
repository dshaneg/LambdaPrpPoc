console.log('Loading function');

var AWS = require('aws-sdk');
AWS.config.update({region: 'us-east-1'});

var moment = require('moment');
moment().format();

// https://aws.amazon.com/blogs/compute/getting-nodejs-and-lambda-to-play-nicely/
// without calling done, succeed for fail, sns will follow its retry policy 
// because it assumes the function failed if it doesn't get a response.
// Using promises to synchronize the possibly many asynchronous calls so we can call succeed/fail when done.
var Promise = require('promise');

var s3 = new AWS.S3({ apiVersion: '2006-03-01' });

//var s3GetAsync = Promise.denodeify(s3.getObject);
// not sure why the denodeify didn't work
var s3GetAsync = function(params) {
    return new Promise(function(fulfill, reject) {
        s3.getObject(params, function(err, data) {
            if (err) { reject(err); }
            else { fulfill(data); }
        });
    });
};

var docClient = new AWS.DynamoDB.DocumentClient( { endpoint: "http://localhost:8000" } );
var dynamo = require('./dynamo')(docClient);

exports.handler = function(event, context) {
    console.log('Received event:', JSON.stringify(event, null, 2));
    
    var params = getS3Params(event);
    
    s3GetAsync(params)
        .then(function(data){
            // may need to deal with the json.parse for exceptions. will promise swallow?
            return processEvent(JSON.parse(data.Body), params);
        })
        .then(function(val) {
            console.log(val);
            context.succeed(val);
        })
        .catch(context.fail);
};

function getS3Params(event) {
    // based on the SNS event triggered from dropping an object into S3
    var s3Info = JSON.parse(event.Records[0].Sns.Message).Records[0].s3;

    return { Bucket: s3Info.bucket.name, Key: s3Info.object.key };
}

function processEvent(orderCompleteEvent, source) {
    var serializedItems = filterItems(orderCompleteEvent, source);

    return Promise.all(serializedItems.map(dynamo.persist));
}

function filterItems(orderCompleteEvent, source){
    var serialized = [];
    var items = orderCompleteEvent.lineItems;
    var item;
    var i;
    
    for (i = 0; i < items.length; i++){
        item = items[i];
        if (item.serialNumber){
            serialized.push(buildProvenanceEntry(item, orderCompleteEvent, source));
        }
    }
    
    return serialized;
}

function buildProvenanceEntry(item, orderCompleteEvent, source) {
     // need a real product id--sku can be different for the same product
     var entry = { 
        productId: item.product.sku.skuNumber,
        serialNumber: item.serialNumber, 
        provenance: [ { 
            source: source,
            orderId: orderCompleteEvent.number, 
            sequence: item.sequenceNumber,
            lineId: item.identifier, 
            orderDate: orderCompleteEvent.completedDate, 
            action: item.lineItemType 
        } ] 
    }
    
    var warranty = findWarranty(item, orderCompleteEvent.completedDate);
    
    if (warranty) {
        entry.provenance[0].warranty = warranty;
    }
    
    return entry;
}

function findWarranty( item, orderCompletedDate ) {
    var i;
    var childProduct;
    
    if ( !item.children ) { return; }
        
    for ( i = 0; i < item.children.length; i +=1 ) {
        childProduct = item.children[i].lineItem.product;
        
        if ( childProduct.isPrp ) {
            return {
                expireDate: getExpireDate(childProduct, orderCompletedDate),
                program: "PRP"
            };
        }
    }
}

function getExpireDate(warrantyProduct, orderCompletedDate) {
    // need metadata in the product or a service that can provide the length of time for a warranty.
    if (warrantyProduct.name.indexOf("1 Year") !== -1) {
        return moment(orderCompletedDate).add(1, 'years').toISOString(); 
    }
    else {
        return moment(orderCompletedDate).add(2, 'years').toISOString();
    }
}


