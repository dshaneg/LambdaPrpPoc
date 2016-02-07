console.log('Loading function');

var AWS = require('aws-sdk');
AWS.config.update({region: 'us-east-1'});

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
            if (err) reject(err);
            else fulfill(data);
        });
    });
};

var docClient = new AWS.DynamoDB.DocumentClient(/*{ endpoint: "http://localhost:8000" }*/);
var dynamo = require('./dynamo')(docClient);

exports.handler = function(event, context) {
    console.log('Received event:', JSON.stringify(event, null, 2));
    
    var params = getS3Params(event);
    
    s3GetAsync(params)
        .then(function(data){
            return processEvent(JSON.parse(data.Body), params); // may need to deal with the json.parse for exceptions. will promise swallow?
        })
        .then(context.succeed)
        .catch(context.fail);
};

function getS3Params(event) {
    // based on the SNS event triggered from dropping an object into S3
    var s3Info = JSON.parse(event.Records[0].Sns.Message).Records[0].s3;

    return { Bucket: s3Info.bucket.name, Key: s3Info.object.key };
}

function processEvent(orderCompleteEvent, source) {
    var filteredItems = filterItems(orderCompleteEvent, source);

    matchWarranties(filteredItems.serialized, filteredItems.warranties);

    return Promise.all(filteredItems.serialized.map(dynamo.persist));
}

function filterItems(orderCompleteEvent, source){
    var serialized = [];
    var warranties = [];
    
    var items = orderCompleteEvent.items;
    var item;
    for (var i = 0; i < items.length; i++){
        item = items[i];
        if (item.serial){
            serialized.push(
                { 
                    productId: item.sku, 
                    serialNumber: item.serial, 
                    provenance: [ { 
                        source: source,
                        orderId: orderCompleteEvent.orderId, 
                        itemId: item.itemId, 
                        orderDateUtc: orderCompleteEvent.orderDateUtc, 
                        action: item.action 
                    } ] 
                });
        } else if (item.warranty){
            warranties.push(item.warranty);
        }
    }
    
    return { serialized: serialized, warranties: warranties }
}

function matchWarranties(serialized, warranties) {
    for (var i = 0; i<serialized.length; i++) {
        for(var j = 0; j<warranties.length; j++){
            if (serialized[i].provenance[0].itemId === warranties[j].coversItemId) {
                serialized[i].provenance[0].warranty = { program: warranties[j].program, expireDate: warranties[j].expireDate };
            } 
        }
    }
}

