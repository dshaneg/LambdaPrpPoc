console.log('Loading function');

var AWS = require('aws-sdk');
AWS.config.update({region: 'us-east-1'});

var s3 = new AWS.S3({ apiVersion: '2006-03-01' });

var docClient = new AWS.DynamoDB.DocumentClient(/*{ endpoint: "http://localhost:8000" }*/);
var dynamo = require('./dynamo')(docClient);

exports.handler = function(event, context) {
    console.log('Received event:', JSON.stringify(event, null, 2));
    
    var s3Info = JSON.parse(event.Records[0].Sns.Message).Records[0].s3;

    var params = { Bucket: s3Info.bucket.name, Key: s3Info.object.key };
    
    s3.getObject(params, function(err, data) {
        if (err){
            console.log(err);
        } else {
            processEvent(JSON.parse(data.Body), params);
        }
    });
};

function processEvent(orderCompleteEvent, source) {
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
    
    matchWarranties(serialized, warranties);
    
    dynamo.persistAll(serialized);
}

function matchWarranties(serialized, warranties) {
    for (var i = 0; i<serialized.length; i++) {
        for(var j = 0; j<warranties.length; j++){
            if (serialized[i].provenance[0].itemId === warranties[j].coversItemId) {
                serialized[i].provenance[0].warranty = { program: warranties[j].program, expireDate: warranties[j].expireDate };
            } 
        }
    }
    
    return serialized;
}

