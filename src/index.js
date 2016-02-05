console.log('Loading function');

//require('dotenv').config();

var AWS = require('aws-sdk');
var doc = require('dynamodb-doc');

//AWS.config.update({region: 'us-east-1'});

var s3 = new AWS.S3({ apiVersion: '2006-03-01' });
var dynamo = new doc.DynamoDB();

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
                    sku: item.sku, 
                    serial: item.serial, 
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
    
    dispatchSerialized(serialized);
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

function dispatchSerialized(serialized) {
    for(var i = 0; i<serialized.length; i++){
        console.log("Need to write: ", JSON.stringify(serialized[i], null, 2));
    }
}

/*
 * Provide an event that contains the following keys:
 *
 *   - operation: one of the operations in the switch statement below
 *   - tableName: required for operations that interact with DynamoDB
 *   - payload: a parameter to pass to the operation being performed
 *//*
{
    var operation = event.operation;

    if (event.tableName) {
        event.payload.TableName = event.tableName;
    }

    switch (operation) {
        case 'create':
            dynamo.putItem(event.payload, context.done);
            break;
        case 'read':
            dynamo.getItem(event.payload, context.done);
            break;
        case 'update':
            dynamo.updateItem(event.payload, context.done);
            break;
        case 'delete':
            dynamo.deleteItem(event.payload, context.done);
            break;
        case 'list':
            dynamo.scan(event.payload, context.done);
            break;
        case 'echo':
            context.succeed(event.payload);
            break;
        case 'ping':
            context.succeed('pong');
            break;
        default:
            context.fail(new Error('Unrecognized operation "' + operation + '"'));
    }}
}
*/