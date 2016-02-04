console.log('Loading function');

require('dotenv').config();

var doc = require('dynamodb-doc');
var async = require('async');

var aws = require('aws-sdk');
aws.config.update({region: 'us-east-1'});

var sqs = new aws.SQS({ apiVersion: '2012-11-05' });
var s3 = new aws.S3({ apiVersion: '2006-03-01' });
var dynamo = new doc.DynamoDB();

var config = {
  QueueUrl: 'https://sqs.us-east-1.amazonaws.com/562785646348/OrderComplete_Warranty',
  MaxNumberOfMessages: 10
}

exports.handler = function(event, context) {
    console.log('Received event:', JSON.stringify(event, null, 2));

    getMessages();
    

    
    //context.succeed();
    //context.fail();
};

function handleMessage(message) {
    var body = JSON.parse(message.Body);
    if (body.Event && body.Event === 's3:TestEvent'){
        console.log('Found test message. Ignore and remove from queue.')
        deleteMessage(message);
    } else {
        var s3Info = body.Records[0].s3;
        var params = { Bucket: s3Info.bucket.name, Key: s3Info.object.key };
        s3.getObject(params, function(err, data) {
            if (err){
                console.log(err);
            } else {
                processEvent(JSON.parse(data.Body), message);
            }
        });
        console.log(body.Records[0].s3.object.key);
    }
}

function processEvent(event, message) {
    console.log(event);
    
    var serialized = [];
    var warranties = [];
    
    var items = event.items;
    var item;
    for (var i = 0; i < items.length; i++){
        item = items[i];
        if (item.serial){
            serialized.push(
                { 
                    sku: item.sku, 
                    serial: item.serial, 
                    provenance: [ { orderId: event.orderId, itemId: item.itemId, orderDateUtc: event.orderDateUtc, action: item.action } ] 
                });
        } else if (item.warranty){
            warranties.push(item.warranty);
        }
    }
    
    var serialized2 = matchWarranties(serialized, warranties);
    
    dispatchSerialized(serialized, message);
}

function matchWarranties(serialized, warranties) {
    console.log(serialized[0].provenance);
    console.log(warranties);
    for (var i = 0; i<serialized.length; i++) {
        for(var j = 0; j<warranties.length; j++){
            if (serialized[i].provenance[0].itemId === warranties[j].coversItemId) {
                serialized[i].provenance[0].warranty = { program: warranties[j].program, expireDate: warranties[j].expireDate };
            } 
        }
    }
    
    return serialized;
}

function dispatchSerialized(serialized, message) {
    for(var i = 0; i<serialized.length; i++){
        console.log(serialized[i]);
    }
    deleteMessage(message);
}

var receiveMessageParams = {
  QueueUrl: config.QueueUrl,
  MaxNumberOfMessages: config.MaxNumberOfMessages
};

function getMessages() {
  console.log(receiveMessageParams);
  sqs.receiveMessage(receiveMessageParams, receiveMessageCallback);
}

function deleteMessage(message){
      // Delete the message when we've successfully processed it
      var deleteMessageParams = {
        QueueUrl: config.QueueUrl,
        ReceiptHandle: message.ReceiptHandle
      };

      console.log('delete message here');
      //sqs.deleteMessage(deleteMessageParams, deleteMessageCallback);    
}

function receiveMessageCallback(err, data) {
  console.log(data);
  if (err){
      console.log(err);
  } else if (data && data.Messages && data.Messages.length > 0) {
        var messages = data.Messages;
        console.log('got data ' + messages.length);
        for(var i=0; i<messages.length; i++){
            handleMessage(messages[i]);
        }
  }
    /*if (data && data.Messages && data.Messages.length > 0) {
    for (var i=0; i < data.Messages.length; i++) {
      process.stdout.write(".");
      //console.log("do something with the message here...");
      //
      deleteMessage(data.Messages[i]);
    }
  } else {
    process.stdout.write("-");
    setTimeout(getMessages(), 100);
  }*/
}

function deleteMessageCallback(err, data) {
    if (err){
        console.log(err);
    } else {
        console.log("deleted message");
        console.log(data);
    }
}

/**
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