var Promise = require('promise');

module.exports = function(docClient){

    var dynamoPutAsync = function(params) {
        return new Promise(function(fulfill, reject) {
            docClient.put(params, function(err, data) {
                if (err) reject(err);
                else fulfill(data);
            });
        });
    };

    var dynamoGetAsync = function(params) {
        return new Promise(function(fulfill, reject) {
            docClient.get(params, function(err, data) {
                if (err) reject(err);
                else fulfill(data);
            });
        });
    };

    return {
        persist: function(serializedProduct) {
            console.log("Need to write: ", JSON.stringify(serializedProduct, null, 2));

            // attempt a write first, assuming the more likely case that this product doesn't yet exist in our data store.
            // if it exists, we'll fall into the routine that pulls the record, updates it, then puts it back.

            var params = {
                TableName: 'SerialNumberProvenance',
                Item: serializedProduct,
                ConditionExpression: "attribute_not_exists(productId) and attribute_not_exists(serialNumber)"
            };
            
            return dynamoPutAsync(params)
                .then(function() {
                    return resolve(serializedProduct, "Wrote new record.");
                }, function(err) {
                    if (err.code === 'ConditionalCheckFailedException'){
                        return persistExisting(serializedProduct);
                    }
                    return Promise.reject(err);
                });
        }
    }

    function persistExisting(serializedProduct){
        var params = {
            TableName: 'SerialNumberProvenance',
            Key: {
                productId: serializedProduct.productId,
                serialNumber: serializedProduct.serialNumber
            },
            
        };
        
        return dynamoGetAsync(params)
            .then(function(data) {
                return dispatchReadResult(data.Item, serializedProduct);
            });
    }
    
    function dispatchReadResult(data, serializedProduct) {
        console.log("read: ", JSON.stringify(data, null, 2));
        
        // todo: need to do etags or something to identify write conflicts

        if (provenanceEntryExists(data, serializedProduct.provenance[0])) {
            return resolve(serializedProduct, "Duplicate record. No action taken.");
        }
        
        // is it worth ordering these? It should be very rare that they could get out of order, 
        // and it'd be more efficient to order in the service that reads the entries if needed.
        data.provenance.push(serializedProduct.provenance[0]);
        
        var params = {
            TableName: 'SerialNumberProvenance',
            Item: data
        };
        
        return dynamoPutAsync(params)
            .then(function() {
                return resolve(serializedProduct, "Updated.");
            });
    }
    
    function provenanceEntryExists(data, provenanceEntry){
        var current;
        for(var i=0; i<data.provenance.length; i++){
            current = data.provenance[i];
            
            if (current.orderId === provenanceEntry.orderId && current.itemId === provenanceEntry.itemId){
                return true;
            }
        }
        
        return false;
    }
    
    function resolve(serializedProduct, message) {
        return Promise.resolve({ 
            productId: serializedProduct.productId, 
            serialNumber: serializedProduct.serialNumber, 
            result: message });
    }
}