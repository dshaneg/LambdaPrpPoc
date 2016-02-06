module.exports = function(docClient){
    
    return {
        persistAll: function (serializedProducts) {
            for(var i = 0; i<serializedProducts.length; i++) {
                persist(serializedProducts[i]);
            }
        }
    }

    function persist(serializedProduct) {
        console.log("Need to write: ", JSON.stringify(serializedProduct, null, 2));

        var params = {
            TableName: 'SerialNumberProvenance',
            Item: serializedProduct,
            ConditionExpression: "productId <> :productId and serialNumber <> :serialNumber",
            ExpressionAttributeValues:{
                ":productId": serializedProduct.productId,
                ":serialNumber": serializedProduct.serialNumber
            }            
        };
        
        docClient.put(params, function(err, data) {
            if (err) {
                if (err.code === 'ConditionalCheckFailedException'){
                    console.log("found existing record.")
                    persistExisting(serializedProduct);
                }
                console.log(err);
            }
            else {
                console.log("wrote new record");
            }
        });
    }
    
    function persistExisting(serializedProduct){
        var params = {
            TableName: 'SerialNumberProvenance',
            Key: {
                productId: serializedProduct.productId,
                serialNumber: serializedProduct.serialNumber
            },
            
        };
        
        docClient.get(params, function (err, data) {
            if (err) {
                console.log(err);
            } else {
                dispatchSerializedRead(data.Item, serializedProduct);
            }
            
        });
    }
    
    function dispatchSerializedRead(data, serializedProduct) {
        console.log("read: ", JSON.stringify(data, null, 2));
        
        // need to do etags or something to identify write conflicts

        if (provenanceEntryExists(data, serializedProduct.provenance[0])) {
            console.log("Duplicate update attempt. Bailing.")
            return;
        }
        
        // is it worth ordering these? It should be very rare that they could get out of order, 
        // and it'd be more efficient to order in the service that reads the entries if needed.
        data.provenance.push(serializedProduct.provenance[0]);
        
        var params = {
            TableName: 'SerialNumberProvenance',
            Item: data
        };
        
        docClient.put(params, function(err, data) {
            if (err) {
                console.log(err);
            }
            else {
                console.log(data);
            }
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
}