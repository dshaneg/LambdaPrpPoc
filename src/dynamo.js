module.exports = function(docClient){
    
    function dispatchSerializedRead(data, serializedProduct) {
        console.log("read: ", JSON.stringify(data, null, 2));
        
        // need to do etags or something to identify write conflicts
        
        var params = {
            TableName: 'SerialNumberProvenance',
        };
        
        // update or insert
        if (data) {
            data.provenance.push(serializedProduct.provenance[0]);
            params.Item = data;
        } else {
            params.Item = serializedProduct;
        }
        
        docClient.put(params, function(err, data) {
            if (err) {
                console.log(err);
            }
            else {
                console.log(data);
            }
        });
    }
    
    function persist(serializedProduct){
        console.log("Need to write: ", JSON.stringify(serializedProduct, null, 2));
        
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

    return {
        persistAll: function (serializedProducts) {
            for(var i = 0; i<serializedProducts.length; i++){
                persist(serializedProducts[i]);
            }
        }
    }
}