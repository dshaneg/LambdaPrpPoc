# LambdaPrpPoc
Proof of concept aws lambda function using s3, sns, lambda, and dynamodb. With promises!

Getting my brain wrapped around aws and its plethora of services at the same time as learning node.

This code is made for aws lambda, and doesn't stand on its own very well. I'm using [node-lambda](https://www.npmjs.com/package/node-lambda) 
for local interactive testing--no unit tests so far. I've also got the local version of dynamodb running--however, my local tests still hit
s3 in the cloud.

I'm triggering this lambda from SNS, which is subscribed to the put event of an s3 bucket. The lambda function then takes the content of the
s3 bucket, munges the content and then stores the result in dynamodb.

I'm using the [promise](https://www.npmjs.com/package/promise) library to synchronize the results of all the parallel calls to the various services 
so that lambda can exit gracefully with succeed() or fail(). Through trial and error I figured out that if you don't succeed(), then at least
SNS assumes you've failed and kicks off its retry logic. Here's a [blog post](https://aws.amazon.com/blogs/compute/getting-nodejs-and-lambda-to-play-nicely/)
that lays out the problem with lambda, node and asynchronous calls.
