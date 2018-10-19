var AWS = require('aws-sdk');
var path = require('path');
var stream = require('stream');
var crypto = require('crypto');
const readline = require('readline');
var utils = require('./utils');

// var params = {
//   Bucket: 'STRING_VALUE', /* required */
//   ContinuationToken: 'STRING_VALUE',
//   Delimiter: 'STRING_VALUE',
//   EncodingType: url,
//   FetchOwner: true || false,
//   MaxKeys: 0,
//   Prefix: 'STRING_VALUE',
//   RequestPayer: requester,
//   StartAfter: 'STRING_VALUE'
// };

var s3Params = {
    Bucket: '<bucket>',   
    Prefix: '<s3-key-prefix>'
};

var s3 = new AWS.S3();
/*
 * The AWS credentials are picked up from the environment.
 * They belong to the IAM role assigned to the Lambda function.
 * Since the ES requests are signed using these credentials,
 * make sure to apply a policy that permits ES domain operations
 * to the role.
 */
var credentials = new AWS.EnvironmentCredentials('AWS');

var keyCounter = 0;
var lineCounter = 0;
var rec = 0;

function s3ToLogs(bucket, key, context) {
    keyCounter  += 1;
    // Note: The Lambda function should be configured to filter for .log files
    // (as part of the Event Source "suffix" setting).
    
    if(typeof key != 'undefined') {
        key = key.replace('+', ' ');
        var s3Stream = s3.getObject({Bucket: bucket, Key: key}).createReadStream();
        const r1 = readline.createInterface(s3Stream);
        // console.log("Read Stream==>" + s3Stream);
        var arr = [];
        var json = '';
        
        // Flow: S3 file stream -> Log Line stream -> Log Record stream -> ES
        r1
          .on('line', function(line) {
              lineCounter = lineCounter + 1;
                arr = JSON.parse(line).data;
    //          line = JSON.stringify(JSON.parse(line)
    //                     .data[0]);
              for(var item of arr) {
                rec = rec + 1;
                // console.log("File, Line, rec# " + keyCounter + ", " + lineCounter + ", " + rec);
                json = JSON.stringify(item);
                console.log(json);
                // indexDocument(json, utils.md5(json));
              }
          });
    
        r1.on('error', function() {
            console.log(
                'Error getting object "' + key + '" from bucket "' + bucket + '".  ' +
                'Make sure they exist and your bucket is in the same region as this function.');
            context.fail();
        });
    }
}

function logESJSONs(context) {
    s3.listObjectsV2(s3Params, function (err, data) {
        if (err) {
            console.log(err, err.stack); // an error occurred
        } else {
            var contents = data.Contents;
            contents.forEach(function (content) {
                // allKeys.push(content.Key);
                // console.log('Indexing => ' + content.Key);
                s3ToLogs(s3Params.Bucket, content.Key, context);
            });

            if (data.IsTruncated) {
                s3Params.ContinuationToken = data.NextContinuationToken;
                console.log("get further list...");
                s3ToLogs();
            } 

        }
    });
}

/* Lambda "main": Execution starts here */
exports.handler = function(event, context) {
    console.log('Received event: ', JSON.stringify(event, null, 2));
    // var bucket = 'upen-data';
    // var objKey = 'samples/es/movies3.json';
    // s3ToLogs(bucket, objKey, context);
    logESJSONs(context);
}
