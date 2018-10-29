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

var allKeys = [];
var isTruncated = true;
var marker = '';

var s3Params = {
    Bucket: '<bucket-name>',   
    MaxKeys: 10000000,
    Prefix: '<data-prefix>'
    
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

var keyCounter = 0,
  lineCounter = 0,
  rec = 0,
  myContext = null,
  i = 0,
  retry = 0,
  startIndexing = false,
  maxTries = 5;


function s3ToLogs(bucket, key, context) {
    keyCounter  += 1;
    if(typeof key != 'undefined') {
        key = key.replace('+', ' ');
        var s3Stream = s3.getObject({Bucket: bucket, Key: key}).createReadStream().promise();
        const r1 = readline.createInterface(s3Stream);
        // console.log("Read Stream==>" + s3Stream);
        var arr = [];
        var json = '';
        
        // Flow: S3 file stream -> Log Line stream -> Log Record stream -> ES
        r1
          .on('line', function(line) {
              lineCounter = lineCounter + 1;
                  arr = JSON.parse(line).data;
                  for(var item of arr) {
                    rec = rec + 1;
                    item.file = key;
                    json = JSON.stringify(item);
                    console.log(json);
                  }
            });
    
        r1
          .on('error', function() {
            console.log(
                'Error getting object "' + key + '" from bucket "' + bucket + '".  ' +
                'Make sure they exist and your bucket is in the same region as this function.');
            context.fail();
        });
    }
}

async function listAllObjectsFromS3BucketAndIndexToES(context) {
  let isTruncated = true;
  let marker;
  let i = 0;
  let key = '';
  let arr = [];
  let json = '';
  
  while(isTruncated) {
    if (marker) 
        s3Params.Marker = marker;
    try {
      const response = await s3.listObjects(s3Params).promise();
      response.Contents.forEach(item => {
        key = item.Key;
        if(key.indexOf('json') > 0) {
            // console.log("[" + i + "] -> " + key);
            i++;
            if(typeof key != 'undefined') {
                key = key.replace('+', ' ');
                var s3Stream = s3.getObject({Bucket: s3Params.Bucket, Key: key}).createReadStream();
                const r1 = readline.createInterface(s3Stream);
                r1
                  .on('line', function(line) {
                    //   console.log(line);
                      lineCounter = lineCounter + 1;
                          arr = JSON.parse(line).data;
                          if(arr != null && arr.length > 0) {
                              for(var jsonItem of arr) {
                                rec = rec + 1;
                                jsonItem.file = key;
                                json = JSON.stringify(jsonItem);
                                console.log(json);
                              }
                          }
                    });
            
                r1
                  .on('error', function() {
                    console.log(
                        'Error getting object "' + key + '" from bucket "' + s3Params.Bucket + '".  ' +
                        'Make sure they exist and your bucket is in the same region as this function.');
                    context.fail();
                });
            }
        }
      });
      isTruncated = response.IsTruncated;
      console.log('isTruncated=>' + isTruncated);
      if (isTruncated) {
        marker = response.Contents.slice(-1)[0].Key;
      }
  } catch(error) {
      console.log(error);
      console.log('Total = ' + i);
      throw error;
    }
    console.log('Total = ' + i);
  }
}

// function getAllS3Keys(context) {
//     s3.listObjectsV2(s3Params, function (err, data) {
//         if (err) {
//             console.log(err, err.stack); // an error occurred
//         } 
//         else {
//             // while(isTruncated) {
//             var contents = data.Contents;
//             contents.forEach(function (content) {
//                 allKeys.push(content.Key);
//                 console.log("Key Count [" + (keyCounter++) + "]");
//                 // console.log('Indexing => ' + content.Key);
//                 s3ToLogs(s3Params.Bucket, content.Key, context);
//             });

//             isTruncated = data.IsTruncated;
//             if (isTruncated) {
//                 s3Params.ContinuationToken = data.NextContinuationToken;
//                 // marker = data.NextMarker;
//                 // console.log("marker = " + marker);
//                 console.log("get further list...");
//                 getAllS3Keys(context);
//             } 
//         }

//         // }
//     });
// }

function getAllS3Keys() {
    s3.listObjectsV2(s3Params, function (err, data) {
        if (err) {
            console.log(err, err.stack); // an error occurred
        } 
        else {
            while(data.IsTruncated) {
                s3Params.ContinuationToken = data.NextContinuationToken;
                console.log("Total Keys Fetched so far [" + allKeys.length + "], getting further list...");
                
                var contents = data.Contents;
                contents.forEach(function (content) {
                    if(content.Key.indexOf('json') > 0) {
                        allKeys.push(content.Key);
                    }
                });
            }

            if(!data.IsTruncated) {
                indexToES(myContext);
            }
        }
    });
}



const sleep = (milliseconds) => {
  return new Promise(resolve => setTimeout(resolve, milliseconds))
}

function indexToES(context) {
    console.log("TOTAL/Processed KEYS = [" + allKeys.length + "/" + i + "]" );
    if(allKeys.length > 0) {
        allKeys.forEach(function (s3Key) {
            i++;
            if(i%5000 == 0) {
                sleep(5000).then(() => {
                  console.log("sleeping for 5 secs");
                });            
            }
            
            try {
                // console.log("Key [" + s3Key + "]");
                s3ToLogs(s3Params.Bucket, s3Key, context);
            }
            catch(ex) {
                console.log(ex);
                console.log("Error thrown at KEY# " + i);
                retry++;
                if(retry == maxTries) {
                    throw ('Max tries Reached. Exiting now');
                }
            }
        });
    }
    
    startIndexing = false;
}


/* Lambda "main": Execution starts here */
exports.handler = function(event, context) {
    listAllObjectsFromS3BucketAndIndexToES(context);
};
