
var AWS = require('aws-sdk');
// var LineStream = require('byline').LineStream;
// var parse = require('clf-parser');  // Apache Common Log Format
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
    Bucket: 'upen-data',   
    Prefix: 'samples/es/bulk/'
};

/* Globals */
var esDomain = {
    domain: 'search-upen-movies-3fez4ibnfh3ld6kfqetziwcvju.us-east-1.es.amazonaws.com',
    region: 'us-east-1',
    index: 'upen-movies',
    doctype: 'movie'
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
var endpoint = new AWS.Endpoint(esDomain.domain);
var request = new AWS.HttpRequest(endpoint, esDomain.region);

/*
 * Get the log file from the given S3 bucket and key.  Parse it and add
 * each log record to the ES domain.
 */
function s3ToESOrig(bucket, key, context) {
    console.log('S3ToES ==> bucket, Key = ' + bucket + ', ' + key);
    // Note: The Lambda function should be configured to filter for .log files
    // (as part of the Event Source "suffix" setting).

    var s3Stream = s3.getObject({Bucket: bucket, Key: key}).createReadStream();

    // Flow: S3 file stream -> Log Line stream -> Log Record stream -> ES
    s3Stream
      .on('data', function(parsedEntry) {
        //   console.log('Parsed Entry ' + parsedEntry);
        //   postDocumentToES(parsedEntry, context);
          indexDocument(parsedEntry);
      });

    s3Stream.on('error', function() {
        console.log(
            'Error getting object "' + key + '" from bucket "' + bucket + '".  ' +
            'Make sure they exist and your bucket is in the same region as this function.');
        context.fail();
    });
}

function s3ToES(bucket, key, context) {
    console.log('In S3ToES==> bukcet, Key = ' + bucket + ', ' + key);
    // Note: The Lambda function should be configured to filter for .log files
    // (as part of the Event Source "suffix" setting).

    var counter = 0;
    var s3Stream = s3.getObject({Bucket: bucket, Key: key}).createReadStream();

    const r1 = readline.createInterface(s3Stream);

    // Flow: S3 file stream -> Log Line stream -> Log Record stream -> ES
    r1
      .on('line', function(line) {
          counter = counter + 1;
        //   console.log('Parsed Entry ' + parsedEntry);
        //   postDocumentToES(parsedEntry, context);
          indexDocument(line, utils.md5(line));
      });

    r1.on('error', function() {
        console.log(
            'Error getting object "' + key + '" from bucket "' + bucket + '".  ' +
            'Make sure they exist and your bucket is in the same region as this function.');
        context.fail();
    });
}

function indexDocument(document, id) {
  console.log('Indexing Document # ' + id);
  console.log(' ==>' + document);
  
  request.method = 'PUT';
  request.path = '/' + esDomain.index + '/' + esDomain.doctype + '/' + id;
  console.log("request path == >" + request.path);
  console.log('-----------------------------------------------------------');

  request.body = document;
  request.headers['host'] = esDomain.domain;
  request.headers['Content-Type'] = 'application/json';

  var signer = new AWS.Signers.V4(request, 'es');
  signer.addAuthorization(credentials, new Date());

  var client = new AWS.HttpClient();
  client.handleRequest(request, null, function(response) {
    console.log(response.statusCode + ' ' + response.statusMessage);
    var responseBody = '';
    response.on('data', function (chunk) {
      responseBody += chunk;
    });
    response.on('end', function (chunk) {
      console.log('DONE');
      console.log('Response body: ' + JSON.parse(responseBody));
    });
  }, function(error) {
    console.log('Error: ' + error);
  });
}

function indexS3Docs(context) {
    s3.listObjectsV2(s3Params, function (err, data) {
        if (err) {
            console.log(err, err.stack); // an error occurred
        } else {
            var contents = data.Contents;
            contents.forEach(function (content) {
                // allKeys.push(content.Key);
                console.log('Indexing => ' + content.Key);
                s3ToES(s3Params.Bucket, content.Key, context);
            });

            if (data.IsTruncated) {
                s3Params.ContinuationToken = data.NextContinuationToken;
                console.log("get further list...");
                indexS3Docs();
            } 

        }
    });
}

/* Lambda "main": Execution starts here */
exports.handler = function(event, context) {
    console.log('Received event: ', JSON.stringify(event, null, 2));
    // var bucket = 'upen-data';
    // var objKey = 'samples/es/movies3.json';
    // s3ToES(bucket, objKey, context);
    indexS3Docs(context);
}
