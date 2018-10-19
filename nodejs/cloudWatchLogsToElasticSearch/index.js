var crypto = require('crypto'),
    datevalidate = require('datevalidator'),
    https = require('https'),
    moment = require('moment-timezone'),
    validate = require('validator'),
    zlib = require('zlib');

var endPoint = process.env.ENDPOINT,
    indexPattern = process.env.INDEX_PATTERN;

function streamLogs(event, exeCallback) {
    getInput(event, function (err, logData) {
        if (err) {
            exeCallback(err, null);
        } else {
            var bulkData = transform(logData);

            postData(bulkData, function (err, success, statusCode, failedItems) {
                if (err) {
                    exeCallback(err, null);
                } else {}
                    exeCallback(null, statusCode);
            });
        }
    });
}

function getInput(event, exeCallback) {
    var zippedInput = new Buffer(event.awslogs.data, 'base64');

    zlib.gunzip(zippedInput, function (err, buffer) {
        if (err) {
            exeCallback(err, null);
        } else {
            var logData = JSON.parse(buffer.toString('utf8'));
            console.log('logData=>' + buffer.toString('utf8'));

            exeCallback(null, logData);
        }
    });
}

function transform(logData) {
    if (logData.messageType === 'CONTROL_MESSAGE') {
        return null;
    } else {
        var bulkData = '';
        var msgAsStr = ''

        logData.logEvents.forEach(function (logEvent) {
            var action = { "index": {} },
                logName = logData.logStream.split(' '),
                logType = logData.logGroup.split('-'),
                timeStamp = moment.utc(new Date(1 * logEvent.timestamp)).format('YYYY-MM-DDTHH:mm:ss.SSSZ'),
                source = buildSource(logEvent.message, logEvent.extractedFields, logData.logStream);

            indiceName = [
                indexPattern + timeStamp.split('-')[0],
                timeStamp.split('-')[1],
                timeStamp.split('-')[2].split('T')[0]
            ].join('.');

            logName = logName[logName.length - 1];
            logType = logType[logType.length - 1];

            action.index._index = indiceName;
            action.index._type = logType;
            action.index._id = logEvent.id;

            // source['@log_group'] = logData.logGroup;
            // source['@log_name'] = logName;
            // source['@log_stream'] = logData.logStream;
            // source['@message'] = logEvent.message;
            // source['@owner'] = logData.owner;
            // source['@timestamp'] = timeStamp;

            source['@message'] = extractJson(logEvent.message);
            console.log('Source Message ==> ' + source['@message']);
            if(source['@message'] != null && source['@message'] != '') {
                bulkData += [
                    JSON.stringify(action),
                    JSON.stringify(JSON.parse(source['@message']))
                ].join('\n') + '\n';
            }

        });

        return bulkData;
    }
}

function buildSource(message, fields, logStream) {
    if (fields) {
        var source = {};

        for (var field in fields) {
            if (fields.hasOwnProperty(field) && fields[field]) {
                var fieldVal = fields[field];

                if (fieldVal.length > 0) {
                    var isDateTime = 'FALSE';
                    var noSource = 'FALSE';
                    var startindex = message.indexOf('STARTOFFIELDS');

                    jsonSubString = extractJson(fieldVal);
                    fieldVal = fieldVal.substring(fieldVal.replace(/^[a-z]|^[0-9]/i, ''));
                    fieldVal = fieldVal.substring(fieldVal.replace(/^[a-z]+$|^[0-9]+$/i, ''));

                    if (fieldVal.split(' ')[1] === '-0400') {
                        fieldVal = moment(new Date(fieldVal.split(' ')[0].replace(/:/, ' '))).tz('America/New_York').format('YYYY-MM-DDTHH:mm:ss.SSS');
                        fieldVal = moment().utc(fieldVal).format('YYYY-MM-DDTHH:mm:ss.SSSZ');
                        isDateTime = 'TRUE';
                    }

                    if (jsonSubString !== null) { source[field] = JSON.parse(jsonSubString); }
                    
                    if (datevalidate.validate(fieldVal) || isDateTime === 'TRUE') {
                        field = "date_time";
                    } else if (validate.isIP(fieldVal)) {
                        field = "ip_address";
                    } else if (validate.isEmail(fieldVal)) {
                        field = "email_address";
                    } else if (validate.isFQDN(fieldVal)) {
                        field = "domain_name";
                    } else if (validate.isURL(fieldVal)) {
                        field = "web_address";
                    } else if (fieldVal.substring(0, 2) === "ip") {
                        field = "host_name";
                    } else if (fieldVal.indexOf("HealthChecker") > -1) {
                        field = "check_source";
                    } else if (logStream.indexOf("access_log") > -1) {
                        if (fieldVal.length === 3 && isFinite(1 * fieldVal)) {
                            field = "http_code";
                        } else if (fieldVal.split(' ')[2]) {
                            source["req_type"] = fieldVal.split(' ')[0];
                            source["target_url"] = fieldVal.split(' ')[1];
                            source["user_agent"] = fieldVal.split(' ')[2];

                            noSource = 'TRUE';
                        }
                    } 
                    // else {
                    //     if (fieldVal !== '-') { field = "unknown_field"; }
                    // }

                    if (startindex >= 0) {
                        var endindex = message.indexOf('ENDOFFIELDS');

                        startindex = (startindex + 13);

                        var customFields = message.substring(startindex, endindex);
                        var appLogKeyValues = customFields.split('|').join('=').split('=');

                        appLogKeyValues.pop();
                        appLogKeyValues.shift();

                        for (var i = 0; i < appLogKeyValues.length; i++) {
                            if (i % 2 === 0) {
                                source[appLogKeyValues[i]] = appLogKeyValues[i + 1];
                            }
                        }

                        noSource = 'TRUE';
                    }

                    if (isFinite(1 * field)) { noSource = 'TRUE'; }

                    if (noSource === 'FALSE') { source[field] = fieldVal; }
                }
            }
        }
        return source;
    } else {
        return null;
    }
}

function extractJson(message) {
    var jsonStart = message.indexOf('{');

    if (jsonStart < 0) {
        return null;
    } else {
        var jsonSubString = message.substring(jsonStart);

        return isValidJson(jsonSubString) ? jsonSubString : null;
    }
}

function isValidJson(message) {
    try {
        JSON.parse(message);
    } catch (e) { return false; }

    return true;
}

function postData(body, exeCallback) {
    console.log('in postData BulkData ==> ' + body);
    var requestParams = buildRequest(endPoint, body);

    var request = https.request(requestParams, function(response) {
        var responseBody = '';
        response.on('data', function(chunk) {
            responseBody += chunk;
        });
        response.on('end', function() {
            var info = JSON.parse(responseBody);
            var failedItems;
            var success;
            
            if (response.statusCode >= 200 && response.statusCode < 299) {
                failedItems = info.items.filter(function(x) {
                    return x.index.status >= 300;
                });

                success = { 
                    "attemptedItems": info.items.length,
                    "successfulItems": info.items.length - failedItems.length,
                    "failedItems": failedItems.length
                };
            }

            var error = response.statusCode !== 200 || info.errors === true ? {
                "statusCode": response.statusCode,
                "responseBody": responseBody
            } : null
            
            if(error != null) {
                console.log('error == ' + JSON.stringify(error) );
            }

            exeCallback(error, success, response.statusCode, failedItems);
        });
    }).on('error', function(e) {
        exeCallback(e);
    });
    request.end(requestParams.body);
}

function buildRequest(endPoint, body) {
    var endPointParts = endPoint.match(/^([^\.]+)\.?([^\.]*)\.?([^\.]*)\.amazonaws\.com$/);
    var region = endPointParts[2];
    var service = endPointParts[3];
    var datetime = (new Date()).toISOString().replace(/[:\-]|\.\d{3}/g, '');
    var date = datetime.substr(0, 8);
    var kDate = hmac('AWS4' + process.env.AWS_SECRET_ACCESS_KEY, date);
    var kRegion = hmac(kDate, region);
    var kService = hmac(kRegion, service);
    var kSigning = hmac(kService, 'aws4_request');
    
    var request = {
        host: endPoint,
        method: 'POST',
        path: '/_bulk',
        body: body,
        headers: { 
            'Content-Type': 'application/json',
            'Host': endPoint,
            'Content-Length': Buffer.byteLength(body),
            'X-Amz-Security-Token': process.env.AWS_SESSION_TOKEN,
            'X-Amz-Date': datetime
        }
    };

    var canonicalHeaders = Object.keys(request.headers)
        .sort(function(a, b) { return a.toLowerCase() < b.toLowerCase() ? -1 : 1; })
        .map(function(k) { return k.toLowerCase() + ':' + request.headers[k]; })
        .join('\n');

    var signedHeaders = Object.keys(request.headers)
        .map(function(k) { return k.toLowerCase(); })
        .sort()
        .join(';');

    var canonicalString = [
        request.method,
        request.path, '',
        canonicalHeaders, '',
        signedHeaders,
        hash(request.body, 'hex'),
    ].join('\n');

    var credentialString = [ date, region, service, 'aws4_request' ].join('/');

    var stringToSign = [
        'AWS4-HMAC-SHA256',
        datetime,
        credentialString,
        hash(canonicalString, 'hex')
    ] .join('\n');

    request.headers.Authorization = [
        'AWS4-HMAC-SHA256 Credential=' + process.env.AWS_ACCESS_KEY_ID + '/' + credentialString,
        'SignedHeaders=' + signedHeaders,
        'Signature=' + hmac(kSigning, stringToSign, 'hex')
    ].join(', ');

    return request;
}

function hmac(key, str, encoding) {
    return crypto.createHmac('sha256', key).update(str, 'utf8').digest(encoding);
}

function hash(str, encoding) {
    return crypto.createHash('sha256').update(str, 'utf8').digest(encoding);
}

exports.handler = function (event, context, callback) {
    streamLogs(event, function (err, data) {
        console.log("In The Function exports.handler");
        if (err) {
            callback(err, null);
        } else {
            callback(null, data);
        }
    });
};
