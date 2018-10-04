Indexes from S3 to Elastic Search
---------------------------------

1. Create Elastic Search Domain in AWS
2. Copy Domain name from ElasticSearch Overview in AWS Console. It should the whole text after 'https://" in EndPoint.
3. Change the following in index.js as per env:
    var s3Params = {
        Bucket: '<bucket-name>',   
        Prefix: '<Key Prefix. Could be folder prefix>
    };

    /* Globals */
    var esDomain = {
        domain: '<domain-name>',
        region: 'us-east-1',
        index: '<index-name>',
        doctype: '<doc-type>'
    };
4. Creates indices with unique MD5 hash of each record. This ensures that records are NEVER duplicated even if they come from different source.
