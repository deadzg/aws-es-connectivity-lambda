/*
* Importing dependencies
*/
var AWS = require('aws-sdk');
var path = require('path');

/*
* Configuration for AWS Elastic Search
*/
var esDomain = {
    region: '#####',
    endpoint: '###',
    index: '###',
    doctype: '###'
};

/*
* Handle function to be executed
*/
exports.handler = function(event, context, callback) {
  var jsonDoc = '{"director": "Burton, Tim", "genre": ["Comedy","Sci-Fi"], "year": 1996, "actor": ["Jack Nicholson","Pierce Brosnan","Sarah Jessica Parker"], "title": "Mars Attacks!"}'
  postToES(jsonDoc, context);
  searchEs('q=Jack');

    
};

var endpoint = new AWS.Endpoint(esDomain.endpoint);
var creds = new AWS.EnvironmentCredentials('AWS');

/*
 * Post the given document to Elasticsearch
 */
function postToES(doc, context) {
    console.log('Came into postToES')
    var req = new AWS.HttpRequest(endpoint);
    
    req.method = 'POST';
    req.path = path.join('/', esDomain.index, esDomain.doctype);
    req.region = esDomain.region;
    req.headers['presigned-expires'] = false;
    req.headers['Host'] = endpoint.host;
    req.headers['Content-Type'] = 'application/json';
    req.body = doc;

    var signer = new AWS.Signers.V4(req , 'es');  // es: service code
    signer.addAuthorization(creds, new Date());

    var send = new AWS.NodeHttpClient();

    // Making the actual call to the ES
    send.handleRequest(req, null, function(httpResp) {
        console.log('Into handle request');
        
        var respBody = '';
        httpResp.on('data', function (chunk) {
            respBody += chunk;
            JSON.stringify(respBody);
        });
        httpResp.on('end', function (chunk) {
            console.log('Response: ' + respBody);
            context.succeed('Lambda added document ' + doc);
        });
    }, function(err) {
        console.log('Error: ' + err);
        context.fail('Lambda failed with error ' + err);
    });
}

/*
* Fetch the result from ES
* Sample curl: curl -XGET 'https://search-videos-wy7pxpriu6r5vupoxwjlsbyn3q.ap-south-1.es.amazonaws.com/videos/_search?q=Jack'
*/
function searchEs(query) {
    console.log('Came into searchEs')
    var req = new AWS.HttpRequest(endpoint);
    
    req.method = 'GET';
    req.path = path.join('/', esDomain.index, '_search');
    req.region = esDomain.region;
    req.headers['presigned-expires'] = false;
    req.headers['Host'] = endpoint.host;
    req.headers['Content-Type'] = 'application/json';
    req.query = query;

    var signer = new AWS.Signers.V4(req , 'es');  // es: service code
    signer.addAuthorization(creds, new Date());

    var send = new AWS.NodeHttpClient();

    send.handleRequest(req, null, function(httpResp) {
        console.log('Into handle request');
        
        var respBody = '';
        httpResp.on('data', function (chunk) {
            respBody += chunk;
            JSON.stringify(respBody);
        });
        httpResp.on('end', function (chunk) {
            console.log('Response: ' + JSON.stringify(respBody));
        });
    }, function(err) {
        console.log('Error: ' + err);
        context.fail('Lambda failed with error ' + err);
    });
}