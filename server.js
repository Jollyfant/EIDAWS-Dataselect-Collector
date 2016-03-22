'use strict';

var COLLECTOR = require('express')();
var http = require('http');
var parseString = require('xml2js').parseString;
var bunyan = require('bunyan');

var log = bunyan.createLogger({
  name: 'ODC-EIDA Dataselect Collector',
  streams: [{
    level: 'info',
    stream: process.stdout
  }, {
    level: 'info',
    path: './log'
  }]
});

// Some server settings
var SERVICE = {
  'NAME': 'ODC-WFCATALOG-COLLECTOR',
  'HOST': '127.0.0.1',
  'PORT': 3002
}

COLLECTOR.get('/router', function(req, res, next) {
 
  req.service = {
    id: generateRequestID()
  }

  res.on("finish", function() {
    log.info({
      'type': "request summary",
      'code': res.statusCode,
      'method': req.method,
      'id': req.service.id,
    }, "request ended");
  });

  // Set the default response timeout to 2 minutes
  // Scraping requests will timeout in 30 seconds
  res.setTimeout(2 * 60 * 1000);

  // Attempt to sanitize the dates
  var starttime = Date.parse(req.query.start);
  var endtime = Date.parse(req.query.end);
  if(isNaN(starttime)) {
    return res.status(400).send("Start time is invalid.");
  }
  if(isNaN(endtime)) {
    return res.status(400).send("End time is invalid.");
  }

  // Set up request to GFZ for routing service
  // Simply forward the query
  var options = {
    host: 'geofon.gfz-potsdam.de',
    path: '/eidaws/routing/1/query' + req._parsedUrl.search,
    method: 'GET'
  }

  log.info({
    'id': req.service.id
  }, 'Starting request to GFZ for routing information');

  req.service.routingRequestStart = new Date();

  http.request(options, function(response) {

    var body = "";

    response.on('data', function(data) {
      body += data;
    });

    response.on('end', function() {

      log.info({
        'id': req.service.id,
        'code': res.statusCode
      }, "routing request");

      if(response.statusCode === 204) {
        return res.status(204).end();
      } else if(response.statusCode !== 200) {
        return res.status(400).send("Could not fetch request info: " + body);
      }

      // Parse the routingXML from GFZ
      parseString(body, function(err, result) {

        // Set up a container for the requests
        var requests = new Array();

        // To collect all requests, loop over the datacenters
        for(var i = 0; i < result.service.datacenter.length; i++) {
          var datacenter = result.service.datacenter[i];

          // Get the URL and the network, station, location, and channel
          // Start & endtime are identical are taken from the user input
          var url = datacenter.url;
          for(var j = 0; j < datacenter.params.length; j++) {
            var reference = datacenter.params[j];
            requests.push(url + "?net=" + reference.net + "&sta=" + reference.sta + "&loc=" + reference.loc + "&cha=" + reference.cha + "&start=" + req.query.start + "&end=" + req.query.end); 
          }

        }

        req.routedRequests = requests;

        log.info({
          id: req.service.id,
          retrievalTime: (new Date() - req.service.routingRequestStart), 
        }, "routing request succesful");

        next();

      });
    });
  }).end();
  

});

function parseUrl(url) {

  var host = url.split("http://")[1];
  var path = host.substring(host.indexOf("/"));
  var host = host.substring(0, host.indexOf("/"));

  return {
    'host': host,
    'path': path
  }

}

COLLECTOR.get('/router', function(req, res, next) {

  // Options
  var FDSN_REQUEST_TIMEOUT = 30000;
  var NUMBER_OF_THREADS = 20;

  var requestCounter = 0;
  var fullBuffer;
  var requestStart = new Date();
  var threads = new Array();
  var nThreads = Math.min(NUMBER_OF_THREADS, req.routedRequests.length);
  var queryTime;

  log.info({
    id: req.service.id,
    nRequests: req.routedRequests.length
  }, "starting requests");

  // Open the specificied number of threads to make requests
  for(var i = 0; i < nThreads; i++) {
    threads.push(true);
    getRequest(i);
  }

  // Create a request and set the thread activity to true
  function getRequest(i) {

    var start = new Date();
    threads[i] = false;

    // Handle one of the requested URLs
    var requestedUrl = req.routedRequests.pop();
    if(!requestedUrl) return;

    requestCounter++;

    // Parse the URL returned by the routing service to a host & path
    var url = parseUrl(requestedUrl);

    // Request options
    log.info({
      id: req.service.id,
      thread: i,
      requested: url.host
    }, "thread requesting data");

    var options = {
      host: url.host,
      path: url.path,
      method: 'GET'
    }

    var tempBuffer;

    // Start the GET request to a webservice
    var reqqie = http.get(options, function(response) {

      queryTime = (new Date() - start); 

      log.info({
        id: req.service.id,
        queryTime: queryTime
      }, "thread request first response");

      // Clear the custom timeout set on aborting the request
      clearTimeout(timeout);

      // When data is returned, pass it to a temporary buffer
      response.on('data', function(data) {
        if(response.statusCode === 200) {
          tempBuffer = !tempBuffer ? new Buffer(data) : Buffer.concat([tempBuffer, new Buffer(data)])
        }
      });

      // Concatenate the full response to the buffer
      response.on('end', function(data) {

        log.info({
          id: req.service.id,
          thread: i,
          code: response.statusCode,
          nBytes: tempBuffer ? tempBuffer.length : 0,
          retrievalTime: (new Date() - start)
        }, "thread request ended");

        if(response.statusCode === 200) {
          fullBuffer = fullBuffer ? Buffer.concat([fullBuffer, tempBuffer]) : tempBuffer;
        }

        threadFinished(i);

      });

    });

    // Error when the connection is timed out
    reqqie.on('error', function(e) {
      log.info({
        id: req.service.id,
        thread: i,
      }, "thread timed out");
      threadFinished(i);
    });

    // Custom timeout on the request
    var timeout = setTimeout(function () {
      reqqie.abort();
    }, FDSN_REQUEST_TIMEOUT);


    function threadFinished(i) {

      threads[i] = true;

      // If there are more requests to make, start a new thread
      if(req.routedRequests.length > 0) {
        getRequest(i);
        return;
      }

      // Only end the request if every thread has finished
      if(!threads.every(Boolean)) {
        return;
      }

      if(!fullBuffer) {
        return res.status(204).end();
      }

      log.info({
        id: req.service.id,
        nBytes: fullBuffer.length,
        totalRetrievalTime: (new Date() - requestStart), 
      }, "routing completed");

      res.setHeader('Content-Type', 'application/vnd.fdsn.mseed');
      return res.status(200).send(fullBuffer);

    }

  }

});

// Start the HTTP server
COLLECTOR.listen(SERVICE.PORT, SERVICE.HOST, function() {
  console.log(SERVICE.NAME + ' Webservice has been started on: ' + SERVICE.HOST + ":" + SERVICE.PORT);
});


function generateRequestID() {
  function s4() {
    return Math.floor((1 + Math.random()) * 0x10000).toString(16).substring(1);
  }
  return s4() + s4() + '-' + s4() + '-' + s4() + '-' + s4() + '-' + s4() + s4() + s4();
}
