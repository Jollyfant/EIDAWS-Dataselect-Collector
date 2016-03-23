/*
 * EIDAWS-Dataselect-Collector
 * NodeJS dataselect collector for multiple EIDA nodes routed by GFZ.
 *
 * Written by:
 *   Mathijs Koymans (koymans@knmi.nl)
 *   Jollyfant @ GitHub
 * 
 * 23th of Match, 2016
 */

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
    path: './logs/ODC-DATASELECT-COLLECTOR.log'
  }]
});

// Some server settings, to be moved to a cfg file
var SERVICE = {
  'NAME': 'ODC-DATASELECT-COLLECTOR',
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
      'nBytes': req.service.nBytesTotal,
    }, "request ended");
  });

  // Disable timeout of server for long requests
  // Collector requests will timeout in 30 seconds anyway
  res.setTimeout(0);

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

        // Shuffle the order of the requests randomly so we spread
        // the load more evenly through time across nodes
        requests.sort(function(a, b) {
          return Math.random() < 0.5; 
        });

        req.routedRequests = requests;

        log.info({
          id: req.service.id,
          nRequests: req.routedRequests.length,
          retrievalTime: (new Date() - req.service.routingRequestStart), 
        }, "routing request succesful");

        next();

      });
    });
  }).end();  

});

/* function parseUrl
 * separates host and path from an url
 */
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
  var queryTime;
  var threads = new Array();
  var nThreads = Math.min(NUMBER_OF_THREADS, req.routedRequests.length);
  var queryTime;

  log.info({
    id: req.service.id,
    nRequests: req.routedRequests.length
  }, "starting requests");

  // Open the specificied number of threads to make requests
  for(var i = 0; i < nThreads; i++) {
    threads.push(false);
    getRequest(i);
  }

  // Create a request and set the thread to false
  function getRequest(i) {

    var start = new Date();
    threads[i] = false;

    // Handle one of the requested URLs
    var requestedUrl = req.routedRequests.pop();
    if(!requestedUrl) { 
      return;
    }

    var url = parseUrl(requestedUrl);

    requestCounter++;

    log.info({
      id: req.service.id,
      thread: i,
      requested: url.host
    }, "thread requesting data");


    // Parse the URL returned by the routing service to a host & path
    var options = {
      host: url.host,
      path: url.path,
      method: 'GET'
    }

    // Start the GET request to a webservice
    var requestBuffer;
    var reqqie = http.get(options, function(response) {

      queryTime = (new Date() - start); 

      log.info({
        id: req.service.id,
        thread: i,
        queryTime: queryTime,
        requested: url.host
      }, "thread request first response");

      // Clear the custom timeout set on aborting the request
      clearTimeout(timeout);

      // When data is returned, pass it to a temporary buffer
      response.on('data', function(data) {
        if(response.statusCode === 200) {
          requestBuffer = !requestBuffer ? new Buffer(data) : Buffer.concat([requestBuffer, new Buffer(data)])
        }
      });

      // Concatenate the response to the full buffer
      response.on('end', function(data) {

        log.info({
          id: req.service.id,
          thread: i,
          code: response.statusCode,
          requested: url.host,
          nBytes: requestBuffer ? requestBuffer.length : 0,
          retrievalTime: (new Date() - start)
        }, "thread request ended");

        if(response.statusCode === 200) {
          fullBuffer = fullBuffer ? Buffer.concat([fullBuffer, requestBuffer]) : requestBuffer;
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


    // When a thread is finished with its request
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

      var nBytesTotal = fullBuffer.length;
      req.service.nBytesTotal = nBytesTotal;

      log.info({
        id: req.service.id,
        nBytes: nBytesTotal,
        totalRetrievalTime: (new Date() - requestStart), 
      }, "threads pooled");

      res.setHeader('Content-Type', 'application/vnd.fdsn.mseed');
      res.status(200).send(fullBuffer);

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
