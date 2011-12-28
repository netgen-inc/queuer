var Hook = require('hook.io').Hook;
var express = require('express');
var _ = require('underscore');
var mon = require('./lib/monitor');
var argv = require('optimist').argv;
var Hash = require('hashish');

var startup = new Date();

var hook = new Hook( {
    name: 'hook-server',
    'hook-host': '0.0.0.0'
});

hook.on('hook::ready', function(){
  hook.on('*::task-finished', function( task ){
    monitor.emit('task-' + task.uri, 'finished');
  });
  hook.on('*::task-error', function( task ){
    monitor.emit('task-' + task.uri, 'error');
  });
});

hook.connect();

var Queue = function(key){
  this.queue = [];
};

var queues = {};

var monitor = mon.createMonitor();

var app = express.createServer();
app.use(express.bodyParser());

app.get('/', function(req, res){
    res.send('Server OK');
});

app.get('/queue/:key', function(req, res){
    var key = req.params.key;
    if( queues[key] === undefined || queues[key].length == 0) {
      res.send('Queue is empty', 404);
    } else {
      var task = queues[key].shift();
      var info = { queue: key, uri: task };
      monitor.once('task-' + task, function( result ){
        hook.emit('task-debug-' + result, info);
        if( result !='finished' ){
          queues[key].push( task );
          hook.emit('queued', info);
        }
      });
      res.send( task );
      setTimeout(function(){
        monitor.emit('task-' + task, 'timeout');
      },60000);
      
    }
});

app.put('/queue/:key', function(req, res){
    if( req.body.uri === undefined || req.body.uri === null ) {
        res.send('ERROR:NO_URI');
    } else {
      var key = req.params.key;
      if( queues[key] === undefined ) queues[key] = [];
      queues[key].push( req.body.uri );
      hook.emit('queued', key);
      res.send('OK');
    }
});

app.post('/queue/:key', function(req, res){
  var key = req.params.key;
  if( queues[key] === undefined ) {
    res.send('Queue is empty', 404);
  } else {
    switch( req.body.action ) {
      case 'state':
        var state = { length: queues[key].length };
        res.send( JSON.stringify( state ) );
      break;

      case 'clean':
        queues[key] = [];
        res.send( 'OK' );
      break;

      default:
        res.send('ERROR:UNKNOW_ACTION', 401);
      break;
    }
  }
});

app.get('/state', function(req, res){
  res.send( JSON.stringify( { startup: startup.toLocaleString(), queues: Hash.map( queues, function( v, k ){
    return { length: v.length };
  } ) } ) );
});

app.listen(argv.p || 3000);
