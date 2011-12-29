var Hook = require('hook.io').Hook;
var express = require('express');
var _ = require('underscore');
var mon = require('./lib/monitor');
var argv = require('optimist').argv;
var Hash = require('hashish');
var Queue = require('./lib/redis');
var de = require('../event/lib/devent').createDEvent('queuer');

var startup = new Date();
/*
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

hook.connect();*/

de.on('task-finished', function( task ){
//console.log(['finish', task]);
  monitor.emit('task-' + task.uri, 'finished');
});

de.on('task-error', function( task ){
//console.log(['error', task]);
  monitor.emit('task-' + task.uri, 'error');
});

var queues = new Queue();

var monitor = mon.createMonitor();

var app = express.createServer();
app.use(express.bodyParser());

app.get('/', function(req, res){
    res.send('Server OK');
});

app.get('/queue/:key', function(req, res){
    var key = req.params.key;
    queues.dequeue(key, function( info ){
      if( info === null ) {
        res.send('Queue is empty', 404);
      } else {
        monitor.once('task-' + info.uri, function( result ){
          //hook.emit('task-debug-' + result, info);
          de.emit('task-debug-' + result, info);
          if( result !='finished' ){
            info.retry++;
            queues.enqueue(key, info);
            de.emit('queued', info);
            //hook.emit('queued', info);
          }
        });
        res.send( JSON.stringify( info ) );
        setTimeout(function(){
          monitor.emit('task-' + info.uri, 'timeout');
        },60000);
      }
    });
});

app.put('/queue/:key', function(req, res){
    if( req.body.uri === undefined || req.body.uri === null ) {
        res.send('ERROR:NO_URI');
    } else {
      var key = req.params.key;
      var task = req.body.uri;
      var info = { queue: key, uri: task, retry:0 };
      queues.enqueue(key, info);
      de.emit('queued', key);
      //hook.emit('queued', key);
      res.send('OK');
    }
});

app.post('/queue/:key', function(req, res){
  var key = req.params.key;
  switch( req.body.action ) {
    case 'state':
      queues.length( key, function( len ) {
        var state = { length: len };
        res.send( JSON.stringify( state ) );
      });
    break;

    case 'clean':
    case 'flush':
      queues.flush( key );
      res.send( 'OK' );
    break;

    default:
      res.send('ERROR:UNKNOW_ACTION', 401);
    break;
  }
});

app.get('/state', function(req, res){
  queues.state( function( state ) {
    res.send( JSON.stringify( { startup: startup.toLocaleString(), queues: state } ) );
  };
});

app.listen(argv.p || 3000);
