var Hook = require('hook.io').Hook;
var express = require('express');
var mon = require('./lib/monitor');

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

hook.start();

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

app.listen(3000);
