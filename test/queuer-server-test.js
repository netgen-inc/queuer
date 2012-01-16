var vows = require('vows');
var assert = require('assert');
var request = require('request');
var events = require('events');
var devent = require('devent').createDEvent('test');

var uri = 'mysql://127.0.0.1:3306/spider?spider#1';
var base_url = 'http://127.0.0.1:3000/queue';
var queue_name = 'test';
var url = base_url + '/' + queue_name;

var assertStatus = function(code) {
  return function(e, r, b) {
    assert.equal(r.statusCode, code);
  };
};
var assertError = function(error) {
  return function(e, r, b) {
    assert.equal(e, error);
  };
};
var assertBody = function(body) {
  return function(e, r, b) {
    assert.equal(b, body);
  };
};

vows.describe('Test Queue Server').addBatch({
  'flush the test queue' : {
    topic : function() {
      request({
        method : 'POST',
        uri : url,
        headers : {
          'Content-Type' : 'application/x-www-form-urlencoded'
        },
        body : 'action=flush'
      }, this.callback);
    },
    'should not receive error' : assertError(null),
    'status code should be 200' : assertStatus(200),
    'body should be "OK"' : assertBody('OK')
  }
}).addBatch({
  'pop task from a empty queue' : {
    topic : function() {
      request(url, this.callback);
    },
    'should not receive error' : assertError(null),
    'status code should be 200' : assertStatus(404),
    'body should be "OK"' : assertBody('Queue is empty')
  }
}).addBatch({
  'add a task to queue' : {
    topic : function() {
      request({
        method : 'PUT',
        uri : url,
        headers : {
          'Content-Type' : 'application/x-www-form-urlencoded'
        },
        body : 'uri=' + uri
      }, this.callback);
    },
    'should not receive error' : assertError(null),
    'status code should be 200' : assertStatus(200),
    'body should be "OK"' : assertBody('OK')
  }
}).addBatch({
  'get state of queue server' : {
    topic : function() {
      request('http://127.0.0.1:3000/state', this.callback);
    },
    'should not receive error' : assertError(null),
    'status code should be 200' : assertStatus(200),
    'body should be JASON String' : function(e, r, b) {
      var state = JSON.parse(b);
      assert.equal(state.queues.test.length, 1);
    }
  }
}).addBatch({
  'pop task from a none empty queue' : {
    topic : function() {
      request(url, this.callback);
    },
    'should not receive error' : assertError(null),
    'status code should be 200' : assertStatus(200),
    'body should be a task' : function(e, r, b) {
      var task = JSON.parse(b);
      assert.equal(task.uri, uri);
      assert.equal(task.queue, queue_name);
      assert.equal(task.retry, 0);
    }
  }
}).addBatch({
  'emit "task-error"' : {
    topic : function() {
      var promise = new (events.EventEmitter);
      var error_timeout_id = setTimeout(function() {
        promise.emit('error', 'can not recieve "task-error"');
      }, 2000);
      devent.on('task-error', function(task) {
        promise.emit('success', task);
        clearTimeout(error_timeout_id);
      });
      setTimeout(function() {
        var task1 = {
          queue : queue_name,
          uri : uri,
          retry : 0
        };
        devent.emit('task-error', task1);
      }, 1000);

      return promise;
    },
    'task error' : function(err, task) {
      var task1 = {
        queue : queue_name,
        uri : uri,
        retry : 0
      };
      assert.deepEqual(err, null);
      assert.deepEqual(task, task1);
    }
  }
}).addBatch({
  'get state of a queue affter emmit task error' : {
    topic : function() {
      request({
        method : 'POST',
        uri : url,
        headers : {
          'Content-Type' : 'application/x-www-form-urlencoded'
        },
        body : 'action=state'
      }, this.callback);
    },
    'should not receive error' : assertError(null),
    'status code should be 200' : assertStatus(200),
    'body should be JASON String' : function(e, r, b) {
      var state = JSON.parse(b);
      assert.equal(state.length, 1);
    }
  }
}).addBatch({
  'pop task from a none empty queue again' : {
    topic : function() {
      request(url, this.callback);
    },
    'should not receive error' : assertError(null),
    'status code should be 200' : assertStatus(200),
    'body should be a task' : function(e, r, b) {
      var task = JSON.parse(b);
      assert.equal(task.uri, uri);
      assert.equal(task.queue, queue_name);
      assert.equal(task.retry, 1);
    }
  }
}).addBatch({
  'deal with task over time(60s)' : {
    topic : function() {
      var promise = new (events.EventEmitter);
      var error_timeout_id = setTimeout(function() {
        promise.emit('error', 'can not recieve "task-finished"');
      }, 62000);
      devent.on('task-finished', function(task) {
        promise.emit('success', task);
        clearTimeout(error_timeout_id);
      });
      setTimeout(function() {
        var task1 = {
          queue : queue_name,
          uri : uri,
          retry : 0
        };
        devent.emit('task-finished', task1);
      }, 61000);

      return promise;
    },
    'task finished' : function(err, task) {
      assert.equal(err, null);
      assert.equal(task.queue, queue_name);
      assert.equal(task.uri, uri);
    }
  }
}).addBatch({
  'get state of a queue affter deal with task over time' : {
    topic : function() {
      request({
        method : 'POST',
        uri : url,
        headers : {
          'Content-Type' : 'application/x-www-form-urlencoded'
        },
        body : 'action=state'
      }, this.callback);
    },
    'should not receive error' : assertError(null),
    'status code should be 200' : assertStatus(200),
    'body should be JASON String' : function(e, r, b) {
      var state = JSON.parse(b);
      assert.equal(state.length, 1);
    }
  }
}).addBatch({
  'pop task from a none empty queue after emit task-finished over time' : {
    topic : function() {
      request(url, this.callback);
    },
    'should not receive error' : assertError(null),
    'status code should be 200' : assertStatus(200),
    'body should be a task' : function(e, r, b) {
      var task = JSON.parse(b);
      assert.equal(task.uri, uri);
      assert.equal(task.queue, queue_name);
      assert.equal(task.retry, 2);
    }
  }
}).addBatch({
  ' emit "task-finished"' : {
    topic : function() {
      var promise = new (events.EventEmitter);
      var error_timeout_id = setTimeout(function() {
        promise.emit('error', 'can not recieve "task-finished"');
      }, 2000);
      devent.on('task-finished', function(task) {
        promise.emit('success', task);
        clearTimeout(error_timeout_id);
      });
      setTimeout(function() {
        var task1 = {
          queue : queue_name,
          uri : uri,
          retry : 0
        };
        devent.emit('task-finished', task1);
      }, 1000);

      return promise;
    },
    'task finished' : function(err, task) {
      assert.equal(err, null);
      assert.equal(task.queue, queue_name);
      assert.equal(task.uri, uri);
    }
  }
}).addBatch({
  'get state of a queue affter deal with task in time' : {
    topic : function() {
      request({
        method : 'POST',
        uri : url,
        headers : {
          'Content-Type' : 'application/x-www-form-urlencoded'
        },
        body : 'action=state'
      }, this.callback);
    },
    'should not receive error' : assertError(null),
    'status code should be 200' : assertStatus(200),
    'body should be JASON String' : function(e, r, b) {
      var state = JSON.parse(b);
      assert.equal(state.length, 0);
    }
  }
}).run();