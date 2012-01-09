var request = require('request');

var tryPop = function( url, cb ){
  request(url, function (error, response, body) {
    if ( error ) {
      cb( error );
    } else {
      switch( response.statusCode ) {
        case 200:
          try {
            cb( null, JSON.parse(body) );
          } catch (e) {
            cb( 'not json response' );
          }
          break;
        case 404:
          cb( 'empty' );
          break;
        default:
          cb( 'unknown' );
      }
    }
  })
};

var Queue = function(base, queue){
  this.base  = base;
  this.queue = queue;
};

Queue.prototype.dequeue = function(cb){
  tryPop( this.base + '/' + this.queue, cb );
};

Queue.prototype.enqueue = function(uri, cb){
  request( { method : 'PUT',
             uri    : this.base + '/' + this.queue,
             headers:{'Content-Type': 'application/x-www-form-urlencoded'},
             body   : 'uri=' + uri 
           }, function (error, response, body) {
             if( error ) {
               console.log( [uri, error] );
             }
           });
};

exports.getQueue = function( base, queue ){
  return new Queue( base, queue );
};
