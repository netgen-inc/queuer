var util = require("util");
var events = require("events");

function Monitor() {
    events.EventEmitter.call(this);
}

util.inherits(Monitor, events.EventEmitter);

exports.createMonitor = function(){
  return new Monitor();
};
