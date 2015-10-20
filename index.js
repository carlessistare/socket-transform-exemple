
// SERVER SOCKET
var net = require('net');
var server = net.createServer(function(c) {
  console.log('--> client connected');
  c.on('data', function (data) {
    console.log("Received: " + data.toString());
  });
});
server.listen(8124, function() {
  console.log('--> server bound');
  createClient();
});

// STREAMS
var util = require('util'),
  Readable = require('stream').Readable,
  Transform = require('stream').Transform;

var KafkaBatchTransform = function (options) {
  Transform.call(this, options)
};
util.inherits(KafkaBatchTransform, Transform);
KafkaBatchTransform.prototype._transform = function (chunk, enc, done) {
  var data = chunk.toString();
  addData(data, this._transformBuffer.bind(this));
  done();
};
KafkaBatchTransform.prototype._transformBuffer = function () {
  this.push(buffer);
  buffer = "";
};

// BUFFER
var buffer = "",
  bufferTimer = null;

var addData = function (data, callback) {
  buffer += data;
  clearTimeout(bufferTimer);
  bufferTimer = setTimeout(function () { 
    callback(); 
  }, 2000);
}

// CLIENT SOCKET
var createClient = function (){
  var client = net.connect({port: 8124}, function() {
    console.log('--> connected to server');
    process.stdin.setEncoding('utf8');

    var transform = new KafkaBatchTransform();
    process.stdin.pipe(transform);

    transform.on('readable', function () {
      while (bulkMessage = transform.read()) {
        bulkMessageString = bulkMessage.toString();
        client.write("Transformed: \n" + bulkMessageString);
      }
    });

  });
};