var util = require('util');

var AvroError = function(name) {
    Error.call(this);
    this.name = name;
    this.message = util.format.apply(null, arguments);
    Error.captureStackTrace(this, arguments.callee);
};

var AvroIOError = function() {
    AvroError.call(this, 'Avro IO Error');
};

var AvroFileError = function() {
    AvroError.call(this, 'Avro File Error');
};

var AvroBlockError = function() {
    AvroError.call(this, 'Avro Block Error');
};

var AvroBlockDelayReadError = function() {
    AvroError.call(this, 'Avro Block Delay Read Error');
};

var AvroInvalidSchemaError = function() {
    AvroError.call(this, 'Avro Invalid Schema Error');
};

var AvroDataValidationError = function() {
    AvroError.call(this, 'Avro Data Validation Error');
    this.fieldPath = [];
};

util.inherits(AvroError, Error);
util.inherits(AvroIOError, AvroError);
util.inherits(AvroFileError, AvroError);
util.inherits(AvroBlockError, AvroError);
util.inherits(AvroBlockDelayReadError, AvroError);
util.inherits(AvroInvalidSchemaError, AvroError);
util.inherits(AvroDataValidationError, AvroError);

exports.IOError = AvroIOError;
exports.FileError = AvroFileError;
exports.BlockError = AvroBlockError;
exports.BlockDelayReadError = AvroBlockDelayReadError;
exports.InvalidSchemaError = AvroInvalidSchemaError;
exports.DataValidationError = AvroDataValidationError;
