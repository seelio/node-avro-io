Node Avro IO
============

[![Build Status](https://secure.travis-ci.org/seelio/node-avro-io.png)](http://travis-ci.org/seelio/node-avro-io)

Implements the [avro spec](http://avro.apache.org/docs/current/spec.html)

This status of this repository is *initial release*

```bash
npm install node-avro-io
```

or

```bash
npm install git://github.com/seelio/node-avro-io.git
```

Serializing data to an avro binary file
```
var avro = require('node-avro-io').DataFile.AvroFile();

var schema = {type: 'string'};
var writer = avro.open('test.avro', schema, { flags: 'w', codec: 'deflate' });

writer.write('The quick brown fox jumped over the lazy dogs');
writer.write('Another entry');
writer.end();
```

Deserializing data to from avro binary file
```
var avro = require('node-avro-io').DataFile.AvroFile();

var reader = avro.open('test.avro', null, { flags: 'r' });
reader.on('readable', function(data) {
    console.log(reader.read());
});
```
...lots more to follow...

For now see test/*

TODO:

- Avro RPC
- Support for Trevni (column major data serialization)
