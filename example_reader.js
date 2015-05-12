reader = require("./index").DataFile.AvroFile()
    .open('test.avro', null, { flags: 'r' })
reader.on('readable', function() {
    console.log(reader.read());
});
