var libpath = process.env['MOCHA_COV'] ? __dirname + '/../lib-cov/' : __dirname + '/../lib/';
var fs = require('fs');
var should = require('should');
require('buffertools');
var DataFile = require(libpath + 'datafile');
var Avro = require(libpath + 'schema');
var util = require('util');

var dataFile;
describe('AvroFile', function(){
    dataFile = __dirname + "/../test/data/test.avrofile.avro";
    var avroFile;
    before(function(){
        avroFile = DataFile.AvroFile();
        if (fs.existsSync(dataFile))
            fs.unlinkSync(dataFile);
    });
    after(function(){
       if (fs.existsSync(dataFile)) fs.unlinkSync(dataFile);
    });
    describe('open()', function(){
        it('should open a file for writing and return a writer', function(done){
            var schema = Avro.Schema({ "type": "string" });
            var writer = avroFile.open(dataFile, schema, { flags: 'w' });
            writer
                .on('error', function(err) {
                    done(err);
                })
                .on('finish', function() {
                    fs.existsSync(dataFile).should.be.true;
                    done();
                });
            writer.should.be.an.instanceof(DataFile.Writer)
            writer.write('testing');
            writer.end();
        });
        it('should open a file for reading and return a reader', function(done){
            var reader = avroFile.open(dataFile, null, { flags: 'r' });
            reader.should.be.an.instanceof(DataFile.Reader);
            reader
                .on('readable', function() {
                    //console.error('data()');
                    reader.read().should.equal("testing");
                })
                .on('error', function(err) {
                    //console.error('error()');
                    if (fs.existsSync(dataFile)) fs.unlinkSync(dataFile);
                    done(err);
                })
                .on('end', function() {
                    //console.error('end()');
                    done();
                });
        });
        it('should throw an error if an unsupported codec is passed as an option', function(){
            (function() {
                avroFile.open(null, null, { codec: 'non-existant'});
            }).should.throwError();
        });
        it('should throw an error if an unsupported operation is passed as an option', function(){
            (function() {
                avroFile.open(null, null, { flags: 'x'});
            }).should.throwError();
        });
    });
});
describe('Block()', function(){
    describe('length', function() {
        it('should return the current length of a Block', function(){
            var block = new DataFile.Block();
            block.length.should.equal(0);
            block.write(0x10);
            block.length.should.equal(1);
        });
    });
    describe('flush()', function(){
        it('should flush the buffer by setting the offset of 0', function(){
            var block = new DataFile.Block();
            block.write(0x55);
            block.flush();
            block.length.should.equal(0);
            block.offset.should.equal(0);
        });
    });
    describe('_bufferSize()', function(){
        it('should return the size we passed to it', function(){
            var block = new DataFile.Block();
            block._bufferSize(5).should.equal(5);
        });
        it('should double the existing buffer size', function(){
            var block = new DataFile.Block(256);
            block._bufferSize(128).should.equal(512);
        });
        it('should increase the buffer size by the size we pass in', function(){
            var block = new DataFile.Block(256);
            block._bufferSize(512).should.equal(768);
        });
        it('should return the sum of the num of bytes written and requested', function(){
            var block = new DataFile.Block(16384);
            block.write(0x25);
            block._bufferSize(16).should.equal(17);
        });
    });
    describe('_canReUseBuffer()', function(){
        it('should indicate whether an empty buffer can accommodate the requested size', function(){
            var block = new DataFile.Block();
            block._canReUseBuffer(0).should.equal(true);
            block._canReUseBuffer(5).should.equal(false);
        });
        it('should indicate whether a partially full buffer can accommodate the requested size', function(){
            var block = new DataFile.Block(8);
            block.write([0x23,0x24,0x25,0x26,0x27]);
            block._canReUseBuffer(2).should.equal(true);
            block._canReUseBuffer(4).should.equal(false);
            block.read(3);
            block._canReUseBuffer(5).should.equal(true);
            block._canReUseBuffer(7).should.equal(false);
        });
    });
    describe('_resizeIfRequired()', function(){
        it('should allocate a new buffer', function(){
            var block = new DataFile.Block();
            var buf1 = block._buffer;
            block._resizeIfRequired(8);
            var buf2 = block._buffer;
            buf1.should.not.equal(buf2);
            buf1.length.should.equal(0);
            buf2.length.should.equal(8);
        });
        it('should allocate a new buffer and copy over old buffer contents', function(){
            var block = new DataFile.Block(8);
            block.write([0x53, 0x65, 0x65, 0x6c, 0x69, 0x6f]);
            block._resizeIfRequired(4);
            block._buffer.length.should.equal(16);
            block.isEqual([0x53, 0x65, 0x65, 0x6c, 0x69, 0x6f]).should.be.true;
        });
        it('should reuse existing buffer', function(){
            var block = new DataFile.Block(16);
            var buf1 = block._buffer;
            block.write([0x53, 0x65, 0x65, 0x6c, 0x69, 0x6f]);
            block._resizeIfRequired(4);
            block._buffer.should.equal(buf1);

            block.read(3);
            block._resizeIfRequired(4);
            block._buffer.should.equal(buf1);
            block.isEqual([0x6c, 0x69, 0x6f]).should.be.true;
            block.offset.should.equal(0);
            block.remainingBytes.should.equal(3);

            block.read(3);
            block._resizeIfRequired(4);
            block._buffer.should.equal(buf1);
            block.offset.should.equal(0);
            block.remainingBytes.should.equal(0);
            block.length.should.equal(0);
        });
        it('should create a new buffer even though existing buffer is big enough', function(){
            var block = new DataFile.Block(16);
            var buf1 = block._buffer;
            block.write([0x53, 0x65, 0x65, 0x6c, 0x69, 0x6f]);
            block.reUseBuffer = false;
            block._resizeIfRequired(4);
            var buf2 = block._buffer;
            buf1.should.not.equal(buf2);
            buf2.length.should.equal(16);
            block.offset.should.equal(0);
            block.length.should.equal(6);
        });
    });
    describe('write()', function(){
        it('should write a single byte into the buffer', function(){
            var block = new DataFile.Block();
            block.write(0x20);
            block.isEqual([0x20]).should.be.true;
            block.offset.should.equal(0);
            block.length.should.equal(1);
            block.remainingBytes.should.equal(1);
        });
        it('should write an array of bytes into the buffer', function() {
            var block = new DataFile.Block();
            var bArray = [0x10, 0x20, 0x30, 0x40, 0x50, 0x60];
            block.write(bArray);
            block.isEqual(bArray).should.be.true;
            block.offset.should.equal(0);
            block.length.should.equal(6);
            block.remainingBytes.should.equal(6);
        });
        it('should write the contents of a Buffer into the buffer', function() {
            var block = new DataFile.Block();
            var buf1 = new Buffer([0x53, 0x65, 0x65, 0x6c, 0x69, 0x6f]);
            block.write(buf1);
            block.isEqual(buf1).should.be.true;
            var buf2 = new Buffer([0x60, 0x61, 0x62]);
            block.write(buf2);
            block.isEqual(Buffer.concat([buf1, buf2])).should.be.true;
        });
    });
    describe('skip()', function(){
        it('should skip n bytes of the block', function(){
            var block = new DataFile.Block(32);
            block.write([0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
            block.skip(3);
            block.offset.should.equal(3);
            block.skip(2);
            block.offset.should.equal(5);
        });
        it('should throw an error if you try to skip past the end of the written amount', function(){
            (function() {
                var block = new DataFile.Block(32);
                block.write([0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
                block.skip(7);                
            }).should.throwError();
        });
    });
    describe('read()', function(){
        it('should throw an error when trying to read more bytes than exist', function(){
            (function() {
                var block = new DataFile.Block(32);
                block.read(8);
            }).should.throwError();
        });
        it('should throw an error when passing in a negative size value', function(){
            (function() {
                var block = new DataFile.Block(32);
                block.read(-3);
            }).should.throwError();
        });
        it('should return the next byte when passed 1', function(){
            var block = new DataFile.Block(32);
            block.write([0x53, 0x65, 0x65, 0x6c, 0x69, 0x6f]);
            block.read(1).should.equal(0x53);
            block.read(1).should.equal(0x65);
            block.read(1).should.equal(0x65);
            block.read(1).should.equal(0x6c);
            block.read(1).should.equal(0x69);
            block.read(1).should.equal(0x6f);
            (function() {
                block.read(1);
            }).should.throwError();
        });
        it('should return n bytes when called via read(n)', function(){
            var block = new DataFile.Block(32);
            block.write([0x53, 0x65, 0x65, 0x6c, 0x69, 0x6f]);
            var buf = block.read(2);
            buf.length.should.equal(2);
            buf[0].should.equal(0x53);
            buf[1].should.equal(0x65);
            buf = block.read(4);
            buf.length.should.equal(4);
            buf[0].should.equal(0x65);
            buf[1].should.equal(0x6c);
            buf[2].should.equal(0x69);
            buf[3].should.equal(0x6f);
        });
    });
    describe('isEqual()', function(){
        it('should correctly when the passed in array/buffer is the same', function(){
            var block = DataFile.Block(8);
            block.write([0x53, 0x65, 0x65, 0x6c, 0x69, 0x6f]);
            block.isEqual([0x53, 0x65, 0x65, 0x6c, 0x69, 0x6f]).should.equal.true;
            block.isEqual([0x53, 0x65, 0x65]).should.equal.false;
            block.isEqual(new Buffer([0x53, 0x65, 0x65, 0x6c, 0x69, 0x6f])).should.equal.true;
        });
        it('should throw an error when neither an array or buffer is passed in', function(){
            (function() {
                var block = DataFile.Block(8);
                block.write([0x53, 0x65, 0x65, 0x6c, 0x69, 0x6f]);
                block.isEqual(null);
            }).should.throwError();

            (function() {
                var block = DataFile.Block(8);
                block.write([0x53, 0x65, 0x65, 0x6c, 0x69, 0x6f]);
                block.isEqual(0x53);
            }).should.throwError();
        });
    });
    describe('slice()', function(){
        it('should return a the written part of the Block', function(){
            var block = new DataFile.Block(32);
            block.write([0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
            block.slice().equals(new Buffer([0x01, 0x02, 0x03, 0x04, 0x05, 0x06])).should.be.true;
        });
        it('should return the specified sub section of a block', function(){
            var block = new DataFile.Block(32);
            block.write([0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
            block.slice(2,5).equals(new Buffer([0x03, 0x04, 0x05])).should.be.true;          
        });
    });
    describe('toBuffer()', function(){
        it('should return a buffer with the contents of the block', function(){
            var block = new DataFile.Block(64);
            block.write([0x11, 0x21, 0x31, 0x41, 0x51, 0x61, 0x71]);
            var buf1 = block.toBuffer();
            Buffer.isBuffer(buf1).should.be.true;
            buf1[0].should.equal(0x11);
            buf1[1].should.equal(0x21);
            buf1[2].should.equal(0x31);
            buf1[3].should.equal(0x41);
            buf1[4].should.equal(0x51);
            buf1[5].should.equal(0x61);
        });
    });
});

describe('Writer()', function(){
    var avroFile;
    dataFile = __dirname + "/../test/data/test.writer.avro";
    beforeEach(function(){
        avroFile = DataFile.AvroFile();
    });
    after(function(){
        if (fs.existsSync(dataFile)) fs.unlinkSync(dataFile);
    });
    it('should write data to a file stream using a pipe', function(done){
        var schema = {type: 'string'};
        var fileStream = fs.createWriteStream(dataFile);
        var writer = DataFile.Writer(schema, "null");
        writer.pipe(fileStream);
        writer
            .on('error', function(err) {
                done(err);
            })
            .on('finish', function() {
                fs.existsSync(dataFile).should.be.true;
                done();
            });
        writer.write("hello world");
        writer.end();
    });      
    it('should read back data from the written file', function(done){
        var reader = DataFile.Reader();
        var fileStream = fs.createReadStream(dataFile);
        fileStream.pipe(reader);
        reader
            .on('readable', function() {
                reader.read().should.equal("hello world");
            })
            .on('error', function(err) {
                done(err);
            })
            .on('end', function() {
                done();
            });
    });
    function randomString() {
        var i;
        var result = "";
        var stringSize = Math.floor(Math.random() * 512);
        for (i = 0; i < stringSize; i++) 
            result += String.fromCharCode(Math.floor(Math.random() * 0xFF));
        return result;
    }
    function schemaGenerator() {
        return { 
            "testBoolean": Math.floor(Math.random() * 2) == 0 ? false : true,
            "testString": randomString(), 
            "testLong": Math.floor(Math.random() * 1E10),
            "testDouble": Math.random(),
            "testBytes": new Buffer(randomString())
        };  
    }
    it('should write a sequence marker after 16k of data to a file stream', function(done) {
        dataFile = __dirname + "/../test/data/test.writer.random.avro";
        var schema = {
            "name": "testLargeDataSet",
            "type": "record",
            "fields": [
                {"name":"testBoolean","type": "boolean"},
                {"name":"testString","type": "string"},
                {"name":"testLong","type": "long"},
                {"name":"testDouble","type": "double"},
                {"name":"testBytes","type": "bytes"}
            ]
        };
        var writer = DataFile.Writer(schema, "null");
        var fileStream = fs.createWriteStream(dataFile);
        writer.pipe(fileStream);
        writer
            .on('finish', function() {
                fs.existsSync(dataFile).should.be.true;
                fs.unlinkSync(dataFile);
                done();
            })
            .on('error', function(err) {
                if (fs.existsSync(dataFile)) fs.unlinkSync(dataFile);
                done(err);
            });
        var i = 0;
        var delay = 0;
        while(i++ < 20) {
            writer.write(schemaGenerator());
        }
        writer.end();
    });
    describe('_generateSyncMarker()', function(){
        it('should generate a 16 byte sequence to be used as a marker', function(){
            var writer = DataFile.Writer();
            should.not.exist(writer._generateSyncMarker(-5));
            should.not.exist(writer._generateSyncMarker(0));
            writer._generateSyncMarker(16).length.should.equal(16);
            writer._generateSyncMarker(2).length.should.equal(2);
        });
    });
    describe('compressData()', function(){
        it('should compress a given buffer with deflate and return the compressed buffer', function(done){
            var reader = DataFile.Reader();
            var writer = DataFile.Writer();
            writer.compressData(new Buffer([0x15, 0x25, 0x35, 0x45, 0x55, 0x65]), "deflate", function(err, data) {
                data.equals(new Buffer([0x13, 0x55, 0x35, 0x75, 0x0d, 0x4d, 0x05, 0x00])).should.be.true;
                reader.decompressData(data, "deflate", function(err, data) {
                    data.equals(new Buffer([0x15, 0x25, 0x35, 0x45, 0x55, 0x65])).should.be.true;
                      done();
                })
              })
        });
        it('should compress a given buffer with snappy and return the compressed buffer', function(done){
            var reader = DataFile.Reader();
            var writer = DataFile.Writer();
            writer.compressData(new Buffer("compress this text"), "snappy", function(err, data) {
                reader.decompressData(data, "snappy", function(err, data) {
                    if (err) done(err);
                    data.toString().should.equal("compress this text");
                    done();
                });
              });
        });
        it('should return an error if an unsupported codec is passed as a parameter', function(done){
            var writer = DataFile.Writer();
            writer.compressData(new Buffer([0x13, 0x55, 0x35, 0x75]), "unsupported", function(err, data) {
                should.exist(err);
                err.should.be.an.instanceof(Error);
                done();
            });
        });
    });
    describe('write()', function() {
        it('should write a schema and associated data to a file', function(done) {
            var schema = {type: "string"};
            var data = "The quick brown fox jumped over the lazy dogs";
            var writer = avroFile.open(dataFile, schema, { flags: 'w', codec: "deflate" });
            writer
                .on('error', function(err) {
                    done(err);
                })
                .on('finish', function() {
                    fs.existsSync(dataFile).should.be.true;
                    done();
                });
            writer.write(data);
            writer.write(data);
            writer.write(data);
            writer.end();
        });
        it('should write a series of integers to a file and read them back as integers', function(done) {
            aFile = __dirname + "/../test/data/test.int.avro";
            var schema = { "type": "int" };
            var writer = avroFile.open(aFile, schema, { flags: 'w', codec: "deflate" });
            writer
                .on('error', function(err) {
                    done(err);
                })
                .on('finish', function() {
                    fs.existsSync(aFile).should.be.true;
                    setTimeout(function() {
                        var reader = avroFile.open(aFile, null, { flags: 'r' });
                        reader.should.be.an.instanceof(DataFile.Reader);
                        var results = [];
                        reader
                            .on('readable', function() {
                                results.push(reader.read());
                            })
                            .on('error', function(err) {
                                console.error(err);
                                done(err);
                            })
                            .on('end', function() {
                                results.should.eql([1,14,0,552]);
                                done();
                            });
                    }, 500);
                });
            writer.write(1);
            writer.write(14);
            writer.write(0);
            writer.write(552);
            writer.end();
        });
    });
});
describe('Reader()', function(){

    describe('streaming', function () {
        
        it('should read a large avro data stream compressed with deflate', function(done){
            
            var count = 0;
            var fileStream = fs.createReadStream(__dirname + "/data/log.deflate.avro");

            var reader = fileStream.pipe(DataFile.Reader());
            reader
                .on('error', function(err) {
                    done(err);
                })
                .on('end', function(err) {
                    count.should.equal(4096);
                    done();
                })
                .on('header', function(data) {
                    //console.log('\nHeader\n',util.inspect(data, {colors:true, depth:null}));
                    data.should.not
                })
                .on('readable', function() {
                    reader.read();
                    count++;
                    //console.log(data.time, data.request.path, data.request.body.rememberMe || '[]' , data.response.status);
                });
        });

        it('should read a large avro data stream compressed with snappy', function(done){
            
            var count = 0;
            var fileStream = fs.createReadStream(__dirname + "/data/log.snappy.avro");

            var reader = fileStream.pipe(DataFile.Reader());
            reader
                .on('error', function(err) {
                    done(err);
                })
                .on('end', function(err) {
                    count.should.equal(4096);
                    done();
                })
                .on('header', function(data) {
                    //console.log('\nHeader\n',util.inspect(data, {colors:true, depth:null}));
                })
                .on('readable', function() {
                    reader.read();
                    count++;
                    //console.log(data.time, data.request.path, data.request.body.rememberMe || '[]' , data.response.status);
                });
        });    
    });
    
    describe('decompressData()', function(){
        it('should compress a given buffer with deflate and return the compressed buffer', function(done){
            var reader = DataFile.Reader();
            reader.decompressData(new Buffer([0x13, 0x55, 0x35, 0x75, 0x0d, 0x4d, 0x05, 0x00]), "deflate", function(err, data) {
                data.equals(new Buffer([0x15, 0x25, 0x35, 0x45, 0x55, 0x65])).should.be.true;
                done();
            });
        });
        it('should compress a given buffer with snappy and return the compressed buffer', function(done){
            var reader = DataFile.Reader();
            reader.decompressData(new Buffer([0x12, 0x44, 0x63, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 
                                              0x73, 0x20, 0x74, 0x68, 0x69, 0x73, 0x20, 0x74, 0x65, 
                                              0x78, 0x74, 0x6c, 0x25, 0xd9, 0x04]), "snappy", function(err, data) {
                if (err) done(err);
                data.toString().should.equal("compress this text");
                done();
            });
        });
        it('should just return the same data if the codec is null', function(done){
            var reader = DataFile.Reader();
            reader.decompressData(new Buffer([0x13, 0x55, 0x35, 0x75, 0x0d, 0x4d, 0x05, 0x00]), "null", function(err, data) {
                data.equals(new Buffer([0x13, 0x55, 0x35, 0x75, 0x0d, 0x4d, 0x05, 0x00])).should.be.true;
                done();
            });
        });
        it('should return an error if an unsupported codec is passed as a parameter', function(done) {
            var reader = DataFile.Reader();
            reader.decompressData(new Buffer([0x13, 0x55, 0x35, 0x75]), "unsupported", function(err, data) {
                should.exist(err);
                err.should.be.an.instanceof(Error);
                done();
            });
        })
    })
    describe('writing then reading', function() {
        it('should read an avro data file written and return the same data', function(done){
            
            var dataFile = __dirname + "/data/test-array-strings.avro";
            var schema = {type: 'string'};
            var fileStream = fs.createWriteStream(dataFile);
            var writer = DataFile.Writer(schema);
            var source = [
                "The quick brown fox jumped over the lazy dogs", 
                "The time has come for all good men to come to the aid of...",
                "Humpty dumpty sat on the wall, humpty dumpty had a great fall..."
            ];
            writer.pipe(fileStream);
            writer
                .on('error', function(err) {
                    done(err);
                })
                .on('finish', function() {

                    var fileStream = fs.createReadStream(dataFile);
                    var reader = fileStream.pipe(DataFile.Reader());

                    reader.should.be.an.instanceof(DataFile.Reader);
                    var count = 0;
                    reader
                        .on('readable', function() {
                            reader.read().should.equal(source[count++]);
                        })
                        .on('error', function(err) {
                            console.error(err);
                            done(err);
                        })
                        .on('header', function(data) {
                            //console.log(data);
                        })
                        .on('end', function() {
                            count.should.equal(3);
                            done();
                        });
                });
            writer.write(source[0]);
            writer.write(source[1]);
            writer.write(source[2]);
            writer.end();
                
        });
    });
});
