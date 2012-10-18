var assert = require("assert");
var should = require("should");
var fs = require("fs");

var DataFile = require(__dirname + "/../lib/datafile");

describe('DataFile', function(){
    var testFile = __dirname + "/../test/data/test.avro";
    var dataFile;
    before(function(){
        dataFile = DataFile();
        if (fs.existsSync(testFile))
            fs.unlinkSync(testFile);
    });
    after(function(){
        fs.unlinkSync(testFile);
    })
    describe('open()', function(){
        it('should open a file for writing if passed a w flag and write an avro header', function(done){
            var schema = "int";
            dataFile.open(testFile, schema, { flags: 'w' });
            dataFile.write(1, function(err) {
                dataFile.close();
                fs.existsSync(testFile).should.be.true;                
                done();
            });
        });
        it('should open a file for reading if passed a r flag', function(done){
            var schema = "int";
            dataFile.open(testFile, schema, { flags: 'r' });
            dataFile.read(function(err, data) {
                if (err) 
                    done(err);
                else {
                    dataFile.close();
                    data.should.equal(1);
                    fs.unlinkSync(testFile); 
                    done();               
                }
            });
        });
        it('should throw an error if an unsupported codec is passed as an option', function(){
            (function() {
                dataFile.open(null, null, { codec: 'non-existant'});
            }).should.throwError();
        })
    });
    describe('write()', function() {
        it('should write a schema and associated data to a file', function(done) {
            var schema = "string";  //{ "type": "string" };
            var data = "The quick brown fox jumped over the lazy dogs";
            var dataFile = DataFile();
            dataFile.open(testFile, schema, { flags: 'w', codec: "deflate" });
            dataFile.write(data, function(err) {
                dataFile.write(data, function(err) {
                    dataFile.write(data, function(err) {
                        dataFile.close();
                        fs.existsSync(testFile).should.be.true; 
                        done();                       
                    });                  
                });                
            });
        });
    });
    describe('read()', function() {
        it('should read an avro data file', function(done){
            var schema = { "type": "string" };
            var dataFile = DataFile()
            dataFile.open(testFile, schema, { flags: 'r' });
            dataFile.read(function(err, data) {
                data.should.equal("The quick brown fox jumped over the lazy dogs");
                dataFile.close();
                done();
            });
        });
    });
})