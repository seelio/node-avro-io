var avro = require('./index').DataFile.AvroFile();
var schema = {
            "name": "data",
            "type": "record",
            "fields": [
                {"name":"key", "type": "string"},
                {"name":"value", "type": "string"},
                {"name":"flag", "type": "boolean"},
                {"name":"subrecord",
                 "type":"record",
                 "fields":[
                    {"name":"key", "type":"string"},
                    {"name":"value", "type":["string","int","null"]}
                ]}
            ]
};
var writer = avro.open("test.avro", schema, { flags: 'w', codec: 'deflate' });
writer.write({
    key: "john",
    value: "hive",
    flag: true,
    subrecord: {
        key: "preference",
        value: 2
    }
});
writer.write({
    key: "eric",
    value: "lola",
    flag: true,
    subrecord: {
        key: "postcode",
        value: null
    }
});
writer.write({
    key: "fred",
    value: "wonka",
    flag: false,
    subrecord: {
        key: "city",
        value: "Ann Arbor"
    }
});
writer.end();
