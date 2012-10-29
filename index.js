module.exports = wwwNpmSearch;

var elasticSearch = require('elasticsearchclient');
var esServerOptions = {
  host: 'localhost',
  port: 9200
};

function wwwNpmSearch(options) {
  options = options || {};
  esServerOptions.host = options.host || esServerOptions.host;
  esServerOptions.port = options.port || esServerOptions.port;
  esClient = {};
  esClient.reindexRegistry = reindexRegistry;
  esClient.implode = implode;
  esClient.createRegistry = createRegistry;
  esClient.createRiver = createRiver;
  esClient.setRegistryMaps = setRegistryMaps;


  esClient.client = new elasticSearch(esServerOptions);
  var client = esClient.client;

  function reindexRegistry(index, type) {
    esClient.implode('_river');
    esClient.implode(index);
    esClient.createRegistry(index, type);
  }

  function implode(index) {
    client.createCall({path: index, method:"DELETE"}, esServerOptions)
    .on('data', function(data) { console.log(data); })
    .exec();
  }

  function createRegistry(index, type) {
    client.createIndex(index, {}).on('data', function(data) {
      console.log(data);
      esClient.setRegistryMaps(index, type, esClient.createRiver);
    }).exec();
  }

  function createRiver(index_name, type) {
    var path = "_river/" + index_name + "_river/_meta";
    var options = {
      "type" : "couchdb",
      "couchdb" : {
          "host" : "localhost",
          "port" : 15984,
          "db" : "registry",
          "filter" : null,
          "ignore_attachments":true,
          "script": "if(ctx.doc.versions){ctx.doc.versions = null};"
        },
       "index" : {
          "index" : index_name,
          "type" : type,
          "bulk_size" : "100",
          "bulk_timeout" : "10ms"
      }
    };
    client.createCall({data: options, path: path, method:"PUT"}, esServerOptions)
    .on('data', function(data) { console.log(data); })
    .exec();
  }

  function setRegistryMaps(index, type, cb) {
    var mappings = {};
    mappings[type] = {};
    mappings[type].properties = {};
    var maps = mappings[type].properties;
    maps.name = {"type" : "multi_field",
                  "fields": {
                    "name": {"type": "string", "index": "not_analyzed", "boost": 5} ,
                    "autocomplete" : {"type" : "string", "index" : "analyzed"}
                  }
                };
    maps.author = {"type" : "multi_field",
                  "fields": {
                    "author": {"type": "string", "index": "not_analyzed", "boost": 2} ,
                    "autocomplete" : {"type" : "string", "index" : "analyzed"}
                  }
                };
    maps.description = {"type" : "string"};
    maps.readme = {"type" : "string", "boost": 0.2};
    maps.keywords = {"type" : "string", "index_name" : "keywords"};
    client.putMapping(index, type, mappings).on('data', function(data) {
        console.log(data);
        cb(index, type);
      })
    .exec();
  }

  //* Search
  function searchRegistry(query, cb){
      client.search('npm', 'module', query).on(
        'data',
        function(data) {
          return cb(data);
      }).exec();
  }

  return esClient;

}