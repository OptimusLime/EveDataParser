var fs = require('fs');
var path = require('path');
//reads large files line by line
var byline = require('byline');
//This is a flow library for handling async tasks sequentially
var Q = require('q');

//we have our mongodb interaction library -- defines schema
var mongoose = require('mongoose');

//turn a JSON file into a database schema. Shweet.
var Generator = require('../DBParser/generator.js');
var Maps = require("../IDMapping/IDMapLoader.js");
var RegionItemCompiler = require("./CompileRegionItem.js");

//using redis for quick insert/retrieval of batch info
var redis = require("redis")
    , client = redis.createClient();

var metaInformationKey = "regionInformation";
var compileSchemaName = "compileModel";
var rawSchemaName = "RawTradeSchema";
//output our object
module.exports = DBCompiler;


var qConnectMongoose = function()
{
    var deferred = Q.defer();
    // connect to Mongo when the app initializes
    var mongooseConnection = mongoose.createConnection('mongodb://localhost/eve');

    mongooseConnection.on('error', function(e)
    {
        console.log('Mongoose connection error');
        console.log(e);
//        console.error.bind(console, 'connection error:');
        deferred.reject(e);

    });

    mongooseConnection.on('open', function(){
        deferred.resolve(mongooseConnection);
    });

    return deferred.promise;
};


var qReadFile = function(file)
{
    var deferred = Q.defer();

    fs.readFile(file, function(err, data)
    {
        if(err)
            deferred.reject(err);
        else
            deferred.resolve(data);
    });

    return deferred.promise;
};



function DBCompiler()
{
    var self = this;

    //we need to create our object
    self.generator = new Generator();
    self.redisClient = client;


    self.qRedisMultiGet = function(keys)
    {
        var deferred = Q.defer();

        self.redisClient.mget(keys, function(err, values)
        {
            if(err)
            {
                deferred.reject(err);
                return;
            }

            var mappedValues = {};
            for(var i=0; i < values.length; i++)
            {
                if(values[i])
                    mappedValues[keys[i]] = JSON.parse(values[i]);
            }

            deferred.resolve(mappedValues);
        });

        return deferred.promise;


    }
    self.qRedisGetObject = function(key)
    {
        var deferred = Q.defer();

        self.redisClient.get(key, function(err, value)
        {
            if(err)
            {
                deferred.reject(err);
                return;
            }

            deferred.resolve(JSON.parse(value));
        });

        return deferred.promise;
    };

    self.qFlushRedisDB = function(dbVar)
    {
        var deferred = Q.defer();

        self.qRedisSwitchDB(dbVar)
            .done(function()
            {
                self.redisClient.FLUSHDB(function(err)
                {
                    console.log("Flush returned! ", err);
                    if(err)
                        deferred.reject(err);
                    else
                        deferred.resolve();
                });
            }, function(err)
            {
                deferred.reject(err);
            });

        return deferred.promise;

    }
    self.qRedisSwitchDB = function(dbVar)
    {
        var deferred = Q.defer();

        self.redisClient.select(dbVar, function(err,res){
            // you'll want to check that the select was successful here
            // if(err) return err;

            if(err)
                deferred.reject(err);
            else
                deferred.resolve();
        });

        return deferred.promise;
    };

    self.qRawDBCount = function()
    {
        var deferred= Q.defer();
        self.RawModel.count({}, function(err, count)
        {
            if(err)
                deferred.reject(err);
            else
                deferred.resolve(count);
        });
        return deferred.promise;
    };

    self.qConnect = function()
    {
        var deferred = Q.defer();

        qConnectMongoose()
            .then(function(connection)
            {
                self.connection = connection;
                self.generator.setConnection(connection);

                return qReadFile(path.resolve(__dirname, "./compileSchema.json"));
            })
            .then(function(compileSchema)
            {
                //so we have read in the trade schema
                var comSchema = JSON.parse(compileSchema);

                //now we have the actual schema
                //we load it into our database as it's own database
                self.generator.loadSingleSchema(compileSchemaName, comSchema);

                //store the class inside the parser, we're ready to proceed with Dump parsing
                self.CompileModel = self.generator.getSchemaModel(compileSchemaName);

                return qReadFile(path.resolve(__dirname, "../DBParser/rawTradeSchema.json"));
            })
            .done(function(rawSchema)
            {
                //so we have read in the trade schema
                var tradeSchema = JSON.parse(rawSchema);

                //now we have the actual schema
                //we load it into our database as it's own database
                self.generator.loadSingleSchema(rawSchemaName, tradeSchema);

                self.RawModel = self.generator.getSchemaModel(rawSchemaName);


                self.qRawDBCount().done(
                    function(count)
                    {
                        console.log("Raw Model count: "  + count);
                        self.ItemCompiler = new RegionItemCompiler(self.CompileModel,self.RawModel);

                        deferred.resolve();
                    },
                    function(err)
                    {
                        deferred.reject(err);
                    }
                )



            }, function(err){
                //oops failed connect
                deferred.reject(err);
            });

        return deferred.promise;
    };

    self.qGetMetaList = function()
    {
        return self.qRedisSwitchDB(1)
            .then(function()
            {
                console.log('Fetching meta info about regions')

                return self.qRedisGetObject(metaInformationKey)
            });
    };

    //assume connected
    self.qCompileRegionData = function(startRegionIx)
    {
        var deferred = Q.defer();

        //we'll investigate these keys
        var regionKeysInvestigate = [];

        //we're connected, now we need to switch redis

        //flush the second redisDB
//        self.qFlushRedisDB(2)
//            .then(function()
//            {
//                return  self.qRedisSwitchDB(1)
//            })
        self.qRedisSwitchDB(1)
            .then(function()
            {
                console.log('Fetching meta info about regions')

               return self.qRedisGetObject(metaInformationKey)
            })
            .then(function(metaData)
            {
                //metaData has a list of all the regions

                var keyList = metaData.keyList;
//                var maxRegions = Math.min(2, keyList.length);
//                var maxRegions = keyList.length;
//
//                for(var i=0; i < maxRegions; i++)
//                {
//                    var region = keyList[i];
//                    //push meta keys to investigate
//                    regionKeysInvestigate.push(region + "_meta");
//                }

                regionKeysInvestigate.push(keyList[startRegionIx]+"_meta");

                console.log("REgion Keys to Investigate: " , regionKeysInvestigate);

                return self.qRedisMultiGet(regionKeysInvestigate);
            })
            .then(function(regionToMetaMap)
            {
                //how many items to inverstigate this time
                var itemsToInvestigate = [];

                for(var regionMeta in regionToMetaMap)
                {
                    var region =  regionMeta.split("_")[0];
                    var regionObject = regionToMetaMap[regionMeta];
                    var items = regionObject.itemList;

                    //this is just for dev purposes -- convenient
                    //don't go over number
//                    var maxItems = Math.min(50, items.length);
                    var maxItems = items.length;


                    for(var i=0; i < maxItems; i++)
                    {
                        itemsToInvestigate.push({region: region, item: items[i]});
                    }
                }

//                console.log('Investigate: ', itemsToInvestigate);

                var promiseList = [];
                for(var i=0; i < itemsToInvestigate.length; i++)
                {
                    var iti = itemsToInvestigate[i];
                    promiseList.push(self.ItemCompiler.qProcessRegionItem(iti.region, iti.item));
                }

//                console.log("Item investigation")

                //                self.ItemCompiler.qProcessRegionItem()

                return Q.all(promiseList);
            })
            .done(function()
            {
                console.log("Item investigation completed!")
                deferred.resolve();

            },function(err)
            {
                deferred.reject(err);
            });

        return deferred.promise;
    };


}


