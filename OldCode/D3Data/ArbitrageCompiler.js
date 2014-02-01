//we need to fetch information from the database and answer our questions
//we store this information inside of redis for quick access

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

//using redis for quick insert/retrieval of batch info
var redis = require("redis")
    , client = redis.createClient();

var metaInformationKey = "regionInformation";
var metaAllItemsKey = "allItemsList";
var finalSchemaName = "finalSchema";
var regionSchemaName = "regionSchema";

var lastKnownDBId = 10;
var arbitrageDBId = 11;
var sumArbyID = 12;

module.exports = ArbitrageCreator;

function numberWithCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

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

function ArbitrageCreator()
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
    };

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

    self.qRedisMultiSetObject = function(setDictionary)
    {
        var deferred = Q.defer();

        var mSetList = [];
        for(var key in setDictionary)
        {
            var value = setDictionary[key];
            if(typeof value != "string")
                value = JSON.stringify(value);

            mSetList.push(key);
            mSetList.push(value);
        }



        self.redisClient.mset(mSetList, function(err)
        {
            if(err)
            {
                deferred.reject(err);
                return;
            }

            deferred.resolve();
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
    };

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
    self.qRedisSetObject = function(key, value)
    {
        var deferred = Q.defer();

        if(typeof value != "string")
            value = JSON.stringify(value);

        self.redisClient.set(key, value, function(err)
        {
            if(err)
            {
                deferred.reject(err);
                return;
            }

            deferred.resolve();
        });

        return deferred.promise;
    };
    self.qRedisSetArbitrage = function(key, arbitrageObject)
    {
        var deferred = Q.defer();

        self.qRedisSwitchDB(arbitrageDBId)
            .then(function()
            {
                return self.qRedisSetObject(key, arbitrageObject);
            })
            .done(function()
            {
                deferred.resolve();
            }, function(err)
            {
                deferred.reject(err);
            })

        return deferred.promise;
    };

    self.qGetAllMetaInfo = function()
    {
        var deferred = Q.defer();

        self.qGetMetaItemList()
            .then(function(itemList)
            {
                self.knownItems = itemList.knownMap;
                return self.qGetMetaList();
            })
            .done(function(regionInfo)
            {
                self.allRegions = regionInfo.keyList;

                deferred.resolve();
            },function(err){deferred.reject(err);})

        return deferred.promise;
    };

    self.qGetMetaItemList = function()
    {
        return self.qRedisSwitchDB(3)
            .then(function()
            {
                console.log('Fetching meta info about regions')

                return self.qRedisGetObject(metaAllItemsKey)
            });
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

    self.qConnect = function()
    {
        var deferred = Q.defer();


        qConnectMongoose()
            .then(function(connection)
            {
                self.connection = connection;
                self.generator.setConnection(connection);

                return qReadFile(path.resolve(__dirname, "../DBFinal/finalSchema.json"));
            })
            .then(function(regionSchema)
            {
                //so we have read in the trade schema
                var comSchema = JSON.parse(regionSchema);

                //now we have the actual schema
                //we load it into our database as it's own database
                self.generator.loadSingleSchema(finalSchemaName, comSchema);

                //store the class inside the parser, we're ready to proceed with Dump parsing
                self.ItemModel = self.generator.getSchemaModel(finalSchemaName);

                return qReadFile(path.resolve(__dirname, "../DBSorting/sortSchema.json"));
            })
            .then(function(regionSchema)
            {
                //so we have read in the trade schema
                var comSchema = JSON.parse(regionSchema);

                //now we have the actual schema
                //we load it into our database as it's own database
                self.generator.loadSingleSchema(regionSchemaName, comSchema);

                //store the class inside the parser, we're ready to proceed with Dump parsing
                self.RegionModel = self.generator.getSchemaModel(regionSchemaName);

                return self.qGetAllMetaInfo();
            })
            .done(function()
            {
                var itemList = [];
                for(var key in self.knownItems)
                    itemList.push(key);

                //we grab the region list as the first thing to do
                self.itemList = itemList;
                deferred.resolve();

            }, function(err){
                //oops failed connect
                deferred.reject(err);
            });

        return deferred.promise;
    };

    self.qFetchMongoDB = function(Model, findParams)
    {

        var deferred= Q.defer();

        Model.find(findParams, function(err, docs)
        {
            if(err)
                deferred.reject(err);
            else
                deferred.resolve(docs);
        });

        return deferred.promise;

    }

    self.qSaveMongoDB = function(mObject)
    {
        var deferred= Q.defer();

        var rObject = new self.FinalModel(mObject);

        rObject.save(function(err)
        {
            if(err)
                deferred.reject(err);
            else
                deferred.resolve();
        });

        return deferred.promise;
    };

    self.qClearMongoDB = function()
    {
        var deferred= Q.defer();

        self.FinalModel.remove({}, function(err)
        {
            if(err)
                deferred.reject(err);
            else
                deferred.resolve();
        });

        return deferred.promise;
    };
    self.qRedisKeySearch = function(keySearch)
    {

        var d2= Q.defer();


//                self.redisClient.keys(regionKey + "_*", function(err, vals)
        self.redisClient.keys(keySearch, function(err, regionItemKeys)
        {
            if(err)
                d2.reject(err);
            else
            {
                d2.resolve(regionItemKeys);
            }
        });
        return d2.promise;
    };

    self.qSumArbitrage = function()
    {
        var deferred = Q.defer();

         var failedFetch = false;

        //the region we're going to investigate!
        self.qRedisSwitchDB(arbitrageDBId)
            .then(function()
            {
                //we need to get our infomration from mongo
                return self.qRedisMultiGet(self.itemList);
            })
            .then(function(allItemArbitrage)
            {

                var sorted = [];
               for(var itemID in allItemArbitrage)
               {
//                   allItemArbitrage[itemID].total = allItemArbitrage[itemID].total.replace(/,/g, "");
                   allItemArbitrage[itemID].itemName = Maps.GlobalMaps.ItemIDToName[allItemArbitrage[itemID].itemID];
                   sorted.push(allItemArbitrage[itemID]);
               }

                sorted.sort(function(a,b){return parseFloat(b.total.replace(/,/g, "")) - parseFloat(a.total.replace(/,/g, ""));});

                sorted = sorted.slice(0,30);

                var absoluteTotal = 0;

                for(var i=0; i < sorted.length; i++)
                    absoluteTotal += parseFloat(sorted[i].total.replace(/,/g, ""));

                //top ten arbitrage opportunities
                console.log(sorted.slice(0,30));
                console.log("TOTAL Arbitrage from listed objects: " + numberWithCommas(absoluteTotal));
            })
            .done(function()
            {
                if(failedFetch)
                    console.log('No known region info for : ', regionKey);

                deferred.resolve();
            },function(err)
            {
                deferred.reject(err);
            })

        return deferred.promise;
    };


}

