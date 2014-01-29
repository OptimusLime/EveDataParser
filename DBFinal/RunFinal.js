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

//output our object
module.exports = DBSorter;


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



function DBSorter()
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

    self.qRegionDBCount = function()
    {
        var deferred= Q.defer();
        self.FinalModel.count({}, function(err, count)
        {
            if(err)
                deferred.reject(err);
            else
                deferred.resolve(count);
        });
        return deferred.promise;
    };

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

    self.qRedisGetItemMeta = function(itemList)
    {
        var deferred= Q.defer();

        var finalItemList;

        self.qRedisSwitchDB(3)
            .then(function()
            {
                return self.qRedisGetObject(metaAllItemsKey);
            })
            .then(function(allItemsObject){

                finalItemList = allItemsObject;

//                console.log(itemList);

                return self.qRedisMultiGet(itemList);
            })
            .done(function(itemToRegionInfoMap)
            {
                console.log('Meta Resolved')
                //we have both sets of info from redis
                deferred.resolve({itemList: finalItemList, itemToRegion: itemToRegionInfoMap});
            }, function(err)
            {
                    deferred.reject(err);
            });

        return deferred.promise;
    };

    self.qRedisSetMeta = function(knownItems, itemToRegionMap)
    {
        var deferred= Q.defer();

        self.qRedisSwitchDB(3)
            .then(function()
            {
                //won't have this name, we just make it easier to save by doing this
                itemToRegionMap[metaAllItemsKey] = knownItems;

                return self.qRedisMultiSetObject(itemToRegionMap);
            })
            .done(function()
            {
                //done saving here
               deferred.resolve();

            }, function(err)
            {
                deferred.reject(err);
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
                self.FinalModel = self.generator.getSchemaModel(finalSchemaName);

                return self.qGetMetaItemList();
            })
            .done(function(metaData)
            {
                var itemList = [];
                for(var key in metaData.knownMap)
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

    self.qGetMetaItemList = function()
    {
        return self.qRedisSwitchDB(3)
            .then(function()
            {
                console.log('Fetching meta info about regions')

                return self.qRedisGetObject(metaAllItemsKey)
            });
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

    //assume connected
    self.qFinalizeData = function(startRegionIx)
    {
        console.log(startRegionIx);
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

        var itemToRegionMap = {};

        var itemKey = self.itemList[startRegionIx];

        var compileItem =
        {
            "itemID" : itemKey,
            "confirmedBought": [],
            "confirmedSold": []
        };

        var emptyRegion = false;

//        self.qClearMongoDB()
//            .then(function(){
//                return self.qRedisSwitchDB(2);
//            })
        self.qRedisSwitchDB(3)
            .then(function()
            {
                return self.qRedisGetObject(itemKey);
            })
            .then(function(regionItemKeys)
            {
                console.log(regionItemKeys);
                if(regionItemKeys.regions.length == 0)
                {
                    emptyRegion = true;
                    //WE DON'T need to look any further, it's empty
                }
                else{

                    var combinedKeys = [];
                    for(var i=0; i < regionItemKeys.regions.length; i++)
                    {
                        var gKey = regionItemKeys.regions[i] + "|" + itemKey;
                        combinedKeys.push(gKey);
                    }

                    console.log('Looking for region Item keys: ', combinedKeys);

                    return self.qRedisMultiGet(combinedKeys);
                }
            })
            .then(function(allRegionInfo)
            {
                if(emptyRegion)
                    return;

//                console.log('Item returns: ', allRegionInfo);

                console.log('Items across regions returned');

                for(var rKey in allRegionInfo)
                {
                    var cItem = allRegionInfo[rKey];
                    var keySplit = rKey.split('|');
                    var region = keySplit[0];

                    //add to items that we know of!
                    //Note that we've seen this item in this region

                    //we need to know what item we've saving to our big mongo object lists
                    for(var i=0; i < cItem.confirmedBought.length; i++)
                        cItem.confirmedBought[i].regionID = region;

                    for(var i=0; i < cItem.confirmedSold.length; i++)
                        cItem.confirmedSold[i].regionID = region;

                    //now we concatenate all the items for the region
                    compileItem.confirmedBought = compileItem.confirmedBought.concat(cItem.confirmedBought);
                    compileItem.confirmedSold = compileItem.confirmedSold.concat(cItem.confirmedSold);
                }

                //now we've compiled a big old list of confirmed sales -- we can do whatever we want with them!

                console.log('Saving region to mongo size: ', compileItem.confirmedBought.length + " + " + compileItem.confirmedSold.length);

                return self.qSaveMongoDB(compileItem);
            })
            .done(function()
            {
                if(emptyRegion)
                    console.log('Item was empty: ' + itemKey);
                else
                    console.log("Item investigation completed!")
                deferred.resolve();

            },function(err)
            {
                deferred.reject(err);
            });
//            .then(function(metaData)
//            {
//                //metaData has a list of all the regions
//
//                var keyList = metaData.keyList;
////                var maxRegions = Math.min(2, keyList.length);
////                var maxRegions = keyList.length;
////
////                for(var i=0; i < maxRegions; i++)
////                {
////                    var region = keyList[i];
////                    //push meta keys to investigate
////                    regionKeysInvestigate.push(region + "_meta");
////                }
//
//                regionKeysInvestigate.push(keyList[startRegionIx]+"_meta");
//
//                console.log("REgion Keys to Investigate: " , regionKeysInvestigate);
//
//                return self.qRedisMultiGet(regionKeysInvestigate);
//            })
//            .then(function(regionToMetaMap)
//            {
//                //how many items to inverstigate this time
//                var itemsToInvestigate = [];
//
//                for(var regionMeta in regionToMetaMap)
//                {
//                    var region =  regionMeta.split("_")[0];
//                    var regionObject = regionToMetaMap[regionMeta];
//                    var items = regionObject.itemList;
//
//                    //this is just for dev purposes -- convenient
//                    //don't go over number
////                    var maxItems = Math.min(50, items.length);
//                    var maxItems = items.length;
//
//
//                    for(var i=0; i < maxItems; i++)
//                    {
//                        itemsToInvestigate.push({region: region, item: items[i]});
//                    }
//                }
//
////                console.log('Investigate: ', itemsToInvestigate);
//
//                var promiseList = [];
//                for(var i=0; i < itemsToInvestigate.length; i++)
//                {
//                    var iti = itemsToInvestigate[i];
//                    promiseList.push(self.ItemCompiler.qProcessRegionItem(iti.region, iti.item));
//                }
//
////                console.log("Item investigation")
//
//                //                self.ItemCompiler.qProcessRegionItem()
//
//                return Q.all(promiseList);
//            })


        return deferred.promise;
    };


}


