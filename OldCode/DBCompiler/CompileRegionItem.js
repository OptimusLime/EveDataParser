var fs = require('fs');
var path = require('path');
//This is a flow library for handling async tasks sequentially
var Q = require('q');

//using redis for quick insert/retrieval of batch info
var redis = require("redis")
    , client = redis.createClient();


var lastKnownDBId = 10;

module.exports = RegionItemCompiler;

function numberWithCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function RegionItemCompiler(CompileModel, RawModel)
{
    //we need to process this client

    var self = this;

    self.redisClient = client;
    self.CompileModel = CompileModel;
    self.RawModel = RawModel;
    self.allStationToTradeLists = {};
    self.preProcessItems = {};

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

    self.qFlushRedisDB = function(dbVar)
    {
        var deferred = Q.defer();

        if(dbVar == undefined)
        {
            deferred.reject("undefined db var flush attempted!");
            return;
        }
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

    self.batchKey = function(gKey, num)
    {
        return gKey + "_" + num;
    }
    self.groupKey = function(rID, iID)
    {
        return rID + "|" + iID;
    };


    self.qFetchRawBatches = function(batchList)
    {
        var deferred = Q.defer();

        var finishMapping = function(queryDocs)
        {
            var mappedObjects = {};
            var mappedCounts = {};

            for(var i=0; i < queryDocs.length; i++)
            {
                var qDoc = queryDocs[i];
                var oData = qDoc.observedData;

                for(var d=0; d < oData.length; d++)
                {
                    var tData = oData[d];
                    //make sure to track the itemID
                    tData.itemID = oData.itemID;

                    if(!mappedObjects[tData.tradeID])
                        mappedObjects[tData.tradeID] = [];

                    mappedObjects[tData.tradeID].push(tData);
                    mappedCounts[tData.tradeID] = mappedObjects[tData.tradeID].length;
                }
            }

            return {objects: mappedObjects, counts: mappedCounts};
        };

//        console.log('Prior to Batch Request: ', batchList);

        self.RawModel.find({batchID : {$in: batchList}}).lean().exec(function(err,docs)
        {
            console.log('Raw batch return: ', docs.length);

            if(docs.length != batchList.length){

                var uBatch = {};
                for(var i=0; i < batchList.length; i++)
                {
                    uBatch[batchList[i]] = batchList[i];
                }
                for(var i=0; i < docs.length; i++)
                {
                    var d = docs[i];

                    //remove the batch object we already have
                    delete uBatch[d.batchID];
                }

                var missingCount = Object.keys(uBatch).length;

                //now we go get um
                self.RawModel.find({batchID : {$in: Object.keys(uBatch)}}).lean().exec(function(err,missing)
                {
                    if(missing.length != missingCount)
                    {
                        console.log(uBatch);
                        console.log(missing.length + " out of " + missingCount + " misses requested: ");
                        deferred.reject("Error! Number of batch objects not equal to list size.");
                    }
                    else
                    {
                        docs = docs.concat(missing);

                        var mappedObjects = finishMapping(docs);

                        //have all our documents mapped out
                        deferred.resolve(mappedObjects);
                    }
                });
            }
            else
            {

                var mappedObjects = finishMapping(docs);

                //have all our documents mapped out
                deferred.resolve(mappedObjects);
            }

        });

        return deferred.promise;


    };

    self.qPreProcessRegionItem = function(regionID, itemID)
    {
        var deferred = Q.defer();

        var gKey = self.groupKey(regionID, itemID);

        self.qRedisSwitchDB(0)
            .then(function()
            {
                //get our meta information from redis
                return self.qRedisGetObject(gKey);
            }).done(function(gMetaInfo){

                self.preProcessItems[gKey] = gMetaInfo;
                deferred.resolve();
            }, function(err)
            {
                deferred.reject(err);
            });


        return deferred.promise;

    }

    //Let's create functions for processing an region/item
    self.qProcessRegionItem = function(regionID, itemID)
    {
        var deferred = Q.defer();

        var gKey = self.groupKey(regionID, itemID);

        var investigateTrades;
        var useableData = false;

        var stationItemCompiled = {};

        var confirmedSales = {};

        self.qRedisSwitchDB(2)
            .then(function()
            {
                var gMetaInfo = self.preProcessItems[gKey];
//                console.log("Meta returned: ", gMetaInfo);
                //we now have batch information
                var total = gMetaInfo.totalCount;
                var bSize = gMetaInfo.batchSize;

                var numBatches = Math.ceil(total/bSize);

                var compileBatchKeys = [];

                for(var i=0; i < numBatches; i++)
                {
                    compileBatchKeys.push(self.batchKey(gKey, i));
                }

//                console.log('Fetching batch: ', compileBatchKeys);

                return self.qFetchRawBatches(compileBatchKeys);
            })
            .then(function(mappedBatches)
            {
                //batches broken up by trade numbers
//                console.log('Mapped return ', mappedBatches.objects);
//                console.log('Mapped count ', mappedBatches.counts);
//                console.log("Map keys: ", Object.keys(mappedBatches.objects));

                for(var key in mappedBatches.counts)
                {
                    //need at least 2 pieces of information to get anything at all
                    if(mappedBatches.counts[key] > 1)
                    {
                        useableData = true;
                    }
                }

                if(useableData)
                    investigateTrades = mappedBatches;
                else
                //will cause a skip at next step
                    investigateTrades = {};

                //now we have redis set to the right database, we enter meta info
                //derka derk

                //we need to create meta info about this process

                var compileInfo =
                {
                    totalData : 0,

                    totalSoldPrice : 0,
                    totalSoldQuantity : 0,
                    avgPricePerSellOrder : "",
                    confirmedSold : [],

                    totalBoughtPrice : 0,
                    totalBoughtQuantity : 0,
                    avgPricePerBuyOrder: "",
                    confirmedBought : []
                };

                useableData = false;

                for(var key in investigateTrades.counts)
                {
                    var minVol = Number.MAX_VALUE;
                    var maxVol = Number.MIN_VALUE;

                    //need at least 2 pieces of information to get anything at all
                    if(investigateTrades.counts[key] > 0)
                    {
                        //

                        var iTrade = investigateTrades.objects[key];
                        for(var i=0; i < iTrade.length; i++)
                        {
                            minVol = Math.min(iTrade[i].volRemain, minVol);
                            maxVol = Math.max(iTrade[i].volRemain, maxVol);
                        }

                        if(minVol != maxVol && iTrade.length > 1){


                            //recorded buy/sell for this object in this region

                            compileInfo.totalData++;

                            var isBuy = iTrade[0].bid == "1";
                            var price = iTrade[0].price;
                            var quantity = maxVol- minVol;

                            var confirmed = {price: price, quantity: quantity, stationID: iTrade[0].stationID};

                            //we are a buy order!
                            if(isBuy)
                            {
                                compileInfo.confirmedBought.push(confirmed);
                                compileInfo.totalBoughtPrice += quantity*price
                                compileInfo.totalBoughtQuantity += quantity;
                                compileInfo.avgPricePerBuyOrder = numberWithCommas((compileInfo.totalBoughtPrice/compileInfo.totalBoughtQuantity).toFixed(2));

                            }
                            else
                            {
                                compileInfo.confirmedSold.push(confirmed);
                                compileInfo.totalSoldPrice += quantity*price;
                                compileInfo.totalSoldQuantity += quantity;
//                                console.log('Saw ', (maxVol-minVol), " sold");
//                                console.log(iTrade);
                                compileInfo.avgPricePerSellOrder = numberWithCommas((compileInfo.totalSoldPrice/compileInfo.totalSoldQuantity).toFixed(2));
                            }
                        }


                        //
                        var cTrade = iTrade[0];
                        //for each trade, compile list
                        if(!stationItemCompiled[cTrade.stationID])
                            stationItemCompiled[cTrade.stationID] = [];

                        //only track what remains!
                        cTrade.volRemain = minVol;
                        stationItemCompiled[cTrade.stationID].push(cTrade);
                    }
                }

                //need buy/sell info for this part
                useableData = (compileInfo.totalBoughtQuantity > 0 && compileInfo.totalSoldQuantity > 0);

                if(useableData)
                {
                    return self.qRedisSetObject(self.groupKey(regionID, itemID), compileInfo)
                }
                else
                {
                    console.log('No usable info found!: ', regionID, "|", itemID);

                    //if you don't have data, don't save anything,
                    //all done for now
                    //TODO: Mark processed information for region/item -- so data can be combined
//                    deferred.resolve();
                }
            })
            .then(function(){
                //now we want some other information
                var count = 0;

                if(!self.allStationToTradeLists[regionID])
                    self.allStationToTradeLists[regionID] = {};

                var multiObject = self.allStationToTradeLists[regionID];

                for(var stationID in stationItemCompiled)
                {
                    var gKey = self.groupKey(stationID, itemID);
                    multiObject[gKey] = {stationID: stationID, itemID: itemID, trades: stationItemCompiled[stationID]};
                }

//                if(count)
//                    return self.qRedisMultiSetObject(multiObject);
            })
            .done(function(){

                deferred.resolve();
            }
            , function(err)
            {
                deferred.reject(err);
            });

        return deferred.promise;

    };

    self.qPostProcessInfomation = function(regionID)
    {
        var deferred = Q.defer();

        self.qRedisSwitchDB(lastKnownDBId)
//            .then(function(){
//                return self.qFlushRedisDB(lastKnownDBId);
//            })
            .then(function(){
                var shouldWrite = false;
                for(var key in self.allStationToTradeLists[regionID])
                {
                    //if we have even 1 key, it should be written
                    shouldWrite = true;
                    break;
                }
                //if we don't have anything to write, don't send it!
                if(shouldWrite)
                    return self.qRedisMultiSetObject(self.allStationToTradeLists[regionID]);
            })
            .done(function(){

                deferred.resolve();
            }
            , function(err)
            {
                deferred.reject(err);
            });


        return deferred.promise;

    }




}