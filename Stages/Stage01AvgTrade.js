var QFunctions = require("../Globals/qFunctions.js");
var ParseFunctions = require("../Globals/parseFunctions.js");

module.exports = Stage01Class;

function Stage01Class()
{
    var self = this;

    var topFiveMap, topFiveList;

    //just a simple wrapper, doesn't do anything but send back what it got in
    self.qPreProcessStage = function()
    {
        var preArgs = arguments;

        topFiveMap = QFunctions.Maps.TopFiveMap;
        topFiveList = QFunctions.Maps.TopFiveStations;

        //send back our arguments for next piece
        return QFunctions.wrapQFunction(function(success, failure)
        {
            //fetch objects from Stage1

            QFunctions.qCountMongoObjects(QFunctions.MongoModels.AllTradeModel, {modID: 0})
                .then(function(mongoDBCount)
                {
                    //oops, called it regions -- old habits die hard
                    self.fileCount = mongoDBCount;
                })
                .done(function()
                {
                    success.apply(self, preArgs);
                }, function(err)
                {
                    failure(err);
                });
        });
    };

    var uniqueStationTrades = {};

    //processing a dump file -- skip first line!
    self.qProcessStage = function()
    {
        //this is how we batch objects
//        fileID = fileName;

        return QFunctions.wrapQFunction(function(success, failure)
        {
            var chunkCount = 0;

//            QFunctions.qFlushRedisDB(ParseFunctions.redisIdentifiers.Stage1Count)
            QFunctions.qRedisSwitchDB(ParseFunctions.redisIdentifiers.Stage01AvgTrade)
                .then(function()
                {
                    //flush the redisDB!
                    return QFunctions.qFlushRedisDB(ParseFunctions.redisIdentifiers.Stage01AvgTrade);
                })
                .then(function()
                {
                    //we need to process all the stations
                    return self.qAllBatchesProcess();
                })
                .done(function()
                {
                 success();
                }, function(err)
                {
                    failure(err);
                })


        });
    };

    self.sumRedisInformation = function(redisAvg)
    {
        var confirmed = 0, suspected = 0;
        var cPrice = 0, sPrice = 0;
        var fileCount =  redisAvg.time;
        for(var batch in redisAvg.modTradeMap)
        {
            for(var stationID in redisAvg.modTradeMap[batch]){
                confirmed += redisAvg.modTradeMap[batch][stationID].confirmed;
                suspected += redisAvg.modTradeMap[batch][stationID].suspected;


                cPrice += redisAvg.modTradeMap[batch][stationID].confirmedTotalPrice;
                sPrice += redisAvg.modTradeMap[batch][stationID].suspectedTotalPrice;
            }
        }

        return {confirmed: confirmed, suspected: suspected, confirmedTotalPrice: cPrice, suspectedTotalPrice: sPrice, time: fileCount};
    };

    self.qSingleBatchProcess = function(batchIx)
    {
        return QFunctions.wrapQFunction(function(success, failure)
        {
            //We must query all the stations and all their batches

            QFunctions.qGetLeanMongoObjects(QFunctions.MongoModels.AllTradeModel, {modID: batchIx})
                .then(function(docs)
                {
                    //we have all the docs across the five regions

                    var filesToTrades = {};
                    var filesTradeCount = {};

                    var allFileCount = 0;

                    for(var i=0; i < docs.length; i++)
                    {
                        var batch = docs[i];
                        if(!filesToTrades[batch.fileID])
                        {
                            filesToTrades[batch.fileID] = {};
                        }
                        var tTrades = 0;

                        for(var t=0; t < batch.observedData.length; t++)
                        {
                            var trade = batch.observedData[t];

                            filesToTrades[batch.fileID][trade.tradeID] = trade;
                            tTrades++;
                            allFileCount++;
                        }
                        filesTradeCount[batch.fileID] = tTrades;
                    }

                    //we have all the files, and all the trades.

                    var dumpFiles = Object.keys(filesToTrades);

                    //sort alphabetically -- happens to be chronological
                    dumpFiles.sort();

                    var knownTrades = {};

                    //start with the first day as all new trades
                    var newTradesPerDay = filesToTrades[dumpFiles[0]];

                    var suspectedFulfilled = {}, suspectedExpired = {};

                    var lastFileIdx = 0;

                    var cloneTradeKeys = function(tradeMap)
                    {
                        var clone = {};
                        for(var key in tradeMap)
                        {
                            clone[key] = true;
                        }
                        return clone;
                    };

                    var lastCloneKeys = cloneTradeKeys(filesToTrades[dumpFiles[0]]);


                    for(var i=1; i < dumpFiles.length; i++)
                    {
                        var fileID = dumpFiles[i];

//                        var addedToday = {};

                        //check if we know these trades
                        for(var tradeID in filesToTrades[fileID])
                        {
                            tTrades++;
                            var nTrade = filesToTrades[fileID][tradeID];
                            if(newTradesPerDay[tradeID])
                            {
                                //this isn't a new trade
                                var previousTrade = newTradesPerDay[tradeID];

                                var pMinVol = previousTrade.minVolRemain;
                                //take the minimum volume of the two -- and carry on
                                previousTrade.minVolRemain = Math.min(pMinVol, nTrade.minVolRemain);

//                                if(previousTrade.minVolRemain != pMinVol)
//                                {
//                                    console.log("min max change seen!");
//                                    console.log(previousTrade.minVolRemain);
//                                    console.log(previousTrade.maxVolRemain);
//                                }

                                //note that today, we saw this trade again! It would have to be in this bin
                                delete lastCloneKeys[tradeID];
                            }
                            else
                            {
                                //this is a new trade!
                                newTradesPerDay[tradeID] = nTrade;

//                                if(!addedToday[tradeID])
//                                    addedToday[tradeID] = {};
//
//                                if(!addedToday[tradeID][nTrade.itemID])
//                                    addedToday[tradeID][nTrade.itemID] = [];
//
//                                //keep a list of the items traded in a day
//                                //this is so we can tell if it was a suspected repost, or legitimate
//                                addedToday[tradeID][nTrade.itemID].push(nTrade);
                            }
                        }

//                        console.log(addedToday)

                        //these are ALL unseen trades the next day
                        for(var tradeID in lastCloneKeys)
                        {
                            //take from the last known information
                            //so our suspected trade is such
                            var suspected = newTradesPerDay[tradeID];

                            //check if we have information on other items posted
//                            var othersToday = (addedToday[tradeID] && addedToday[tradeID][suspected.itemID]) ? addedToday[tradeID][suspected.itemID] : undefined;
//
//                            if(addedToday[tradeID])
//                                console.log(addedToday[tradeID]);

//                            var isRepost = false;

                            if(suspected.duration == "1 day")
                                suspectedExpired[tradeID] = true;
                            else
                                suspectedFulfilled[tradeID] = suspected;



//                            console.log("Issued: " , suspected.issued, " duration: ", suspected.duration, " day ", fileID);
//                            if(othersToday)
//                            {
//                                for(var ot=0; ot < othersToday.length; ot++)
//                                {
//                                    console.log(othersToday[ot]);
//                                    console.log(suspected);
//                                    if(othersToday[ot].stationID == suspected.stationID
//                                        && othersToday[ot].volRemain == suspected.volRemain)
//                                    {
//                                        //probably a repost
//                                        console.log('!!!-I SUSPECT a repost');
//                                        isRepost = true;
//                                        break;
//                                    }
//                                }
//                            }

//                            if(!isRepost)
//                                suspectedFulfilled[tradeID] = suspected;
//                            else
//                                suspectedRepost[tradeID] = suspected;

                        }

                        lastCloneKeys = cloneTradeKeys(filesToTrades[dumpFiles[i]]);

                        lastFileIdx++;
                    }

                    console.log(dumpFiles);
                    console.log("Total trade count: ", Object.keys(newTradesPerDay).length, " out of ", allFileCount, " entries.");
                    console.log("Suspected sold count: ", Object.keys(suspectedFulfilled).length, " out of ", allFileCount, " entries.");
                    console.log("Suspected expired count: ", Object.keys(suspectedExpired).length, " out of ", allFileCount, " entries.");

                    //now we need to go through and create a mapping from item to station sales per day

                    var findItems = {};

                    //all trades
                    for(var tradeID in newTradesPerDay)
                    {
                        var trade = newTradesPerDay[tradeID];
                        findItems[trade.itemID] = true;
                    }



//                    console.log(suspectedFulfilled);
//                    console.log('Investigating itemID, found: ', stationToBatches)

                    QFunctions.qRedisSwitchDB(ParseFunctions.redisIdentifiers.Stage01AvgTrade)
                        .then(function()
                        {
                            return QFunctions.qRedisMultiGet(Object.keys(findItems));
                        })
                        .then(function(itemMap)
                        {

                            for(var itemID in findItems)
                            {
                                var redisInfo = itemMap[itemID];
                                if(!redisInfo)
                                {
                                    redisInfo = {
                                        modTradeMap : {},
                                        time : self.fileCount
                                    };

                                    for(var stationID in topFiveMap){

                                        redisInfo.modTradeMap[stationID] = {
                                            confirmed: 0, suspected:0,
                                            confirmedTotalPrice:0, suspectedTotalPrice:0
                                        };
                                    }
                                }

                                //clear out this information for the batch -- we'll be setting this below
//                                redisInfo.modTradeMap[batchIx] = {};
                                //file count reprsents the number of days we're analyzing


                                itemMap[itemID] = redisInfo;
                            }

                            //itemMap now has a full item list
                            //we can proceed


                            //all trades
                            for(var tradeID in newTradesPerDay)
                            {
                                var trade = newTradesPerDay[tradeID];
                                var stationID = trade.stationID;
                                //get redis info
                                var redisInfo = itemMap[trade.itemID];
                                //that is guaranteed to exist, and be correct

                                if(trade.maxVolRemain - trade.minVolRemain > 0)
                                {
                                    //guaranteed to exist
                                    var stationInfo = redisInfo.modTradeMap[stationID];

                                    //add the confirmed information
                                    stationInfo.confirmed += (trade.maxVolRemain - trade.minVolRemain);
                                    stationInfo.confirmedTotalPrice += (trade.maxVolRemain - trade.minVolRemain)*parseFloat(trade.price);
                                }
//                                if(stationInfo.confirmed)
//                                    console.log(stationInfo);
                            }

                            for(var tradeID in suspectedFulfilled)
                            {
                                var trade = suspectedFulfilled[tradeID];
                                var stationID = trade.stationID;
                                //get redis info
                                var redisInfo = itemMap[trade.itemID];
                                //that is guaranteed to exist, and be correct

                                //guaranteed to exist
                                var stationInfo = redisInfo.modTradeMap[stationID];

                                //add the suspected information
                                //since you only know what's left, you assume the rest sold
                                stationInfo.suspected += trade.minVolRemain;
                                stationInfo.suspectedTotalPrice += trade.minVolRemain*parseFloat(trade.price);

                            }


//                            for(var key in itemMap)
//                            {
//                                var sum = self.sumRedisInformation(itemMap[key])
//                                if(sum.confirmed){
//                                    console.log(sum);
//                                    if(sum.confirmed > 0 && sum.confirmed == sum.suspected){
////                                        console.log(itemMap[key].modTradeMap[batchIx]);
//                                        //                                    console.log(itemMap[key].modTradeMap);
//                                    }
//                                }
//
//                            }

                            console.log("Saving (to redis) ", Object.keys(itemMap).length, " items.");
                            return QFunctions.qRedisSetDBThenMultiSet(ParseFunctions.redisIdentifiers.Stage01AvgTrade, itemMap)

                        })
                        .then(function()
                        {
                            console.log('Saved to Redis. Onwards to more processing!');


                        })
                        .done(function(){success();}, function(err){ failure(err);})










                });
        });
    };

    self.qAllBatchesProcess = function()
    {
        var batchIx = 0;

        //trick! just 1 item to investigate
        self.totalBatches = ParseFunctions.MongoMetaKeys.SplitTradeMod;

        return QFunctions.wrapQFunction(function(success, failure)
        {

            var continueTillSaved = function()
            {
                if(batchIx == self.totalBatches)
                {
                    //alldone!
                    success();
                }
                else
                {

                    console.log('Examining item: ' + batchIx, " of ", self.totalBatches);

                    self.qSingleBatchProcess(batchIx)
                        .done(function()
                        {
                            console.log('Batch ', batchIx, " Processed.")
                            batchIx++;
                            //once finished, just call the function again -- away it will go!
                            continueTillSaved();
                        }, function(err)
                        {
                            failure(err);
                        })
                }
            };

            //start the process, it'll finish on its own
            continueTillSaved();

        });
    };

    //sends back what it got in
    self.qPostProcessStage = function()
    {
        console.log('Post process dump files');
        var postArgs = arguments;
        //send back our arguments for next piece
        return QFunctions.wrapQFunction(function(success, failure)
        {
            console.log('Post Processing Stage 01 AvgTrade')

            success();
//            QFunctions.qRedisSwitchDB(ParseFunctions.redisIdentifiers.Stage1Count)
//                .then(function()
//                {
//                    return QFunctions.qRedisSetObject(allInfoKey, {allItems: allItems, allRegions: allRegions});
//                })
//                .then(function()
//                {
//                    return QFunctions.qRedisMultiSetObject(stationToItemMap);
//                })
//                .then(function()
//                {
//                    return QFunctions.qRedisMultiSetObject(itemToStationMap);
//                })
//                .then(function()
//                {
//                    //remove all files where our file ID == our file ID
//                    return QFunctions.qMongoClearDB(QFunctions.MongoModels.RawModel, {fileID: fileID});
//                })
//                .then(function()
//                {
//                    console.log('MongoDB cleared, Setting station information');
//                    //now we have to save all this information to mongodb
//                    //off we go -- huzzah!
//                    self.stationsToSaveList = Object.keys(uniqueStationTrades);
//                    console.log("Need to save ", self.stationsToSaveList.length, " stations")
//                    return self.qSaveAllStations();
//                    //now we have our compiled objects for saving
//
//                })
//                .done(function()
//                {
//                    //now we have to save all this information to mongodb
//                    success();
//                }, function(err)
//                {
//                    failure(err);
//                });
//

//            success.apply(self, postArgs);
        });
    };
}

