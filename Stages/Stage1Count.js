var QFunctions = require("../Globals/qFunctions.js");
var ParseFunctions = require("../Globals/parseFunctions.js");

module.exports = Stage1Class;

function Stage1Class()
{
    var self = this;

    var topFiveMap, topFiveList;

    var fileID;

    //just a simple wrapper, doesn't do anything but send back what it got in
    self.qPreProcessStage = function()
    {
        var preArgs = arguments;

        topFiveMap = QFunctions.Maps.TopFiveMap;
        topFiveList = QFunctions.Maps.TopFiveStations;

        //send back our arguments for next piece
        return QFunctions.wrapQFunction(function(success, failure)
        {
            success.apply(self, preArgs);
        });
    };

    var uniqueStationTrades = {};
//    var stationCount = {};

    //processing a dump file -- skip first line!
    self.qProcessStage = function(fileName)
    {
        //this is how we batch objects
        fileID = fileName;

        return QFunctions.wrapQFunction(function(success, failure)
        {
            var chunkCount = 0;

//            QFunctions.qFlushRedisDB(ParseFunctions.redisIdentifiers.Stage1Count)
            QFunctions.qRedisSwitchDB(ParseFunctions.redisIdentifiers.Stage1Count)
                .then(function()
                {
                    QFunctions.chunkProcessLocalFile(fileName,
                        {chunkSize: 40000, skipFirstLine: true, maxLineCount: 0},
                        {
                            readLine : function(line)
                            {
                                var innerObject= ParseFunctions.processTradeLine(line);

                                //nothing to do! We don't care about this station
                                //this is a top five search
                                if(!topFiveMap[innerObject.stationID])
                                    return;

                                //this is our unique stationID/item key
//                        var batchKey = ParseFunctions.batchKey(innerObject.stationID, innerObject.itemID);
                                var tradeID = innerObject.tradeID;

                                var stationHolder = uniqueStationTrades[innerObject.stationID];

                                if(!stationHolder)
                                {
                                    stationHolder = {unique : 0};
                                    uniqueStationTrades[innerObject.stationID] = stationHolder;
                                }
                                var itemHolder = stationHolder[innerObject.itemID];

                                if(!itemHolder)
                                {
                                    itemHolder  = {unique : 0};
                                    uniqueStationTrades[innerObject.stationID][innerObject.itemID] = itemHolder;
                                    stationHolder[innerObject.itemID] = itemHolder;

                                    stationHolder.unique++;
                                }

                                //
                                if(!itemHolder[tradeID])
                                {
                                    itemHolder[tradeID] = {trades:[], min: Number.MAX_VALUE, max: Number.MIN_VALUE, tradeID: tradeID};
                                    itemHolder.unique++;
                                }

                                var tObject = itemHolder[tradeID];
                                var volRemain = parseFloat(innerObject.volRemain);

                                if(volRemain < tObject.min || volRemain > tObject.max)
                                {
                                    //keep the trade, lower than higher than what we saw before
                                    tObject.trades.push(innerObject);
                                    tObject.min = Math.min(volRemain, tObject.min);
                                    tObject.max = Math.max(volRemain, tObject.max);
                                }

                                //now we have all the info we need for each stationID/itemID batch
                            },
                            processChunk : function(resumeFn)
                            {
//                        //from each chunk, ONLY take the top ten stations of info
//                        var stationCount = {};
//
//                        for(var batchKey in uniqueStationTrades)
//                        {
//                            var keyPieces = ParseFunctions.invertKey(batchKey);
//                            if(!stationCount[keyPieces[0]])
//                            {
//                                stationCount[keyPieces[0]] = 0;
//                            }
//                            stationCount[keyPieces[0]] +=  uniqueStationTrades[batchKey].unique;
//                        }
//
//                        var sorter = [];
//                        for(var key in stationCount)
//                        {
//                            sorter.push({key: key, value: stationCount[key]});
//                        }
//                        sorter.sort(function(a,b){
//                           return b.value - a.value;
//                        });
//
//                        //take top ten stations
//                        sorter = sorter.slice(0,10);
//
//                        //now create a small map
//                        var allowedMap = {};
//                        for(var i=0; i < sorter.length; i++)
//                            allowedMap[sorter[i].key] = true;
//
//
//                        var allowedInfo = {};
//                        for(var batchKey in uniqueStationTrades)
//                        {
//                            var keyPieces = ParseFunctions.invertKey(batchKey);
//
//                            //junk the piece
//                            if(allowedMap[keyPieces[0]])
//                                allowedInfo[batchKey] = uniqueStationTrades[batchKey];
//                        }
//
//                        //dump the old
//                        uniqueStationTrades = allowedInfo;


                                console.log('Process chunk: ' +  chunkCount++);
                                resumeFn();
                            },
                            endFile : function()
                            {
//                        console.log(uniqueStationTrades);
                                console.log('Finished Counting Dump File');

                                //we clear out the batch file

                                success();
                            }
                        }
                    );
                })

        });
    };

    self.qSaveAllStations = function()
    {
        var stationIx = 0;

        return QFunctions.wrapQFunction(function(success, failure)
        {

            var continueTillSaved = function()
            {
                if(stationIx == self.stationsToSaveList.length)
                {
                    //alldone!
                    success();
                }
                else
                {
                    var stationID = self.stationsToSaveList[stationIx];

                    console.log('Saving station: ' + stationID, " with ", Object.keys(uniqueStationTrades[stationID]).length, " items!");
                    self.qSaveSingleStation(stationID)
                        .done(function()
                        {
                            console.log('Station: ' + QFunctions.Maps.StationIDToName[stationID] + " all saved.")
                            stationIx++;
                            //once finished, just call the function again -- away it will go!
                            continueTillSaved();
                        }, function(err)
                        {
                            failure(err);
                        })
                }
            }

            //start the process, it'll finish on its own
            continueTillSaved();

        });
    };

    self.qSaveSingleStation = function(stationID)
    {
        return QFunctions.wrapQFunction(function(success, failure)
        {
            var rawMongoObjects = [];
            console.log('Builing raw mongo objects');

            var itemBatchCount = {};
            var batchSize = 500;

            for(var itemID in uniqueStationTrades[stationID])
            {
                if(itemID == "unique")
                    continue;

                itemBatchCount[itemID] = Math.ceil(uniqueStationTrades[stationID][itemID].unique/batchSize);
            }

            var finalBatchCount = 0;
            for(var itemID in uniqueStationTrades[stationID])
            {
                if(itemID == "unique")
                    continue;

                var stationItemTradeMap = uniqueStationTrades[stationID][itemID];

                var batchID = ParseFunctions.batchKey(stationID, itemID);

                //breaking the item into batches
                //batchcount = # of items / batchSize
                var totalBatch = [];
                var bCount = itemBatchCount[itemID];
                finalBatchCount+= bCount;

                for(var i=0; i < bCount; i++)
                {
                    var compiledSchema = {
                        fileID : fileID,
                        batchID : batchID,
                        batchIx: i,
                        totalBatchCount : bCount,
                        stationID: stationID,
                        itemID :  itemID,
                        wasProcessed : false,
                        observedData: []
                    };
                    totalBatch.push(compiledSchema);
                    rawMongoObjects.push(compiledSchema);
                }



                var tCount = 0;
                //need to figure out what batch you belong to, then add the data to the batch
                for(var trade in stationItemTradeMap)
                {
                    if(trade == "unique")
                        continue;

                    //for each trade, don't save anything more than ONE object
                    //not the min and max volume seen.
                    //this is importnant for followup information
                    //this holds confirmed buy/sell information
                    var cTrade = stationItemTradeMap[trade];

                    var dataTrade = cTrade.trades[0];
                    dataTrade.minVolRemain = cTrade.min;
                    dataTrade.maxVolRemain = cTrade.max;

//                    if(Math.abs(dataTrade.minVolRemain - dataTrade.maxVolRemain) > 0)
//                    {
//                        console.log("Buy/Sell spotted!", dataTrade);
//                    }

                    var batchIx = Math.floor(tCount/batchSize);

                    //stick it in a batch!
                    totalBatch[batchIx].observedData.push(dataTrade);

                    tCount++;
                }
                //                console.log(" after observed: " + mongoObj.observedData.length);


            }

//            console.log(rawMongoObjects)
            console.log("Saving: " + rawMongoObjects.length + " in " + finalBatchCount, " batches");

            //we have all our raw objects

            QFunctions.qChunkSaveMongoModels(QFunctions.MongoModels.RawModel, rawMongoObjects, 200)
                .done(function()
                {
                    //we've saved all objects, 100 at a time!
                    success();
                }, function(err)
                {
                    failure(err);
                });
        });
    };

    //sends back what it got in
    self.qPostProcessStage = function()
    {
        console.log('Post process dump file' + fileID);
        var postArgs = arguments;
        //send back our arguments for next piece
        return QFunctions.wrapQFunction(function(success, failure)
        {
            var stationToItemMap = {};
            var itemToStationMap = {};
            var allItems = [];
            var allRegions = [];

            for(var stationID in uniqueStationTrades)
            {
                var itemMap = uniqueStationTrades[stationID];
                var stationMeta = stationID + "_itemList";
                stationToItemMap[stationMeta] = {};
                allRegions.push(stationID);

                for(var itemID in itemMap)
                {
                    stationToItemMap[stationMeta][itemID] = uniqueStationTrades[stationID][itemID].unique;

                    var itemMeta =  itemID + "_regionList";

                    if(!itemToStationMap[itemMeta]){
                        itemToStationMap[itemMeta] = {};
                        allItems.push(itemID);
                    }

                    itemToStationMap[itemMeta][stationID] = true;
                }
            }

            console.log('Setting redis info')

            QFunctions.qRedisSwitchDB(ParseFunctions.redisIdentifiers.Stage1Count)
                .then(function()
                {
                    return QFunctions.qRedisSetObject(ParseFunctions.redisMetaKeys.S1Summary, {allItems: allItems, allRegions: allRegions});
                })
                .then(function()
                {
                    return QFunctions.qRedisMultiSetObject(stationToItemMap);
                })
                .then(function()
                {
                    return QFunctions.qRedisMultiSetObject(itemToStationMap);
                })
                .then(function()
                {
                    //remove all files where our file ID == our file ID
                    return QFunctions.qMongoClearDB(QFunctions.MongoModels.RawModel, {fileID: fileID});
                })
                .then(function()
                {
                    console.log('MongoDB cleared, Setting station information');
                    //now we have to save all this information to mongodb
                    //off we go -- huzzah!
                    self.stationsToSaveList = Object.keys(uniqueStationTrades);
                    console.log("Need to save ", self.stationsToSaveList.length, " stations")
                    return self.qSaveAllStations();
                    //now we have our compiled objects for saving

                })
                .done(function()
                {
                    //now we have to save all this information to mongodb
                    success();
                }, function(err)
                {
                    failure(err);
                });


//            success.apply(self, postArgs);
        });
    };
}

