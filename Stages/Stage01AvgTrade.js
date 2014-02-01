var QFunctions = require("../Globals/qFunctions.js");
var ParseFunctions = require("../Globals/parseFunctions.js");

module.exports = Stage01Class;

function Stage01Class()
{
    var self = this;

    var topFiveMap, topFiveList;

    var fileID;

    //just a simple wrapper, doesn't do anything but send back what it got in
    self.qPreProcessStage = function(fileName)
    {
        var preArgs = arguments;

        topFiveMap = QFunctions.Maps.TopFiveMap;
        topFiveList = QFunctions.Maps.TopFiveStations;
        fileID = fileName;

        //send back our arguments for next piece
        return QFunctions.wrapQFunction(function(success, failure)
        {
            //fetch objects from Stage1

            QFunctions.qRedisSetDBThenGetObject(ParseFunctions.redisIdentifiers.Stage1Count, ParseFunctions.redisMetaKeys.S1Summary)
                .then(function(summaryInfo)
                {
                    //oops, called it regions -- old habits die hard
                    self.allStations = summaryInfo.allRegions;
                    self.allItems = summaryInfo.allItems;
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
            QFunctions.qRedisSwitchDB(ParseFunctions.redisIdentifiers.Stage2Algorithm)
                .then(function()
                {
                    //we need to process all the stations
                    return self.qAllItemsProcess();
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

    self.qSingleItemProcess = function(itemID)
    {
        return QFunctions.wrapQFunction(function(success, failure)
        {
            //We must query all the stations and all their batches

            QFunctions.qGetLeanMongoObjects(QFunctions.MongoModels.RawModel, {itemID: itemID, fileID: fileID})
                .then(function(docs)
                {
                    //we have all the docs across the five regions
                    var stationToBatches = {};

                    var totalBoughtAtStation = {};

                    for(var i=0; i < docs.length; i++)
                    {
                        var mDoc = docs[i];
                        var stationID = mDoc.stationID;
                        //each document is a batch of trades

                        if(!stationToBatches[stationID])
                        {
                            stationToBatches[stationID] = [];
                            totalBoughtAtStation[stationID] = 0;
                        }

                        for(var d=0; d < mDoc.observedData.length; d++)
                        {
                            //if it's a sell order ,we'll take a look!
                            if(mDoc.observedData[d].bid == "0"){

                                mDoc.observedData[d].price = parseFloat(mDoc.observedData[d].price);
                                mDoc.observedData[d].minVolRemain = parseFloat(mDoc.observedData[d].minVolRemain);
                                mDoc.observedData[d].maxVolRemain = parseFloat(mDoc.observedData[d].maxVolRemain);
                                mDoc.observedData[d].quantity = mDoc.observedData[d].minVolRemain;
                                stationToBatches[stationID].push(mDoc.observedData[d]);
                                totalBoughtAtStation[stationID] += (mDoc.observedData[d].maxVolRemain - mDoc.observedData[d].minVolRemain);
                            }
                        }
                    }

                    var highestStationPrice = [];

                    for(var stationID in stationToBatches)
                    {
                        var stationSort = stationToBatches[stationID];
                        //sort station goods by price
                        stationSort.sort(function(a,b) { return b.price - a.price;});

                        var totalQuantity = 0;
                        //take the bottom cheapest 10 percent of goods, and calculate
                        for(var i=0; i < stationSort.length; i++)
                        {
                            totalQuantity += stationSort[i].quantity;
                        }
                        var tenPercent = Math.ceil(.1*totalQuantity);
                        var tpSave = tenPercent;
                        var totalPrice = 0;

                        for(var i=0; i < stationSort.length; i++)
                        {
                            var remQuantity = Math.min(stationSort[i].quantity ,tenPercent);

                            //add that amount * price
                            totalPrice += remQuantity*stationSort[i].price;
                            tenPercent -= remQuantity;

                            if(tenPercent == 0)
                                break;

                        }

                        var avgPrice = totalPrice/tpSave;

                        highestStationPrice.push({stationID: stationID, price: avgPrice, quantity: tpSave});
                    }

                    //sort the prices
                    highestStationPrice.sort(function(a,b) { return b.price - a.price;});

                    //we have the cheapest prices
                    console.log('Highest prices: ', highestStationPrice);
                    console.log('Known purchase quantity: ', totalBoughtAtStation);

//                    console.log('Investigating itemID, found: ', stationToBatches)
                    success();

                });
        });
    };

    self.qAllItemsProcess = function()
    {
        var itemIx = 0;

        //trick! just 1 item to investigate
        self.allItems = self.allItems.slice(0,1);

        return QFunctions.wrapQFunction(function(success, failure)
        {

            var continueTillSaved = function()
            {
                if(itemIx == self.allItems.length)
                {
                    //alldone!
                    success();
                }
                else
                {
                    var itemID = self.allItems[itemIx];

                    console.log('Examining item: ' + itemID, " of ", self.allItems.length);

                    self.qSingleItemProcess(itemID)
                        .done(function()
                        {
                            console.log('Item: ' + QFunctions.Maps.ItemIDToName[itemID] + " all processed.")
                            itemIx++;
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
        console.log('Post process dump file' + fileID);
        var postArgs = arguments;
        //send back our arguments for next piece
        return QFunctions.wrapQFunction(function(success, failure)
        {
            console.log('Post Processing Stage 2 Algorithm')

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

