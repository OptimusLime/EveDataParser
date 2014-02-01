var QFunctions = require("../Globals/qFunctions.js");
var ParseFunctions = require("../Globals/parseFunctions.js");

module.exports = Stage1Class;

function Stage1Class()
{
    var self = this;

    var topFiveMap, topFiveList;

    var fileID;

    var splitTradeMod = 1000;
    var uniqueModTrades = {};

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

                                var modID = tradeID % splitTradeMod;

                                var itemHolder = uniqueModTrades[modID];
                                if(!itemHolder){

                                    itemHolder = {unique: 0};
                                    uniqueModTrades[modID] = itemHolder;
                                }

                                //now check if we've seen this trade before
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

    self.qSaveAllModGroups = function()
    {
        var modIx = 0;

        return QFunctions.wrapQFunction(function(success, failure)
        {

            var continueTillSaved = function()
            {
                if(modIx == self.modsToSaveList.length)
                {
                    //alldone!
                    success();
                }
                else
                {
                    var modID = self.modsToSaveList[modIx];

                    console.log('Saving mod group: ' + modID, " with ", Object.keys(uniqueModTrades[modID]).length, " items!");
                    self.qSaveSingleModGroup(modID)
                        .done(function()
                        {
                            console.log('Mod group ' + modID + ' all saved.')
                            modIx++;
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

    self.qSaveSingleModGroup = function(modID)
    {
        return QFunctions.wrapQFunction(function(success, failure)
        {

            console.log('Builing raw mongo objects');

            var rawMongoObjects = [];

            var compiledSchema = {

                fileID : fileID,
                modID : modID,
                observedData: []
            };


            //need to figure out what batch you belong to, then add the data to the batch
            for(var tradeID in uniqueModTrades[modID])
            {
                if(tradeID == "unique")
                    continue;

                //for each trade, don't save anything more than ONE object
                //not the min and max volume seen.
                //this is importnant for followup information
                //this holds confirmed buy/sell information
                var cTrade = uniqueModTrades[modID][tradeID];

                var dataTrade = cTrade.trades[0];
                dataTrade.minVolRemain = cTrade.min;
                dataTrade.maxVolRemain = cTrade.max;

                //stick it in the batch!
                compiledSchema.observedData.push(dataTrade);
            }

            rawMongoObjects.push(compiledSchema);



            console.log("Saving trade batches: " + rawMongoObjects.length);

            //we have all our raw objects

            QFunctions.qChunkSaveMongoModels(QFunctions.MongoModels.AllTradeModel, rawMongoObjects, 200)
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
            console.log('Saving Mod Group info')

            var allModIds =[];
            for(var modID in uniqueModTrades)
            {
                if(modID == "unique")
                    continue;
                allModIds.push(modID);
            }
            //save this list, we'll go through it, 1 by 1, saving the groups to mongo
            self.modsToSaveList = allModIds;

            //remove all files where our file ID == our file ID
            //this ensures no dupes
            QFunctions.qMongoClearDB(QFunctions.MongoModels.AllTradeModel, {fileID: fileID})
                .then(function()
                {
                    return self.qSaveAllModGroups()
                })
                .done(function()
                {
                    //now we have to save all this information to mongodb
                    success();
                }, function(err)
                {
                    failure(err);
                });
        });
    };
}

