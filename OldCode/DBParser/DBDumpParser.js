var fs = require('fs');
var path = require('path');
//reads large files line by line
var byline = require('byline');
//This is a flow library for handling async tasks sequentially
var Q = require('q');

//we have our mongodb interaction library -- defines schema
var mongoose = require('mongoose');

//turn a JSON file into a database schema. Shweet.
var Generator = require('./generator.js');
var Maps = require("../IDMapping/IDMapLoader.js");

//using redis for quick insert/retrieval of batch info
var redis = require("redis")
    , client = redis.createClient();

module.exports = DBParser;

var metaInformationKey = "regionInformation";
var rawSchemaName = "RawTradeSchema";


var batchTradeSize = 500;

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

var clearRedisDatabase = function()
{
    var deferred = Q.defer()

    qReadFile('q:ids')
        .then(function(idCount)
        {
            console.log('Clearing jobs: ', idCount);

            var promises = [];
            for(var i=0; i < idCount; i++)
            {
                promises.push(qRemoveJob(i));
            }

            return Q.all(promises);
        })
        .then(function()
        {
            console.log('Jobs cleared, setting count to 0');
            return qRedisSet('q:ids', 0);
        })
        .then(function()
        {
            console.log('Cleared jobs, and reset count, removing initial tile locks');
            //we have cleared the jobs
            //we've cleared the job count
            //now we must clear the locks set for each tile
            var iTiles = initialTiles();
            var tCount = iTiles.count;
            var promises = [];
            for(var i=0; i < iTiles.length; i++)
            {
                var tileID = iTiles[i];
                promises.push(qRedisSet(tileID, null));
            }
            return Q.all(promises);
        })
        .done(function(){

            console.log('Finished with redisDB clear');
            deferred.resolve();

        }, function(err)
        {
            console.error(err);
            deferred.reject(err);
        });

    return deferred.promise;
};

function trim11 (str) {
    str = str.replace(/^\s+/, '');
    for (var i = str.length - 1; i >= 0; i--) {
        if (/\S/.test(str.charAt(i))) {
            str = str.substring(0, i + 1);
            break;
        }
    }
    return str;
}

var ixTradeID = 0;
var ixRegionID = 1;
var ixSystemID = 2;
var ixStationID = 3;
var ixName = 4;
var ixBuySell = 5;
var ixPrice = 6;
var ixMinVolume = 7;
var ixRemaining = 8;
var ixVolEnter = 9;
var ixIssuedTime = 10;
var ixDuration = 11;
var ixRange = 12;
var ixReportedBy = 13;
var ixReported = 14;

var convertSplitLineToDataObject = function(delimited)
{
    //trim everything of preceding/leading spaces
    return {
        "tradeID" : trim11(delimited[ixTradeID]),
        "regionID": trim11(delimited[ixRegionID]),
        "systemID": trim11(delimited[ixSystemID]),
        "stationID": trim11(delimited[ixStationID]),
        "itemID": trim11(delimited[ixName]),
        "bid": trim11(delimited[ixBuySell]),
        "price": trim11(delimited[ixPrice]),
        "minVolume": trim11(delimited[ixMinVolume]),
        "volRemain": trim11(delimited[ixRemaining]),
        "volEnter": trim11(delimited[ixVolEnter]),
        "issued": trim11(delimited[ixIssuedTime]),
        "duration": trim11(delimited[ixDuration]),
        "range": trim11(delimited[ixRange]),
        "reportedBy": trim11(delimited[ixReportedBy]),
        "reportedTime": trim11(delimited[ixReported])
    };
};






function DBParser()
{
    var self = this;

    //we need to create our object
    self.generator = new Generator();


    self.qFlushRedis = function()
    {
        var deferred = Q.defer()

        client.FLUSHDB(function(err)
        {
            if(err)
                deferred.reject(err);
            else
                deferred.resolve();
        });
        return deferred.promise;
    };

    self.qFlushDatabase = function()
    {
        var deferred = Q.defer()

        //flush redis first, then mongoDB
        self.qFlushRedis().done(function()
        {
            self.RawTrade.remove({}, function(err) {
            if(err)
                deferred.reject(err);
            else
                deferred.resolve();
        });

        }, function(err)
        {
            deferred.reject(err);
        })







        return deferred.promise;
    };


    self.qConnect = function()
    {
        var deferred = Q.defer()


        qConnectMongoose()
            .then(function(connection)
            {
                self.connection = connection;
                self.generator.setConnection(connection);

                return qReadFile(path.resolve(__dirname, "./rawTradeSchema.json"));
            })
            .done(function(tradeBuffer)
            {
                //so we have read in the trade schema
                var tradeSchema = JSON.parse(tradeBuffer);

                //now we have the actual schema
                //we load it into our database as it's own database
                self.generator.loadSingleSchema(rawSchemaName, tradeSchema);

                //store the class inside the parser, we're ready to proceed with Dump parsing
                self.RawTrade = self.generator.getSchemaModel(rawSchemaName);

                deferred.resolve();

            }, function(err)
            {
               console.log("Failed to connection and init: ", err);
                throw new Error(err);
                deferred.reject(err);
            });

        return deferred.promise;
    };

    self.qBatchFindMetaInfo = function(idList)
    {
        var deferred = Q.defer();
        var mappedObjects = {};

//        console.log(idList);

        client.mget(idList, function(err, docs)
        {
            if(err)
            {
                deferred.reject(err);
                return;
            }

            for(var i=0; i < docs.length; i++)
            {
                //mget returns null for anything not retrieved
                if(docs[i])
                {
                    var queryDoc = JSON.parse(docs[i]);
                    //create the key from our basic query info
                    mappedObjects[idList[i]] = queryDoc;
                }
            }
            deferred.resolve(mappedObjects);
        });

        return deferred.promise;
    };

    self.qFindBatchObjects = function(batchList)
    {
        var deferred = Q.defer();
        var mappedObjects = {};

        self.RawTrade.find({batchID : {$in: batchList}}).exec(function(err,docs)
        {
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
                self.RawTrade.find({batchID : {$in: Object.keys(uBatch)}}).exec(function(err,missing)
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

                        for(var i=0; i < docs.length; i++)
                        {
                            mappedObjects[docs[i].batchID] = docs[i];
                        }

                        //have all our documents mapped out
                        deferred.resolve(mappedObjects);
                    }
                });
            }
            else
            {

                for(var i=0; i < docs.length; i++)
                {
                    mappedObjects[docs[i].batchID] = docs[i];
                }

                //have all our documents mapped out
                deferred.resolve(mappedObjects);
            }

        });

        return deferred.promise;
    };

//    self.qBatchFindIDs = function(idList)
//    {
//        var deferred = Q.defer();
//        var mappedObjects = {};
//
//        self.RawTrade.find({tradeID : {$in: idList}}).exec(function(err,doc)
//        {
//            if(err)
//            {
//                deferred.reject(err);
//                return;
//            }
//
//            for(var i=0; i < doc.length; i++)
//            {
//                var queryDoc = doc[i];
//                mappedObjects[self.groupKey(queryDoc)] = queryDoc;
//            }
//            deferred.resolve(mappedObjects);
//        });
//
//        return deferred.promise;
//    };


//    self.qUpdateSingleTrade = function(tradeObject)
//    {
//        var deferred = Q.defer();
//
//        self.RawTrade.find({tradeID : tradeObject.tradeID})
//            .exec(function(err, docs)
//            {
//                if(docs.length == 0)
//                {
//                    //we have no object matching this description! Let's make a new one!
//                    tradeObject.save(function(err)
//                    {
//                        if(err)
//                            deferred.reject("Error in trade save: ", err);
//                        else
//                        //done!
//                        deferred.resolve();
//                    });
//                }
//                else
//                {
//                    if(docs.length > 1)
//                    {
//                        console.log('Error!')
//                        deferred.reject("Duplicate object in DB");
//                        return;
//                    }
//
//                    var queryTrade = docs[0];
//
//                    //concat our arrays together, for science!
//                    queryTrade.observedData = queryTrade.observedData.concat(tradeObject.observedData);
//                    //note this should ruin any previous processed info
//                    queryTrade.wasProcessed = false;
//
//                    queryTrade.markModified("observedData");
//                    queryTrade.save(function(err) {
//                        if(err)
//                            deferred.reject("Error in trade update save: ", err);
//                        else
//                        //done!
//                            deferred.resolve();
//                    });
//                }
//            });
//
//        return deferred.promise;
//    };

    self.qSaveDB = function(modelToSave)
    {
        var deferred = Q.defer();

        modelToSave.save(function(err)
        {
            if(err)
                deferred.reject(err);
            else
                deferred.resolve();
        });

        return deferred.promise;
    };

    self.qSaveKeyValues = function(valueMap)
    {
        var deferred = Q.defer();

        var saveFormat = [];

//        console.log("Trying to save: ", valueMap);
        for(var key in valueMap)
        {
            saveFormat.push(key);
            if(typeof valueMap[key] != "string")
                saveFormat.push(JSON.stringify(valueMap[key]));
            else
                saveFormat.push(valueMap[key]);
        }

        client.mset(saveFormat, function(err)
        {
            if(err)
                deferred.reject(err);
            else
                deferred.resolve();
        });

        return deferred.promise;
    };

//    self.qConcurrentFetch = function(metaInfo, batchInfo)
//    {
//
//        var deferred = Q.defer();
//
//        Q.all(self.qFindBatchObjects(batchInfo), self.qWriteRedisInfo(metaInfo))
//            .done(function(infoArray)
//        {
//                deferred.resolve(infoArray);
//        }, function(err)
//            {
//                deferred.reject(err);
//            });
//
//        return deferred.promise;
//    };



    self.qProcessRawChunkIntoDB = function(dbEntryByTradeID)
    {
        var deferred = Q.defer();

        console.log('Chunking data');

        //we need to collect all our objects into a list of entries
        var dbEntries = {}, dbCounts = {};
        var dbGroupKeys = [];
        for(var groupKey in dbEntryByTradeID)
        {
            //we count how many objects we have for each group key
            dbCounts[groupKey] = dbEntryByTradeID[groupKey].observedData.length;
            dbEntries[groupKey] = dbEntryByTradeID[groupKey];//(new self.RawTrade(dbEntryByTradeID[groupKey]));
            dbGroupKeys.push(groupKey);
        }

        var metaUpdate, batchKeys;

//        console.log("Attempt keys: ", dbGroupKeys)

        self.qBatchFindMetaInfo(dbGroupKeys)
            .then(function(mappedMeta)
            {
                //
                var fetchExisting = [];

                batchKeys = [];

//                console.log(mappedMeta);

                //we have a collection of mapped documents, so we either update, or create new
                for(var gKey in mappedMeta)
                {
                    //we have a group key, and the meta info
                    var metaInfo = mappedMeta[gKey];

                    var lastBatchNumber = Math.floor(metaInfo.totalCount/metaInfo.batchSize);


                    //if this was equal, we'd have an EXACT match
                    //if we had an exact match, then the batch entry wasn't created yet
                    if(lastBatchNumber*metaInfo.batchSize != metaInfo.totalCount)
                    {
                        //fetch the last batch object for this guy
                        fetchExisting.push(self.batchKey(gKey, lastBatchNumber));
                    }
                    else
                    {
                        console.log('EXACT MATCH DETECTED -- likely no batch file exists');
                    }

                    //we need to know all the batches we're interested in
                    batchKeys.push(self.batchKey(gKey, lastBatchNumber));

                    //update the meta info
                    metaInfo.totalCount += dbCounts[gKey];

                    //deleting the group key, we'll need to create meta info for everything that failed in this retrieval
                    delete dbCounts[gKey];
                }

                //we need to create new meta information and new batch objects for each object in here
                for(var gKey in dbCounts)
                {
                    mappedMeta[gKey] =
                    {
                        totalCount : dbCounts[gKey],
                        batchSize: batchTradeSize
                    };

                    //this is our first batch friend!
                    batchKeys.push(self.batchKey(gKey, 0));
                }

                //Meta info setup for all the objects, and it's all up to date
                metaUpdate = mappedMeta;

                //we'll update meta after we have finished everyghing
                //we need to fetch first

//                console.log('Finding batches: ', fetchExisting);

                return self.qFindBatchObjects(fetchExisting)
            })
            .then(function(mappedBatch)
            {

                console.log('Mapped batch reutnr mongod');
                //here, we're going to create all of our batches, ready for saving
                var batchesToSave = [];

                //batchKey holds all our batch keys duh
                for(var i=0; i < batchKeys.length; i++)
                {
                    //grab the batch key
                    var bKey = batchKeys[i];
                    //split it to find component parts
                    //the group key and the batch number
                    var splitBatch = bKey.split("_");

                    var groupKey = splitBatch[0];
                    //parse the batch number from the split object
                    var currentBatchNum = parseInt(splitBatch[1]);

                    //we'll slice up the current arrays
                    var startIx, endIx;

                    //here are the objects we want added (from our db entries above)
                    var toBeAdded = dbEntries[groupKey];
                    //how many chunks of info will be batched up
                    var totalAddCount = toBeAdded.observedData.length;

                    //you already have previously mapped info
                    //merge needed!
                    if(mappedBatch[bKey])
                    {
                        //we already have a batch setup, let's pump it full of stuff
                        var batchDocument = mappedBatch[bKey];
                        //use the group key to get our entry info

                        var remainingInBatch = batchTradeSize - batchDocument.observedData.length;

                        if(remainingInBatch > 0)
                        {
                            startIx = 0;
                            endIx = remainingInBatch;
                            var sliceRemaining = toBeAdded.observedData.slice(startIx, endIx);
                            batchDocument.observedData = batchDocument.observedData.concat(sliceRemaining);

                            //note that we modified data here

                            batchDocument.markModified("observedData");
                            batchesToSave.push(batchDocument);
                        }

                        startIx = remainingInBatch;
                        totalAddCount -= remainingInBatch;
                        //now we move our end forward -- either by the amount remaining or the full batch size
                        endIx += Math.min(totalAddCount, batchTradeSize);
                        currentBatchNum++;

                    }
                    else
                    {
                        startIx = 0;
                        endIx =  Math.min(totalAddCount, batchTradeSize);
                    }

                    var leftToBatch = Math.ceil(totalAddCount/batchTradeSize);

                        for(var x=0; x < leftToBatch; x++)
                        {
                            var amount = Math.min(totalAddCount, batchTradeSize);

                            //we need to create a batch for each time we do this
                            var newEntry = new self.RawTrade(
                            {
                                "batchID" : self.batchKey(groupKey, currentBatchNum),
                                "tradeID" :  dbEntries[groupKey].tradeID,
                                "stationID" : dbEntries[groupKey].stationID,
                                "systemID" : dbEntries[groupKey].systemID,
                                "regionID" : dbEntries[groupKey].regionID,
                                "itemID" : dbEntries[groupKey].itemID,
                                "isBuyOrder" : (dbEntries[groupKey].bid == "1"),
                                "wasProcessed" : false,
                                "observedData" : toBeAdded.observedData.slice(startIx, endIx)
                            });

                            //now we add this entry for saving
                            batchesToSave.push(newEntry);

                            startIx += amount; endIx += batchTradeSize;
                            totalAddCount -= amount;
                            currentBatchNum++;
                        }
                }

                console.log('Saving all')

                //then save all documents at the same time
                var promiseList = [];
                for(var i=0; i < batchesToSave.length; i++)
                {
                    promiseList.push(self.qSaveDB(batchesToSave[i]));
                }

                //make sure they all complete as a batch
                return Q.all(promiseList)
            })
            .then(function()
            {
                console.log('Updating meta info');
                //now we have to save all our meta things, -- last piece for completion!
                return self.qSaveKeyValues(metaUpdate);
            })
            .done(function()
            {
                deferred.resolve();
            }, function(err)
            {
                //nope, didn't work
                deferred.reject(err);
            });

        //we search for all these ids in a batch
        //then whatever we don't find, we save
//        self.qBatchFindIDs(dbGroupKeys)
//            .then(function(mappedDocuments)
//            {
//                var saveDocuments = [];
//
//                //we have a collection of mapped documents, so we either update, or create new
//                for(var tID in mappedDocuments)
//                {
//                    //if we have it in mapped documents, we do a merge
//                    var existingTrade = mappedDocuments[tID];
//                    var queryTrade = dbEntries[tID];
//
//
//                    //concat our arrays together, for science!
//                    existingTrade.observedData = existingTrade.observedData.concat(queryTrade.observedData);
//                    //note this should ruin any previous processed info
//                    existingTrade.wasProcessed = false;
//
//                    //we've updated some info, make sure it knows the array has changed
//                    existingTrade.markModified("observedData");
//
//                    //now, we'll save this later
//                    saveDocuments.push(existingTrade);
//
//                    //remove from db entries
//                    delete dbEntries[tID];
//                }
//
//                //what's left was never found in the db, we just save them straight up
//                for(var tID in dbEntries)
//                {
//                    saveDocuments.push(dbEntries[tID]);
//                }
//                if(saveDocuments.length != dbGroupKeys.length)
//                {
//                    deferred.reject("ERROR! Incorrect number of saved items, something went wrong")
//                    return;
//                }
//
//                //then save all documents at the same time
//                var promiseList = [];
//                for(var i=0; i < saveDocuments.length; i++)
//                {
//                    promiseList.push(self.qSaveDB(saveDocuments[i]));
//                }
//
//                //make sure they all complete as a batch
//                Q.all(promiseList)
//                    .done(function()
//                    {
//                        deferred.resolve();
//                    }, function(err)
//                    {
//                        //nope, didn't work
//                        deferred.reject(err);
//                    });
//            });

        return deferred.promise;
    };


    self.qRawDBCount = function()
    {
        var deferred= Q.defer();
        self.RawTrade.count({}, function(err, count)
        {
            if(err)
            deferred.reject(err);
            else
            deferred.resolve(count);
        });
        return deferred.promise;
    };

    //We write a function for loading up the file, then parsing line by line

    self.batchKey = function(gKey, batchNumber)
    {
        return gKey + "_" + batchNumber;
    };

    self.groupKeyComponents = function(gKey)
    {
        return gKey.split('|');
    };

    self.groupKey = function(innerObject)
    {
//        return innerObject.tradeID;//
        return innerObject.regionID + "|" + innerObject.itemID;
    };

    self.qParseLocalFile = function(file)
    {
        var deferred = Q.defer();

        //fix the path for searching -- then let's dod this thang
        file = path.resolve(file);

        //so let's go through the stream of lines one by one, adding to the database
        //we'll make a promise to add to the database for each line (processing batches at a time)

        var skipFirstLine = true;
        var lineCount = 0;
        var maxLines = 25000;

        var maxToProcess = 100000;
        var totalProcessed = 0;

        var dbEntryByTradeID = {};

        var stream = fs.createReadStream(file);
        stream = byline.createStream(stream);

        stream.on('data', function(line) {
            if(skipFirstLine)
            {
                skipFirstLine = false;
            }
            else
            {
                //we need to quickly parse this, then get it into a database suitable form

                //get rid of any quotation marks
                var delimited = line.toString().replace("\r", "").replace(/"/g, "").split(",");

                //have our inner object created from the split line
                var innerObject= convertSplitLineToDataObject(delimited);

//                console.log('inner obj: ', innerObject);

                //now we check if we've seen it before during local processing
                //we don't want duplicates!
                var groupKey = self.groupKey(innerObject);
//                console.log(innerObject);

//                console.log(innerObject.reportedBy, " time: ", innerObject.reportedTime);

                if(!dbEntryByTradeID[groupKey])
                {
                    dbEntryByTradeID[groupKey]=
                    {
                        "tradeID" : innerObject.tradeID,
                        "stationID" : innerObject.stationID,
                        "systemID" : innerObject.systemID,
                        "regionID" : innerObject.regionID,
                        "itemID" : innerObject.itemID,
                        "isBuyOrder" : (innerObject.bid == "1"),
                        "wasProcessed" : false,
                        "observedData" : [innerObject]
                    };
                }
                else
                {
                    //we prevent duplicate adds on each object
//                    var oData = dbEntryByTradeID[groupKey].observedData;
//                    var addData = true;
//                    for(var i=0; i < oData.length; i++)
//                    {
//                        if(innerObject.volRemain == oData[i].volRemain)
//                        {
//                            addData = false;
//                        }
//                    }

//                    if(addData)
//                    {
                        //otherwise, we already have observed data! Push is in
                        dbEntryByTradeID[groupKey].observedData.push(innerObject);
                        //note this should ruin any previous processed info
                        dbEntryByTradeID[groupKey].wasProcessed = false;
//                    }
                }

                lineCount++;

                if(lineCount == maxLines)
                {
                    //halt the stream
                    stream.pause();

                    console.log('Saving chunk of parsed objects');

                    //process the lines
                    self.qProcessRawChunkIntoDB(dbEntryByTradeID)
                        .done(function()
                        {
                            //here we would continue -- but not us boy-o
                            //just testing for right now
                            totalProcessed += maxLines;

                            console.log('Finished processing chunk. Next please @ ', totalProcessed);
                            dbEntryByTradeID = {};
                            lineCount = 0;

                            //now we go forever!
//                            if(totalProcessed >= maxToProcess)
//                                deferred.resolve();
//                            else
                                stream.resume();

                        }, function(err)
                        {
                            deferred.reject(err);
                        });


                    //continue the stream
                }
            }
        });

        stream.on('end', function()
        {
            //almost finished!
            //make sure everything is saved
            if(lineCount > 0)
            {
                //process the lines
                self.qProcessRawChunkIntoDB(dbEntryByTradeID)
                    .done(function()
                    {
                        //here we would continue -- but not us boy-o
                        //just testing for right now
                        totalProcessed += lineCount;

                        console.log('Last chunk processed. Final @ ', totalProcessed);
                        dbEntryByTradeID = {};
                        lineCount = 0;

                        deferred.resolve();

                    }, function(err)
                    {
                        deferred.reject(err);
                    });
            }




        });



        return deferred.promise;
   }


    self.qProcessMapIntoDB = function(mapOfItems)
    {
        var deferred = Q.defer();

        var metaObjects = {};

        var allKeyList = [];

        //now we create a list of items
        for(var key in mapOfItems)
        {
            var valueObject = mapOfItems[key];

            //flatten our object into a list
            var valueList = [];
            for(var vKey in valueObject)
                valueList.push(vKey);


            var metaKey = key + "_meta";

            metaObjects[metaKey] = JSON.stringify(
            {
                "key" : key,
                "metaKey" : metaKey,
                "itemList" : valueList
            });

            allKeyList.push(key);
        }

        metaObjects[metaInformationKey] =JSON.stringify(
        {
            metaKey : metaInformationKey,
            keyList : allKeyList
        });

        self.qSaveKeyValues(metaObjects)
            .done(function()
            {
                deferred.resolve();
            }, function(err)
            {
                deferred.reject(err);
            });


        return deferred.promise;
    }

    self.qSwitchDB = function(dbVar)
    {
        var deferred = Q.defer();

        client.select(dbVar, function(err,res){
            // you'll want to check that the select was successful here
            // if(err) return err;

            if(err)
                deferred.reject(err);
            else
                deferred.resolve();
        });

        return deferred.promise;
    };

    self.qGroupKeyMap = function(file)
    {
        var deferred = Q.defer();

        //fix the path for searching -- then let's dod this thang
        file = path.resolve(file);

        //so let's go through the stream of lines one by one, adding to the database
        //we'll make a promise to add to the database for each line (processing batches at a time)

        var skipFirstLine = true;
        var lineCount = 0;
        var maxLines = 100000;

        var maxToProcess = 100000;
        var totalProcessed = 0;


        var regionsAndItems = {};


        var stream = fs.createReadStream(file);
        stream = byline.createStream(stream);

        stream.on('data', function(line) {
            if(skipFirstLine)
            {
                skipFirstLine = false;
            }
            else
            {
                //we need to quickly parse this, then get it into a database suitable form

                //get rid of any quotation marks
                var delimited = line.toString().replace("\r", "").replace(/"/g, "").split(",");

                //have our inner object created from the split line
                var innerObject= convertSplitLineToDataObject(delimited);


                var groupKey = self.groupKey(innerObject);

                //get the first and second part of the group key
                var gComponents = self.groupKeyComponents(groupKey);

                var key = gComponents[0];
                var value = gComponents[1];

                if(!value)
                {
                    throw new Error("Group key must have 2 pieces for this to work in parsing");
                }


                //create our object as well as the start of the value
                if(!regionsAndItems[key])
                    regionsAndItems[key] = {};


                regionsAndItems[key][value] = true;

                //that's it, we have all our regions and their itemIDs
                lineCount++;

                if(lineCount == maxLines)
                {
                    //halt the stream
                    stream.pause();

                    console.log('Saving chunk of parsed objects');

                    //process the lines
                    self.qProcessMapIntoDB(regionsAndItems)
                        .done(function()
                        {
                            //here we would continue -- but not us boy-o
                            //just testing for right now
                            totalProcessed += maxLines;

                            console.log('Finished processing chunk. Next please @ ', totalProcessed);
                            lineCount = 0;

                            //now we go forever!
//                            if(totalProcessed >= maxToProcess)
//                                deferred.resolve();
//                            else
                                stream.resume();

                        }, function(err)
                        {
                            deferred.reject(err);
                        });


                    //continue the stream
                }
            }
        });

        stream.on('end', function()
        {
            //almost finished!
            //make sure everything is saved
            if(lineCount > 0)
            {
                //process the lines
                self.qProcessMapIntoDB(regionsAndItems)
                    .done(function()
                    {
                        //here we would continue -- but not us boy-o
                        //just testing for right now
                        totalProcessed += lineCount;

                        console.log('Last chunk processed. Final @ ', totalProcessed);
                        lineCount = 0;

                        deferred.resolve();

                    }, function(err)
                    {
                        deferred.reject(err);
                    });
            }




        });



        return deferred.promise;
    }





}
