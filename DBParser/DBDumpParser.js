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

module.exports = DBParser;

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
        "typeID": trim11(delimited[ixName]),
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




var rawSchemaName = "RawTradeSchema";


function DBParser()
{
    var self = this;

    //we need to create our object
    self.generator = new Generator();

    self.qFlushDatabase = function()
    {
        var deferred = Q.defer()


        self.RawTrade.remove({}, function(err) {
            if(err)
                deferred.reject(err);
            else
                deferred.resolve();
        });

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

    self.qBatchFindIDs = function(idList)
    {
        var deferred = Q.defer();
        var mappedObjects = {};

        self.RawTrade.find({tradeID : {$in: idList}}).exec(function(err,doc)
        {
            if(err)
            {
                deferred.reject(err);
                return;
            }

            for(var i=0; i < doc.length; i++)
            {
                var queryDoc = doc[i];
                mappedObjects[queryDoc.tradeID] = queryDoc;
            }
            deferred.resolve(mappedObjects);
        });

        return deferred.promise;
    };


    self.qUpdateSingleTrade = function(tradeObject)
    {
        var deferred = Q.defer();

        self.RawTrade.find({tradeID : tradeObject.tradeID})
            .exec(function(err, docs)
            {
                if(docs.length == 0)
                {
                    //we have no object matching this description! Let's make a new one!
                    tradeObject.save(function(err)
                    {
                        if(err)
                            deferred.reject("Error in trade save: ", err);
                        else
                        //done!
                        deferred.resolve();
                    });
                }
                else
                {
                    if(docs.length > 1)
                    {
                        console.log('Error!')
                        deferred.reject("Duplicate object in DB");
                        return;
                    }

                    var queryTrade = docs[0];

                    //concat our arrays together, for science!
                    queryTrade.observedData = queryTrade.observedData.concat(tradeObject.observedData);
                    //note this should ruin any previous processed info
                    queryTrade.wasProcessed = false;

                    queryTrade.markModified("observedData");
                    queryTrade.save(function(err) {
                        if(err)
                            deferred.reject("Error in trade update save: ", err);
                        else
                        //done!
                            deferred.resolve();
                    });
                }
            });

        return deferred.promise;
    };

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

    self.qProcessRawChunkIntoDB = function(dbEntryByTradeID)
    {
        var deferred = Q.defer();

        //we need to collect all our objects into a list of entries
        var dbEntries = {};
        var dbTradeIDs = [];
        for(var tradeID in dbEntryByTradeID)
        {
//            console.log(dbEntryByTradeID[stationID]);
            //create a databasae object from our JSON entries
            dbEntries[tradeID] = (new self.RawTrade(dbEntryByTradeID[tradeID]));
            dbTradeIDs.push(tradeID);
        }


        //we search for all these ids in a batch
        //then whatever we don't find, we save
        self.qBatchFindIDs(dbTradeIDs)
            .then(function(mappedDocuments)
            {
                var saveDocuments = [];

                //we have a collection of mapped documents, so we either update, or create new
                for(var tID in mappedDocuments)
                {
                    //if we have it in mapped documents, we do a merge
                    var existingTrade = mappedDocuments[tID];
                    var queryTrade = dbEntries[tID];


                    //concat our arrays together, for science!
                    existingTrade.observedData = existingTrade.observedData.concat(queryTrade.observedData);
                    //note this should ruin any previous processed info
                    existingTrade.wasProcessed = false;

                    //we've updated some info, make sure it knows the array has changed
                    existingTrade.markModified("observedData");

                    //now, we'll save this later
                    saveDocuments.push(existingTrade);

                    //remove from db entries
                    delete dbEntries[tID];
                }

                //what's left was never found in the db, we just save them straight up
                for(var tID in dbEntries)
                {
                    saveDocuments.push(dbEntries[tID]);
                }
                if(saveDocuments.length != dbTradeIDs.length)
                {
                    deferred.reject("ERROR! Incorrect number of saved items, something went wrong")
                    return;
                }

                //then save all documents at the same time
                var promiseList = [];
                for(var i=0; i < saveDocuments.length; i++)
                {
                    promiseList.push(self.qSaveDB(saveDocuments[i]));
                }

                //make sure they all complete as a batch
                Q.all(promiseList)
                    .done(function()
                    {
                        deferred.resolve();
                    }, function(err)
                    {
                        //nope, didn't work
                        deferred.reject(err);
                    });
            });

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

    self.groupKey = function(innerObject)
    {
        return innerObject.tradeID;// innerObject.stationID + "|" + innerObject.itemID;
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
        var maxLines = 10000;

        var maxToProcess = 1000000;
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

//                console.log(innerObject.reportedBy, " time: ", innerObject.reportedTime);

                if(!dbEntryByTradeID[groupKey])
                {
                    dbEntryByTradeID[groupKey]=
                    {
                        "tradeID" : innerObject.tradeID,
                        "stationID" : innerObject.stationID,
                        "itemID" : innerObject.itemID,
                        "isBuyOrder" : (innerObject.bid == "1"),
                        "wasProcessed" : false,
                        "observedData" : [innerObject]
                    };
                }
                else
                {
                    //we prevent duplicate adds on each object
                    var oData = dbEntryByTradeID[groupKey].observedData;
                    var addData = true;
                    for(var i=0; i < oData.length; i++)
                    {
                        if(innerObject.volRemain == oData[i].volRemain)
                        {
                            addData = false;
                        }
                    }

                    if(addData)
                    {
                        //otherwise, we already have observed data! Push is in
                        dbEntryByTradeID[groupKey].observedData.push(innerObject);
                        //note this should ruin any previous processed info
                        dbEntryByTradeID[groupKey].wasProcessed = false;
                    }
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
}
