var fs = require('fs');
var path = require('path');
//reads large files line by line
var byline = require('byline');
//This is a flow library for handling async tasks sequentially
var Q = require('q');

//using redis for quick insert/retrieval
var redis = require("redis")
    , client = redis.createClient();

//turn a JSON file into a database schema. Shweet.
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

var qWriteListToFile = function(file, list)
{
    var deferred = Q.defer();

    var toFileString = "";
    for(var i=0; i < list.length; i++)
    {
        var eol = (i%1000 == 0 && i > 0)
        var eof = (i==list.length -1);

        //add | to delimit, except at end of line
        toFileString += list[i].toString() + ((!eol && !eof) ? "|" : "");

        if(eol)
        {
            toFileString += "\n";
        }
    }

    fs.writeFile(file, toFileString, function(err, data)
    {
        if(err)
            deferred.reject(err);
        else
            deferred.resolve(data);
    });

    return deferred.promise;
};

//var qRedisConnect = function()
//{
//    var deferred = Q.defer();
//
//    client.on("error", function (err) {
//        console.log("Error " + err);
//        deferred.reject(err);
//    });
//
//    client.on("connect", function()
//    {
//        console.log('Reddit connected!');
//
//        deferred.resolve();
//    });
//
//    return deferred.promise;
//};


function runSample() {
// Set a value
    client.set("string key", "Hello World", function (err, reply) {
        console.log(reply.toString());
    });
// Get a value
    client.get("string key", function (err, reply) {
        console.log(reply.toString());
    });
}

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
        "reportedTime": trim11(delimited[ixReported])
    };
};

var rawSchemaName = "RawTradeSchema";


function DBParser()
{
    var self = this;

    self.qFlushDatabase = function()
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

    //connect to the database
    self.qConnect = function()
    {
        var deferred = Q.defer()

        self.connection = client;

        setTimeout(function()
        {
            deferred.resolve();
        },0);

        return deferred.promise;
    };

    self.qBatchFindIDs = function(idList)
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
                    mappedObjects[self.groupKey(queryDoc)] = queryDoc;
                }
            }
            deferred.resolve(mappedObjects);
        });

        return deferred.promise;
    };

    self.qSaveKeyValues = function(valueMap)
    {
        var deferred = Q.defer();

        var saveFormat = [];

        for(var key in valueMap)
        {
            saveFormat.push(key);
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

    self.qProcessRawChunkIntoDB = function(dbEntryByTradeID)
    {
        console.log('Process chunk!');
        var deferred = Q.defer();

        //we need to collect all our objects into a list of entries
        var dbEntries = {};
        var dbTradeIDs = [];

        for(var tradeID in dbEntryByTradeID)
        {
            //we have the chunks, note them in a separate mapping
            dbEntries[tradeID] = dbEntryByTradeID[tradeID];
            dbTradeIDs.push(tradeID);
        }

//        console.log("Searching for: ", dbTradeIDs.length);

        //we search for all these ids in a batch
        //then whatever we don't find, we save
        self.qBatchFindIDs(dbTradeIDs)
            .then(function(mappedDocuments)
            {
//                console.log('Mapped returns!');

                var saveDocuments = {};
                var saveCount = 0;

                //we have a collection of mapped documents, so we either update, or create new
                for(var tID in mappedDocuments)
                {
//                    console.log('Updating : ' + tID);
                    //if we have it in mapped documents, we do a merge
                    var existingTrade = mappedDocuments[tID];
                    var queryTrade = dbEntries[tID];

                    //we've updated some info, we'll have to save the new merged object
                    self.mergeEntries(queryTrade, existingTrade);

                    //now, we'll save this later
                    saveDocuments[tID] = JSON.stringify(existingTrade);
                    saveCount++;

                    //remove from db entries
                    delete dbEntries[tID];
                }

                //what's left was never found in the db, we just save them straight up
                for(var tID in dbEntries)
                {
                    saveDocuments[tID] = JSON.stringify(dbEntries[tID]);
                    saveCount++;
                }
                if(dbTradeIDs.length != saveCount)
                {
                    deferred.reject("Failed to have correct number of saving items. ");
                    return;
                }


                console.log('Ready to save documents!');


                self.qSaveKeyValues(saveDocuments)
                    .done(function()
                    {
                        //saved all our keys back into the database
                        deferred.resolve();
                    }, function(err)
                    {
                        //nope, didn't work
                        deferred.reject(err);
                    });
            },function(err)
            {
                deferred.reject(err);
            });

        return deferred.promise;
    };

//    self.qRawDBCount = function()
//    {
//        var deferred= Q.defer();
//        self.RawTrade.count({}, function(err, count)
//        {
//            if(err)
//            deferred.reject(err);
//            else
//            deferred.resolve(count);
//        });
//        return deferred.promise;
//    };

    //We write a function for loading up the file, then parsing line by line

    self.groupKey = function(innerObject)
    {
        return innerObject.tradeID;//innerObject.stationID + "," + innerObject.itemID;
    };

    self.createNewEntry = function(innerObject)
    {
        var entryItem =
        {
            "regionID" : innerObject.regionID,
            "itemID" : innerObject.itemID,
            "systemID" : innerObject.systemID,
            "stationID" : innerObject.stationID,
            "tradeID" : innerObject.tradeID,
            "wasProcessed" : false,
            "sellOrder" : {},
            "buyOrder" : {}
        };

        if(innerObject.bid == "0")
            entryItem.sellOrder[innerObject.tradeID] = [innerObject];
        else
            entryItem.buyOrder[innerObject.tradeID] = [innerObject];

        return entryItem;
    };

    self.updateEntry = function(entryItem, innerObject)
    {
        entryItem.wasProcessed = false;

        //make sure to add to our buy or sell order grouping
        if(innerObject.bid=="0")
        {
            if(!entryItem.sellOrder[innerObject.tradeID])
                entryItem.sellOrder[innerObject.tradeID] = [innerObject];
            else
                entryItem.sellOrder[innerObject.tradeID].push(innerObject);
        }
        else
        {
            if(!entryItem.buyOrder[innerObject.tradeID])
                entryItem.buyOrder[innerObject.tradeID] = [innerObject];
            else
                entryItem.buyOrder[innerObject.tradeID].push(innerObject);
        }
    };

    self.mergeEntries = function(newTrade, existingTrade)
    {
        //if we have it in mapped documents, we do a merge

        var existingSales = existingTrade.sellOrder;
        var existingBuy = existingTrade.buyOrder;

        var newTradeSales = newTrade.sellOrder;
        var newTradeBuy = newTrade.buyOrder;

        for(var key in newTradeSales)
        {
            var inject = newTradeSales[key];
            if(existingSales[key])
                existingSales[key].push(inject);
            else
                existingSales[key] = [inject];
        }

        for(var key in newTradeBuy)
        {
            var inject = newTradeBuy[key];
            if(existingBuy[key])
                existingBuy[key].push(inject);
            else
                existingBuy[key] = [inject];
        }

        existingTrade.wasProcessed = false;
    }

    self.qParseLocalFile = function(file)
    {
        var deferred = Q.defer();

        //fix the path for searching -- then let's dod this thang
        file = path.resolve(file);

        //so let's go through the stream of lines one by one, adding to the database
        //we'll make a promise to add to the database for each line (processing batches at a time)

        var skipFirstLine = true;
        var lineCount = 0;
        var maxLines = 1000;

        var maxToProcess = 10000;
        var totalProcessed = 0;

        var dbEntryByTradeID = {};

        var keyCount = 0;
        var allKeyEntries = {};

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

                if(!allKeyEntries[groupKey])
                {
                    allKeyEntries[groupKey] = true;
                    keyCount++;
                }

//                var tradeID = innerObject.tradeID;

                if(!dbEntryByTradeID[groupKey])
                {
                    dbEntryByTradeID[groupKey] = self.createNewEntry(innerObject);
                }
                else
                {
                    //get the existing item
                    var entryItem = dbEntryByTradeID[groupKey];
                   self.updateEntry(entryItem, innerObject)
                }

                lineCount++;

                if(lineCount == maxLines)
                {
                    //halt the stream
                    stream.pause();

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
//                            {
//                                console.log('Writing to file!');
//                                var keys = [];
//                                for(var key in allKeyEntries)
//                                    keys.push(key);
//
//                                qWriteListToFile(path.resolve(__dirname, "groupKeys.txt"), keys)
//                                    .done(function()
//                                    {
//                                        deferred.resolve();
//                                    }, function(err)
//                                    {
//                                        deferred.reject(err);
//
//                                    });
//                            }
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
                    .then(function()
                    {
                        //here we would continue -- but not us boy-o
                        //just testing for right now
                        totalProcessed += lineCount;

                        console.log('Last chunk processed. Final @ ', totalProcessed);
                        dbEntryByTradeID = {};
                        lineCount = 0;

                        console.log('Writing to file!');
                        var keys = [];
                        for(var key in allKeyEntries)
                            keys.push(key);

                        return qWriteListToFile(path.resolve(__dirname, "groupKeys.txt"), keys);
                    })
                    .done(function()
                    {
                        //successful write to file!
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
