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

//using redis for quick insert/retrieval of batch info
var redis = require("redis");

var metaInformationKey = "regionInformation";
var metaAllItemsKey = "allItemsList";
var finalSchemaName = "finalSchema";
var regionSchemaName = "regionSchema";

var lastKnownDBId = 10;
var arbitrageDBId = 11;

module.exports = new QFunctions();

function QFunctions()
{
    var self = this;

    //this is a global redis client
    //for more specific settings per use, make your own connection

    self.generator = new Generator();


    self.RedisDBName =
    {

    };

    self.Maps =
    {
      ItemIDToName : {},
      StationIDToName : {}
    };

    //all our schema and names
    self.MongoSchemas =
    {
        rawBatch: "RawBatch",
        firstProcessRegion: "FirstProcessRegion",
        secondProcessRegion: "SecondProcessRegion",
        allTrade : "AllTrade"
    };

    self.MongoModels =
    {
        RawModel : {},
        FirstProcessModel : {},
        SecondProcessModel : {},
        AllTradeModel : {}
    };

//    self.qCreateMongooseAndRedisClients = function(mongoParams, redisParams)
//    {
//        return self.wrapQFunction(function(success, failure)
//        {
//            //create redis connection
//           var redisClient = redis.createClient(redisParams);
//
//        };
//    }


    self.qConnect = function(mongoParams, redisParams)
    {
        //need to make mongoose connection, and redis connection

        return self.wrapQFunction(function(success, failure)
        {
            //create redis connection
            //set redis params
            redisParams = redisParams || {};

            self.redisClient = redis.createClient();

//            console.log('Redis Conn Created');

            self.qConnectMongoose()
                .then(function(connection)
                {
//                    console.log('Mongoose started');
                    //We have a mongoose connection, now we need to deal with our generator
                    self.mongooseClient = connection;
                    self.generator.setConnection(connection);

                    var readAll = [];
                    readAll.push(self.qReadFile(path.resolve(__dirname, "../Schemas/rawTradeSchema.json")));
                    readAll.push(self.qReadFile(path.resolve(__dirname, "../Schemas/compileSchema.json")));
                    readAll.push(self.qReadFile(path.resolve(__dirname, "../Schemas/finalSchema.json")));
                    readAll.push(self.qReadFile(path.resolve(__dirname, "../Schemas/allTradeSchema.json")));

                    return Q.all(readAll);
                })
                .then(function(filesRead)
                {
                    //so we have read in the trade schema
                    var rtSchema = JSON.parse(filesRead[0]);
                    var fpRegion = JSON.parse(filesRead[1]);
                    var spRegion = JSON.parse(filesRead[2]);
                    var atSchema = JSON.parse(filesRead[3]);

                    //now we have the actual schema
                    //we load it into our database as it's own database
                    self.generator.loadSingleSchema(self.MongoSchemas.rawBatch, rtSchema);
                    self.generator.loadSingleSchema(self.MongoSchemas.firstProcessRegion, fpRegion);
                    self.generator.loadSingleSchema(self.MongoSchemas.secondProcessRegion, spRegion);
                    self.generator.loadSingleSchema(self.MongoSchemas.allTrade, atSchema);

                    //store the class inside the parser, we're ready to proceed with Dump parsing
                    self.MongoModels.RawModel = self.generator.getSchemaModel(self.MongoSchemas.rawBatch);
                    self.MongoModels.FirstProcessModel = self.generator.getSchemaModel(self.MongoSchemas.firstProcessRegion);
                    self.MongoModels.SecondProcessModel = self.generator.getSchemaModel(self.MongoSchemas.secondProcessRegion);
                    self.MongoModels.AllTradeModel = self.generator.getSchemaModel(self.MongoSchemas.allTrade);

                    var mappingList = [];

                    mappingList.push(self.qReadFile(path.resolve(__dirname, "../MapData/itemNameMap.json")));
                    mappingList.push(self.qReadFile(path.resolve(__dirname, "../MapData/stationMap.json")));
                    mappingList.push(self.qReadFile(path.resolve(__dirname, "../MapData/topTradeHubs.json")));

                    return Q.all(mappingList);
                })
                .then(function(mapBuffers)
                {
                    self.Maps.ItemIDToName = JSON.parse(mapBuffers[0]);
                    self.Maps.StationIDToName = JSON.parse(mapBuffers[1]);

                    var topTrade = JSON.parse(mapBuffers[2]).topStation;
                    var topMap = {};
                    var topFive = [];
                    for(var i=0; i < topTrade.length; i++)
                    {
                        topMap[topTrade[i].key] = topTrade[i].value;
                        if(i < 5)
                            topFive.push(topTrade[i].key);
                    }

                    var topFiveMap = {};
                    for(var i=0; i < topFive.length; i++)
                        topFiveMap[topFive[i]] = self.Maps.StationIDToName[topFive[i]];


                    //contains the names!
                    self.Maps.TopFiveMap = topFiveMap;
                    self.Maps.TopTradeStations = topMap;
                    self.Maps.TopFiveStations = topFive;
                    //that's it for now, we're all done with our loading information
                    //maybe some buffer info?
                })
                .done(function()
                {
                    success();
                }, function(err)
                {
                    failure(err);
                });

        });
    };

    self.wrapQFunction = function(toWrap)
    {
        var deferred = Q.defer();

        var success = function()
        {
            deferred.resolve.apply(this, arguments);
        };

        var failure = function(err)
        {
            deferred.reject.apply(this, arguments);
        };

        toWrap(success, failure);

        return deferred.promise;
    };

    self.qReadFile = function(file)
    {
        return self.wrapQFunction(function(success, failure)
        {
            fs.readFile(file, function(err, data)
            {
                if(err)
                    failure(err);
                else
                    success(data);
            });
        });
    };

    self.qConnectMongoose = function(params)
    {
        return self.wrapQFunction(function(success, failure)
        {
            params = params || {};
            params.dbID = params.dbID || "eve";

            // connect to Mongo when the app initializes
            var mongooseConnection = mongoose.createConnection('mongodb://localhost/'+params.dbID);

            mongooseConnection.on('error', function(e)
            {
                failure(e);
            });

            mongooseConnection.on('open', function(){
                success(mongooseConnection);
            });
        });
    };


    //THIS SECTION IS FOR REDISDB ACTIONS

    self.qRedisMultiGet = function(keys)
    {
        return self.wrapQFunction(function(success, failure)
        {
            self.redisClient.mget(keys, function(err, values)
            {
                if(err)
                {
                    failure(err);
                    return;
                }

                var mappedValues = {};
                for(var i=0; i < values.length; i++)
                {
                    if(values[i])
                        mappedValues[keys[i]] = JSON.parse(values[i]);
                }

                success(mappedValues);
            });


        });
    };

    self.qRedisGetObject = function(key)
    {
        return self.wrapQFunction(function(success, failure)
        {
            self.redisClient.get(key, function(err, value)
            {
                if(err)
                {
                    failure(err);
                    return;
                }

                success(JSON.parse(value));
            });
        });
    };

    self.qRedisMultiSetObject = function(setDictionary)
    {
        return self.wrapQFunction(function(success, failure)
        {
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
                    failure(err);
                    return;
                }

                success();
            });
        });
    };

    self.qFlushRedisDB = function(dbVar)
    {
        return self.wrapQFunction(function(success, failure)
        {
            self.qRedisSwitchDB(dbVar)
                .done(function()
                {
                    self.redisClient.FLUSHDB(function(err)
                    {
                        console.log("Flush returned! ", err ? "Failed " + err : "success.");
                        if(err)
                            failure(err);
                        else
                            success();
                    });
                }, function(err)
                {
                    failure(err);
                });
        });
    };

    self.qRedisSwitchDB = function(dbVar)
    {
        return self.wrapQFunction(function(success, failure)
        {
            self.redisClient.select(dbVar, function(err){
                // you'll want to check that the select was successful here
                // if(err) return err;
                if(err)
                    failure(err);
                else
                    success();
            });
        });
    };

    self.qRedisSetObject = function(key, value)
    {
        return self.wrapQFunction(function(success, failure)
        {
            if(typeof value != "string")
                value = JSON.stringify(value);

            self.redisClient.set(key, value, function(err)
            {
                if(err)
                {
                    failure(err);
                    return;
                }

                success();
            });

        });
    };

    self.qRedisSetDBThenSetObject = function(dbVal, key, obj)
    {
        return self.wrapQFunction(function(success, failure)
        {
            self.qRedisSwitchDB(dbVal)
                .then(function()
                {
                    return self.qRedisSetObject(key, obj);
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

    self.qRedisSetDBThenGetObject = function(dbVal, key)
    {
        return self.wrapQFunction(function(success, failure)
        {
            self.qRedisSwitchDB(dbVal)
                .then(function()
                {
                    return self.qRedisGetObject(key);
                })
                .done(function(value)
                {
                    success(value);
                }, function(err)
                {
                    failure(err);
                })
        });
    };

    //Streamin API for the big files

    var openLocalFileStream = function(localFilePath)
    {
        console.log(localFilePath);
        var stream = fs.createReadStream(path.resolve(localFilePath));
        stream = byline.createStream(stream);
        stream.pause();

        //for pausing and continuing
        return stream;
    };

    var chunkOptions =
    {
        readLine : {},
        endFile : {},
        processChunk : {}
    };

    self.chunkProcessLocalFile = function(file, chunkOptions, processFunctions)
    {
        var fStream = openLocalFileStream(file);

        var lineCount = 0;
        var totalLines = 0;
        var maxLineCount = chunkOptions.maxLineCount || Number.MAX_VALUE;

        var pauseStream = function()
        {
            fStream.pause();
        };
        var resumeStream = function()
        {
            fStream.resume();
        };

        var endFunction = function()
        {
            //we still need to process the final chunk of lines
            //we pause the stream
            pauseStream();

            //we must do line chunks processing
            //pass resume for when to start again
            processFunctions.processChunk(function()
            {
                //we're done, so the stream is already at the end
                processFunctions.endFile();
            });

        };

        var lineFunction = function(line)
        {
            if(chunkOptions.skipFirstLine)
            {
                chunkOptions.skipFirstLine = false;
            }
            else
            {
                //process the line
                processFunctions.readLine(line);
                //note that we've added another line of processing!
                lineCount++;

                if(totalLines + lineCount == maxLineCount)
                {
                    //skip to the end funciton -- it'll ppause the stream
                    //process the chunk, and call end function
                    endFunction();
                }
                else if(lineCount == chunkOptions.chunkSize)
                {
                    //we pause the stream
                    pauseStream();

                    //once resume is called, line count will need to be reset already
                    totalLines += lineCount;
                    lineCount = 0;

                    //we must do line chunks processing
                    //pass resume for when to start again
                    processFunctions.processChunk(resumeStream);

                }
            }
        };



        fStream.on('data', lineFunction);
        fStream.on('end', endFunction);

        //all setup, start the processing
        fStream.resume();

    };


    self.qChunkSaveMongoModels = function(MongoModel, preMongoObject, batchSize)
    {
        //need to save all these models
        return self.wrapQFunction(function(success, failure)
        {
            var startIx = 0;
            var endIx = Math.min(startIx + batchSize, preMongoObject.length);

            var currentBatch = 0;
            var maxBatch = Math.ceil(preMongoObject.length/batchSize);

            var checkFinished = function(processNextStep)
            {
                if(endIx == preMongoObject.length)
                {
                    console.log('Chunk saved: ' + preMongoObject.length);
                    //all done, finish up
                    success();
                }
                else
                {
                    startIx += batchSize;
                    endIx = Math.min(startIx + batchSize, preMongoObject.length);
                    processNextStep(startIx, endIx);
                }
            };

            var saveBatch = function(start, end)
            {
                var promised = [];
                for(var i=start; i< end; i++)
                {
                    promised.push(new MongoModel(preMongoObject[i]));
                }

                //save all of these!
                self.qSaveMongoBatch(promised)
                    .then(function()
                    {
                        currentBatch++;
                        console.log("Mongo batch succeed: ", currentBatch, " out of ", maxBatch);
                        //we've saved everything so far! Next piece
                        checkFinished(saveBatch);
                    }, function(err)
                    {
                        console.log('Mongo batch fail!');
                        failure(err);
                    })
            };


            console.log("Starting batch ", currentBatch);
            //start teh process, it'll finish itself
            saveBatch(startIx, endIx);
        });
    };

    //MONGO Functions for saving
    self.qSaveMongoBatch = function(objects)
    {
        var allSave = [];
        for(var i=0; i < objects.length; i++)
            allSave.push(self.qSaveMongoObject(objects[i]));

//        console.log('Saving Batches in Q: ' + allSave.length);
//        console.log(objects);

        return Q.all(allSave);
    };

    self.qSaveMongoObject = function(object)
    {
       return self.wrapQFunction(function(success, failure)
       {
           //
//           console.log(object);
           object.save(function(err)
           {
               if(err)
                    failure(err);
               else
                    success();
           })

       });
    };

    self.qGetLeanMongoObjects = function(Model, query)
    {
        return self.wrapQFunction(function(success, failure)
        {
            Model.find(query).lean().exec(function(err, docs)
            {
                if(err)
                failure(err);
                else
                success(docs);
            });
        });
    };

    self.qMongoClearDB = function(ModelToClear, clearQuery)
    {
        return self.wrapQFunction(function(success, failure)
        {
            ModelToClear.remove(clearQuery, function(err)
            {
                if(err)
                    failure(err);
                else
                    success();
            });
        })
    };


}