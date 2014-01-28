var fs = require('fs');
var path = require('path');

//This is a flow library for handling async tasks sequentially
var Q = require('q');
Q.longStackSupport = true;

//turn a JSON file into a database schema. Shweet.
var Maps = require("./IDMapping/IDMapLoader.js");

//var DBRedisParser = require('./DBParser/DBRedisParser.js');
//var parser = new DBRedisParser();

var Compiler = require('./DBCompiler/RunCompiler.js');

var compiler = new Compiler();


var totalRegions = 0;
var startIx = 0;
var regionsAtATime = 0;

//process 1 region at a time

var processNextRegion = function()
{
    compiler.qCompileRegionData(startIx)
        .done(function()
        {
            startIx++;
            if(startIx == totalRegions)
            {
                console.log("Finished processing all ", totalRegions, " regions!!!");
            }
            else
            {
                setTimeout(processNextRegion, 0);
            }

        }, function(err)
        {
            console.log('Compilation failed!');
            throw err;
        })
};


Maps.qEnsureMapsLoaded()
    .then(function()
    {
        return compiler.qConnect();
    })
    .then(function()
    {
        //switch to database 1!
//        return compiler.qCompileData();
        return compiler.qGetMetaList();
    })
    .then(function(metaData)
    {
        var keyList = metaData.keyList;
//                var maxRegions = Math.min(2, keyList.length);
        totalRegions = keyList.length;

        setTimeout(processNextRegion, 0);
    })
    .done(function()
    {
        console.log('Beginning processing regions!');
    },function(err)
    {
        console.log('Oh no error!', err);
    });




