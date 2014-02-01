var fs = require('fs');
var path = require('path');

//This is a flow library for handling async tasks sequentially
var Q = require('q');
Q.longStackSupport = true;

//turn a JSON file into a database schema. Shweet.
var Maps = require("../IDMapping/IDMapLoader.js");

//var DBRedisParser = require('./DBParser/DBRedisParser.js');
//var parser = new DBRedisParser();

var Compiler = require('./D3DataCreator.js');

var compiler = new Compiler();


var totalRegions = 0;
var startIx = 0;
var regionsAtATime = 0;

//process 1 region at a time


var savedMeta = {};
var processNextRegion = function()
{
//    compiler.qCreateRegionData(startIx)
    compiler.qCreateItemData(startIx)
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
    .then(function(regionData)
    {
        var keyList = regionData.keyList;
        savedMeta.keyList = keyList;
        return compiler.qGetMetaItemList();
    })
    .then(function(iList)
    {
        var itemList = [];
        for(var key in iList.knownMap)
            itemList.push(key);

        savedMeta.itemList = itemList;
        //
//                var maxRegions = Math.min(2, keyList.length);
        totalRegions = savedMeta.itemList.length;//4;//keyList.length;
//        totalRegions = keyList.length;
//        startIx = totalRegions - 50;
//        startIx = 1613;
//        totalRegions = 1614;

        console.log("Starting: ", startIx, " out of ", totalRegions)

        setTimeout(processNextRegion, 0);
    })
    .done(function()
    {
        console.log('Beginning processing regions! \n');
    },function(err)
    {
        console.log('Oh no error!', err);
    });




