var fs = require('fs');
var path = require('path');


//This is a flow library for handling async tasks sequentially
var Q = require('q');
Q.longStackSupport = true;

//turn a JSON file into a database schema. Shweet.
var Maps = require("./IDMapping/IDMapLoader.js");


var DBParser = require('./DBParser/DBDumpParser.js');
var parser = new DBParser();

//var DBRedisParser = require('./DBParser/DBRedisParser.js');
//var parser = new DBRedisParser();

//flush our database first
var FLUSHTHEDATABASE = false;

parser.qConnect()
    .then(parser.qFlushDatabase)
    .done(function()
    {
        //nuffin left to do
        console.log('DB Flushed');
    });

//var fs = require('fs');
//var path = require('path');
//
//var DBParser = require('./DBParser/DBDumpParser.js');
//
//
//var Q = require('q');
//Q.longStackSupport = true;
//
////turn a JSON file into a database schema. Shweet.
//var Maps = require("./IDMapping/IDMapLoader.js");
////
//var parser = new DBParser();
//
//Maps.qEnsureMapsLoaded()
//    .then(function()
//    {
//        return parser.qConnect();
//    })
//    .then(function()
//    {
//        return parser.qRawDBCount();
//    })
//    .done(function(count)
//    {
//        console.log('Counted items: ' + count);
//        console.log('Parse dump success! Onwards and upwards!');
//    },function(err)
//    {
//        console.log('Oh no error!', err);
//    });
//
//
//
