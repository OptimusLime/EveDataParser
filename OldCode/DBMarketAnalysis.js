var fs = require('fs');
var path = require('path');


//This is a flow library for handling async tasks sequentially
var Q = require('q');
Q.longStackSupport = true;

//turn a JSON file into a database schema. Shweet.
var Maps = require("./IDMapping/IDMapLoader.js");

//var DBRedisParser = require('./DBParser/DBRedisParser.js');
//var parser = new DBRedisParser();

var DBParser = require('./DBParser/DBDumpParser.js');
var parser = new DBParser();

//flush our database first
var FLUSHTHEDATABASE = false;

if(FLUSHTHEDATABASE)
{
    parser.qFlushDatabase()
        .done(function()
        {
            //nuffin left to do
            console.log('DB Flushed');
        });
}
else
{
    Maps.qEnsureMapsLoaded()
        .then(function()
        {
            return parser.qConnect();
        })
        .then(function()
        {
            //switch to database 1!
            return parser.qSwitchDB(1);
        })
        .then(function()
        {
//            return parser.qGroupKeyMap("Z:/Eve/Data/2014-01-24.dump");
//            return parser.qParseLocalFile("Z:/Eve/Data/2014-01-24.dump")
        })
        .done(function()
        {
            console.log('Parse dump success! Onwards and upwards!');
        },function(err)
        {
            console.log('Oh no error!', err);
        });
}



