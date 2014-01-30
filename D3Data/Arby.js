var fs = require('fs');
var path = require('path');

//This is a flow library for handling async tasks sequentially
var Q = require('q');
Q.longStackSupport = true;

//turn a JSON file into a database schema. Shweet.
var Maps = require("../IDMapping/IDMapLoader.js");

//var DBRedisParser = require('./DBParser/DBRedisParser.js');
//var parser = new DBRedisParser();

var Compiler = require('./ArbitrageCompiler.js');

var compiler = new Compiler();

Maps.qEnsureMapsLoaded()
    .then(function()
    {
        return compiler.qConnect();
    }).then(function()
    {
        compiler.qSumArbitrage();
    })
    .done(function()
    {
        console.log('Beginning processing regions! \n');
    },function(err)
    {
        console.log('Oh no error!', err);
    });




