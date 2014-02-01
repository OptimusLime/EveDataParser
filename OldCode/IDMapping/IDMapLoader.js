var fs = require('fs');
var path = require('path');

//This is a flow library for handling async tasks sequentially
var Q = require('q');

var MapManager = {GlobalMaps : undefined};

module.exports = MapManager;

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

function MapManagerClass()
{
    var self = this;

    self.ItemIDToName = {};
    self.StationIDToName = {};
    self.ItemNameToID = {};
    self.StationNameToID = {};

    self.StationNameList = [];
    self.ItemNameList = [];

    self.loadMaps = function()
    {
        var deferred = Q.defer();

        qReadFile(path.resolve(__dirname, "./stationMap.json"))
            .then(function(mapBuffer)
            {
              self.StationIDToName = JSON.parse(mapBuffer);

                //reverse the map to store the opposite
                //name => ID
                for(var id in self.StationIDToName)
                {
                    var name = self.StationIDToName[id];
                    self.StationNameToID[name] = id;
                    self.StationNameList.push(name);
                }

                return qReadFile(path.resolve(__dirname, "./itemNameMap.json"));
            })
            .then(function(itemBuffer)
            {
                self.ItemIDToName = JSON.parse(itemBuffer);

                //reverse the map to store the opposite
                //name => ID
                for(var id in self.ItemIDToName)
                {
                    var name = self.ItemIDToName[id];
                    self.ItemNameToID[name] = id;
                    self.ItemNameList.push(name);
                }

            })
            .done(function()
            {
                deferred.resolve();
            }, function(err){
                deferred.reject(err);
            });

        return deferred.promise;
    }
}

MapManager.qEnsureMapsLoaded = function()
{
    MapManager.GlobalMaps = new MapManagerClass();

    return MapManager.GlobalMaps.loadMaps();
};



