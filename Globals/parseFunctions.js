var fs = require('fs');
var path = require('path');
//reads large files line by line
var byline = require('byline');
//This is a flow library for handling async tasks sequentially
var Q = require('q');

module.exports = new ParseFunctions();


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

function ParseFunctions()
{
    var self = this;

    self.redisIdentifiers =
    {
        Stage1Count : 13,
        Stage01AvgTrade : 2,
        Stage2Algorithm : 1
    };
    self.redisMetaKeys =
    {
        S1Summary : "summaryInformation"
    };
    self.MongoMetaKeys =
    {
        SplitTradeMod : 1000
    };

    self.processTradeLine = function(line)
    {
        var delimited = line.toString().replace("\r", "").replace(/"/g, "").split(",");

        //have our inner object created from the split line
        return convertSplitLineToDataObject(delimited);
    };

    self.batchKey = function(first, second)
    {
        return first + "|" + second;
    };

    self.invertKey = function(key)
    {
        return key.split("|");
    };


}