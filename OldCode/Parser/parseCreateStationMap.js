var fs = require('fs');
var path = require('path'),
    byline = require('byline');

//var stationPath = path.resolve("Z:/Eve/Data/stations.sql");
var stationPath = path.resolve("Z:/Eve/Data/systems_stations_regions_const.sql");
//systems_stations_regions_const.sql

var stationSavePath =  path.resolve("Z:/Eve/Data/stationMap.json");
var stationMap = {};

var stream = fs.createReadStream(stationPath);
stream = byline.createStream(stream);

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


var insideInfo = false;

stream.on('data', function(line) {

    var ls = line.toString();

    if(ls.indexOf("Data for Name: systems; Type: TABLE DATA; Schema: public; Owner: evec") != -1)
        insideInfo = false;

    if(insideInfo)
    {
        var lsLength = ls.length;

        if(lsLength > 8)
        {
            //pull the first 8 digits

            var stationID = ls.substring(0, 8);
            var stationName =  trim11(ls.substring(8, lsLength-8));
            stationMap[stationID] = stationName;

//            console.log("ID:" + stationID + "END0", "Name:" + stationName + "0END0");
        }

    }
    if(ls.indexOf("COPY stations (stationid, stationname, systemid) FROM stdin;") != -1)
        insideInfo = true;






//    if(ls.indexOf('(') != -1 && ls.indexOf(')') != -1)
//    {
//        var sqlInfo = ls.substring(ls.indexOf('(')+1, ls.lastIndexOf(')'));
//
//        sqlInfo = sqlInfo.replace(/'/g, "");
//
//        var splitInfo = sqlInfo.split(',');
//        if(splitInfo.length == 3)
//        {
//            console.log(splitInfo);
//            splitInfo[0] =  splitInfo[0].replace(/ /g, "");
//            splitInfo[1] =  splitInfo[1].replace(" ", "");
//            splitInfo[2] =  splitInfo[2].replace(/ /g, "");
//
//            stationMap[splitInfo[0]] = splitInfo[1];
//        }
//
//    }
});
stream.addListener('end', function()
{
    //save unique orders as json file

    console.log('End station stream!');
    fs.writeFile(stationSavePath, JSON.stringify(stationMap), function(err)
    {
        if(err)
            console.log('Failed write station map');
        else
            console.log('Saved Station Map JSON.');

    });

});





