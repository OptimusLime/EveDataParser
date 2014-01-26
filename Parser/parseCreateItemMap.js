var fs = require('fs');
var path = require('path'),
    byline = require('byline');

var itemPath = path.resolve("Z:/Eve/Data/types.txt");

var itemSavePath =  path.resolve("Z:/Eve/Data/itemNameMap.json");
var itemNameMap = {};

var stream = fs.createReadStream(itemPath);
stream = byline.createStream(stream);

var lineCount = 0;

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


stream.on('data', function(line) {

    var ls = line.toString();

    if(lineCount > 1)
    {
        var splitItem = ls.split('|');
//        console.log(ls);

        if(splitItem.length > 1)
        {
            splitItem[0] = trim11(splitItem[0]);//.replace(/ /g, "");
            splitItem[1] = trim11(splitItem[1]);

            var itemID = splitItem[0];
            var name = splitItem[1];

            itemNameMap[itemID] = name;


//        console.log(splitItem);
            console.log("Item: ", itemID, "Name: ", name);
        }
//        console.log(ls.split('|'));

    }
    lineCount++;



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
    fs.writeFile(itemSavePath, JSON.stringify(itemNameMap), function(err)
    {
        if(err)
            console.log('Failed write item map');
        else
            console.log('Saved Item Name Map JSON.');

    });

});





