var fs = require('fs');
var path =require('path');
module.exports = DumpParser;
var byline = require('byline');

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

function DumpParser(file, uniqueName, finished)
{
//  var p =  path.resolve(__dirname)
//    console.log(p)

    var itemIDMap, stationIDMap;

    fs.readFile(path.resolve("Z:/Eve/Data/stationMap.json"), function(err, data)
    {
        stationIDMap = JSON.parse(data);

        fs.readFile(path.resolve("Z:/Eve/Data/itemNameMap.json"), function(err, data)
        {
            itemIDMap = JSON.parse(data);

            file= path.resolve(file);
            console.log('Checking file: ', path.resolve(file))

            var checkCount = 1;
            var cnt = 0;
            var lineCount = 0;

            var uniqueOrders = {};
            var uniqueCount = 0;

            var confirmedPurchases = {};
            var confirmedPurchasesByStation = {};

            var confirmedSales = {};
            var confirmedSalesByStation = {};

            var stream = fs.createReadStream(file);
            stream = byline.createStream(stream);

            stream.on('data', function(line) {

                if(lineCount > 0)
                {
                    if(lineCount%10000 == 1)
                        console.log("At line: " + lineCount);

                    //get rid of any quotation marks
                    var delimited = line.toString().replace("\r", "").replace(/"/g, "").split(",");

                    var ixBuyOrder = 0;
                    var ixStationID = 3;
                    var ixName = 4;
                    var ixBuySell = 5;
                    var ixPrice = 6;
                    var ixRemaining = 8;

                    var oID = delimited[ixBuyOrder];

                    if(!uniqueOrders[oID])
                    {
                        uniqueCount++;
                        uniqueOrders[oID] = delimited;
                    }
                    else
                    {
                        //let's check if any buying or selling happened

                        if(uniqueOrders[oID][ixRemaining] != delimited[ixRemaining])
                        {
                            var itemID = delimited[ixName];
                            var stationID = delimited[ixStationID];
                            var pricePU = parseFloat(delimited[ixPrice]);
                            var quanitityDifference = parseInt(uniqueOrders[oID][ixRemaining]) - parseInt(delimited[ixRemaining]);

                            var itemName = itemIDMap[itemID];
                            var stationName = stationIDMap[stationID];

                            if(!stationName)
                                console.log("Cant find: " + stationID + "...skipping.");

                            //don't look at antyhign we don't know the name for
                            if(quanitityDifference > 0 && stationName)
                            {

                                if(uniqueOrders[oID][ixBuySell] == "1")
                                {
                                    //a buy order has changed quantities!
                                    //this confirms a PURCHASE at that price
                                    //someone actually fulfilled part of a buy order


                                    if(!confirmedPurchases[itemName])
                                        confirmedPurchases[itemName] = {};

                                    if(!confirmedPurchases[itemName][stationName])
                                        confirmedPurchases[itemName][stationName] = [];


                                    var purchaseData =    {
                                        item: itemName,
                                        station: stationName,
                                        quantity: quanitityDifference,
                                        pricePerUnit: pricePU,
                                        total: quanitityDifference*pricePU,
                                        fullData: delimited
                                    };


                                    confirmedPurchases[itemName][stationName].push(
                                        purchaseData
                                    );

                                    if(!confirmedPurchasesByStation[stationName])
                                        confirmedPurchasesByStation[stationName] = {};
                                    if(!confirmedPurchasesByStation[stationName][itemName])
                                        confirmedPurchasesByStation[stationName][itemName] = [];

                                    confirmedPurchasesByStation[stationName][itemName].push(purchaseData);


                                }
                                else
                                {
                                    //otherwise, an item has been sold to someone else
                                    //this is a confirmed sale
                                    //someone actually offloaded some item

                                    if(!confirmedSales[itemName])
                                        confirmedSales[itemName] = {};

                                    if(!confirmedSales[itemName][stationName])
                                        confirmedSales[itemName][stationName] = [];


                                    var saleData =    {
                                        item: itemName,
                                        station: stationName,
                                        quantity: quanitityDifference,
                                        pricePerUnit: pricePU,
                                        total: quanitityDifference*pricePU,
                                        fullData: delimited
                                    };

                                    confirmedSales[itemName][stationName].push(saleData);

                                    if(!confirmedSalesByStation[stationName])
                                        confirmedSalesByStation[stationName] = {};
                                    if(!confirmedSalesByStation[stationName][itemName])
                                        confirmedSalesByStation[stationName][itemName] = [];

                                    confirmedSalesByStation[stationName][itemName].push(saleData);

            //                        console.log('Sell recorded!');
                                }

                                    //we only update if the difference is greater than zero (otherwise, we could be updating an out of order change
                                    uniqueOrders[oID] = delimited;

                            }



                        }
                    }
                }

                //....
                //When done set the line back to empty, and resume the strem
                lineCount++;
            });
            stream.addListener('end', function()
            {
                //save unique orders as json file

                console.log('End stream!');
                console.log("Line count: ", lineCount, " Unique: ", uniqueCount);

                fs.writeFile(path.resolve("Z:/Eve/Data/" + uniqueName + "_purchaseData.json"),
                    JSON.stringify({purchaseData: confirmedPurchases, purchaseDataByStation: confirmedPurchasesByStation}),
                    function(err)
                {
                    if(err)
                        console.log('Failed write purchase data');
                    else
                        console.log('Saved confirmed purchase data.');

                });


                fs.writeFile(path.resolve("Z:/Eve/Data/" + uniqueName + "_saleData.json"),
                    JSON.stringify({saleData: confirmedSales, saleDataByStation: confirmedSalesByStation}),
                    function(err)
                {
                    if(err)
                        console.log('Failed write sale data');
                    else
                        console.log('Saved confirmed sale data.');

                });

                fs.writeFile(path.resolve("Z:/Eve/Data/" + uniqueName + ".json"), JSON.stringify(uniqueOrders), function(err)
                {
                    if(err)
                        console.log('Failed write unique');
                    else
                        console.log('Saved Unique.');

                });





            });
        });
    });
}


//Create the JSON parser for going more in depth on unique objects
DumpParser.JSONParser = function(file)
{
    file= path.resolve(file);
    console.log('Checking JSON file: ', path.resolve(file))

    var uniqueBuyOrders = {};
    var uniqueSellOrders = {};

    var itemIDMap, stationIDMap, uniqueLoadedOrders;


    fs.readFile(path.resolve("Z:/Eve/Data/stationMap.json"), function(err, data)
    {

        stationIDMap = JSON.parse(data);

        fs.readFile(path.resolve("Z:/Eve/Data/itemNameMap.json"), function(err, data)
        {
            itemIDMap = JSON.parse(data);


            //now we have our item mapping abilities, we need to make a big pull


//            for(var x in stationIDMap)
//            {
//                console.log(typeof x);
//                break;
//            }
//            console.log(stationIDMap["60008494"]);
//
//            console.log(itemIDMap["36"]);


            fs.readFile(file, function(err, data)
            {
                uniqueLoadedOrders = JSON.parse(data);


//                console.log(stationIDMap);
//                console.log(itemIDMap);


                var ixBuyOrder = 0;
                var ixStationID = 3;
                var ixNameID = 4;
                var ixBuySell = 5;
                var ixPrice = 6;

                for(var orders in uniqueLoadedOrders)
                {
                    var orderData = uniqueLoadedOrders[orders];

                    //remove quotations from everything
                    for(var i=0; i < orderData.length; i++)
                        orderData[i] = orderData[i].replace(/"/g, "");

                    var itemID = orderData[ixNameID];
                    var stationID =  orderData[ixStationID];

//                    console.log(typeof itemID);
//                    console.log('ItemID: ' + itemID + " station: " + stationID);

                    var itemName = itemIDMap[itemID];
                    var stationName = stationIDMap[stationID];
//                    var price =

                    if(orderData[ixBuySell] == "1" )
                        console.log(orderData);





//                    console.log("Station: " + stationName + " item: " + itemName + " price: " + orderData[ixPrice] + (orderData[ixBuySell] == "1" ? " Buy" : " Sell"));



                }

//                console.log(uniqueLoadedOrders);



//                var oID = delimited[ixBuyOrder];
//
//                if(!uniqueOrders[oID])
//                {
//                    uniqueCount++;
//                    uniqueOrders[oID] = delimited;
//                }
            });






        });
    });


};


//
//    var stream = fs.createReadStream(file, {
//            flags: 'r',
//            encoding: 'utf-8',
//            fd: null,
//            bufferSize: 1
//        }),
//        line ='';
//
//    stream.addListener('end', function()
//    {
//        //save unique orders as json file
//
//        console.log('End stream!');
//        console.log("Line count: ", lineCount, " Unique: ", uniqueCount);
//        fs.writeFile(path.resolve("Z:/Eve/Data/uniqueDump1_24.json"), JSON.stringify(uniqueOrders), function(err)
//        {
//            if(err)
//                console.log('Failed write unique');
//            else
//                console.log('Saved Unique.');
//
//        });
//
//    });
//
//    //start reading the file
//    stream.addListener('data', function (char) {
//        // pause stream if a newline char is found
//        stream.pause();
//        if(char == '\n'){
//            (function(){
//                //do whatever you want to do with the line here.
//
////                if(cnt++ < checkCount)
////                {
////                    console.log(line);
//
//
////                    console.log(delimited)
//
//                    if(lineCount > 0)
//                    {
//
//                        if(lineCount%1000 == 1)
//                            console.log("At line: " + lineCount);
//
//                        var delimited = line.replace("\r", "").split(",");
//
//                        var ixBuyOrder = 0;
//                        var ixStationID = 3;
//                        var ixName = 4;
//                        var ixBuySell = 5;
//                        var ixPrice = 6;
//
//                        var oID = delimited[ixBuyOrder];
//
//                        if(!uniqueOrders[oID])
//                        {
//                            uniqueCount++;
//                            uniqueOrders[oID] = delimited;
//                        }
//                    }
////                }
////                else
////                    return;
//
//                //....
//                //When done set the line back to empty, and resume the strem
//                line = '';
//                lineCount++;
//                stream.resume();
//            })();
//        }
//        else{
//            //add the new char to the current line
//            line += char;
//            stream.resume();
//        }
//    })




