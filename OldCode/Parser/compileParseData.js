var fs = require("fs");

module.exports = CompileParser;


var flattenList = function(listData)
{
    var flattened = [];
    for(var i=0; i < listData.length; i++)
    {
        var data = listData[i];
        for(var r=0; r < data.quantity; r++)
            flattened.push(data.pricePerUnit);
    }

    return flattened;
};

var flatQuantity = function(listData)
{
    var quantity = 0;
    for(var i=0; i < listData.length; i++)
    {
        var data = listData[i];
        quantity += data.quantity;
    }

    return quantity;
};
var sumDataList = function(list)
{

    var sum = 0;
    for(var i=0; i < list.length; i++)
        sum += list[i].quantity*list[i].pricePerUnit;

    return sum;
};
//uniqueDump1_24_purchaseData.json
function CompileParser(purchaseFile, saleFile, finished)
{
    var self = this;

    fs.readFile(purchaseFile, function(err, purchaseBuffer)
    {
        if(err){
            finished(err);
            return;
        }

        fs.readFile(saleFile, function(err, saleBuffer)
        {
            if(err){
                finished(err);
                return;
            }

            var purchaseJSON = JSON.parse(purchaseBuffer);
            var saleJSON = JSON.parse(saleBuffer);

            //now let's pull out our useful info

            var purchaseByItem = purchaseJSON.purchaseData;
            var purchaseByStation = purchaseJSON.purchaseDataByStation;
            //now we can quickly index into our objects this way
            var saleByItem = saleJSON.saleData;
            var saleByStation = saleJSON.saleDataByStation;

            var differencePriceMap = {};
            var differenceRegionMap = {};

            var sCount = 0;
            for(var station in saleByStation)
                sCount++;

            var pCount = 0;

            for(var station in purchaseByStation)
                pCount++;

            var stationMap = (sCount > pCount ? saleByStation : purchaseByStation);

            var stationInfo = {};

            for(var stationName in stationMap)
            {
                //don't investigate a station unless both purchase and sale have seen it!
                if(!purchaseByStation[stationName] && saleByStation[stationName])
                    continue;

                if(stationName)

                console.log("Investigatin: " + stationName)

                if(stationInfo[stationName] == undefined)
                {
                    stationInfo[stationName] = {wcProfitTotal: 0, boughtValueTotal:0, soldValueTotal:0};
                }
                sCount = 0;
                for(var station in saleByStation[stationName])
                    sCount++;

                pCount = 0;
                for(var station in purchaseByStation[stationName])
                    pCount++;

                var itemMap =  (sCount > pCount ? saleByStation[stationName] : purchaseByStation[stationName]);

                for(var itemName in itemMap)
                {
//                    console.log(itemName);
                    var purchaseList = purchaseByStation[stationName][itemName];
                    var saleList = saleByStation[stationName][itemName];

                    //we have overlapping lists for the same items!
                    if(purchaseList && saleList)
                    {
                        var flatPurchaseQuantity = flatQuantity(purchaseList);
                        var flatSaleQuantity = flatQuantity(saleList);

//                        console.log(flatPurchaseQuantity);
//                        console.log(flatSaleQuantity);

                        //now we have flat purchase and flat sales -- that is each object sold is a single sale at that price

                        //sort sales descending -- this is the MAX the item cost
                        saleList.sort(function(a,b){return b-a});
                        //sort purchases ascending, this is the min people bought for
                        purchaseList.sort();

                        //the highest buying and the lowest selling is WORST case scenario

                        //choose the smaller of the two
                        var shortest = Math.min(flatPurchaseQuantity, flatSaleQuantity);


                        var pQuant = shortest;
                        var tPurchases = 0;
                       for(var p=0; p < purchaseList.length; p++)
                       {
                           var amount = Math.min(purchaseList[p].quantity, pQuant);
                           tPurchases += amount*purchaseList[p].pricePerUnit;
                           pQuant -= amount;
                           if(pQuant ==0)
                            break;
                       }
                        var sQuant = shortest;
                        var tSales = 0;
                        for(var p=0; p < saleList.length; p++)
                        {
                            var amount = Math.min(saleList[p].quantity, sQuant);

                            tSales += amount*saleList[p].pricePerUnit;
                            sQuant -= amount;

                            if(sQuant ==0)
                                break;
                        }

                        //worst case, we have all our confirmed purchases minus the confirmed costs (worst case scenario)
                        var worstCaseProfit = (tSales - tPurchases);



                        var totalBought = sumDataList(saleList);
                        var totalSold = sumDataList(purchaseList);

                        //each station is measured by the sum of all the profit
                        //most profitable station is the winner!
                        stationInfo[stationName].wcProfitTotal += worstCaseProfit;
                        stationInfo[stationName].boughtValueTotal += totalBought;
                        stationInfo[stationName].soldValueTotal += totalSold;


                        if(!differencePriceMap[itemName])
                            differencePriceMap[itemName] = {};

                        differencePriceMap[itemName][stationName] =
                        {
                            wcProfit: worstCaseProfit,
                            wcAverageMarkup: worstCaseProfit/shortest,
                            totalBought: totalBought,
                            totalSold: totalSold,
                            quantityEqual: shortest,
                            quantityBought: flatSaleQuantity,
                            quantitySold: flatPurchaseQuantity
                        };

                        if(!differenceRegionMap[stationName])
                        {
                            differenceRegionMap[stationName] = {};
                        }
                        differenceRegionMap[stationName][itemName] = differencePriceMap[itemName][stationName];

                    }
                }
            }

            var profitSorted = [];
            for(var stationName in stationInfo)
                profitSorted.push([stationName, stationInfo[stationName]]);

            //sort by most profitable
//            profitSorted.sort(function(a,b){return (b[1].boughtValueTotal + b[1].soldValueTotal) - (a[1].boughtValueTotal + a[1].soldValueTotal)});
            profitSorted.sort(function(a,b){return (b[1].wcProfitTotal) - (a[1].wcProfitTotal)});

            //log the 10 most profitable stations!
            console.log(profitSorted.slice(0,10));

            //top 6 stations, and the top 50 items
            var mostProfitableStations = profitSorted.slice(0,6);

            //needs to be a parameter in the future
            var maxBestRegionItems = 50;

            var data = [];
            for(var i=0; i < mostProfitableStations.length; i++)
            {
                var station  = mostProfitableStations[i][0];
                //now check our differenceMap for this station

                var itemsInRegion = differenceRegionMap[station];

                for(var item in itemsInRegion)
                {
                    var itemData = itemsInRegion[item];
                    //we don't care about money losing items
                    if(itemData.wcProfit > 0)
                        data.push({station: station, item: item, data: itemData});
                }
            }

//        console.log(data.length);

//            data.sort(function(a,b){ return b.data.wcProfit - a.data.wcProfit;});
            data.sort(function(a,b){ return b.data.wcAverageMarkup - a.data.wcAverageMarkup;});

            var topOfEachCount = {};


            var compiledBestData = [];

            for(var i=0; i < data.length; i++)
            {
                var elem = data[i];
                if(topOfEachCount[elem.station] == undefined)
                    topOfEachCount[elem.station] = 0;

                if(topOfEachCount[elem.station] < maxBestRegionItems)
                {
                    compiledBestData.push(elem);
                    topOfEachCount[elem.station]++;
                }
            }

//            console.log(topBuyMarket);
//        console.log(data);
            console.log(compiledBestData);

            self.compiledBestData = compiledBestData;


        });
    });
}


