var fs = require("fs");

module.exports = DailyDataParser;


function DailyDataParser(buyFile, sellFile, finishParse)
{

    var self = this;

    fs.readFile(buyFile, function(err, buyData)
    {
        if(err){
            finishParse(err);
            return;
        }

        fs.readFile(sellFile, function(err, sellData)
        {
            if(err){
                finishParse(err);
                return;
            }

            var buyString = buyData.toString();
            var sellString = sellData.toString();

            //we have buy and sell data, now we need to parse it
            var buyLines = buyString.split("\n");
            var sellLines = sellString.split("\n");

            self.buyLines = buyLines;
            self.sellLines = sellLines;

            console.log(buyLines.length);
            console.log(buyLines[0]);


            var locationBuyCount = {};
            var locationSellCount = {};

            var buyPriceMap = {};
            var sellPriceMap = {};
            var differencePriceMap = {};
            var differenceRegionMap = {};

            for(var b=1; b < buyLines.length; b++)
            {
                var item = buyLines[b];
                item = item.replace("\r", "");
                var splitItem = item.split(",");
                var location = splitItem[1];

                if(locationBuyCount[location] == undefined)
                    locationBuyCount[location] = 0;

                locationBuyCount[location]++;

                //grab the name, and then set the mapping equal to price
                //we'll deal with location later
                var buyPriceRegion = buyPriceMap[splitItem[0]];
                if(!buyPriceRegion)
                {
                    buyPriceRegion = {};
                    buyPriceMap[splitItem[0]] = buyPriceRegion;
                }
                var buyPriceList = buyPriceRegion[location];
                if(!buyPriceList)
                {
                    buyPriceList = [];
                    buyPriceRegion[location] = buyPriceList
                }

                buyPriceList.push(parseFloat(splitItem[2]));
            }

            for(var s=1; s < buyLines.length; s++)
            {
                var item = sellLines[s];
                item = item.replace("\r", "");
                var splitItem = item.split(",");

                //handle some sense of location for now
                var location = splitItem[1];
                if(locationSellCount[location] == undefined)
                    locationSellCount[location] = 0;
                locationSellCount[location]++;

                //grab the name, and then set the mapping equal to price
                //we'll deal with location later

                //first we check if we have a list yet or not

                var sellPriceRegion = sellPriceMap[splitItem[0]];
                if(!sellPriceRegion)
                {
                    sellPriceRegion = {};
                    sellPriceMap[splitItem[0]] = sellPriceRegion;
                }
                var sellPriceList = sellPriceRegion[location];
                if(!sellPriceList)
                {
                    //if we don't, make a new list and store it
                    sellPriceList = [];
                    sellPriceRegion[location] = sellPriceList;
                }
                //push our price onto the list for this item
                sellPriceList.push(parseFloat(splitItem[2]));
            }


            var largerMap = buyLines.length > sellLines.length ? buyPriceMap : sellPriceMap;

            for(var item in largerMap)
            {
                for(var region in largerMap[item])
                {
                    //get the buy item, making sure the item exists
                    var buyItemData = buyPriceMap[item] ? buyPriceMap[item][region] : undefined;
                    var sellItemData = sellPriceMap[item] ? sellPriceMap[item][region] : undefined;


                    //if we have buy and sell information, let's sum up
                    if(buyItemData && sellItemData)
                    {
                        //sort buys descending
                        buyItemData.sort(function(a,b){return b-a});
                        //sort sells ascending
                        sellItemData.sort();

                        //the highest buying and the lowest selling is WORST case scenario

                        //choose the smaller of the two
                        var shortest = Math.min(buyItemData.length, sellItemData.length);

                        var worstCaseProfit = 0;
                        for(var m=0; m < shortest; m++)
                        {
                            worstCaseProfit += (sellItemData[m] - buyItemData[m]);
                        }

                        var totalBought = self.sumList(buyItemData);
                        var totalSold = self.sumList(sellItemData);

                        if(!differencePriceMap[item])
                            differencePriceMap[item] = {};

                        differencePriceMap[item][region] =
                        {
                            wcProfit: worstCaseProfit,
                            wcAverageMarkup: worstCaseProfit/shortest,
                            totalBought: totalBought,
                            totalSold: totalSold,
                            quantityEqual: shortest,
                            quantityBought: buyItemData.length,
                            quantitySold: sellItemData.length
                        };

                        if(!differenceRegionMap[region])
                        {
                            differenceRegionMap[region] = {};
                        }
                        differenceRegionMap[region][item] = differencePriceMap[item][region];




                    }
                }
            }


            var sortedBuy = [];
            for (var market in locationBuyCount)
                sortedBuy.push([market, locationBuyCount[market]])
            sortedBuy.sort(function(a, b) {return b[1] - a[1]})


            console.log("Locations purchased number: ", sortedBuy);


            var sortedSell = [];
            for (var market in locationSellCount)
                sortedSell.push([market, locationSellCount[market]])
            sortedSell.sort(function(a, b) {return b[1] - a[1]})

            console.log("Locations Sold number: ", sortedSell);

//            console.log("Difference maps: ", differencePriceMap)

            var bestBuyRegion = sortedBuy[0][0];

            var bestItemsInRegion = [];
            for(var itemName in differencePriceMap)
            {
                var existingItem = differencePriceMap[itemName][bestBuyRegion];
                if(existingItem)
                {
                    bestItemsInRegion.push([itemName, existingItem]);
                }
            }

            bestItemsInRegion.sort(function(a, b) {return b[1].wcProfit - a[1].wcProfit});



            console.log("Top ten items in ", bestBuyRegion, " items: ", bestItemsInRegion.slice(0,10));


            self.sortedBuyRegions = sortedBuy;
            self.sortedSellRegions = sortedSell;
            self.differenceMap = differencePriceMap;
            self.differenceRegionMap = differenceRegionMap;

            var topBuyMarket = sortedBuy.slice(0,6);

            var data = [];
            for(var i=0; i < topBuyMarket.length; i++)
            {
                var region  = topBuyMarket[i][0];
                //now check our differenceMap for this region

                var itemsInRegion = differenceRegionMap[region];

                for(var item in itemsInRegion)
                {
                    var itemData = itemsInRegion[item];
                    //we don't care about money losing items
                    if(itemData.wcProfit > 0)
                        data.push({region: region, item: item, data: itemData});
                }
            }

//        console.log(data.length);

            data.sort(function(a,b){ return b.data.wcProfit - a.data.wcProfit;});

            var topOfEachCount = {};

            //needs to be a parameter in the future
            var maxBestRegionItems = 50;

            var compiledBestData = [];

            for(var i=0; i < data.length; i++)
            {
                var elem = data[i];
                if(topOfEachCount[elem.region] == undefined)
                    topOfEachCount[elem.region] = 0;

                if(topOfEachCount[elem.region] < maxBestRegionItems)
                {
                    compiledBestData.push(elem);
                    topOfEachCount[elem.region]++;
                }
            }

//            console.log(topBuyMarket);
//        console.log(data);
//            console.log(compiledBestData);

            self.compiledBestData = compiledBestData;

            finishParse();
        });
    });
}


DailyDataParser.prototype.sumList = function(list)
{

    var sum = 0;
    for(var i=0; i < list.length; i++)
        sum += list[i];

    return sum;
}

