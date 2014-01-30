//we need to fetch information from the database and answer our questions
//we store this information inside of redis for quick access

var fs = require('fs');
var path = require('path');
//reads large files line by line
var byline = require('byline');
//This is a flow library for handling async tasks sequentially
var Q = require('q');

//we have our mongodb interaction library -- defines schema
var mongoose = require('mongoose');

//turn a JSON file into a database schema. Shweet.
var Generator = require('../DBParser/generator.js');
var Maps = require("../IDMapping/IDMapLoader.js");

//using redis for quick insert/retrieval of batch info
var redis = require("redis")
    , client = redis.createClient();

var metaInformationKey = "regionInformation";
var metaAllItemsKey = "allItemsList";
var finalSchemaName = "finalSchema";
var regionSchemaName = "regionSchema";

var lastKnownDBId = 10;
var arbitrageDBId = 11;

module.exports = D3DataCreator;

function numberWithCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

var qConnectMongoose = function()
{
    var deferred = Q.defer();
    // connect to Mongo when the app initializes
    var mongooseConnection = mongoose.createConnection('mongodb://localhost/eve');

    mongooseConnection.on('error', function(e)
    {
        console.log('Mongoose connection error');
        console.log(e);
//        console.error.bind(console, 'connection error:');
        deferred.reject(e);

    });

    mongooseConnection.on('open', function(){
        deferred.resolve(mongooseConnection);
    });

    return deferred.promise;
};


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

function D3DataCreator()
{
    var self = this;

    //we need to create our object
    self.generator = new Generator();
    self.redisClient = client;



    self.qRedisMultiGet = function(keys)
    {
        var deferred = Q.defer();

        self.redisClient.mget(keys, function(err, values)
        {
            if(err)
            {
                deferred.reject(err);
                return;
            }

            var mappedValues = {};
            for(var i=0; i < values.length; i++)
            {
                if(values[i])
                    mappedValues[keys[i]] = JSON.parse(values[i]);
            }

            deferred.resolve(mappedValues);
        });

        return deferred.promise;
    };

    self.qRedisGetObject = function(key)
    {
        var deferred = Q.defer();

        self.redisClient.get(key, function(err, value)
        {
            if(err)
            {
                deferred.reject(err);
                return;
            }

            deferred.resolve(JSON.parse(value));
        });

        return deferred.promise;
    };

    self.qRedisMultiSetObject = function(setDictionary)
    {
        var deferred = Q.defer();

        var mSetList = [];
        for(var key in setDictionary)
        {
            var value = setDictionary[key];
            if(typeof value != "string")
                value = JSON.stringify(value);

            mSetList.push(key);
            mSetList.push(value);
        }



        self.redisClient.mset(mSetList, function(err)
        {
            if(err)
            {
                deferred.reject(err);
                return;
            }

            deferred.resolve();
        });

        return deferred.promise;
    };

    self.qFlushRedisDB = function(dbVar)
    {
        var deferred = Q.defer();

        self.qRedisSwitchDB(dbVar)
            .done(function()
            {
                self.redisClient.FLUSHDB(function(err)
                {
                    console.log("Flush returned! ", err);
                    if(err)
                        deferred.reject(err);
                    else
                        deferred.resolve();
                });
            }, function(err)
            {
                deferred.reject(err);
            });

        return deferred.promise;
    };

    self.qRedisSwitchDB = function(dbVar)
    {
        var deferred = Q.defer();

        self.redisClient.select(dbVar, function(err,res){
            // you'll want to check that the select was successful here
            // if(err) return err;
            if(err)
                deferred.reject(err);
            else
                deferred.resolve();
        });

        return deferred.promise;
    };
    self.qRedisSetObject = function(key, value)
    {
        var deferred = Q.defer();

        if(typeof value != "string")
            value = JSON.stringify(value);

        self.redisClient.set(key, value, function(err)
        {
            if(err)
            {
                deferred.reject(err);
                return;
            }

            deferred.resolve();
        });

        return deferred.promise;
    };
    self.qRedisSetArbitrage = function(key, arbitrageObject)
    {
        var deferred = Q.defer();

        self.qRedisSwitchDB(arbitrageDBId)
            .then(function()
            {
                return self.qRedisSetObject(key, arbitrageObject);
            })
            .done(function()
            {
                deferred.resolve();
            }, function(err)
            {
                deferred.reject(err);
            })

        return deferred.promise;
    };

    self.qGetAllMetaInfo = function()
    {
        var deferred = Q.defer();

        self.qGetMetaItemList()
            .then(function(itemList)
            {
                self.knownItems = itemList.knownMap;
                return self.qGetMetaList();
            })
            .done(function(regionInfo)
            {
                self.allRegions = regionInfo.keyList;

                deferred.resolve();
            },function(err){deferred.reject(err);})

        return deferred.promise;
    };

    self.qGetMetaItemList = function()
    {
        return self.qRedisSwitchDB(3)
            .then(function()
            {
                console.log('Fetching meta info about regions')

                return self.qRedisGetObject(metaAllItemsKey)
            });
    };

    self.qGetMetaList = function()
    {
        return self.qRedisSwitchDB(1)
            .then(function()
            {
                console.log('Fetching meta info about regions')

                return self.qRedisGetObject(metaInformationKey)
            });
    };

    self.qConnect = function()
    {
        var deferred = Q.defer();


        qConnectMongoose()
            .then(function(connection)
            {
                self.connection = connection;
                self.generator.setConnection(connection);

                return qReadFile(path.resolve(__dirname, "../DBFinal/finalSchema.json"));
            })
            .then(function(regionSchema)
            {
                //so we have read in the trade schema
                var comSchema = JSON.parse(regionSchema);

                //now we have the actual schema
                //we load it into our database as it's own database
                self.generator.loadSingleSchema(finalSchemaName, comSchema);

                //store the class inside the parser, we're ready to proceed with Dump parsing
                self.ItemModel = self.generator.getSchemaModel(finalSchemaName);

                return qReadFile(path.resolve(__dirname, "../DBSorting/sortSchema.json"));
            })
            .then(function(regionSchema)
            {
                //so we have read in the trade schema
                var comSchema = JSON.parse(regionSchema);

                //now we have the actual schema
                //we load it into our database as it's own database
                self.generator.loadSingleSchema(regionSchemaName, comSchema);

                //store the class inside the parser, we're ready to proceed with Dump parsing
                self.RegionModel = self.generator.getSchemaModel(regionSchemaName);

                return self.qGetAllMetaInfo();
            })
            .done(function()
            {
                var itemList = [];
                for(var key in self.knownItems)
                    itemList.push(key);

                //we grab the region list as the first thing to do
                self.itemList = itemList;
                deferred.resolve();

            }, function(err){
                //oops failed connect
                deferred.reject(err);
            });

        return deferred.promise;
    };

    self.qFetchMongoDB = function(Model, findParams)
    {

        var deferred= Q.defer();

        Model.find(findParams, function(err, docs)
        {
            if(err)
                deferred.reject(err);
            else
                deferred.resolve(docs);
        });

        return deferred.promise;

    }

    self.qSaveMongoDB = function(mObject)
    {
        var deferred= Q.defer();

        var rObject = new self.FinalModel(mObject);

        rObject.save(function(err)
        {
            if(err)
                deferred.reject(err);
            else
                deferred.resolve();
        });

        return deferred.promise;
    };

    self.qClearMongoDB = function()
    {
        var deferred= Q.defer();

        self.FinalModel.remove({}, function(err)
        {
            if(err)
                deferred.reject(err);
            else
                deferred.resolve();
        });

        return deferred.promise;
    };

    self.qCreateRegionData = function(startIx)
    {
        var deferred = Q.defer();

        var regionKey = self.allRegions[startIx];
        var failedFetch = false;

        //the region we're going to investigate!
        self.qRedisSwitchDB(4)
            .then(function()
            {
                //we need to get our infomration from mongo
                return self.qFetchMongoDB(self.RegionModel, {regionID: regionKey});
            })
            .then(function(fullRegion)
            {
                if(fullRegion.length == 0)
                {
                    failedFetch = true;
                    return;
                }

                var regionOfInterst = fullRegion[0];

                var sold = regionOfInterst.confirmedSold;
                var itemToSell = {};
                var itemToMinMaxSell = {};


                for(var i=0; i < sold.length; i++)
                {
                    var soldInfo = sold[i];
                    if(!itemToSell[soldInfo.itemID])
                    {
                        itemToSell[soldInfo.itemID] = [];
                    }
                    itemToSell[soldInfo.itemID].push(soldInfo);
                }

                for(var itemID in itemToSell)
                {
                    var itemList = itemToSell[itemID];

                    var soldItems = {min: "", max : ""};
                    var minMaxSold = {min: Number.MAX_VALUE, max: Number.MIN_VALUE};
                    var minMaxSoldQuantity = {min:0, max:0};

                    for(var i=0;  i < itemList.length; i++)
                    {
                        itemList[i].price = parseFloat(itemList[i].price);
                        itemList[i].quantity = parseFloat(itemList[i].quantity);

                        minMaxSold.min = Math.min(minMaxSold.min, itemList[i].price);
                        minMaxSold.max = Math.max(minMaxSold.max, itemList[i].price);

                        if(minMaxSold.min == itemList[i].price)
                        {
                            minMaxSoldQuantity.min = Math.max(minMaxSoldQuantity.min, itemList[i].quantity);
                            soldItems.min = itemList[i].itemID;
                        }

                        minMaxSold.max = Math.max(minMaxSold.max, itemList[i].price);
                        if(minMaxSold.max == itemList[i].price)
                        {
                            minMaxSoldQuantity.max = Math.max(minMaxSoldQuantity.max, itemList[i].quantity);
                            soldItems.max = itemList[i].itemID;
                        }
                    }

                    //adjust max quanitty to be equal to the largest min quanitty
                    //this is for profit calculations
                    minMaxSoldQuantity.max = Math.min(minMaxSoldQuantity.max, minMaxSoldQuantity.min);
                    minMaxSoldQuantity.min = minMaxSoldQuantity.max;//Math.min(minMaxSoldQuantity.max, minMaxSoldQuantity.min);

//                    console.log(minMaxSoldQuantity);

                    itemToMinMaxSell[itemID] = {price: minMaxSold, quantity: minMaxSoldQuantity};
                }

                var bought = regionOfInterst.confirmedBought;
                var itemToBuy = {};
                var itemToMinMaxBuy = {};

                for(var i=0; i < bought.length; i++)
                {
                    var buyInfo = bought[i];
                    if(!itemToBuy[buyInfo.itemID])
                    {
                        itemToBuy[buyInfo.itemID] = [];
                    }
                    itemToBuy[buyInfo.itemID].push(buyInfo);
                }

                for(var itemID in itemToBuy)
                {
                    var itemList = itemToBuy[itemID];

                    var boughtItems = {min: "", max : ""};
                    var minMaxBought = {min: Number.MAX_VALUE, max: Number.MIN_VALUE};
                    var minMaxBoughtQuantity = {min:0, max:0};

                    for(var i=0;  i < itemList.length; i++)
                    {
                        itemList[i].price = parseFloat(itemList[i].price);
                        itemList[i].quantity = parseFloat(itemList[i].quantity);

                        minMaxBought.min = Math.min(minMaxBought.min, itemList[i].price);

                        if(minMaxBought.min == itemList[i].price)
                        {
                            minMaxBoughtQuantity.min = Math.max(minMaxBoughtQuantity.min, itemList[i].quantity);
                            boughtItems.min = itemList[i].itemID;
                        }

                        minMaxBought.max = Math.max(minMaxBought.max, itemList[i].price);
                        if(minMaxBought.max == itemList[i].price)
                        {
                            minMaxBoughtQuantity.max = Math.max(minMaxBoughtQuantity.max, itemList[i].quantity);
                            boughtItems.max = itemList[i].itemID;
                        }
                    }


                    itemToMinMaxBuy[itemID] = {price: minMaxBought, quantity: minMaxBoughtQuantity};
                }

//                console.log("Item Buy info: ", itemToMinMaxBuy)
//                console.log("Item Sell info: ", itemToMinMaxSell);

                var profitSort = [];

                for(var itemID in itemToMinMaxSell)
                {
                    var price = itemToMinMaxSell[itemID].price;
                    var quantity = itemToMinMaxSell[itemID].quantity;
                    var profitRatio = price.max/price.min;
                    var totalProfit = price.max*quantity.max - price.min*quantity.min;
                    var maxCanBuy = quantity.min;
                    var maxCanSell = quantity.max;

                    profitSort.push({
                        item: Maps.GlobalMaps.ItemIDToName[itemID],
                        itemID: itemID,
                        topPrice: price.max,
                        lowPrice: price.min,
                        profitRatio: profitRatio,
                        totalProfit: totalProfit,
                        maxCanBuy: maxCanBuy,
                        maxCanSell: maxCanSell
                    });
                }


//                profitSort.sort(function(a,b){return b.profitRatio - a.profitRatio;});
                profitSort.sort(function(a,b){return b.totalProfit - a.totalProfit;});

                console.log("Top ten most proiftable: ", profitSort.slice(0,10));

                //okay so now we have information
//                console.log("Minmax sold: ", minMaxSold, "of ", soldItems, " @ ", minMaxSoldQuantity);
//                console.log("Minmax bought: ", minMaxBought, "of ", boughtItems, " @ ", minMaxBoughtQuantity, " \n \n");
//                console.log("Fetched region info: ", fullRegion[0])

            })
            .done(function()
            {
                if(failedFetch)
                    console.log('No known region info for : ', regionKey);

                deferred.resolve();
            },function(err)
            {
                deferred.reject(err);
            })

        return deferred.promise;
    };

    self.qRedisKeySearch = function(keySearch)
    {

        var d2= Q.defer();


//                self.redisClient.keys(regionKey + "_*", function(err, vals)
        self.redisClient.keys(keySearch, function(err, regionItemKeys)
        {
            if(err)
                d2.reject(err);
            else
            {
                d2.resolve(regionItemKeys);
            }
        });
        return d2.promise;
    };

    self.qCreateItemData = function(startIx)
    {
        var deferred = Q.defer();

        var itemID = self.itemList[startIx];

        var noKeysFound = false;

        //Search for raw item information,
        //look for arbitrage!!!!
//        self.qRedisSwitchDB(lastKnownDBId)
//        self.qFlushRedisDB(arbitrageDBId)
//            .then(function()
//            {
//               return self.qRedisSwitchDB(lastKnownDBId);
//            })
        self.qRedisSwitchDB(lastKnownDBId)
            .then(function()
            {
                //we need to get our infomration from mongo
                return self.qRedisKeySearch("*|"+itemID);
            })
            .then(function(allRedisKeys)
            {
                //this is stationID|itemID
                //we need to fetch these from redis -- as long as the item keys match
                var verifiedKeys = [];
                for(var i=0; i < allRedisKeys.length; i++)
                {
                    if(allRedisKeys[i].split("|")[1] == itemID)
                    {
                        verifiedKeys.push(allRedisKeys[i]);
                    }
                }

                if(verifiedKeys.length == 0){
                    noKeysFound = true;
                }
                else
                    return self.qRedisMultiGet(verifiedKeys);
            })
            .then(function(redisObjects)
            {
//                console.log(redisObjects);
                if(noKeysFound)
                    return;

                var bidOrders = [];
                var askOrders = [];

                for(var key in redisObjects)
                {
                    var obj = redisObjects[key];

                    for(var i=0; i < obj.trades.length; i++)
                    {
                        var trade = obj.trades[i];

                        if(trade.bid == "1")
                            bidOrders.push(trade);
                        else
                            askOrders.push(trade);
                    }
                }

                //descending buys
                bidOrders.sort(function(a,b){return b.price - a.price;});

                //ascending sells
                askOrders.sort(function(a,b){return a.price - b.price;});


                var lastCount = 0;

                var bidIx = 0;
                var aIx = 0;

                var worthIt = true;

                //total possible arbitrage
                var arbitrageOpportunity = 0;

                var printDetails = itemID == "16273";

//                console.log(bidOrders.slice(0, 5))
//                console.log(askOrders.slice(0, 5))

//                if(bidOrders.length)
//                console.log(bidOrders[0].price)
//                if(askOrders.length)
//                console.log(askOrders[0].price)

                if(printDetails)
                {
                    console.log("Def Asks: ", askOrders.slice(0,5));
                    console.log("Def Bids: ",bidOrders.slice(0,5));
                }

                while(aIx < askOrders.length && bidIx < bidOrders.length && worthIt)
                {
                    var aOrder = askOrders[aIx];
                    var bOrder = bidOrders[bidIx];

                    var aPrice = parseFloat(aOrder.price);
                    var bPrice = parseFloat(bOrder.price);

                    var aQuant = parseInt(aOrder.volRemain);
                    var bQuant = parseInt(bOrder.volRemain);

                    aOrder.price = aPrice;
                    bOrder.price = bPrice;
                    aOrder.volRemain = aQuant;
                    bOrder.volRemain = bQuant;

                    if(bPrice > aPrice)
                    {
                        //This is how much we can sell at that rate
                        var sellAmount = Math.min(aQuant, bQuant);

                        aOrder.volRemain -= sellAmount;
                        bOrder.volRemain -= sellAmount;

                        arbitrageOpportunity += (bPrice - aPrice)*sellAmount;

                        if(aOrder.volRemain == 0)
                            aIx++;
                        if(bOrder.volRemain == 0)
                            bidIx++;

                    }
                    //if that's not the case anymore, it's not worth it
                    else
                        worthIt = false;

                    //hard limit at collecting from 5 locations -- pickup or delivery
                    if(aIx > 5 || bidIx > 5)
                        worthIt = false;

                }
                if(printDetails)
                {
                    console.log("A Ix: " , aIx, " b Ix", bidIx);
                    console.log(askOrders[aIx]);
                    console.log(bidOrders[bidIx]);
                }
                //if we have any counts, then there is a chance for arbitrage


//                console.log('All Bid: ', bidOrders.length, " all ask: ", askOrders.length);
                console.log('Arbitrage opportunity for ',
                    Maps.GlobalMaps.ItemIDToName[itemID],
                    " @ ", numberWithCommas(arbitrageOpportunity.toFixed(2)));

                var arbitrage = {total: numberWithCommas(arbitrageOpportunity.toFixed(2)),
                    itemID: itemID,
                    itemName: Maps.GlobalMaps.ItemIDToName[itemID],
                    bid:  bidOrders.length,
                    ask: askOrders.length
                };

                if(arbitrageOpportunity >0)
                {
//                    console.log("Recorded arbitrage!", arbitrage);
                    return self.qRedisSetArbitrage(itemID, arbitrage);
                }
                else
                    console.log('No arbitrage: ', itemID);

            })
            .done(function()
            {
                deferred.resolve();
            },function(err)
            {
                deferred.reject(err);
            });


        return deferred.promise;
    }



}

