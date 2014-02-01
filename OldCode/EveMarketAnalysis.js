var express = require('express')
    , http = require('http')
    , path = require('path');

var DataParser = require("./Parser/parseFile.js");


var buyFileTest = __dirname + "/SampleData/Buy1_4_14.txt";
var sellFileTest = __dirname + "/SampleData/Sell1_4_14.txt";


var createExpressServer = function(callback)
{

    var options =  {port: 3000};

//create an express app
    var app = express();

    app.use(express.errorHandler());
    app.use(express.cookieParser());
    app.use(express.bodyParser());

    app.use('/html', express.static(path.join(__dirname, 'Visual')));

    //    Origin, X-Requested-With, Content-Type, Accept
    //need cross domain origin for us!
    app.use(function(req, res, next) {
        console.log('Handling cross');
        res.header("Access-Control-Allow-Origin", "*");
        res.header("Access-Control-Allow-Methods", 'OPTIONS, POST, GET, PUT, DELETE');
        res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
        next();
    });

    app.options('*', function(req,res)
    {
        res.send(200);
    });

    var server = http.createServer(app);

//we listen for the server, as soon as we access the database object
    server.listen(options.port || 80, function(){
        console.log("Express server listening on port " + options.port || 80);

        //finished, no error
        callback(null, app);
    });
};


var parser = new DataParser(buyFileTest, sellFileTest, function(err)
{
    if(err)
    {
        //do nothing
        return;
    }
    //we have our parser completed

    console.log('Parse complete!');

    createExpressServer(function(err, app)
    {
        //home page
        app.get('/d3BestData', function(req,res)
        {
            console.log('Sending D3 Data from parser');

            res.send({
//                    difference: parser.differenceMap,
//                    differenceRegion: parser.differenceRegionMap,
                    compiledBestData : parser.compiledBestData,
                    sortedBuyRegions: parser.sortedBuyRegions,
                    sortedSellRegions: parser.sortedSellRegions
                });
        });

    });
});

