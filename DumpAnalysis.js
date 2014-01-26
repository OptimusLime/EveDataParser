var express = require('express')
    , http = require('http')
    , path = require('path');

var DumpParser = require("./Parser/parseDumpFile.js");
var DumpParserJSON = DumpParser.JSONParser;

var CompileParser = require("./Parser/compileParseData.js");


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


var compParser = new CompileParser("Z:/Eve/Data/uniqueDump1_24_purchaseData.json", "Z:/Eve/Data/uniqueDump1_24_saleData.json", function()
{


});

//var parser = new DumpParser(
//    "Z:/Eve/Data/2014-01-24.dump",
//    "uniqueDump1_24",
//    function()
//{
//    //done
//
//
//
//
//});



//var fs = require('fs'), byline = require('byline'), path =require('path');
//
//
//var stream = fs.createReadStream(path.resolve("Z:/Eve/Data/2014-01-24.dump"));;
//stream = byline.createStream(stream);
//
//var cnt = 0;
//stream.on('data', function(line) {
//    if(!cnt++)
//        console.log(line.toString());
//});

//
//var uniqueParser = new DumpParserJSON("Z:/Eve/Data/uniqueDump1_24.json",function()
//{
//
//});






//{
//    if(err)
//    {
//        //do nothing
//        return;
//    }
//    //we have our parser completed
//
//    console.log('Parse complete!');
//
//    createExpressServer(function(err, app)
//    {
//        //home page
//        app.get('/d3BestData', function(req,res)
//        {
//            console.log('Sending D3 Data from parser');
//
//            res.send({
////                    difference: parser.differenceMap,
////                    differenceRegion: parser.differenceRegionMap,
//                    compiledBestData : parser.compiledBestData,
//                    sortedBuyRegions: parser.sortedBuyRegions,
//                    sortedSellRegions: parser.sortedSellRegions
//                });
//        });
//
//    });
//});
//
