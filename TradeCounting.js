//this file will run our stages on a DUMP file -- through and through -- or single stages
var Stage0TradeClass = require("./Stages/Stage0Trades.js");
var Stage01Avg = require("./Stages/Stage01AvgTrade.js");
var QFunctions = require("./Globals/qFunctions.js");

var currentStage = 0;
var allStages =
    [
        new Stage0TradeClass(),
        new Stage01Avg()
    ];
var stageCount = allStages.length;

var stageInput =
    [
        ["/eveDB/tmpData/2014-01-31.dump"],
        []
    ];


var processNextStage = function()
{
    var nextStage = allStages[currentStage];
    var nextInput = stageInput[currentStage];

    nextStage.qPreProcessStage.apply(nextStage, nextInput)
        .then(function(preProcessInfo)
        {
            return nextStage.qProcessStage.apply(nextStage, arguments);
        })
        .then(function(processInfo)
        {
            return nextStage.qPostProcessStage.apply(nextStage, arguments);
        })
        .done(function()
        {
            currentStage++;
            if(currentStage >= stageCount)
            {
                console.log("Finished processing all ", stageCount, " stages!");
            }
            else
            {
                setTimeout(processNextStage, 0);
            }
        }, function(err)
        {
            console.log('Stage processing failed!');
            throw err;
        });
};

var runSelectStages = function(start, finish)
{

    currentStage = start;
    stageCount = finish;
    //Run through all the stages
    setTimeout(processNextStage, 0);
};


QFunctions.qConnect({},{})
    .then(function(){
        //only run select stages
        runSelectStages(1,2);
    });













