//this file will run our stages on a DUMP file -- through and through -- or single stages
var Stage0Class = require("./Stages/Stage0Trades.js");
var Stage1Class = require("./Stages/Stage1Count.js");
var Stage2Class = require("./Stages/Stage2Algorithm.js");
var QFunctions = require("./Globals/qFunctions.js");


var currentStage = 0;
var allStages =
    [
        new Stage0Class(),
        new Stage1Class(),
        new Stage2Class()
    ];
var stageCount = allStages.length;

var stageInput =
    [
        ["Z:/Eve/Data/2014-01-30.dump"],
        ["Z:/Eve/Data/2014-01-30.dump"],
        ["Z:/Eve/Data/2014-01-30.dump"]
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
            if(currentStage == stageCount)
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
        runSelectStages(0,1);


    });













