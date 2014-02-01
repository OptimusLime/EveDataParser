//Originally came from mongoose-gen
//modified to do more custom behavior for winjs

var util = require("util");
var traverse = require('traverse');

var mongoose = require('mongoose');
var Schema = mongoose.Schema;
var Mixed = mongoose.Types.Mixed;
var ObjectId = mongoose.Schema.ObjectId;

var is = function is (type, obj) {
    var clas = Object.prototype.toString.call(obj).slice(8, -1);
    return obj !== undefined && obj !== null && clas === type;
};

module.exports = function()
{
    //this is us!
    var genObject = this;

    //our connection object
    var connection;

    // functions hash
    var hash = {
        validator: {},
        setter: {},
        getter: {},
        default: {}
    };


    var set = function (param) {
        return function (key, value) {
            if (!is('Function', value)) throw new Error('expected type Function for '+value);
            if (!is('String', key)) throw new Error('expected type String for '+key);

            if (param === 'validator') hash.validator[key] = value;
            if (param === 'setter') hash.setter[key] = value;
            if (param === 'getter') hash.getter[key] = value;
            if (param === 'default') hash.default[key] = value;
        };
    };

    var getConnection = function()
    {
        return connection;
    };

    var setConnection = function (_connection) {
        if (!_connection instanceof mongoose.Connection)
            throw new Error('mongoose.Connection expected but got '+_connection);
        connection = _connection;
    };


    var get = function (param, key) {
        var fn = hash && hash[param] && hash[param][key];
        if (!fn) throw new Error('undefined '+param+' for name '+key);
        return fn;
    };
    var whitelist = ['lowercase', 'uppercase', 'trim', 'match', 'enum', 'min', 'max', 'ref', 'type', 'default', 'required', 'select', 'get', 'set', 'index', 'unique', 'sparse', 'validate'];
    var matchType = function (type) {
        var output;
        switch (type.toLowerCase()) {
            case 'string': output = String; break;
            case 'number': output = Number; break;
            case 'boolean': output = Boolean; break;
            case 'date': output = Date; break;
            case 'buffer': output = Buffer; break;
            case 'objectid': output = ObjectId; break;
            case 'mixed': output = Mixed; break;

            default: throw new Error('unknown type '+type);
        }
        return output;
    };


    var check = function (type, value) {
        if (type === 'match') {
            if (!is('String', value)) throw new Error('expected string for match key');
            return new RegExp(value);
        }
        throw new Error('unexpected type '+type);
    };


    var convert = function (descriptor) {
        var encoded = JSON.stringify(descriptor);
        var decoded = JSON.parse(encoded, function (key, value) {
            if (key === 'type') return matchType(value);
            if (key === 'validate') return get('validator', value);
            if (key === 'get') return get('getter', value);
            if (key === 'set') return get('setter', value);
            if (key === 'default') return get('default', value);
            if (key === 'match') return check(key, value);
            if (key === '') return value; // toplevel object
            //if (whitelist.indexOf(key) === -1) return;
            return value;
        });
        return decoded;
    };


    var schema = function (name, descriptor) {
        if (!is('String', name)) throw new Error('expected string for param name');
        if (!is('Object', descriptor)) throw new Error('expected object for param descriptor');
        if (!connection) throw new Error('expected a mongoose.Connection params. Call setConnection() before schema()');

        var schema = mapConvert(descriptor);

        return connection.model(name, schema);
    };

    var mapConvert = function (obj) {
        var traversed = traverse(obj);
        //this is going to pull our any string values we have and turn them into actual types
        //so 'Number' gets converted to [Function Number] the javasript number object

        //additionally simple types get converted as well
        //{type: 'String', ref: 'Genotype'} will come [Function String]
        // -- stripping the ref aspect, and replacing it with just the type object
        //Inner arrays the same thing [{type: 'Number'}] becomes [[Function Number]]
        // an array of 1 with javascript number object
        var removeTypes = traversed.map(function(node)
        {
            if(this.isLeaf)
            {
                //grab our key and value
                //type objects are always part of a parent object
                if (this.key === 'type')
                    this.parent.update(matchType(node));
                else if(typeof node ==='string' && this.key !== 'ref')
                    this.update( matchType(node));
                else
                    this.remove();
            }
        });

        //Now, inside mongoose, arrays are a bit of a nuisance
        //each array (if it's more than a simple type) must be a schema object
        //therefore, we must go through and replace any complicated arrays
        //unfortunately, this is a recursive process
        var recursiveReplaceArrays = function(node)
        {
//            console.log(this.key + " leaf? " + this.isLeaf);
            //you are a complex array, you must be replaced!
            if(this.key === '0' && this.notLeaf)
            {
                var innerTraverse = traverse(node);
                var schemaReplace = innerTraverse.map(recursiveReplaceArrays);

                var rSchema = new Schema(schemaReplace);
                this.update(rSchema, true);
            }
        };

        var schemaArrays = traverse(removeTypes).map(recursiveReplaceArrays);

        return schemaArrays;
    };

    // private api, just for testing
    genObject._hash = hash;
    genObject._get = get;
    genObject._matchType = matchType;
    genObject._check = check;
    genObject._convert = convert;

    // public api
    genObject.setValidator = set('validator');
    genObject.setSetter = set('setter');
    genObject.setGetter = set('getter');
    genObject.setDefault = set('default');
    genObject.setConnection = setConnection;
    genObject.getConnection = getConnection;
    genObject.schema = schema;
    genObject.mapConvert = mapConvert;


    //set some defaults up!
    genObject.setDefault('zero', function () {
        return 0;
    });

    genObject.setDefault('now', Date.now);


    //return a new generic ID for use as a WIN identifier sent to the client
    genObject.setDefault('newID', function()
    {
        return new ObjectId();//.toString();
    });


    var schemaModels = {};

    var saveSchemaModel = function(type, model)
    {
        if(schemaModels[type] && schemaModels[type] != model)
            throw new Error("Schema model already entered, what gives?");

        schemaModels[type] = model;
    };

    genObject.getSchemaModel = function(type) {return schemaModels[type];};
    genObject.setSchemaModel = function(type, obj){schemaModels[type] = obj;};

    genObject.loadSingleSchema = function(type, bufferObject)
    {
        var schemaObject = bufferObject;//JSON.parse(bufferString);

        //create the schema objects in database
        genObject.schema(type, schemaObject);
        //fetch the model from the database after creating it
        var monObject = connection.model(type);

        //Save the model object
        saveSchemaModel(type, monObject);

        return monObject;
    };




};
