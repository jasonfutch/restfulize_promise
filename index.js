var $actions                = require('./lib/actions');
var $dataTables             = require('./lib/data-tables');
var $errors                 = require('./lib/errors');
var $filters                = require('./lib/filters');
var $formatters             = require('./lib/formatters');
var $get                    = require('./lib/get');
var $insert                 = require('./lib/insert');
var $list                   = require('./lib/list');
var $tasks                  = require('./lib/tasks');
var $sql                    = require('./lib/postgres');
var $redshift               = require('./lib/redshift');
var $elasticsearch          = require('./lib/elasticsearch');
var $request                = require('./lib/request');
var $routines               = require('./lib/routines');
var $sqlString              = require('./lib/sql-string');
var $update                 = require('./lib/update');
var $validators             = require('./lib/validators');

exports.actions             = $actions;
exports.errors              = $errors;
exports.filters             = $filters;
exports.formatters          = $formatters;
exports.get                 = $get;
exports.insert              = $insert;
exports.list                = $list;
exports.tasks               = $tasks;
exports.sql                 = $sql;
exports.redshift            = $redshift;
exports.elastic             = $elasticsearch;
exports.request             = $request;
exports.routines            = $routines;
exports.sqlString           = $sqlString;
exports.update              = $update;
exports.validators          = $validators;

exports.data                = {};
exports.dataObjects         = {};
exports.routineDefinitions  = {};

exports.loadData = (dir,aryDataFiles, bolForceRun) => {
    exports.data = $dataTables.load(dir,aryDataFiles, bolForceRun);
    exports.dataObjects = $dataTables.dataObjects;
    exports.errorCode = $dataTables.errorCode;
    exports.logCode = $dataTables.logCode;
    exports.routineDefinitions = $dataTables.routineDefinitions;
    exports.helpers = $dataTables.helpers;
};