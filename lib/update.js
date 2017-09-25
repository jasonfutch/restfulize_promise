var $restfulize = require('../index');
var $restfulRequest = require('./request');
var $restfulActions = require('./actions');
var $errors = require('./errors');
var $restfulSqlString = require('./sql-string');
var $sql = require('./postgres');
var $extend = require('extend');
const $promise = require("bluebird");

module.exports = function restfulUpdate(req, res, errorHandler){
    var self = this;
    this.errorHandler = errorHandler;

    this.$init = function(){
        if(!self.errorHandler) self.errorHandler = new $errors(req, res);
    };

    this.build = function(tables, o, setting, onComplete){
        return new $promise((resolve, reject) => {
            try {

                var s = {
                    ignoreQuery: false,
                    ignoreBody: false,
                    checkExist: false
                };
                s = $extend(true, s, setting || {});

                //defining query so it won't be populated by restfulRequest
                if (s.ignoreQuery && typeof o.query === 'undefined') o.query = {};

                //defining body so it won't be populated by restfulRequest (mainly for internal updating)
                if (s.ignoreBody && typeof o.body === 'undefined') o.body = {};

                if (typeof o.overwrites === 'undefined') o.overwrites = {};
                o.overwrites.table_list = "*";

                var restfulRequest = new $restfulRequest(tables, req, res, self.errorHandler);
                var restfulActions = new $restfulActions(tables, req, res, self.errorHandler);

                /*
                 * Grabbing data
                 * */
                var obj = restfulRequest.PUT(o);
                if (obj === false) return self.errorHandler.response().then(reject);

                /*
                 * Processing data
                 * */

                var actions = restfulActions.PUT(obj, restfulRequest.params);
                actions.then(function (obj) {
                    var arySQL = [];
                    var restfulSqlString = new $restfulSqlString();

                    $restfulize.helpers.sqlifyTransactions('pre', obj, arySQL);

                    arySQL.push(restfulSqlString.update(obj));

                    $restfulize.helpers.sqlifyTransactions('post', obj, arySQL);

                    var response = {};

                    // console.log(arySQL);
                    if (s.transaction === true) {
                        resolve([arySQL, obj]);
                    } else {
                        $sql.connect().then(function (connection) {
                            connection.transaction(arySQL).then(function () {
                                connection.release();
                                response = {
                                    "status": "success"
                                };
                                resolve([response, obj]);
                            }, function (err) {
                                connection.release();
                                self.errorHandler.error(400, err.code, err + '');
                                return self.errorHandler.response().then(reject);
                            });
                        }, function (err) {
                            connection.release();
                            self.errorHandler.error(400, err.code, err + '');
                            return self.errorHandler.response().then(reject);
                        });
                    }

                }, function () {
                    console.log('$update error');
                    self.errorHandler.response().then(reject);
                });
            } catch (e) {
                console.log(e);
                self.errorHandler.error(500, e + '');
                return self.errorHandler.response().then(reject);
            }
        });
    };

    this.$init();
};
