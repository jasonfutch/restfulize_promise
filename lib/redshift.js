const fs = require("fs");
const $pg = require('pg');
const $promise = require("bluebird");

$pg.defaults.parseInt8 = true;

var $redshift = function(){
    var self = this;
    this.pool = null;
    this.dbConfig = null;

    this.init = function(dbConfig){
        if(!dbConfig) dbConfig = self.dbConfig;
        if(dbConfig!==null){
            self.dbConfig = dbConfig;
            self.pool = new $pg.Pool(self.dbConfig);
        }
    };

    this.connect = function() {
        return new $promise((resolve, reject) => {
            self.pool.connect(function (err, con, done) {
                if (err) {
                    console.log(err);
                    return reject(err);
                }
                con.release = function () {
                    done();
                };
                con.execute = function (strSQL) {
                    return new $promise((resolve, reject) => {
                        con.query(strSQL, function (err, results) {
                            if (err) {
                                return reject(err);
                            }
                            resolve(results.rows);
                        });
                    });
                };
                con.rollback = function (callback) {
                    con.query('ROLLBACK', function (err) {
                        return callback(err);
                    });
                };
                con.commit = function (callback) {
                    con.query('COMMIT', function (err) {
                        return callback(err);
                    });
                };
                con.beginTransaction = function (callback) {
                    con.query('BEGIN', function (err) {
                        return callback(err);
                    });
                };
                con.createDbFromSqlFile = function (fileName, dbName) {
                    console.log('$restfulize.sql.processSqlFile:', fileName);
                    return new $promise((resolve, reject) => {
                        fs.readFile(fileName, "utf8", function (err, data) {
                            if (err) reject(err);

                            data = "CREATE SCHEMA IF NOT EXISTS " + dbName + ";\r\n" + data.replace(/rp_client_database_schema_name/g, dbName);

                            console.log('RUNNING SQL');

                            con.execute(data).then(function (results) {
                                console.log(results);
                                console.log('SQL EXECUTED');
                                con.commit(function (err) {
                                    if (err) con.rollback(function () {
                                        reject(err);
                                    });
                                    console.log('SQL COMMITED');
                                    resolve('success!');
                                })
                            },function (err) {
                                con.rollback(function () {
                                    console.log('SQL FAILED');
                                    reject(err);
                                });
                            });
                        });
                    });
                };
                con.transaction = function (arySQL) {
                    return new $promise((resolve, reject) => {
                        var lngSQL = arySQL.length;
                        if (lngSQL > 0) {
                            var currentPosition = 0;

                            var runQuery = function () {
                                if (lngSQL === currentPosition) {
                                    con.commit(function (err) {
                                        if (err) con.rollback(function () {
                                            reject(err);
                                        });
                                        console.timeEnd('postgres.js:$transaction execute');
                                        resolve('success!');
                                    })
                                } else {
                                    con.execute(arySQL[currentPosition]).then(function (results) {
                                        currentPosition++;
                                        runQuery();
                                    },function (err) {
                                        con.rollback(function () {
                                            reject(err);
                                        });
                                    });
                                }
                            };
                            console.time('postgres.js:$transaction execute');
                            con.beginTransaction(function (err) {
                                if (err) reject(err);
                                runQuery();
                            });
                        } else {
                            reject('Nothing to change or add.');
                        }
                    });
                };
                resolve(con);
            });
        });
    };

    this.init();
};

module.exports = new $redshift();