const $restfulRequest = require('./request');
const $restfulActions = require('./actions');
const $errors = require('./errors');
const $restfulSqlString = require('./sql-string');
const $sql = require('./postgres');
const $elastic = require('./elasticsearch');
const $extend = require('extend');
const $promise = require("bluebird");

module.exports = function restfulList(req,res,errorHandler){
    let self = this;
    this.errorHandler = errorHandler;

    this.$init = function(){
        if(!self.errorHandler) self.errorHandler = new $errors(req,res);
    };

    this.build = function(tables, o){
        return new $promise((resolve, reject) => {
            const restfulRequest = new $restfulRequest(tables, req, res, self.errorHandler);

            const obj = restfulRequest.LIST(o);

            if (obj === false) return self.errorHandler.response().then(reject);

            const restfulSqlString = new $restfulSqlString();
            // if(){
            //
            // }else{
            //
            // }

            let response = {
                q: obj.q.phrase,
                q_type: obj.q_type,
                offset: obj.offset,
                sort: obj.sort,
                limit: obj.limit,
                total: 0,
                columns: obj.columns,
                data: []
            };

            if (obj.show_columns === false) {
                delete response.columns;
            } else {
                const lngColumns = response.columns.length;
                for (let i = 0; i < lngColumns; i++) {
                    delete response.columns[i].actions;
                    delete response.columns[i].define;
                }
            }

            if (obj.q.phrase === '') {
                delete response.q;
                delete response.q_type;
            }

            if(obj._elastic){

                let objSearch = $elastic.buildQuery(obj);

                $elastic.client.count({
                    index: objSearch.index,
                    body: {
                        query: objSearch.body.query
                    }
                }).then(({ count }) => {

                    if(objSearch.body.aggs && objSearch.body.aggs.restfulize_group_total) {

                        objSearch.body.aggs.restfulize_group_total.cardinality.precision_threshold = count;

                    }

                    $elastic.search(objSearch).then((resp) => {
                        response.total = resp.hits.total;
    
                        if(typeof resp.aggregations !== 'undefined' && typeof resp.aggregations.restfulize_group_total !== 'undefined'){
                            response.total = resp.aggregations.restfulize_group_total.value;
                            delete resp.aggregations.restfulize_group_total;
                        }
    
                        if(typeof resp.aggregations !== 'undefined' && Object.keys(resp.aggregations).length>0){
                            response.aggregations = resp.aggregations
                        }
    
                        response.data = $elastic.sourcesFromHits(resp);
                        resolve([response, obj]);
                    },reject);

                }).catch((error) => {

                    reject(error);

                });

            }else {
                const strSQL = restfulSqlString.select(obj);
                const cntSQL = restfulSqlString.select(obj, true);

                // console.timeEnd('list.initialize');
                console.log(strSQL);
                $sql.connect().then(function (connection) {
                    console.time('list.js:$sql execute');
                    connection.execute(strSQL).then(function (rows) {
                        console.timeEnd('list.js:$sql execute');
                        var restfulActions = new $restfulActions(tables, req, res, self.errorHandler);
                        // console.time('list.js:$format');
                        rows = restfulActions.formatResponse(obj, rows);
                        // console.timeEnd('list.js:$format');
                        if (obj.list_controls) {
                            response.data = rows;
                            if ((rows.length > 0 && rows.length === obj.limit) || obj.offset !== 0) {
                                // console.log(cntSQL);
                                console.time('list.js:$sql count');
                                connection.execute(cntSQL).then(function (rows) {
                                    console.timeEnd('list.js:$sql count');
                                    // console.log(rows);
                                    response.total = parseInt(rows[0].total);
                                    connection.release();
                                    resolve([response, obj]);
                                }, function (err) {
                                    connection.release();
                                    self.errorHandler.error(400, err.code, err + '');
                                    return self.errorHandler.response().then(reject);
                                });
                            } else {
                                response.total = rows.length;
                                connection.release();
                                resolve([response, obj]);
                            }
                        } else {
                            response = rows;
                            connection.release();
                            resolve([response, obj]);
                        }
                    }, function (err) {
                        connection.release();
                        self.errorHandler.error(400, err.code, err + '');
                        return self.errorHandler.response().then(reject);
                    });
                });
            }
        });
    };

    this.$init();
};
