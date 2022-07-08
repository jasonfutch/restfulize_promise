var $errors = require('./errors');
var $restfulList = require('./list');
var $restfulGet = require('./get');
var $restfulInsert = require('./insert');
var $restfulUpdate = require('./update');
var $extend = require('extend');
var $sql = require('./postgres');
var _ = require('underscore');
var $promise = require("bluebird");

module.exports = function(req,res) {
    var self = this;

    this.errorHandler = new $errors(req, res);

    var objRead = {
        type: "get",
        overwrites: {
            _section: ""
        },
        defaults: {},
        onBeginList: undefined,
        onBeginRead: undefined,
        onComplete: undefined
    };

    this.read = (tables,map) => {
        return new $promise((resolve, reject) => {
            var obj = $extend(true, {}, objRead);

            var task, lngMap = map.length;

            //make sure datamap has tasks
            if (lngMap === 0) return false;

            if (self.$validateMap(map, 'read') == false) return false;

            var currentID = 0;
            var mainObj = {};
            var runTask = (json) => {
                if (!json) json = {};

                if (currentID === lngMap) {
                    resolve(json);
                    return false;
                }

                var cMap = map[currentID];

                task = $extend(true, $extend(true, {}, obj), cMap);
                if (typeof task.onBeginList !== 'undefined' && task.type === 'list') {
                    task = $extend(true, task, task.onBeginList(json));
                }
                if (typeof task.onBeginRead !== 'undefined' && task.type === 'get') {
                    task = $extend(true, task, task.onBeginRead(json));
                }
                var onComplete = task.onComplete;
                if (typeof onComplete === 'undefined') onComplete = (resp, o) => {
                    return resp;
                };
                var toComplete = ([resp, o]) => {
                    if (currentID === 0) {
                        mainObj = o;
                        json = onComplete(resp, o)
                    } else {
                        (typeof json.data !== 'undefined') ? json.data[task.overwrites._section] = onComplete(resp, o) : json[task.overwrites._section] = onComplete(resp, o);
                    }
                    currentID++;
                    runTask(json);
                };
                self.$action(tables, task, 'read').then(toComplete,reject);
                return true;
            };
            runTask();
        });
    };

    this.extendSQL = (obj,arySQL) => {
        arySQL.forEach((strSQL,idx) => {
            if(_.isObject(strSQL) && typeof strSQL.sql !== 'undefined'){
                obj.push(strSQL);
            }else{
                obj.push({"sql":strSQL});
            }
        });
        return obj;
    };

    this.extractSQL = (obj) => {
        var arySQL = [];
        var lngObj = obj.length;
        for (var i = 0; i < lngObj; i++) {
            arySQL.push(obj[i].sql);
        }
        return arySQL;
    };

    this.applyTransaction = (obj, rejectWithError = false) => {

        return new $promise((resolve, reject) => {

            try {

                var arySQL = self.extractSQL(obj);

                //console.log(arySQL);
                $sql.connect().then(connection => {

                    connection.transaction(arySQL).then(() => {

                        connection.release();

                        response = {
                            "status": "success"
                        };

                        resolve([response, obj]);

                    }, (err) => {

                        console.log(err);
                        connection.release();

                        if (rejectWithError) {

                            return reject(err);

                        }

                        self.errorHandler.error(400, err.code, (err.details || err.message) + '');

                        return self.errorHandler.response().then(reject);

                    });

                }, (err) => {

                    console.log(err);

                    if (rejectWithError) {

                        return reject(err);

                    }

                    self.errorHandler.error(400, err.code, err + '');

                    return self.errorHandler.response().then(reject);

                });

            } catch (e) {

                console.log(e);

                if (rejectWithError) {

                    return reject(e);

                }


                self.errorHandler.error(500, e + '');

                return self.errorHandler.response().then(reject);

            }

        });
    };

    this.write = (tables,maps,autoApply=true,body) => {
        return new $promise((resolve, reject) => {
            try {
                var writes = [];

                var sObj = {};

                var obj = {
                    type: "insert",
                    insertBlankIfEmpty: false,
                    requireIfEmpty: false,
                    multiple: false,
                    shared: [],
                    overwrites: {
                        _section: ""
                    },
                    defaults: {},
                    onBeginMultiple: undefined,
                    onBeginCreate: undefined,
                    onBeginUpdate: undefined,
                    onBeginDelete: undefined,
                    onComplete: undefined
                };

                var runTaskSet = (map, f, sharedObj, passedBody) => {
                    try {
                        var extTask, lngMap = map.length;

                        //make sure datamap has tasks
                        if (lngMap === 0) return false;

                        if (self.$validateMap(map, 'write') == false) return false;

                        var cID = 0;

                        var runTask = (sharedObj, currentID) => {
                            try {
                                if (!sharedObj) sharedObj = {};
                                if (currentID === lngMap) {
                                    f(writes);
                                    return false;
                                }

                                var runTaskAction = (task, callback) => {
                                    try {
                                        //console.log('runTaskAction');
                                        if (!callback) callback = () => {
                                            currentID++;
                                            runTask(sharedObj, currentID);
                                        };
                                        if (typeof task.onBeginCreate !== 'undefined' && task.type === 'insert') {
                                            task = $extend(true, task, task.onBeginCreate(sharedObj, task.body));
                                        }
                                        if (typeof task.onBeginUpdate !== 'undefined' && task.type === 'update') {
                                            task = $extend(true, task, task.onBeginUpdate(sharedObj, task.body));
                                        }
                                        if (typeof task.onBeginDelete !== 'undefined' && task.type === 'delete') {
                                            task = $extend(true, task, task.onBeginDelete(sharedObj, task.body));
                                        }
                                        var onComplete = task.onComplete;
                                        if (typeof onComplete === 'undefined') onComplete = (sql, o, sObj) => {
                                            return {sql: sql, obj: o, sharedObj: sObj}
                                        };
                                        var toComplete = ([sql, o]) => {
                                            try {
                                                var lngShared = task.shared.length;
                                                for (var i = 0; i < lngShared; i++) {
                                                    var sharedItem = task.shared[i];
                                                    if (typeof o.body[sharedItem] !== 'undefined') {
                                                        sharedObj[sharedItem] = o.body[sharedItem];
                                                    } else {
                                                        var sharedField = _.findWhere(o._filters.fields, {alias: sharedItem});
                                                        if (typeof sharedField === 'undefined') _.findWhere(o.filters.fields, {alias: sharedItem});
                                                        if (typeof sharedField !== 'undefined') {
                                                            sharedObj[sharedItem] = sharedField.value;
                                                        }
                                                    }
                                                }
                                                var resp = onComplete(sql, o, sharedObj);
                                                sharedObj = resp.sharedObj;
                                                var lngSQL = resp.sql.length;
                                                for (var i = 0; i < lngSQL; i++) {
                                                    var strSQL = resp.sql[i];
                                                    writes.push({sql: strSQL, obj: resp.obj});
                                                };
                                                // console.log('hmm:toComplete 2');
                                                callback(task.body);
                                            } catch (e) {
                                                console.log(e);
                                            }
                                        };
                                        console.log('runTaskAction: $action');
                                        self.$action(tables, task, 'write').then(toComplete,reject);
                                    } catch (e) {
                                        console.log(e);
                                    }
                                };


                                var extTask = $extend(true, $extend(true, {body: passedBody}, obj), map[currentID]);
                                if (extTask.multiple) {
                                    var multipleFunction = (objCompareData, aryCompareData, compareKey) => {
                                        try {
                                            var section = extTask.overwrites._section;
                                            var arySection = [];
                                            if (section !== '') {
                                                if (typeof passedBody[section] !== 'undefined') {
                                                    if (_.isArray(passedBody[section])) arySection = passedBody[section];
                                                }
                                            } else {
                                                if (_.isArray(passedBody)) arySection = passedBody;
                                            }

                                            var lngSection = arySection.length;
                                            if (lngSection === 0) {
                                                if (extTask.insertBlankIfEmpty) {
                                                    extTask.body = {};
                                                    runTaskAction(extTask);
                                                } else {
                                                    if (extTask.requireIfEmpty) {
                                                        self.errorHandler.error(400, 145, '' + section + ' can not be an empty array.');
                                                        return self.errorHandler.response();
                                                    } else {
                                                        currentID++;
                                                        runTask(sharedObj, currentID);
                                                    }
                                                }
                                            } else {
                                                var sectionCurrentID = -1;
                                                var multipleRepeat = function () {
                                                    try {
                                                        sectionCurrentID++;
                                                        if (lngSection <= sectionCurrentID) {
                                                            if (typeof aryCompareData !== 'undefined') {
                                                                if (aryCompareData.length > 0) {
                                                                    var tempTask = $extend(true, {}, extTask);
                                                                    tempTask.body = aryCompareData[0];
                                                                    tempTask.type = 'delete';
                                                                    aryCompareData.splice(0, 1);
                                                                    runTaskAction(tempTask, function (b) {
                                                                        if (typeof extTask.subtasks !== 'undefined' && extTask.subtasks.length > 0) {
                                                                            runTaskSet(extTask.subtasks, multipleRepeat, sharedObj, b)
                                                                        } else {
                                                                            multipleRepeat();
                                                                        }
                                                                    });
                                                                } else {
                                                                    currentID++;
                                                                    runTask(sharedObj, currentID);
                                                                }
                                                            } else {
                                                                currentID++;
                                                                runTask(sharedObj, currentID);
                                                            }
                                                        } else {
                                                            var tempTask = $extend(true, {}, extTask);
                                                            tempTask.body = arySection[sectionCurrentID];

                                                            if ((tempTask.type === 'update' || tempTask.type === 'crud' || tempTask.type === 'delete') && compareKey.length > 0) {
                                                                if (typeof objCompareData[tempTask.body[compareKey[0].name]] !== 'undefined') {
                                                                    tempTask.body = $extend(true, objCompareData[tempTask.body[compareKey[0].name]], tempTask.body);
                                                                    if (tempTask.type === 'crud') tempTask.type = 'update';
                                                                    aryCompareData = _.reject(aryCompareData, function (objA) {
                                                                        return objA[compareKey[0].name] === tempTask.body[compareKey[0].name];
                                                                    });
                                                                } else {
                                                                    if (tempTask.type === 'crud') tempTask.type = 'insert';
                                                                }
                                                            }
                                                            runTaskAction(tempTask, function (b) {
                                                                if (typeof extTask.subtasks !== 'undefined' && extTask.subtasks.length > 0) {
                                                                    //console.log('found subtasks');
                                                                    runTaskSet(extTask.subtasks, multipleRepeat, sharedObj, b)
                                                                } else {
                                                                    //console.log('NO subtasks');
                                                                    multipleRepeat();
                                                                }
                                                            });
                                                        }
                                                    } catch (e) {
                                                        console.log(e)
                                                    }
                                                };
                                                multipleRepeat();
                                            }
                                        } catch (e) {
                                            console.log(e);
                                        }
                                    };

                                    if (extTask.type === 'update' || extTask.type === 'crud' || extTask.type === 'delete') {
                                        /**
                                         * PULL ORIGINAL LIST TO COMPARE AGAINST
                                         */
                                        var objMultipleRead = $extend(true, {}, objRead);
                                        var task = $extend(true, $extend(true, {}, objMultipleRead), extTask.onBeginMultiple({}));
                                        task.type = 'list';
                                        var toComplete = function ([resp, o]) {
                                            var lngCompareKey = o._compareKey.length;
                                            if (lngCompareKey === 0) console.error('missing _compareKey');
                                            if (lngCompareKey > 1) console.error('_compareKey does not support multiples yet');
                                            var aryCompareData = resp.data;
                                            var lngCompareData = aryCompareData.length;
                                            var objCompareData = {};
                                            for (var i = 0; i < lngCompareData; i++) {
                                                objCompareData[aryCompareData[i][o._compareKey[0].name]] = aryCompareData[i];
                                            }
                                            multipleFunction(objCompareData, aryCompareData, o._compareKey);
                                        };
                                        self.$action(tables, task, 'read').then(toComplete,reject);
                                    } else {
                                        multipleFunction();
                                    }
                                } else {
                                    runTaskAction(extTask);
                                }
                                return true;
                            } catch (e) {
                                console.log(e);
                            }
                        };
                        runTask(sharedObj, cID);
                    } catch (e) {
                        console.log(e);
                        self.errorHandler.error(500, e + '');
                        return self.errorHandler.response().then(reject);
                    }

                };

                if (typeof body === 'undefined') {
                    console.log('BODY UNDEFINED');
                    if (_.isArray(req.body)) {
                        body = $extend(true, [], req.body || []);
                    } else {
                        body = $extend(true, {}, req.body || {});
                    }
                }
                runTaskSet(maps, function(writes){
                    console.log('almost');
                    try{
                        if(autoApply){
                            self.applyTransaction(writes).then(resolve,reject)
                        }else{
                            resolve(writes)
                        }
                    }catch(e){
                        console.log(e)
                    }

                }, sObj, body);
            } catch (e) {
                console.log(e);
                self.errorHandler.error(500, e + '');
                return self.errorHandler.response().then(reject);
            }
        });
    };

    this.$validateMap = function(map,type){
        var lngMap = map.length;
        if(self.$validateSectionLead(map,type)==false) return false;
        for(var i=0;i<lngMap;i++){
            var task = map[i];
            if(task.isArray){
                if(self.$validateMap(task)==false) return false;
            }
        }
        return true;
    };

    this.$validateSectionLead = function(map,type){
        if(!type) type = 'read';

        //get first task;
        var task = map[0];

        //make sure for item is not an array
//        if(task.isArray){
//            console.warn('first task on data map and sub maps can not be an array');
//            return false;
//        }

//        //first task must be get if it has sub sections
//        if((task.type).toLowerCase()!=='get' && type==='read'){
//            console.warn('first task on data map must be a type:GET when it has sub sections');
//            return false;
//        }
        return true;
    };

    this.$action = function(tables,task,type){
        return new $promise((resolve, reject) => {

            var errorReject = function(err){
                reject(err)
            }

            if (type === 'read') {
                switch ((task.type).toLowerCase()) {
                    case 'get':
                        var restfulGet = new $restfulGet(req, res);
                        restfulGet.build(tables, task).then(resolve,errorReject);
                        break;
                    case 'list':
                        var restfulList = new $restfulList(req, res);
                        restfulList.build(tables, task).then(resolve,errorReject);
                        break;
                }
            } else {
                //console.log('$action: '+(task.type).toLowerCase());
                switch ((task.type).toLowerCase()) {
                    case 'insert':
                        var restfulInsert = new $restfulInsert(req, res);
                        restfulInsert.build(tables, task, {transaction: true}).then(resolve,errorReject);
                        break;
                    case 'update':
                    case 'delete':
                        var restfulUpdate = new $restfulUpdate(req, res);
                        restfulUpdate.build(tables, task, {transaction: true}).then(resolve,errorReject);
                        break;
                    default:
                        console.log((task.type).toLowerCase() + ' unknown');
                        self.errorHandler.error(500, 100, (task.type).toLowerCase() + ' unknown');
                        errorReject()
                }
            }
        });
    };
};

