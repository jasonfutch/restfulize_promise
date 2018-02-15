const fs = require("fs");
const $elasticsearch = require('elasticsearch');
const $promise = require("bluebird");

const $elastic = function(){
    let self = this;
    this.client = null;
    this.dbConfig = null;
    this.initialized = false;

    this.init = (dbConfig) => {
        if(!dbConfig) dbConfig = self.dbConfig;
        if(dbConfig!==null){
            self.dbConfig = dbConfig;
            self.client  = new $elasticsearch.Client(self.dbConfig);
            self.initialized = true;

            self.client.ping({
                requestTimeout: 30000,
            }, (err) => {
                if (err) {
                    console.error('elasticsearch cluster is down!');
                }
            });
        }
    };

    this.search = (obj) => {
        return new $promise((resolve, reject) => {
            self.client.search(obj).then(resolve, (err) => reject(self.$$formatError(err)));
        });
    };

    this.bulk = (obj) => {
        return new $promise((resolve, reject) => {
            self.client.bulk(obj).then(resolve, (err) => reject(self.$$formatError(err)));
        });
    };

    this.buildQuery = (obj) => {
        let objSearch = {
            "index": `enspire.${obj.table}`,
            "type" : "index",
            "from" : obj.offset,
            "size" : obj.limit,
            "body" : {}
        };

        if(obj.sorts.length>0){
            objSearch.body.sort=[];

            obj.sorts.forEach((sort) => {
                let objSort = {};
                let arySortName = sort.name.split('.');
                let sortName = arySortName[1];
                if(typeof sort.keyName !== 'undefined'){
                    objSort[`${sortName}.${sort.keyName.replace(/\,/gi,'.')}`] = {"order":sort.dir.toLowerCase()}
                }else{
                    objSort[`${sortName}`] = {"order":sort.dir.toLowerCase()}
                }
                objSearch.body.sort.push(objSort);
            });
        }

        if(obj.groups.length>0){
            let group = obj.groups[0];
            let fieldName = '';
         
            if(typeof group.keyName !== 'undefined'){
                fieldName = `${group.name.replace(obj.namespace+'.','')}.${group.keyName.replace(/\,/gi,'.')}`   
            }else{
                fieldName = group.name.replace(obj.namespace+'.','')
            }

            objSearch.body.collapse = {
                "field" : fieldName
            }

            objSearch.body.aggs = {
                "restfulize_group_total": {
                    "cardinality": {
                        "field": fieldName
                    }
                }
            }
        }

        if(obj.aggregates.length>0){
            if(typeof objSearch.body.aggs === 'undefined'){
                objSearch.body.aggs = {};
            }

            obj.aggregates.forEach((aggregate) => {
                let fieldName = '';
         
                if(typeof aggregate.keyName !== 'undefined'){
                    fieldName = `${aggregate.name.replace(obj.namespace+'.','')}.${aggregate.keyName.replace(/\,/gi,'.')}`   
                }else{
                    fieldName = aggregate.name.replace(obj.namespace+'.','')
                }

                objSearch.body.aggs[aggregate.as] = {};
                objSearch.body.aggs[aggregate.as][aggregate.aggregate] = {
                    field: fieldName
                }
            });
        }

        let objQuery = self.$filter(obj);
        if(Object.keys(objQuery.query).length>0){
            objSearch.body.query = objQuery.query;
        }

        return objSearch;
    };

    this.sourcesFromHits = (resp) => {
        let ary = [];
        let lngHits = resp.hits.hits.length;
        for(let i=0;i<lngHits;i++){
            ary.push(resp.hits.hits[i]._source)
        }
        return ary;
    };

    this.$filter = (obj) => {
        let objQuery = {
            "query":{}
        };

        let search = self.$$combineAndPrepFilters(obj);

        let lngStructure = search.structure.length;
        let cntArg = 0;

        if(lngStructure>0){

            const boolFilter = (ary=[]) => {

                let obj = {
                    "bool" : {}
                };

                let aryFilter = [];
                let lngAry = ary.length;
                for(let i=0;i<lngAry;i++) {
                    let filter = ary[i];
                    if (Array.isArray(filter)) {
                        aryFilter.push(boolFilter(filter));
                    } else {
                        if(filter==="arg"){

                            let objArg = search.fields[cntArg],
                                aryArgName = objArg.name.split('.'),
                                objArgName = aryArgName[aryArgName.length-1],
                                objArgValue = objArg.value,
                                aryArgValue = objArgValue.split(' '),
                                bolIsJsonbSearch = false,
                                objArgument = {};

                            if(typeof objArg.keyName !== 'undefined'){
                                objArgName += `.${objArg.keyName}`;
                            }

                            objArgName = objArgName.replace(/\,/gi,'.');

                            switch(objArg.sep){
                                case '??':
                                    strArg = objArgName+" ? '"+objArgValue+"' ";
                                    break;
                                case '=#':
                                    strArg = objArgName+"="+objArgValue+" ";
                                    break;
                                case '>>':
                                    strArg = objArgName+">'"+objArgValue+"' ";
                                    break;
                                case '<<':
                                    strArg = objArgName+"<'"+objArgValue+"' ";
                                    break;
                                case '>=':
                                    strArg = objArgName+">='"+objArgValue+"' ";
                                    break;
                                case '<=':
                                    strArg = objArgName+"<='"+objArgValue+"' ";
                                    break;
                                case '==':
                                    if(aryArgValue.length>1){
                                        objArgument = {"bool":{"must":[]}};
                                        aryArgValue.forEach((argValue) => {
                                            let objStatement = {"term" : {}};
                                            objStatement.term[objArgName] = argValue;
                                            objArgument.bool.must.push(objStatement)
                                        })
                                    }else{
                                        objArgument = {"term" : {}};
                                        objArgument.term[objArgName] = objArgValue;
                                    }
                                    aryFilter.push(objArgument);
                                    break;
                                case '@>':
                                    strArg = objArgName+"@>'"+objArgValue+"' ";
                                    break;
                                case '!=':
                                    objArgument = {"bool":{"must_not":[]}};
                                    if(aryArgValue.length>1){
                                        aryArgValue.forEach((argValue) => {
                                            let objStatement = {"term" : {}};
                                            objStatement.term[objArgName] = argValue;
                                            objArgument.bool.must_not.push(objStatement)
                                        })
                                    }else{
                                        let objStatement = {"term" : {}};
                                        objStatement.term[objArgName] = objArgValue;
                                        objArgument.bool.must_not.push(objStatement)
                                    }
                                    aryFilter.push(objArgument);
                                    break;
                                case '!^':
                                    strArg = objArgName+" IS NOT NULL ";
                                    break;
                                case '^^':
                                    strArg = objArgName+" IS NULL ";
                                    break;
                                case '%%':
                                    if(aryArgValue.length>1){
                                        objArgument = {"bool":{"must":[]}};
                                        aryArgValue.forEach((argValue) => {
                                            let objStatement = {"wildcard" : {}};
                                            objStatement.wildcard[objArgName] = `*${argValue}*`;
                                            objArgument.bool.must.push(objStatement)
                                        })
                                    }else{
                                        objArgument = {"wildcard" : {}};
                                        objArgument.wildcard[objArgName] = `*${objArgValue}*`;
                                    }
                                    aryFilter.push(objArgument);
                                    break;
                                case '%a':
                                    if(aryArgValue.length>1){
                                        objArgument = {"bool":{"must":[]}};
                                        aryArgValue.forEach((argValue) => {
                                            let objStatement = {"wildcard" : {}};
                                            objStatement.wildcard[objArgName] = `*${argValue}`;
                                            objArgument.bool.must.push(objStatement)
                                        })
                                    }else{
                                        objArgument = {"wildcard" : {}};
                                        objArgument.wildcard[objArgName] = `*${objArgValue}`;
                                    }
                                    aryFilter.push(objArgument);
                                    break;
                                case 'a%':
                                    if(aryArgValue.length>1){
                                        objArgument = {"bool":{"must":[]}};
                                        aryArgValue.forEach((argValue) => {
                                            let objStatement = {"wildcard" : {}};
                                            objStatement.wildcard[objArgName] = `${argValue}*`;
                                            objArgument.bool.must.push(objStatement)
                                        })
                                    }else{
                                        objArgument = {"wildcard" : {}};
                                        objArgument.wildcard[objArgName] = `${objArgValue}*`;
                                    }
                                    aryFilter.push(objArgument);
                                    break;
                            }
                            cntArg++;
                        }
                    }
                }

                if(lngAry>1){
                    switch(ary[1]){
                        case '&&':
                            obj.bool.must = aryFilter;
                            break;
                        case '||':
                            obj.bool.should = aryFilter;
                            break;
                    }
                }else{
                    obj.bool.must = aryFilter;
                }

                return obj;
            };

            objQuery.query = {
                "constant_score": {
                    "filter": boolFilter(search.structure)
                }
            };

        }
        console.log(JSON.stringify(objQuery));
        return objQuery;
    };

    this.$$combineAndPrepFilters = (obj) => {
        const wrapFilter = (ary = [], structure = []) => {
            if (structure.length > 1) {
                if (structure[0] !== "(") {
                    structure.unshift("(");
                }

                if (structure[structure.length - 1] !== ")") {
                    structure.push(")");
                }
            }
            if (ary.length > 0 && structure.length>0) {
                ary.push('&&');
            }
            ary = ary.concat(structure);
            return ary;
        };

        let structure = wrapFilter([],obj.filters.structure);
        structure = wrapFilter(structure,obj._filters.structure);
        structure = wrapFilter(structure,obj._auth.structure);

        let fields = obj.filters.fields;

        if (obj._filters.fields.length > 0) {
            fields = fields.concat(obj._filters.fields);
        }
        if (obj._auth.fields.length > 0) {
            fields = fields.concat(obj._auth.fields);
        }

        const parseIntoArrays = (ary=[]) => {
            let structure = [],
                lngAry = ary.length,
                x=0;
            for(x=0;x<lngAry;x++){
                let item = ary[x];
                if(item === ')'){
                    break;
                }else if(item === '('){
                    let obj = parseIntoArrays(ary.slice(x+1));
                    x = x+(obj.x+1);
                    structure.push(obj.structure);
                }else{
                    structure.push(item)
                }
            }

            return {structure,x}
        };

        let objStructure = parseIntoArrays(structure);
        structure = objStructure.structure;

        console.log(structure);

        return {structure,fields}
    };

    this.$$formatError = (err) => {
        console.log(err)
        return [500,{errors:[{code:3425+"",message:`Elastic Search Exception: ${err.message})`}]}];
    };

    this.init();
};

module.exports = new $elastic();