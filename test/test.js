'use nodent';
'use strict';

require('colors') ;
/* Test afn-redis-cache */

// Test config
const config = {
    //log(){console.log('afn-redis-cache',Array.prototype.slice.call(arguments).toString())},
    redis:"redis://127.0.0.1/13",
    defaultTTL:120,
    asyncTimeOut:30
};

const delay = 500 ;
const redisCache = require('..')(config) ;
const localMemo = require('afn/memo')() ;
const clusterMemo = require('afn/memo')(redisCache) ;

async function sleep(t) {
    setTimeout(function(){ async return },t) ;
}

async function test(x) {
    console.log("test("+x+")") ;
    if (x[0] === '!')
        throw ("Not cachable: "+x) ;
    await sleep(delay) ;
    return x ;
}

var output = 1 ;
function ok(x){ console.log(output++ +JSON.stringify(x).green) }
function err(x){ console.log(output++ +JSON.stringify(x).red) }

const localTest = localMemo(test,{
    ttl:60000, 
    key:function(self, args, asyncFunction) { return args[0] }  
}) ;

const clusterTest = clusterMemo(test,{
    ttl:60000, 
    key:function(self, args, asyncFunction) { return args[0] }  
}) ;

await sleep(500);

localTest("local-first").then(ok,err) ;
localTest("local-second").then(ok,err) ;
localTest("local-first").then(ok,err) ;
await sleep(delay*1.2);
localTest("local-first").then(ok,err) ;
localTest("local-second").then(ok,err) ;

clusterTest("cluster-first").then(ok,err) ;
clusterTest("cluster-second").then(ok,err) ;
clusterTest("cluster-first").then(ok,err) ;
await sleep(delay*1.2);
clusterTest("cluster-first").then(ok,err) ;
clusterTest("cluster-second").then(ok,err) ;

localTest("!local").then(ok,err) ;
await sleep(delay*1.2);
localTest("!local").then(ok,err) ;
localTest("!local").then(ok,err) ;

clusterTest("!cluster").then(ok,err) ;
await sleep(delay*1.2);
clusterTest("!cluster").then(ok,err) ;
clusterTest("!cluster").then(ok,err) ;

await sleep(delay*1.2);

process.exit(0) ;