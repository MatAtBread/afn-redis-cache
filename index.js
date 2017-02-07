'use nodent-promise';
'use strict';

/* An afn memo cache based on REDIS for clustered services */

var redis = require('redis') ;

module.exports = function(config){
    var inProgress = "@promise" ;
    
    if (!config || !config.redis) {
        throw new Error("REDIS configuration missing");
    }

    // Throw away any logging the app isn't capturing
    config.log = config.log || function(){} ;

    // Work out what params to connect to redis with
    var redisArgs = [], redisOpts ; 
    if (typeof config.redis === "string") { // A REDIS url
        redisArgs = [config.redis,redisOpts = {}] ;
    } else {
        redisArgs = [redisOpts = config.redis] ;
    }
    
    // By default, fail connections quickly so caches fall back to local memory
    if (!('enable_offline_queue' in redisOpts)) {
        redisOpts.enable_offline_queue = false ;
        redisOpts.connect_timeout = 1000 ;
    }
    // On failure, fail the command(s) instantly and try to re-establish a connection in 10 seconds
    redisOpts.retry_strategy = redisOpts.retry_strategy || function (options) { 
        config.log("afn-redis-cache retry",options) ;
        setTimeout(connectRedis,10000) ;
        return undefined ;
    } ;
        
    var client ;
    function connectRedis() {
        if (client && client.connected)
            return ;
        client = redis.createClient.apply(redis,redisArgs) ;
        client.on('error',function(err){
            // redisOpts.retry_strategy()
            // Just eat them up - the individual calls should handle the error
            config.log("afn-redis-cache error",err) ;
        }) ;
    }
    
    connectRedis() ;

    // How long results should remain valid for in the cache (default: 1 min)
    if (!('defaultTTL' in config))
        config.defaultTTL = 60 ;
    
    // How long the cache should wait for an async function to 
    // return (and populate the cache) before claiming to be empty
    // (default: 0 - don't wait, different servers in a cluster will re-enter the underlying memoized function)
    if (!('asyncTimeOut' in config))
        config.asyncTimeOut = 0 ;
    
    return {
        hashEncoding:'base64',
        createCache: function(cacheID) {
            if (cacheID) cacheID += ":" ;
            else cacheID = "" ;
            var self ;
            return self = {
                get:async function(key) {
                    var delay = 25, total = 0 ;
                    function waiting(){
                        client.get(cacheID+key,handleRedisResponse) ;
                    }

                    function handleRedisResponse(err,reply){
                        if (err) {
                            config.log("error",key,err) ;
                            async return undefined ;
                        }
                        if (reply===null) {
                            config.log("miss",key) ;
                            async return null ;
                        }
                        
                        try {
                            if (reply === inProgress) {
                                // We're still in progress
                                delay = (delay * 1.3) |0 ;
                                total += delay ;
                                
                                if (total > config.asyncTimeOut * 1000) {
                                    config.log("timeout",key) ;
                                    async return undefined ;
                                } else {
                                    setTimeout(waiting, delay) ;
                                }
                            } else {
                                config.log("reply",key) ;
                                async return JSON.parse(reply) ;
                            }
                        } catch (ex) {
                            config.log("exception",key,ex) ;
                            async return undefined ;
                        }
                    }
                    waiting() ;
                },
                set:async function(key,data,ttl) {
                    // Are we being asked to cache an unresolved promise with no data
                    var serialized ;

                    if (!('data' in data)) {
                        serialized = inProgress ;
                    } else {
                        serialized = JSON.stringify(data) ;
                    }
                    
                    client.setex(cacheID+key, ttl?ttl/1000:config.defaultTTL, serialized, function(err,reply){
                        async return self ;
                    }) ;
                },
                'delete':async function(key) {
                    client.del(cacheID+key, function(err,reply){
                        if (err) async return false ;
                        async return true ;
                    }) ;
                },
                keys:async function() {
                    client.keys(cacheID+":*",function(err,reply){
                        if (err)
                            async return [] ;
                        try {
                            async return reply ;
                        } catch (ex) {
                            async return [] ;
                        }
                    }) ;
                }
            }
        } 
    }
} ;