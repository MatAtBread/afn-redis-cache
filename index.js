'use nodent-promise';

/* An afn memo cache based on REDIS for clustered services */

var redis = require('redis') ;

module.exports = function(config){
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

    return {
        hashEncoding:'base64',
        createCache: function(cacheID) {
            if (cacheID) cacheID += ":" ;
            else cacheID = "" ;
            var self ;
            return self = {
                get:async function(key) {
                    client.get(cacheID+key,function(err,reply){
                        if (err)
                            async return undefined ;
                        try {
                            async return JSON.parse(reply) ;
                        } catch (ex) {
                            async return undefined ;
                        }
                    }) ;
                },
                set:async function(key,data,ttl) {
                    client.setex(cacheID+key, ttl?ttl/1000:(config.defaultTTL || 60), JSON.stringify(data), function(err,reply){
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