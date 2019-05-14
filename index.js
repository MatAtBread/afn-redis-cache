'use nodent-promise';
'use strict';

/* An afn memo cache based on REDIS for clustered services */

var redis = require('redis') ;

function redisSafePattern(s) {
    return s.replace(/([.?*\[\]])/g,"\\$1")
}

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
        redisOpts.enable_offline_queue = true ;
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
                name:typeof config.redis === 'string'? config.redis : JSON.stringify(config.redis),
                get:async function(key) {
                    var delay = 25, total = 0 ;
                    function waiting(){
                        // The script is an "atomic" get-test-set that returns the current value for a key,
                        // of if it doesn't exist sets it to '@promise' and returns null (so the client in this
                        // case will think it missed and populate it)
                      
                        // Importantly, only the FIRST execution of this returns "@new", indicating that the caller
                        // (who will get Promise<undefined> as the resilt) should proceed to populate the cache
                        // Subsequent callers will get an unresolved Promise back, which will resolve when the
                        // first one sets the result, or the whole operation reaches config.asyncTimeOut
                      
                        var script = /* params: keyName=wait-time (in seconds) */
                        "local v = redis.call('GET',KEYS[1]) if (not v) then redis.call('SET',KEYS[1],'"+inProgress+"','EX',ARGV[1]) return '@new' end return v" ;
                        client.eval([script,1,cacheID+key,config.asyncTimeOut],handleRedisResponse) ;
                    }

                    function handleRedisResponse(err,reply){
                        if (err) {
                            config.log("error",key,err) ;
                            async return undefined ;
                        }
                        if (reply===null) {
                            config.log("miss",key) ;
                            async return undefined ;
                        }
                        
                        try {
                            if (reply === "@new")
                              async return undefined ; // No such key - we created a new @promise in it's place
                            if (reply === "@null")
                              async return null ;
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

                    if (data === null)
                      serialized = "@null";
                    else if (data === undefined) // We can't store undefined, as it's used to indicate a cache miss
                      serialized = "@null";
                    else if (data && typeof data.then === "function")
                        serialized = inProgress ;
                    else
                        serialized = JSON.stringify(data) ;
                    
                    client.setex(cacheID+key, ttl?(ttl/1000)|0:config.defaultTTL, serialized, function(err,reply){
                        async return self ;
                    }) ;
                },
                'delete':async function(key) {
                    client.del(cacheID+key, function(err,reply){
                        if (err) async return false ;
                        async return true ;
                    }) ;
                },
                expireKeys:async function(now){
                    // REDIS expires keys automagically
                },
                clear:async function(){
                    client.keys(redisSafePattern(cacheID)+"*",function(err,reply){
                        if (err)
                            async return ;
                        
                        client.del(reply,function(err,reply){
                            async return ;
                        }) ;
                    }) ;
                },
                keys:async function() {
                    client.keys(redisSafePattern(cacheID)+"*",function(err,reply){
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