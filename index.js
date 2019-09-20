'use nodent-promise';
'use strict';

/* An afn memo cache based on REDIS for clustered services */

var redis = require('redis') ;

function redisSafePattern(s) {
  return s.replace(/([.?*\[\]])/g,"\\$1")
}

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
    redisOpts.enable_offline_queue = true ;
    redisOpts.connect_timeout = 1000 ;
  }
  // On failure, fail the command(s) instantly and try to re-establish a connection in 10 seconds
  redisOpts.retry_strategy = redisOpts.retry_strategy || function (options) { 
    config.log("redis-retry","",undefined,options) ;
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
      config.log("redis-error","",undefined,err) ;
    }) ;
  }

  connectRedis() ;

  // How long results should remain valid for in the cache (default: 1 min)
  if (!('defaultTTL' in config))
    config.defaultTTL = 60 ;

  // How long the cache should wait for an async function to 
  // return (and populate the cache) before claiming to be empty
  // 0 - don't wait, different servers in a cluster will re-enter the underlying memoized function)
  if (!('asyncTimeOut' in config))
    config.asyncTimeOut = config.defaultTTL ;

  if (typeof config.log !== 'function')
    config.log = function(){};

  return {
    hashEncoding:'base64',
    createCache: function(cacheID) {
      if (cacheID) cacheID += ":" ;
      else cacheID = "" ;
      var self ;
      return self = {
        name:typeof config.redis === 'string'? config.redis : JSON.stringify(config.redis),
        get:async function(key, options) {
          var start = Date.now(), log;
          options = Object.assign({},config,options);
          if (typeof options.log !== 'function')
            log = function(){};
          else
            log = function(msg,key,data){ options.log(msg,key,Date.now()-start,data) };
          
          var delay = (25+Math.random() * 20)|0; 
          var total = 0;
          var asyncTimeOut = options.asyncTimeOut;

          function waiting(){
            // The script is an "atomic" get-test-set that returns the current value for a key,
            // of if it doesn't exist sets it to '@promise' and returns null (so the client in this
            // case will think it missed and populate it)

            // Importantly, only the FIRST execution of this returns "@new", indicating that the caller
            // (who will get Promise<undefined> as the resilt) should proceed to populate the cache
            // Subsequent callers will get an unresolved Promise back, which will resolve when the
            // first one sets the result, or the whole operation reaches options.asyncTimeOut

            var script = /* params: keyName=wait-time (in seconds) */
              "local v = redis.call('GET',KEYS[1]) if (not v) then redis.call('SET',KEYS[1],'@promise','EX',ARGV[1]) return '@new' end return v" ;
            client.eval([script,1,cacheID+key,asyncTimeOut],handleRedisResponse) ;
          }

          function handleRedisResponse(err,reply){
            if (err) {
              log("error",key,err) ;
              async return undefined ;
            }

            if (reply===null) {
              // This is really an error as the script should never return null
              log("miss",key) ; 
              async return undefined ;
            }

            try {
              if (reply === "@new") {
                log("new",key) ;
                async return undefined ; // No such key - we created a new @promise in it's place so that other getters wait for it to be set
              }
              if (reply === "@null") {
                log("reply",key,null) ;
                async return null ;
              }
              if (reply === "@promise") {
                if (total === 0)
                  log("wait",key,undefined) ;

                // We're still in progress
                if (delay > 5000)
                  delay = 5000 + (Math.random()*1000)|0;
                else
                  delay = 1 + (delay * 1.1) |0 ;
                total += delay ;

                if (asyncTimeOut && total > asyncTimeOut * 1000) {
                  // We should have had a response by now. 
                  // If the key has expired, extend the ttl
                  // but answer the client in the -ve. This means
                  // we should take charge of the lock, but
                  // any other clients will keep waiting
                  var script = /* params: keyName=wait-time (in seconds) */
                    "local v = redis.call('TTL',KEYS[1]) if (v <= 0) then redis.call('EXPIRE',KEYS[1],ARGV[1]) return 0 end return v" ;
                  client.eval([script,1,cacheID+key,asyncTimeOut], function(err,ttlset){
                    if (err) {
                      log("nottl",key,err) ;
                      async return undefined ;
                    } else {
                      if (ttlset === 0) {
                        // We extended the TTL, so return `undefined`
                        log("timeout",key, total) ;
                        async return undefined ;
                      } else {
                        // The data exists with a TTL, so keep waiting
                        log("delayed",key, delay) ;
                        asyncTimeOut = ttlset;
                        delay = 1 + (delay/2.6) | 0 ;
                        total = 0 ;
                        //log("poll",key,delay) ;
                        setTimeout(waiting, delay) ;
                      }
                    }
                  })
                } else {
                  //log("poll",key,delay) ;
                  setTimeout(waiting, delay) ;
                }
              } else {
                var parsed = JSON.parse(reply);
                log("reply",key,parsed) ;
                async return parsed ;
              }
            } catch (ex) {
              log("exception",key,ex) ;
              async return undefined ;
            }
          }
          waiting() ;
        },
        set:async function(key,data,ttl) {
          var serialized ;

          // Are we being asked to cache an unresolved promise with no data
          if (data === null)
            serialized = "@null";
          else if (data === undefined) // We can't store undefined, as it's used to indicate a cache miss
            serialized = "@null";
          else if (data && typeof data.then === "function")
            serialized = "@promise" ;
          else
            serialized = JSON.stringify(data) ;

            config.log("set",key,undefined,serialized) ;
            client.setex(cacheID+key, ttl?(ttl/1000)|0:config.defaultTTL, serialized, function(err,reply){
            async return self ;
          }) ;
        },
        has: async function(key) {
          client.type(key,function(err,reply){
            if (err) 
              async throw err;
            else
              async return reply !== 'none';
          })
        },
        'delete':async function(key) {
          config.log("delete",key) ;
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