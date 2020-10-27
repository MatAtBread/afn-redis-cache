'use strict';
var redis = require('redis');
function redisSafePattern(s) {
  return s.replace(/([.?*\[\]])/g, "\\$1");
}

module.exports = function (config) {
  if (!config || !config.redis) {
    throw new Error("REDIS configuration missing");
  }
  config.log = config.log || function () { };
  var redisArgs = [], redisOpts;
  if (typeof config.redis === "string") {
    redisArgs = [config.redis, redisOpts = {}];
  } else {
    redisArgs = [redisOpts = config.redis];
  }
  if (!('enable_offline_queue' in redisOpts)) {
    redisOpts.enable_offline_queue = true;
    redisOpts.connect_timeout = 1000;
  }
  redisOpts.retry_strategy = redisOpts.retry_strategy || function (options) {
    config.log("redis-retry", "", undefined, options);
    setTimeout(connectRedis, 10000);
    return undefined;
  };
  var client;
  function connectRedis() {
    if (client && client.connected)
      return;
    client = redis.createClient.apply(redis, redisArgs);
    client.on('error', function (err) {
      config.log("redis-error", "", undefined, err);
    });
  }

  connectRedis();
  if (!('defaultTTL' in config))
    config.defaultTTL = 60;
  if (!('asyncTimeOut' in config))
    config.asyncTimeOut = config.defaultTTL;
  if (typeof config.log !== 'function')
    config.log = function () { };
  return {
    hashEncoding: 'base64',
    createCache: function (cacheID) {
      if (cacheID)
        cacheID += ":";
      else
        cacheID = "";
      var self;
      return self = {
        name: typeof config.redis === 'string' ? config.redis : JSON.stringify(config.redis),
        get: function (key, options) {
          return new Promise(function ($return, $error) {
            var start = Date.now(), log;
            options = Object.assign({}, config, options);
            if (typeof options.log !== 'function')
              log = function () { };
            else
              log = function (msg, key, data) {
                options.log(msg, key, Date.now() - start, data);
              };
            var delay = 25 + Math.random() * 20 | 0;
            var total = 0;
            var asyncTimeOut = options.asyncTimeOut;
            function waiting() {
              var script = "local v = redis.call('GET',KEYS[1]) if (not v) then redis.call('SET',KEYS[1],'@promise','EX',ARGV[1]) return '@new' end return v";
              client.eval([script, 1, cacheID + key, asyncTimeOut], handleRedisResponse);
            }

            function handleRedisResponse(err, reply) {
              if (err) {
                log("error", key, err);
                return $return(undefined);
              }
              if (reply === null) {
                log("miss", key);
                return $return(undefined);
              }
              try {
                if (reply.slice(0, 4) === "@new") {
                  log("new", key);
                  return $return(undefined);
                }
                if (reply.slice(0, 5) === "@null") {
                  log("reply", key, null);
                  return $return(null);
                }
                if (reply.slice(0, 8) === "@promise") {
                  if (total === 0)
                    log("wait", key, undefined);
                  if (delay > 5000)
                    delay = 5000 + Math.random() * 1000 | 0;
                  else
                    delay = 1 + delay * 1.1 | 0;
                  total += delay;
                  if (asyncTimeOut && total > asyncTimeOut * 1000) {
                    var script = "local v = redis.call('TTL',KEYS[1]) if (v <= 0) then redis.call('EXPIRE',KEYS[1],ARGV[1]) return 0 end return v";
                    client.eval([script, 1, cacheID + key, asyncTimeOut], function (err, ttlset) {
                      if (err) {
                        log("nottl", key, err);
                        return $return(undefined);
                      } else {
                        if (ttlset === 0) {
                          log("timeout", key, total);
                          return $return(undefined);
                        } else {
                          log("delayed", key, delay);
                          asyncTimeOut = ttlset;
                          delay = 1 + delay / 2.6 | 0;
                          total = 0;
                          setTimeout(waiting, delay);
                        }
                      }
                    });
                  } else {
                    setTimeout(waiting, delay);
                  }
                } else {
                  var parsed = JSON.parse(reply);
                  log("reply", key, parsed);
                  return $return(parsed);
                }
              } catch (ex) {
                log("exception", key, ex);
                return $return(undefined);
              }
            }

            waiting();
          });
        },
        set: function (key, data, ttl) {
          return new Promise(function ($return, $error) {
            var serialized;
            if (data === null)
              serialized = "@null";
            else if (data === undefined)
              serialized = "@null";
            else if (data && typeof data.then === "function")
              serialized = "@promise";
            else
              serialized = JSON.stringify(data);
            config.log("set", key, undefined, serialized);
            client.setex(cacheID + key, ttl ? ttl / 1000 | 0 : config.defaultTTL, serialized, function (err, reply) {
              return $return(self);
            });
          });
        },
        has: function (key) {
          return new Promise(function ($return, $error) {
            client.type(key, function (err, reply) {
              if (err)
                return $error(err);
              else
                return $return(reply !== 'none');
            });
          });
        },
        peek: function (key) {
          return new Promise(function ($return, $error) {
            client.get(cacheID + key, function (err, reply) {
              if (err)
                return $error(err);
              if (reply === null)
                return $return(undefined);
              if (reply[0] !== "@")
                return $return(JSON.parse(reply));
              client.ttl(cacheID + key, function (err, ttl) {
                if (err)
                  return $error(err);
                if (+ttl < 0 || isNaN(+ttl))
                  return $return(undefined);
                if (reply.slice(0, 4) === "@null") {
                  return $return({
                    expires: Date.now() + ttl * 1000,
                    value: null
                  });
                }
                if (reply.slice(0, 8) === "@promise") {
                  return $return({
                    expires: Date.now() + ttl * 1000
                  });
                }
                return $error(new Error("Inavlid cache marker"));
              });
            });
          });
        },
        'delete': function (key) {
          return new Promise(function ($return, $error) {
            config.log("delete", key);
            client.del(cacheID + key, function (err, reply) {
              if (err)
                return $return(false);
              return $return(true);
            });
          });
        },
        expireKeys: function (now) {
          return new Promise(function ($return, $error) {
            return $return();
          });
        },
        clear: function () {
          return new Promise(function ($return, $error) {
            client.keys(redisSafePattern(cacheID) + "*", function (err, reply) {
              if (err)
                return $return();
              client.del(reply, function (err, reply) {
                return $return();
              });
            });
          });
        },
        keys: function () {
          return new Promise(function ($return, $error) {
            client.keys(redisSafePattern(cacheID) + "*", function (err, reply) {
              if (err)
                return $return([]);
              try {
                return $return(reply);
              } catch (ex) {
                return $return([]);
              }
            });
          });
        }
      };
    }
  };
};
