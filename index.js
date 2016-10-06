'use nodent-promise';

/* An afn memo cache based on REDIS for clustered services */

var redis = require('redis') ;

module.exports = function(config){
    var client = redis.createClient(config.redis) ;
    
    return {
        hashEncoding:'base64',
        createCache: function(cacheID) {
            if (cacheID) cacheID += ":" ;
            else cacheID = "" ;
            return {
                get:async function(key) {
                    client.get(cacheID+key,function(err,reply){
                        if (err)
                            async throw err ;
                        try {
                            async return JSON.parse(reply) ;
                        } catch (ex) {
                            async throw ex ;
                        }
                    }) ;
                },
                set:async function(key,data,ttl) {
                    client.setex(cacheID+key, ttl?ttl/1000:(config.defaultTTL || 60), JSON.stringify(data), function(err,reply){
                        if (err) async throw err ; ;
                        async return reply ;
                    }) ;
                },
                'delete':async function(key) {
                    client.del(cacheID+key, function(err,reply){
                        if (err) async throw err ; ;
                        async return reply ;
                    }) ;
                },
                keys:async function() {
                    client.keys(cacheID+":*",function(err,reply){
                        if (err)
                            async throw err ;
                        try {
                            async return JSON.parse(reply) ;
                        } catch (ex) {
                            async throw ex ;
                        }
                    }) ;
                }
            }
        } 
    }
} ;