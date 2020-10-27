# afn-redis-cache

A shared network cache for https://github.com/MatAtBread/afn to speed up clustered API servers

Installation
------------

	npm install --save afn-redis-cache
	
Usage
-----

Require the module

	var configureCache = require('afn-redis-cache') ;
	
Configure the cache and get a function to create caches with that configuration
You can create multiple 'cacheCreators' with different configurations by calling configureCache() multiple times

	var cacheCreator = configureCache({
		defaultTTL:60	// 1 minute by default
		redis:			// Any valid redis createClient parameter (see https://github.com/NodeRedis/node_redis/blob/master/README.md#options-object-properties)
			"redis":"redis://127.0.0.1:6379/13"		//eg. a 'redis_url'
	}) ;
	
Now create a cache	
	
	// "id" is an optional alphanumeric string prepended to cache entries,
	// so you can (on the same redis DB) create multiple caches that won't collide 
	var cache = cacheCreator.createCache(id) ;
	
Use the cache	
	
	// Async cache API - a subset of a JavaScript Map API
	await cache.get(key);
	await cache.set(key,data,ttl);
	await cache.delete:(key);
	await cache.keys();
	