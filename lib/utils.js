var Promise = require('bluebird');
var Redis = require('redis-fast-driver');

var redis_promise;

function get_redis_client() {
    return redis_promise;
}

function init(config) {
    var redis = Promise.promisifyAll(new Redis(config));
    redis_promise = new Promise(function(resolve, reject) {
        redis.on('ready', function() {
            resolve(redis);
        });
        redis.on('error', reject);
    });
}

module.exports = {
    init: init,
    get_redis_client: get_redis_client
};
