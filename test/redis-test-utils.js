var Promise = require('bluebird');
var path = require('path');
var Redis = require('redis-fast-driver');

var config = {
    host: '127.0.0.1',
    port: 6379,
    path: path.resolve(__dirname, '..')
};

var juttle_test_utils = require('juttle/test').utils;
juttle_test_utils.configureAdapter({
    redis: config
});

var test_client = Promise.promisifyAll(new Redis(config));

var connect_promise = new Promise(function(resolve, reject) {
    test_client.on('ready', resolve);
    test_client.on('error', reject);
});

function clear_redis() {
    return connect_promise.then(function() {
        return test_client.rawCallAsync(['FLUSHALL']);
    });
}

module.exports = {
    clear_redis: clear_redis,
};
