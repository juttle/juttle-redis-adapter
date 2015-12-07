var Promise = require('bluebird');
var _ = require('underscore');
var util = require('util');
var expect = require('chai').expect;
var retry = require('bluebird-retry');
var Redis = require('redis-fast-driver');

var test_client = Promise.promisifyAll(new Redis({
    host: '127.0.0.1',
    port: 6379
}));

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
