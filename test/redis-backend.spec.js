var _ = require('underscore');
var Promise = require('bluebird');
var retry = require('bluebird-retry');
var expect = require('chai').expect;
var util = require('util');

var juttle_test_utils = require('juttle/test/runtime/specs/juttle-test-utils');
var check_juttle = juttle_test_utils.check_juttle;
var Juttle = require('juttle/lib/runtime').Juttle;
var Redis = require('../lib');
var test_utils = require('./redis-test-utils');

var backend = Redis({
    address: 'localhost',
    port: 6379
}, Juttle);

Juttle.backends.register(backend.name, backend);

describe('redis source', function() {
    this.timeout(300000);

    describe('HMSET/HMGET', function() {
        var points = [{a: '1', id: 'test', time: new Date().toISOString()}];

        it('writes points with HMSET', function() {
            var program = util.format('emit -points %s | put redis_write_command = "HMSET ${id} a ${a} time ${time}" | write redis', JSON.stringify(points));
            return check_juttle({
                program: program
            });
        });

        it('reads points with HMGET', function() {
            var program = 'read redis -raw "HMGET test a time"';
            return check_juttle({
                program: program
            })
            .then(function(result) {
                var expected = points.map(function(point) {
                    return _.omit(point, 'id');
                });
                expect(result.sinks.table).deep.equal(expected);
            });
        });
    });
});
