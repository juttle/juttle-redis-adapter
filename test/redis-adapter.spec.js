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

var adapter = Redis({
    address: 'localhost',
    port: 6379
}, Juttle);

function _sort_by_value(list) {
    return _.sortBy(list, 'value');
}

Juttle.adapters.register(adapter.name, adapter);

describe('redis source', function() {
    this.timeout(300000);

    after(function() {
        return test_utils.clear_redis();
    });

    function read_redis(command) {
        var program = util.format('read redis -raw "%s"', command);
        return check_juttle({program: program});
    }

    function write_redis(command) {
        var program = util.format('emit -limit 1 | put redis_write_command = "%s" | write redis', command);
        return check_juttle({program: program});
    }

    function test(command, expected_output, massage) {
        massage = massage || function do_nothing(input) { return input; };
        return read_redis(command)
            .then(function(result) {
                expect(result.errors).deep.equal([]);

                var received = massage(result.sinks.table);
                expect(received).deep.equal(expected_output);
            });
    }

    describe('hashset operations', function() {
        var points = [{a: '1', b: '2', c: '3', id: 'test', time: new Date().toISOString()}];
        var stored_points = points.map(function(point) {
            return _.omit(point, 'id');
        });

        it('hmset write', function() {
            var redis_command = 'HMSET ${id} a ${a} b ${b} c ${c} time ${time}';
            var base = 'emit -points %s | put redis_write_command = "%s" | write redis';
            var program = util.format(base, JSON.stringify(points), redis_command);
            return check_juttle({program: program});
        });

        it('hmset read', function() {
            return test('hmset test2 a 1 b 2', []);
        });

        it('hmget', function() {
            var expected = _.pick(stored_points[0], 'a' ,'time');
            return test('hmget test a time', [expected]);
        });

        it('hget', function() {
            return test('hget test a', [{value: '1'}]);
        });

        it('hgetall', function() {
            var program = util.format('hgetall %s', points[0].id);
            return test(program, [stored_points[0]]);
        });

        it('hkeys', function() {
            var expected = [];
            _.each(stored_points[0], function(value, key) { expected.push({key: key}); });
            return test('hkeys test', expected);
        });

        it('hvals', function() {
            var expected = [];
            _.each(stored_points[0], function(value, key) { expected.push({value: value}); });
            return test('hvals test', expected);
        });

        it('hincrby', function() {
            return test('hincrby test a 4', [{value: 5}]);
        });

        it('hincrbyfloat', function() {
            return test('hincrbyfloat test a 0.5', [{value: '5.5'}]);
        });

        it('hset', function() {
            return Promise.all([
                test('hset test aaa 5', [{value: 1}]),
                test('hset test a 5', [{value: 0}])
            ]);
        });

        it('hsetnx', function() {
            return Promise.all([
                test('hsetnx test a 4', [{value: 0}]),
                test('hsetnx test new_hash_key 4', [{value: 1}]),
            ]);
        });

        it('hdel', function() {
            return test('hdel test a', [{value: 1}]);
        });

        it('hexists', function() {
            return Promise.all([
                test('hexists test time', [{value: 1}]),
                test('hexists test a', [{value: 0}])
            ]);
        });

        it.skip('hstrlen', function() { // only in Redis 3.2+
            return test('hstrlen test a', [{value: 1}]);
        });
    });

    describe('string operations', function() {
        it('set write', function() {
            return write_redis('set x hello');
        });

        it('set read', function() {
            return test('set y bananas', []);
        });

        it('append read', function() {
            return test('append y pajamas', [{value: 14}]);
        });

        it('append write', function() {
            return write_redis('append y bananas')
                .then(function() {
                    return test('get y', [{y: 'bananaspajamasbananas'}]);
                });
        });

        it('get', function() {
            return test('get x', [{x: 'hello'}]);
        });

        it('bitcount', function() {
            return test('bitcount x', [{value: 21}]);
        });

        it('bitpos', function() {
            return test('bitpos x 1', [{value: 1}]);
        });

        it('getbit', function() {
            return test('getbit x 4', [{value: 1}]);
        });

        it('getset', function() {
            return test('getset x bye', [{x: 'hello'}])
                .then(function() {
                    return test('get x', [{x: 'bye'}]);
                });
        });

        it('getrange', function() {
            return test('getrange x 1 2', [{value: 'ye'}]);
        });

        it('setrange', function() {
            return test('setrange x 4 thisgotset', [{value: 14}]);
        });

        it('strlen', function() {
            return test('strlen x', [{value: 14}]);
        });

        it('mset', function() {
            return test('mset msk1 one msk2 two msk3 three', []);
        });

        it('rename', function() {
            return test('rename msk1 msk4', []);
        });

        it('renamenx', function() {
            return test('renamenx msk4 msk5', [{value: 1}]);
        });

        it('setbit', function() {
            return test('setbit msk5 0 0', [{value: 0}]);
        });
    });

    describe('list operations', function() {
        it('writes a list with lpush', function() {
            return write_redis('lpush testlist a b c');
        });

        it('brpoplpush', function() {
            return test('brpoplpush testlist bananas 0', [{value: 'a'}]);
        });

        it('lindex', function() {
            return test('lindex testlist 0', [{value: 'c'}]);
        });

        it('linsert', function() {
            return test('linsert testlist after c d', [{value: 3}]);
        });

        it('llen', function() {
            return test('llen testlist', [{value: 3}]);
        });

        it('lpop', function() {
            return test('lpop testlist', [{value: 'c'}]);
        });

        it('lpush', function() {
            return test('lpush testlist aa', [{value: 3}]);
        });

        it('lpushx', function() {
            return test('lpushx testlist aaa', [{value: 4}]);
        });

        it('lrem', function() {
            return test('lrem testlist 1 aaa', [{value: 1}]);
        });

        it('rpop', function() {
            return test('rpop testlist', [{value: 'b'}]);
        });

        it('rpoplpush', function() {
            return test('rpoplpush testlist testlist', [{value: 'd'}]);
        });

        it('rpush', function() {
            return test('rpush testlist e', [{value: 3}]);
        });

        it('rpushx', function() {
            return Promise.all([
                test('rpushx testlist f', [{value: 4}]),
                test('rpushx nosuchlist g', [{value: 0}])
            ]);
        });

        it('blpop', function() {
            return test('blpop testlist testlist testlist 0', [{list: 'testlist', value: 'd'}]);
        });

        it('brpop', function() {
            return test('brpop testlist testlist testlist 0', [{list: 'testlist', value: 'f'}]);
        });

        it('lset', function() {
            return test('lset testlist 0 zero', []);
        });

        it('ltrim', function() {
            return test('ltrim testlist 0 99', []);
        });

        it('lrange', function() {
            var range = [{value: 'zero'}, {value: 'e'}];
            return test('lrange testlist 0 2', range);
        });

        it('linsert write', function() {
            return write_redis('linsert testlist after e f')
                .then(function(result) {
                    expect(result.errors).deep.equal([]);
                    return test('lindex testlist 2', [{value: 'f'}]);
                });
        });
    });

    describe('number operations', function() {
        it('writes a number with set', function() {
            return write_redis('set testnumber 1');
        });

        it('decr', function() {
            return test('decr testnumber', [{value: 0}]);
        });

        it('decrby', function() {
            return test('decrby testnumber 1', [{value: -1}]);
        });

        it('incr', function(){
            return test('incr testnumber', [{value: 0}]);
        });

        it('incrby', function() {
            return test('incrby testnumber 4', [{value: 4}]);
        });

        it('incrbyfloat', function() {
            return test('incrbyfloat testnumber 0.5', [{value: '4.5'}]);
        });
    });

    describe('hyperloglog operations', function() {
        it('pfadd', function() {
            return test('pfadd testpf a b c d e f', [{value: 1}]);
        });

        it('pfcount', function() {
            return test('pfcount testpf', [{value: 6}]);
        });

        it('pfmerge', function() {
            return test('pfmerge testpf2 testpf testpf', []);
        });
    });

    describe('set operations', function() {
        it('sadd', function() {
            return Promise.all([
                test('sadd testset a b c d e', [{value: 5}]),
                test('sadd testsettwo e f g h', [{value: 4}])
            ]);
        });

        it('scard', function() {
            return test('scard testset', [{value: 5}]);
        });

        it('sdiffstore', function() {
            return test('sdiffstore testset testset testset2', [{value: 5}]);
        });

        it('sismember', function() {
            return Promise.all([
                test('sismember testset a', [{value: 1}]),
                test('sismember testset aaa', [{value: 0}])
            ]);
        });

        it('smove', function() {
            return test('smove testset testset a', [{value: 1}]);
        });

        it('srem', function() {
            return test('srem testset a', [{value: 1}]);
        });

        it('sdiff', function() {
            var diff = [{value: 'b'}, {value: 'c'}, {value: 'd'}];
            return test('sdiff testset testsettwo', diff, _sort_by_value);
        });

        it('sinter', function() {
            var inter = [{value: 'e'}];
            return test('sinter testset testsettwo', inter);
        });

        it('smembers', function() {
            var mems = [{value: 'b'}, {value: 'c'}, {value: 'd'}, {value: 'e'}];
            return test('smembers testset', mems, _sort_by_value);
        });

        it('sort', function() {
            var mems = [{value: 'b'}, {value: 'c'}, {value: 'd'}, {value: 'e'}];
            return test('sort testset alpha', mems);
        });

        it('sunion', function() {
            var union = [ { value: 'b' }, { value: 'c' }, { value: 'd' }, { value: 'e' },
                 { value: 'f' }, { value: 'g' }, { value: 'h' } ];
            return test('sunion testset testsettwo', union, _sort_by_value);
        });

        it('spop', function() {
            return read_redis('spop testsettwo')
                .then(function(result) {
                    expect(result.errors).deep.equal([]);
                    var received = result.sinks.table;
                    expect(typeof received[0].value).equal('string');
                });
        });

        it('srandmember', function() {
            return read_redis('srandmember testsettwo')
                .then(function(result) {
                    expect(result.errors).deep.equal([]);
                    var received = result.sinks.table;
                    expect(typeof received[0].value).equal('string');
                });
        });
    });

    describe('sorted set operations', function() {
        it('zadd', function() {
            return test('zadd testsortedset 1 one 2 two 3 three', [{value: 3}]);
        });

        it('zscore', function() {
            return test('zscore testsortedset one', [{value: '1'}]);
        });

        it('zcard', function() {
            return test('zcard testsortedset', [{value: 3}]);
        });

        it('zcount', function() {
            return test('zcount testsortedset 1 2', [{value: 2}]);
        });

        it('zincrby', function() {
            return test('zincrby testsortedset 3 one', [{value: '4'}]);
        });

        it('zinterstore', function() {
            return test('zinterstore newsortedset 2 testsortedset testsortedset WEIGHTS 2 3', [{value: 3}]);
        });

        it('zlexcount', function() {
            return test('zlexcount testsortedset - +', [{value: 3}]);
        });

        it('zrevrank', function() {
            return test('zrevrank testsortedset one', [{value: 0}]);
        });

        it('zrank', function() {
            return test('zrank testsortedset one', [{value: 2}]);
        });

        it('zrange', function() {
            return test('zrange testsortedset 0 1', [{value: 'two'}, {value: 'three'}]);
        });

        it('zrangebylex', function() {
            return test('zrangebylex testsortedset - [u', [{value: 'two'}, {value: 'three'}, {value: 'one'}]);
        });

        it('zrevrangebylex', function() {
            return test('zrevrangebylex testsortedset [u -', [{value: 'one'}, {value: 'three'}, {value: 'two'}]);
        });

        it('zrangebyscore', function() {
            return test('zrangebyscore testsortedset 0 3', [{value: 'two'}, {value: 'three'}]);
        });

        it('zrem', function() {
            return test('zrem testsortedset one', [{value: 1}]);
        });

        it('zremrangebylex', function() {
            return test('zremrangebylex testsortedset [two [three', [{value: 0}]);
        });

        it('zremrangebyrank', function() {
            return test('zremrangebyrank testsortedset 0 1', [{value: 2}]);
        });

        it('zremrangebyscore', function() {
            return test('zremrangebyscore testsortedset 0 1', [{value: 0}]);
        });

        it('zunionstore', function() {
            return test('zunionstore out 2 testsortedset testsortedset', [{value: 0}]);
        });
    });

    describe('miscellaneous operations', function() {
        it('config get', function() {
            return test('config get port', [{port: '6379'}]);
        });

        it('config set', function() {
            return test('config set loglevel notice', []);
        });

        it('config resetstat', function() {
            return test('config resetstat', []);
        });

        it('del', function() {
            return write_redis('set deleteme 4')
                .then(function() {
                    return test('del deleteme', [{value: 1}]);
                });
        });

        it('echo', function() {
            return test('echo hello', [{value: 'hello'}]);
        });

        it('exists', function() {
            return Promise.all([
                test('exists x', [{value: 1}]),
                test('exists thisdoesnotexist', [{value: 0}])
            ]);
        });

        it('lastsave', function() {
            return read_redis('lastsave')
                .then(function(result) {
                    expect(result.errors).deep.equal([]);
                    var received = result.sinks.table;
                    expect(typeof received[0].value).equal('number');
                });
        });

        it('msetnx', function() {
            return test('msetnx mstk1 1 mstk2 2', [{value: 1}]);
        });

        it('randomkey', function() {
            return read_redis('randomkey')
                .then(function(result) {
                    expect(result.errors).deep.equal([]);
                    var received = result.sinks.table;
                    expect(typeof received[0].value).equal('string');
                });
        });

        it('select', function() {
            return test('select 0', []);
        });

        it('setex', function() {
            return test('setex doomed 10 thisisdoomed', []);
        });

        it('keys', function() {
            return read_redis('keys *')
                .then(function(result) {
                    expect(result.errors).deep.equal([]);
                    var received = result.sinks.table;
                    expect(received.length).at.least(1);
                });
        });
    });
});
