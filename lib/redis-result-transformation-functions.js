// Reading from Redis always returns a flat array
// this file contains rules for transforming these
// arrays into arrays of Juttle points given
// a command and its output

function expect_ok(result) {
    if (result === 'OK') {
        return [];
    } else {
        throw new Error('command failed: ' + JSON.stringify(result));
    }
}

module.exports = {
    get: function get(keyword) {
        switch (keyword) {
            case 'hmget':
                return function object_for_hmget(command, result) {
                    var output = {};
                    var fields = command.slice(2);
                    var values = result[0];
                    for (var i = 0; i < fields.length; i++) {
                        output[fields[i]] = values[i];
                    }

                    return [output];
                };
            case 'get':
            case 'getset':
                return function extract_kv(command, result) {
                    var output = {};
                    output[command[1]] = result[0];
                    return [output];
                };
            case 'append':
            case 'bitcount':
            case 'bitpos':
            case 'brpoplpush':
            case 'decr':
            case 'decrby':
            case 'del':
            case 'echo':
            case 'exists':
            case 'getbit':
            case 'getrange':
            case 'hdel':
            case 'hexists':
            case 'hget':
            case 'hincrby':
            case 'hincrbyfloat':
            case 'hset':
            case 'hsetnx':
            case 'hstrlen':
            case 'incr':
            case 'incrby':
            case 'incrbyfloat':
            case 'lastsave':
            case 'lindex':
            case 'linsert':
            case 'llen':
            case 'lpop':
            case 'lpush':
            case 'lpushx':
            case 'lrem':
            case 'msetnx':
            case 'pfadd':
            case 'pfcount':
            case 'randomkey':
            case 'renamenx':
            case 'rpop':
            case 'rpoplpush':
            case 'rpush':
            case 'rpushx':
            case 'sadd':
            case 'scard':
            case 'sdiffstore':
            case 'setbit':
            case 'setrange':
            case 'smove':
            case 'sismember':
            case 'smove':
            case 'srem':
            case 'strlen':
            case 'zadd':
            case 'zcard':
            case 'zcount':
            case 'zincrby':
            case 'zinterstore':
            case 'zlexcount':
            case 'zrank':
            case 'zrem':
            case 'zremrangebylex':
            case 'zremrangebyrank':
            case 'zremrangebyscore':
            case 'zrevrank':
            case 'zscore':
            case 'zunionstore':
                return function pluck_first(command, result) {
                    return [{
                        value: result[0]
                    }];
                };
            case 'config':
                return function handle_config_result(command, result) {
                    var subcommand = command[1].toLowerCase();
                    switch (subcommand) {
                        case 'get':
                            var obj = {};
                            var config = result[0];
                            for (var i = 0; i < config.length; i += 2) {
                                obj[config[i]] = config[i+1];
                            }
                            return [obj];

                        case 'set':
                        case 'resetstat':
                            return expect_ok(result[0]);

                        default:
                            throw new Error('Unsupported config subcommand: ' + subcommand);
                    }
                };
            case 'blpop':
            case 'brpop':
                return function extract_list_and_value(command, result) {
                    var list_and_value = result[0];
                    return [{
                        list: list_and_value[0],
                        value: list_and_value[1]
                    }];
                };
            case 'hmset':
            case 'lset':
            case 'ltrim':
            case 'mset':
            case 'pfmerge':
            case 'rename':
            case 'select':
            case 'set':
            case 'setex':
                return function(command, result) {
                    return expect_ok(result[0]);
                };
            case 'hgetall':
                return function point_from_kv_array(command, result) {
                    var pt = {};
                    var keys_and_values = result[0];
                    for (var i = 0; i < keys_and_values.length; i += 2) {
                        pt[keys_and_values[i]] = keys_and_values[i+1];
                    }
                    return [pt];
                };
            case 'hkeys':
            case 'keys':
                return function as_key_objects(command, result) {
                    var pts = [];
                    var items = result[0];
                    for (var i = 0; i < items.length; i++) {
                        pts.push({key: items[i]});
                    }

                    return pts;
                };
            case 'hvals':
            case 'lrange':
            case 'sdiff':
            case 'sinter':
            case 'smembers':
            case 'sort':
            case 'spop':
            case 'srandmember':
            case 'sunion':
            case 'zrange':
            case 'zrangebylex':
            case 'zrangebyscore':
            case 'zrevrangebylex':
                return function as_value_objects(command, result) {
                    var pts = [];
                    var items = result[0];
                    for (var i = 0; i < items.length; i++) {
                        pts.push({value: items[i]});
                    }

                    return pts;
                };
            default:
                return null;
        }
    }
};
