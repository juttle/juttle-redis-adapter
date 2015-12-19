var redis_utils = require('./utils');

function RedisAdapter(config) {
    redis_utils.init(config);

    return {
        name: 'redis',
        read: require('./read'),
        write: require('./write')
    };
}

module.exports = RedisAdapter;
