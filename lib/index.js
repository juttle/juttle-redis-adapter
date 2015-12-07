var redis_utils = require('./utils');

function RedisBackend(config) {
    redis_utils.init(config);

    return {
        name: 'redis',
        read: require('./read'),
        write: require('./write')
    };
}

module.exports = RedisBackend;
