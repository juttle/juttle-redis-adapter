'use strict';

var Promise = require('bluebird');

var utils = require('./utils');

/* global JuttleAdapterAPI */
var AdapterWrite = JuttleAdapterAPI.AdapterWrite;

class WriteRedis extends AdapterWrite {
    write(points) {
        utils.get_redis_client().then((redis) => {
            return Promise.map(points, (pt) => {
                if (pt.hasOwnProperty('redis_write_command')) {
                    var command = pt.redis_write_command.split(' ');
                    return redis.rawCallAsync(command)
                        .catch((err) => {
                            this.trigger('error', 'bad write command: ' + JSON.stringify(command) + ': ' + err.message);
                        });
                } else {
                    this.trigger('warning', 'point without redis_write_command received by write redis, ignoring');
                }
            });
        });
    }

    eof() {
        return Promise.resolve();
    }
}

module.exports = WriteRedis;
