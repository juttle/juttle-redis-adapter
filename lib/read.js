'use strict';

/* global JuttleAdapterAPI */
var AdapterRead = JuttleAdapterAPI.AdapterRead;

var utils = require('./utils');
var transformations = require('./redis-result-transformation-functions');

class ReadRedis extends AdapterRead {
    constructor(options, params) {
        super(options, params);
        if (!options.hasOwnProperty('raw')) {
            throw new Error('must specify a redis query with -raw');
        }

        this.command = options.raw.split(' ');
    }

    static allowedOptions() {
        return ['raw'];
    }

    static requiredOptions() {
        return ['raw'];
    }

    read(from, to, limit, state) {
        var keyword = this.command[0].toLowerCase();

        var transformation = transformations.get(keyword);
        if (!transformation) {
            throw this.runtimeError('unsupported/invalid Redis command: ' + keyword);
        }

        return utils.get_redis_client().then((redis) => {
            return redis.rawCallAsync(this.command);
        })
        .then((result) => {
            return {
                points: transformation(this.command, result),
                eof: true
            };
        });
    }
}

module.exports = ReadRedis;
