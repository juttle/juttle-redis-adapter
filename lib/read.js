var Promise = require('bluebird');

var Juttle = require('juttle/lib/runtime').Juttle;
var utils = require('./utils');
var transformations = require('./redis-result-transformation-functions');

var Read = Juttle.proc.source.extend({
    procName: 'redis_read',

    initialize: function(options, params) {
        if (!options.hasOwnProperty('raw')) {
            throw new Error('must specify a redis query with -raw');
        }

        this.command = options.raw.split(' ');
    },

    start: function() {
        var self = this;
        var keyword = this.command[0].toLowerCase();

        var transformation = transformations.get(keyword);
        if (!transformation) {
            self.trigger('error', new Error('unsupported/invalid Redis command: ' + keyword));
            return self.emit_eof();
        }
        return utils.get_redis_client().then(function(redis) {
            return redis.rawCallAsync(self.command);
        })
        .then(function(result) {
            var points = transformation(self.command, result);
            if (points.length > 0) {
                self.emit(points);
            }
        })
        .catch(function(err) {
            self.trigger('error', new Error(err.message));
        })
        .finally(function() {
            self.emit_eof();
        });
    }
});

module.exports = Read;
