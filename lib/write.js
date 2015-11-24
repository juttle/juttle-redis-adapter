var Promise = require('bluebird');

var Juttle = require('juttle/lib/runtime').Juttle;
var utils = require('./utils');

var Write = Juttle.proc.sink.extend({
    procName: 'redis_write',
    initialize: function(options) {
        var self = this;
        this.in_progress_writes = 0;
    },
    process: function(points) {
        var self = this;
        this.in_progress_writes++;
        utils.get_redis_client().then(function(redis) {
            return Promise.map(points, function(pt) {
                if (pt.hasOwnProperty('redis_write_command')) {
                    var command = pt.redis_write_command.split(' ');
                    return redis.rawCallAsync(command)
                        .catch(function(err) {
                            self.trigger('error', 'bad write command: ' + JSON.stringify(command) + ': ' + err.message);
                        });
                }
            });
        })
        .finally(function() {
            self.in_progress_writes--;
            self._maybe_done();
        });
    },
    _maybe_done: function() {
        if (this.in_progress_writes === 0) {
            this.emit_eof();
            this.done();
        }
    },
    eof: function(from) {
        this._maybe_done();
    }
});

module.exports = Write;
