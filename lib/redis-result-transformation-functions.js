// Reading from Redis always returns a flat array
// this file contains rules for transforming these
// arrays into arrays of Juttle points given
// a command and its output

module.exports = {
    hmget: function(command, result) {
        var output = {};
        var fields = command.slice(2);
        var values = result[0];
        for (var i = 0; i < fields.length; i++) {
            output[fields[i]] = values[i];
        }

        return [output];
    }
};
