# Juttle Redis Adapter

[![Build Status](https://travis-ci.org/juttle/juttle-redis-adapter.svg)](https://travis-ci.org/juttle/redis-adapter)

The Juttle Redis Adapter enables interaction with [Redis](http://redis.io/). It works with Redis version 3.0 and above.

## Examples

Write a Hash into Redis at the key `my_hash` with values `a = 1`, `b = 2`, `c = 3`:
```juttle
emit -limit 1
| put a = 1, b = 2, c = 3, id = 'my_hash', redis_write_command = 'HMSET ${id} a ${a} b ${b} c ${c} time ${time}'
| write redis
```

Retrieve the value stored at `my_hash:a` above as the point `{value: 1}`:

```juttle
read redis -raw "HGET my_hash a"
```

## Installation

Like Juttle itself, the adapter is installed as a npm package. Both Juttle and
the adapter need to be installed side-by-side:

```bash
$ npm install juttle
$ npm install juttle-redis-adapter
```

## Configuration

The adapter needs to be registered and configured so that it can be used from
within Juttle. To do so, add the following to your `~/.juttle/config.json` file:

```json
{
    "adapters": {
        "juttle-redis-adapter": {
            "host": "localhost",
            "port": 6379
        }
    }
}
```

To connect to a Redis instance elsewhere, change the `address`
and `port` in this configuration.

## Usage

### Read options


Name | Type | Required | Description | Default
-----|------|----------|-------------|---------
`raw` | string | yes | redis command to run | none

### Write

`write` takes no options. It simply executes the Redis command stored in the `redis_write_command` field of each point it processes. To format the `redis_write_command` with fields from the point, use [string templating](https://github.com/juttle/juttle/blob/master/docs/concepts/programming_constructs.md#string-templating) as in the example.

## Contributing

Want to contribute? Awesome! Don’t hesitate to file an issue or open a pull
request.
