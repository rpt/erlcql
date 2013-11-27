# erlCQL [![Build Status][travis_ci_image]][travis_ci]

Cassandra native protocol CQL Erlang client.

## API

### Start

``` erlang
erlcql:start_link(Options :: proplists:proplist()) ->
    {ok, Pid :: pid()} | {error, Reason :: term()}.
```

#### Options

| Option      | Type                    | Default           |
|:----------- |:----------------------- |:----------------- |
| host        | string()                | `"localhost"`     |
| port        | integer()               | `9042`            |
| compression | false &#124; snappy     | `false`           |
| tracing     | boolean()               | `false`           |
| username    | bitstring()             | `<<"cassandra">>` |
| password    | bitstring()             | `<<"cassandra">>` |

## Notes

### Versions

Supported versions: [`v1`][proto_v1].

[travis_ci]: https://travis-ci.org/rpt/erlcql
[travis_ci_image]: https://travis-ci.org/rpt/erlcql.png
[proto_v1]:
https://raw.github.com/apache/cassandra/trunk/doc/native_protocol_v1.spec
