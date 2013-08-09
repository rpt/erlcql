# erlCQL [![Build Status][travis_ci_image]][travis_ci]

Cassandra native protocol CQL Erlang client.

## How to use it?

 * Run `make` to build.
 * Add as a dependency to your `rebar.config`:

``` erlang
{deps, [{erlcql, "^0\.1",
         {git, "https://github.com/rpt/erlcql.git"}, {tag, "0.1.0"}}]}.
```

[travis_ci]: https://travis-ci.org/rpt/erlcql
[travis_ci_image]: https://travis-ci.org/rpt/erlcql.png
