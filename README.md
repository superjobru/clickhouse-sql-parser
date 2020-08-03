An incomplete Rust parser for [Clickhouse](https://clickhouse.tech) SQL dialect.

Inspired by [nom-sql](https://github.com/ms705/nom-sql) and written using [nom](https://github.com/Geal/nom).

Status: basic support for CREATE TABLE statement. Engines options parsed as String. Columns parsed as structs with all options (type, codecs, ttl, comment and so on).

