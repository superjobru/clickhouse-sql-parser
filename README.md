An incomplete Rust parser for [Clickhouse](https://clickhouse.tech) SQL dialect.

Inspired by [nom-sql](https://github.com/ms705/nom-sql) and written using [nom](https://github.com/Geal/nom).

Status: basic support for CREATE TABLE statement. Engines options parsed as String. Columns parsed as structs with all options (type, codecs, ttl, comment and so on).

```
# cargo b --example parse
 ...
     Running `target/debug/examples/parse`

SQL statement: CREATE TABLE `default`.access (
  `remote_addr` String CODEC(ZSTD(1)),
  `remote_addr_long` Int32
) ENGINE = DISTRIBUTED( 'cluster', '', 'access', rand());

[examples/parse.rs:20] &schema = CreateTable(
    CreateTableStatement {
        table: Table {
            name: "access",
            alias: None,
            schema: Some(
                "default",
            ),
        },
        fields: [
            ColumnSpecification {
                column: Column {
                    name: "remote_addr",
                    alias: None,
                    table: Some(
                        "access",
                    ),
                },
                sql_type: String,
                codec: Some(
                    CodecList(
                        [
                            ZSTD(
                                Some(
                                    1,
                                ),
                            ),
                        ],
                    ),
                ),
                ttl: None,
                nullable: false,
                option: None,
                comment: None,
                lowcardinality: false,
            },
            ColumnSpecification {
                column: Column {
                    name: "remote_addr_long",
                    alias: None,
                    table: Some(
                        "access",
                    ),
                },
                sql_type: Int(
                    B32,
                ),
                codec: None,
                ttl: None,
                nullable: false,
                option: None,
                comment: None,
                lowcardinality: false,
            },
        ],
        engine: Distributed(
            EngineDistributed {
                cluster_name: "\'cluster\'",
                schema: "\'\'",
                table: "\'access\'",
                sharding_key: Some(
                    "rand()",
                ),
                policy_name: None,
            },
        ),
    },
)
```

