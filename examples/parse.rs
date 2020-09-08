// vim: set expandtab ts=4 sw=4:
extern crate clickhouse_sql_parser;

fn main() {
    let sql = r#"
CREATE TABLE `default`.`access` (
    `remote_addr` String CODEC(ZSTD(1)),
    `remote_addr_long` Int32
) ENGINE = Distributed('cluster', '', 'access', rand());
    "#.trim();

    let (_rest, schema) = clickhouse_sql_parser::sql_query(sql.as_bytes()).unwrap();
    println!("SQL statement: {}", schema);
    dbg!(&schema);
}
