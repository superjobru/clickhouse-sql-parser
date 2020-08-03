// vim: set expandtab ts=4 sw=4:
use std::str;
use std::str::FromStr;
use std::fmt; 

use nom::{
    IResult,
    error::{ ErrorKind, ParseError},
    branch::alt,
    sequence::{delimited, preceded, tuple},
    combinator::{map, opt, recognize},
    character::complete::{digit1, multispace0, multispace1, one_of, },
    bytes::complete::{tag, tag_no_case, take_until, },
    multi::{many0, separated_list,},
};

use crate::{
    sql_identifier,
    ws_sep_comma,
    column_identifier_no_alias,
    SqlTypeOpts,
    type_identifier,
    ttl_expression,
    statement_terminator,
    schema_table_reference,
    sql_expression,
};
use crate::column::{
    ColumnSpecification,
    ColumnOption,
    Column,
};
use crate::table::Table;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CreateTableStatement {
    pub table: Table,
    pub fields: Vec<ColumnSpecification>,
    //pub indexes: Vec<..>,
    pub engine: Engine,
}

impl fmt::Display for CreateTableStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CREATE TABLE {} (\n",
            self.table
        )?;
        write!(f, "  {}",
            self.fields
                .iter()
                .map(|c| format!("{}", c)) 
                .collect::<Vec<String>>()
                .join(",\n  ")
        )?;
        write!(f,"\n) {};", self.engine)
    }
}


#[derive(Debug, PartialEq)]
pub enum CodecError<I> {
  Nom(I, ErrorKind),
}

impl<I> ParseError<I> for CodecError<I> {
  fn from_error_kind(input: I, kind: ErrorKind) -> Self {
    CodecError::Nom(input, kind)
  }

  fn append(_: I, _: ErrorKind, other: Self) -> Self {
    other
  }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum CodecDeltaLevel {
    L1,
    L2,
    L4,
    L8,
}
impl From<char> for CodecDeltaLevel {
    fn from(t: char) -> CodecDeltaLevel {
        match t {
            '1' => CodecDeltaLevel::L1,
            '2' => CodecDeltaLevel::L2,
            '4' => CodecDeltaLevel::L4,
            '8' => CodecDeltaLevel::L8,
            l => panic!("Unsupported level '{}' for codec delta", l),
        }
    }
}
impl fmt::Display for CodecDeltaLevel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CodecDeltaLevel::L1 => write!(f, "1"),
            CodecDeltaLevel::L2 => write!(f, "2"),
            CodecDeltaLevel::L4 => write!(f, "4"),
            CodecDeltaLevel::L8 => write!(f, "8"),
        }
    }
}


#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Codec {
    None,
    ZSTD(Option<u8>), // from 1 to 22
    LZ4,
    LZ4HC(Option<u8>), // from 1 to 12
    Delta(Option<CodecDeltaLevel>), // 1, 2, 4, 8
    DoubleDelta,
    Gorilla,
    T64,
}
impl fmt::Display for Codec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Codec::None => write!(f, "NONE"),
            Codec::LZ4 => write!(f, "LZ4"),
            Codec::DoubleDelta => write!(f, "DoubleDelta"),
            Codec::Gorilla => write!(f, "Gorilla"),
            Codec::T64 => write!(f, "T64"),
            Codec::ZSTD(None) => write!(f, "ZSTD"),
            Codec::ZSTD(Some(l)) => write!(f, "ZSTD({})", l),
            Codec::LZ4HC(None) => write!(f, "LZ4HC"),
            Codec::LZ4HC(Some(l)) => write!(f, "LZ4HC({})", l),
            Codec::Delta(None) => write!(f, "Delta"),
            Codec::Delta(Some(l)) => write!(f, "Delta({})", l),
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CodecList(pub Vec<Codec>);

impl fmt::Display for CodecList {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}",
            self.0.iter().map(|c| format!("{}", c)).collect::<Vec<String>>().join(", ")
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ColumnTTL {
    column: String,
    interval: Option<String>,
}
impl fmt::Display for ColumnTTL {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,"TTL {}", self.column)?;

        match self.interval {
            Some(ref interval) => write!(f, " + {}", interval),
            None => Ok(()),
        }
    }
}

impl<'a> From<&'a str> for ColumnTTL {
    fn from(t: &str) -> ColumnTTL {
        ColumnTTL {
            column: String::from(t),
            interval: None,
        }
    }
}
impl<'a> From<(&'a str, &'a str)> for ColumnTTL {
    fn from(t: (&str, &str)) -> ColumnTTL {
        ColumnTTL {
            column: String::from(t.0),
            interval: Some(String::from(t.1)),
        }
    }
}


#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Engine {
    Distributed(EngineDistributed),
    Memory,
    MergeTree(EngineMergeTree),
    ReplicatedMergeTree(EngineReplicatedMergeTree),
}

impl fmt::Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Engine::Distributed(e) => write!(f, "ENGINE = {}", e),
            Engine::Memory => write!(f, "ENGINE = Memory"),
            Engine::MergeTree(e) => write!(f, "ENGINE = {}", e),
            Engine::ReplicatedMergeTree(e) => write!(f, "ENGINE = {}", e),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct EngineDistributed {
    cluster_name: String,
    schema: String,
    table: String,
    // The sharding expression can be any expression from constants and table
    // columns that returns an integer.
    sharding_key: Option<String>,
    policy_name: Option<String>,
}

impl fmt::Display for EngineDistributed {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,"DISTRIBUTED( {}, {}, {}",
            self.cluster_name,
            match self.schema.as_str() {
                "" => "''",
                s => s,
            },
            self.table
        )?;
        if let Some(ref expr) = self.sharding_key {
            write!(f,", {}", expr)?;
            if let Some(ref name) = self.policy_name {
                write!(f,", {}", name)?;
            }
        }
        write!(f,")")
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct EngineMergeTree(String);

impl fmt::Display for EngineMergeTree {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // write `self` cause `stack overflow` - why?
        write!(f,"{}", self.0)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct EngineReplicatedMergeTree(String);

impl fmt::Display for EngineReplicatedMergeTree {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // write `self` cause `stack overflow` - why?
        write!(f,"{}", self.0)
    }
}

pub fn creation(i: &[u8]) -> IResult<&[u8], CreateTableStatement>
{
    let (remaining_input, (_, _, _, _, table, _, _, _, fields_list, _, _, _, engine, _)) =
        tuple((
            tag_no_case("create"),
            multispace1,
            tag_no_case("table"),
            multispace1,
            schema_table_reference,
            multispace0,
            tag("("),
            multispace0,
            field_specification_list,
            multispace0,
            tag(")"),
            multispace0,
            engine_spec,
            opt(statement_terminator),
        ))(i)?;

    // "table AS alias" isn't legal in CREATE statements
    assert!(table.alias.is_none());
    // attach table names to columns:
    let fields = fields_list
        .into_iter()
        .map(|field| {
            let column = Column {
                table: Some(table.name.clone()),
                ..field.column
            };

            ColumnSpecification { column, ..field }
        })
        .collect();

    Ok((
        remaining_input,
        CreateTableStatement {
            table,
            fields,
            //indexes,
            engine,
        },
    ))
}

fn engine_distributed(i: &[u8]) -> IResult<&[u8], Engine> {
    // Distributed(logs, default, hits[, sharding_key[, policy_name]])
    map(
        tuple((
            tag_no_case("Distributed"),
            multispace0,
            tag("("),
            multispace0,
            sql_expression, // cluster
            ws_sep_comma,
            alt((
                sql_expression, // schema
                map(tag("''"), |_| "".as_bytes()),
            )),
            ws_sep_comma,
            sql_expression, // table
            opt(tuple((
                ws_sep_comma,
                sql_expression, // sharding_key
                opt(tuple((
                    ws_sep_comma,
                    sql_identifier, // policy_name
                ))),
            ))),
            multispace0,
            tag(")")
        )),
        |(_,_,_,_,cluster,_,schema,_,table,_opt,_,_)| {
            Engine::Distributed(EngineDistributed {
                cluster_name: str::from_utf8(cluster).unwrap().into(),
                schema: str::from_utf8(schema).unwrap().into(),
                table: str::from_utf8(table).unwrap().into(),
                sharding_key: None, // FIXME
                policy_name: None, // FIXME
            })
        }
    )(i)
}

fn engine_merge_tree(i: &[u8]) -> IResult<&[u8], Engine> {
    // MergeTree PARTITION BY toYYYYMMDD(eventDate) PRIMARY KEY metric ORDER BY metric SETTINGS index_granularity = 8192
    // ENGINE = MergeTree()
    // [PARTITION BY expr]
    // [ORDER BY expr]
    // [PRIMARY KEY expr]
    // [SAMPLE BY expr]
    map(
        recognize(tuple((
            tag_no_case("MergeTree"),
            many0(alt((
                engine_merge_tree_partition,
                engine_merge_tree_orderby,
                engine_merge_tree_primary,
                engine_merge_tree_sample,
                engine_merge_tree_ttl,
                engine_merge_tree_settings,
            ))),
        ))),
        |s| {
            Engine::MergeTree(EngineMergeTree(str::from_utf8(s).unwrap().to_string()))
        }
    )(i)
}
fn engine_merge_tree_partition(i: &[u8]) -> IResult<&[u8], &[u8]> {
    recognize(tuple((
        multispace1,
        tag_no_case("PARTITION"),
        multispace1,
        tag_no_case("BY"),
        multispace1,
        sql_expression,
    )))(i)
}
fn engine_merge_tree_orderby(i: &[u8]) -> IResult<&[u8], &[u8]> {
    recognize(tuple((
        multispace1,
        tag_no_case("ORDER"),
        multispace1,
        tag_no_case("BY"),
        multispace1,
        sql_expression,
    )))(i)
}
fn engine_merge_tree_primary(i: &[u8]) -> IResult<&[u8], &[u8]> {
    recognize(tuple((
        multispace1,
        tag_no_case("PRIMARY"),
        multispace1,
        tag_no_case("BY"),
        multispace1,
        sql_expression,
    )))(i)
}
fn engine_merge_tree_sample(i: &[u8]) -> IResult<&[u8], &[u8]> {
    recognize(tuple((
        multispace1,
        tag_no_case("SAMPLE"),
        multispace1,
        tag_no_case("BY"),
        multispace1,
        sql_expression,
    )))(i)
}
fn engine_merge_tree_ttl(i: &[u8]) -> IResult<&[u8], &[u8]> {
    // TTL [expr [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx']], ...
    recognize(tuple((
        multispace0,
        tag_no_case("TTL"),
        multispace0,
        separated_list(ws_sep_comma, tuple((
            ttl_expression,
            multispace0,
            opt(tuple((
                alt((
                    tag_no_case("delete"),
                    tag_no_case("delete"),
                    recognize(tuple((
                        tag_no_case("to"),
                        multispace1,
                        tag_no_case("disk"),
                        multispace1,
                        delimited(tag("'"), take_until("'"), tag("'")),
                    ))),
                    recognize(tuple((
                        tag_no_case("to"),
                        multispace1,
                        tag_no_case("volume"),
                        multispace1,
                        delimited(tag("'"), take_until("'"), tag("'")),
                    ))),
                )),
                multispace0,
            ))),
        ))),
    )))(i)
}
fn engine_merge_tree_settings(i: &[u8]) -> IResult<&[u8], &[u8]> {
    // [SETTINGS name=value, ...]
    recognize(tuple((
        multispace0,
        tag_no_case("SETTINGS"),
        multispace0,
        separated_list(ws_sep_comma, engine_merge_tree_settings_kv)
    )))(i)
}
fn engine_merge_tree_settings_kv(i: &[u8]) -> IResult<&[u8], &[u8]> {
    let numeric = recognize(tuple((
        alt((
            tag_no_case("index_granularity"),
            tag_no_case("index_granularity_bytes"),
            tag_no_case("enable_mixed_granularity_parts"),
            tag_no_case("use_minimalistic_part_header_in_zookeeper"),
            tag_no_case("min_merge_bytes_to_use_direct_io"),
            tag_no_case("merge_with_ttl_timeout"),
            tag_no_case("write_final_mark"),
            tag_no_case("merge_max_block_size"),
            tag_no_case("min_bytes_for_wide_part"),
            tag_no_case("min_rows_for_wide_part"),
        )),
        multispace0,
        tag("="),
        multispace0,
        digit1,
    )));

    let string = recognize(tuple((
        //alt((
            tag_no_case("storage_policy"),
        //)),
        multispace0,
        tag("="),
        multispace0,
        delimited(tag("'"), take_until("'"), tag("'")),
    )));

    alt((
        numeric,
        string,
    ))(i)
}

fn engine_replicated_merge_tree(i: &[u8]) -> IResult<&[u8], Engine> {
    //  ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}', ver)
    //  PARTITION BY toYYYYMM(EventDate)
    //  ORDER BY (CounterID, EventDate, intHash32(UserID))
    //  SAMPLE BY intHash32(UserID)
    map(
        recognize(tuple((
            tag_no_case("ReplicatedMergeTree"),
            multispace0,
            tag("("),
            multispace0,
            sql_expression, // zookeeper path
            multispace0,
            tag(","),
            multispace0,
            sql_expression, // replica name
            opt(tuple((
                multispace0,
                tag(","),
                multispace0,
                sql_expression, // engine params (FIXME: not for all Replicated*MergeTree engines?
            ))),
            multispace0,
            tag(")"),
            multispace0,
            many0(alt((
                engine_merge_tree_partition,
                engine_merge_tree_orderby,
                engine_merge_tree_primary,
                engine_merge_tree_sample,
                engine_merge_tree_ttl,
                engine_merge_tree_settings,
            ))),
        ))),
        |s| {
            Engine::ReplicatedMergeTree(EngineReplicatedMergeTree(str::from_utf8(s).unwrap().to_string()))
        }
    )(i)
}

fn engine_memory(i: &[u8]) -> IResult<&[u8], Engine> {
    map(tag_no_case("memory"), |_| Engine::Memory)(i)
}

fn engine(i: &[u8]) -> IResult<&[u8], Engine> {
    alt((
       engine_distributed, 
       engine_memory,
       engine_merge_tree,
       engine_replicated_merge_tree,
    ))(i)
}

fn engine_spec(i: &[u8]) -> IResult<&[u8], Engine> {
    map(
        tuple((
            tag_no_case("engine"),
            multispace0,
            tag("="),
            multispace0,
            engine,
        )),
        |(_, _, _, _, engine)| engine
    )(i)
}

fn field_specification_opts(i: &[u8]) -> IResult<&[u8], SqlTypeOpts> {
    alt((
        map(delimited(
            tag_no_case("Nullable("),
            type_identifier,
            tag(")"),
        ), |ftype| SqlTypeOpts{ftype, nullable: true, lowcardinality:false}),
        map(delimited(
            tag_no_case("LowCardinality("),
            type_identifier,
            tag(")"),
        ), |ftype| SqlTypeOpts{ftype, nullable: false, lowcardinality:true}),
        map(delimited(
            tag_no_case("LowCardinality(Nullable("),
            type_identifier,
            tag("))"),
        ), |ftype| SqlTypeOpts{ftype, nullable: true, lowcardinality:true}),
        map(type_identifier,
            |ftype| SqlTypeOpts{ftype, nullable: false, lowcardinality: false}),
    ))(i)
}

fn field_specification(i: &[u8]) -> IResult<&[u8], ColumnSpecification> {
    let (remaining_input, (column, field_type, option, comment, codec, ttl)) = tuple((
        column_identifier_no_alias,
        delimited(
            multispace1,
            field_specification_opts,
            multispace0),
        alt((
            opt(column_default),
            opt(column_materialized),
            //column_alias,
        )),
        opt(preceded(multispace0, column_comment)),
        opt(preceded(multispace0, column_codec_list)),
        opt(preceded(multispace0, column_ttl)),
    ))(i)?;

    Ok((
        remaining_input,
        ColumnSpecification {
            column,
            sql_type: field_type.ftype,
            codec,
            ttl,
            nullable: field_type.nullable,
            option,
            comment,
            lowcardinality: field_type.lowcardinality,
        },
    ))
}
pub fn column_comment(i: &[u8]) -> IResult<&[u8], String> {
    let (remaining_input, (_, _, comment)) = tuple((
        tag_no_case("COMMENT"),
        multispace0,
        delimited(tag("'"), take_until("'"), tag("'")),
    ))(i)?;

    Ok((remaining_input, str::from_utf8(comment).unwrap().to_string() ))
}

// Parse rule for a comma-separated list.
pub fn field_specification_list(i: &[u8]) -> IResult<&[u8], Vec<ColumnSpecification>> {
    separated_list(ws_sep_comma, field_specification)(i)
}

pub fn column_codec_list(i: &[u8]) -> IResult<&[u8], CodecList> {

    let (remaining_input, (_, _, list)) = tuple((
        tag_no_case("codec"),
        multispace0,
        delimited(
            delimited(multispace0, tag("("), multispace0),
            separated_list(ws_sep_comma, column_codec),
            delimited(multispace0, tag(")"), multispace0),
        ),
    ))(i)?;

    Ok((remaining_input, CodecList(list)))
}

pub fn column_codec(i: &[u8]) -> IResult<&[u8], Codec> {
    let none = map( tag_no_case("none"), |_| Codec::None);
    let lz4  = map( tag_no_case("lz4"), |_| Codec::LZ4);
    let doubledelta = map( tag_no_case("DoubleDelta"), |_| Codec::DoubleDelta);
    let gorilla = map( tag_no_case("gorilla"), |_| Codec::Gorilla);
    let t64 = map( tag_no_case("t64"), |_| Codec::T64);
    let delta = map(
        preceded(
            tag_no_case("delta"), opt( delimited(
                    delimited(multispace0, tag("("), multispace0),
                    one_of("1248"),
                    delimited(multispace0, tag(")"), multispace0),
            )),
        ),
        |l| match l {
            Some(l) => Codec::Delta(Some(l.into())), // FIXME use try_from and process error
            None => Codec::Delta(None),
        },
    );
    let zstd = map(
        preceded(
            tag_no_case("zstd"), opt( delimited(
                    delimited(multispace0, tag("("), multispace0),
                    digit1,
                    delimited(multispace0, tag(")"), multispace0),
            )),
        ),
        |s: Option<&[u8]>| match s {
            Some(l) => {
                let l = u8::from_str(str::from_utf8(l).unwrap()).unwrap();
                if l >= 1 && l <= 22 {
                    Codec::ZSTD(Some(l)) // FIXME process error
                }else{
                    panic!("Unsupported level '{}' for codec ZSTD", l)
                }
            },
            None => Codec::ZSTD(None),
        },
    );
    let lz4hc = map(
        preceded(
            tag_no_case("LZ4HC"), opt( delimited(
                    delimited(multispace0, tag("("), multispace0),
                    digit1,
                    delimited(multispace0, tag(")"), multispace0),
            )),
        ),
        |s: Option<&[u8]>| match s {
            Some(l) => {
                let l = u8::from_str(str::from_utf8(l).unwrap()).unwrap();
                if l <= 12 {
                    Codec::LZ4HC(Some(l)) // FIXME process error
                }else{
                    panic!("Unsupported level '{}' for codec LZ4HC", l)
                }
            },
            None => Codec::LZ4HC(None),
        },
    );
    alt((
        none,
        zstd,
        lz4hc,
        lz4,
        delta,
        doubledelta,
        gorilla,
        t64,
    ))(i)
}

pub fn column_ttl(i: &[u8]) -> IResult<&[u8], ColumnTTL> {
    let ttl = map(
        tuple((multispace0, tag_no_case("TTL"), multispace0, sql_identifier, multispace0)),
        |(_, _, _, name, _)| ColumnTTL::from(str::from_utf8(name).unwrap()),
    );
    let ttl_interval = map(
        tuple((multispace0, tag_no_case("TTL-FIXME"), multispace0, sql_identifier, multispace0)),
        |(_, _, _, name, _)| ColumnTTL::from(str::from_utf8(name).unwrap()),
    );

    alt((
            ttl,
            ttl_interval,
    ))(i)
}

fn column_default(i: &[u8]) -> IResult<&[u8], ColumnOption> {
    let (remaining_input, (_, _, _, def, _)) = tuple((
        multispace0,
        tag_no_case("default"),
        multispace1,
        sql_expression,
        multispace0,
    ))(i)?;

    Ok((remaining_input, ColumnOption::DefaultValue(
        str::from_utf8(def).unwrap().to_string()
    )))
}

fn column_materialized(i: &[u8]) -> IResult<&[u8], ColumnOption> {
    let (remaining_input, (_, _, _, def, _)) = tuple((
        multispace0,
        tag_no_case("default"),
        multispace1,
        sql_expression,
        multispace0,
    ))(i)?;

    Ok((remaining_input, ColumnOption::Materialized(
        str::from_utf8(def).unwrap().to_string()
    )))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::*;

    #[test]
    fn t_column_ttl() {
        let string = "TTL time_column";
        let res = column_ttl(string.as_bytes());
        assert_eq!(
            res.unwrap().1,
            ColumnTTL::from("time_column"),
        );
    }

    #[test]
    fn t_column_codec() {
        let patterns = vec![
            ( "none", Codec::None ),
            ( "lz4", Codec::LZ4 ),

            ( "delta",    Codec::Delta(None) ),
            ( "delta(4)", Codec::Delta(Some(CodecDeltaLevel::L4)) ),

            ( "zstd",    Codec::ZSTD(None) ),
            ( "zstd(3)", Codec::ZSTD(Some(3)) ),

            ( "lz4hc",     Codec::LZ4HC(None) ),
            ( "lz4hc(11)", Codec::LZ4HC(Some(11)) ),
            ( "lz4hc(0)",  Codec::LZ4HC(Some(0)) ),
        ];
        parse_set_for_test(column_codec, patterns);
    }

    #[test]
    fn t_column_codec_list() {
        let patterns = vec![
            (
                "codec(delta(4),lz4)",
                CodecList(vec![ Codec::Delta(Some(CodecDeltaLevel::L4)), Codec::LZ4 ])
            ),
            (
                "codec( delta ( 4 ) , lz4)",
                CodecList(vec![ Codec::Delta(Some(CodecDeltaLevel::L4)), Codec::LZ4 ])
            ),
            (
                "codec(delta(4))",
                CodecList(vec![ Codec::Delta(Some(CodecDeltaLevel::L4))])
            ),
        ];

        parse_set_for_test(column_codec_list, patterns);
    }

    #[test]
    fn t_field_spec() {
        let patterns = vec![
            ( "LowCardinality(Nullable(String))", SqlTypeOpts{ftype: SqlType::String, nullable: true, lowcardinality: true,} ),
            ( "Nullable(String)", SqlTypeOpts{ftype: SqlType::String, nullable: true, lowcardinality: false,} ),
            ( "Int8", SqlTypeOpts{ftype: SqlType::Int(TypeSize::B8), nullable: false, lowcardinality: false,} ),
        ];
        parse_set_for_test(field_specification_opts, patterns);
    }
    
    #[test]
    fn t_field_opts_display() {
        let patterns = vec![
            ( "String", "String".to_string()),
            ( "Nullable(String)", "Nullable(String)".to_string()),
            ( "LowCardinality(String)", "LowCardinality(String)".to_string()),
            ( "LowCardinality(Nullable(String))", "LowCardinality(Nullable(String))".to_string()),
        ];
        parse_set_for_test(|i| field_specification_opts(i)
                .map(|(_, o)| ("".as_bytes(), format!("{}", o))),
            patterns);
    }

    #[test]
    fn t_engine() {
        let patterns = vec![
            ( "Memory", Engine::Memory ),
            (
                "Distributed('cluster1', 'schema1', 'table1', rand() )",
                Engine::Distributed(EngineDistributed {
                    cluster_name: "'cluster1'".into(),
                    schema: "'schema1'".into(),
                    table: "'table1'".into(),
                    sharding_key: None,
                    policy_name: None,
                })
            ),
            (
                "Distributed('cluster1', '', 'table1', rand() )",
                Engine::Distributed(EngineDistributed {
                    cluster_name: "'cluster1'".into(),
                    schema: "''".into(),
                    table: "'table1'".into(),
                    sharding_key: None,
                    policy_name: None,
                })
            ),
        ];
        parse_set_for_test(engine, patterns);
    }

    #[test]
    fn t_column_display_codec_ttl_nullable() {
        let cs = ColumnSpecification {
            column: "time_local".into(),
            sql_type: SqlType::DateTime(None),
            codec: Some(CodecList(vec![ Codec::Delta(Some(CodecDeltaLevel::L1)), Codec::LZ4, Codec::ZSTD(None) ])),
            ttl: Some(ColumnTTL::from(("1", "2"))),
            nullable: true,
            option: None,
            comment: None,
            lowcardinality: false,
        };

        let exp = "`time_local` Nullable(DateTime) CODEC(Delta(1), LZ4, ZSTD) TTL 1 + 2";
        assert_eq!(exp, format!("{}", cs).as_str());
    }


    #[test]
    fn t_column_display_codec_ttl() {
        let cs = ColumnSpecification {
            column: "time_local".into(),
            sql_type: SqlType::DateTime(None),
            codec: Some(CodecList(vec![ Codec::Delta(Some(CodecDeltaLevel::L1)), Codec::LZ4, Codec::ZSTD(None) ])),
            ttl: Some(ColumnTTL::from(("1", "2"))),
            nullable: false,
            option: None,
            comment: None,
            lowcardinality: false,
        };

        let exp = "`time_local` DateTime CODEC(Delta(1), LZ4, ZSTD) TTL 1 + 2";
        assert_eq!(exp, format!("{}", cs).as_str());
    }

    #[test]
    fn t_column_display() {
        let patterns = vec![
            (
                "`reg` UInt32 DEFAULT CAST(0, 'UInt32') COMMENT 'комментарий' CODEC(Delta(4))",
                "`reg` UInt32 DEFAULT CAST(0, 'UInt32') COMMENT 'комментарий' CODEC(Delta(4))".to_string()
            ),
            (
                "`reg` UInt32 CODEC(Delta(4))",
                "`reg` UInt32 CODEC(Delta(4))".to_string()
            ),
            (
                "`reg` UInt32 CODEC(Delta(4))",
                "`reg` UInt32 CODEC(Delta(4))".to_string()
            ),
        ];
        parse_set_for_test(|i| field_specification(i)
                .map(|(_, o)| ("".as_bytes(), format!("{}", o))),
            patterns);
    }



}
