// vim: set ts=4 sts=4 sw=4 expandtab:
//extern crate nom;

use std::str;
use std::str::FromStr;
use std::fmt; 

use nom::{
    IResult,
    InputLength,
    error::{ ParseError},
    branch::alt,
    sequence::{delimited, preceded, terminated, tuple, pair},
    combinator::{map, opt, not, peek, recognize},
    character::complete::{digit1, multispace0, multispace1, line_ending, one_of},
    character::is_alphanumeric,
    bytes::complete::{is_not, tag, tag_no_case, take, take_until, take_while1},
    multi::{fold_many0, many1, separated_list,},
};
pub use nom::{
    self,
    Err as NomErr,
    error::ErrorKind,
};

mod keywords;
pub mod table;
pub mod column;
pub mod create;

use keywords::sql_keyword;
use table::Table;
use column::Column;
use create::{
    CreateTableStatement,
    creation,
    field_specification_opts,
};

fn eof<I: Copy + InputLength, E: ParseError<I>>(input: I) -> IResult<I, I, E> {
    if input.input_len() == 0 {
        Ok((input, input))
    } else {
        Err(nom::Err::Error(E::from_error_kind(input, ErrorKind::Eof)))
    }
}


pub fn ws_sep_comma(i: &[u8]) -> IResult<&[u8], &[u8]> {
    delimited(multispace0, tag(","), multispace0)(i)
}

pub fn statement_terminator(i: &[u8]) -> IResult<&[u8], ()> {
    let (remaining_input, _) =
        delimited(multispace0, alt((tag(";"), line_ending, eof)), multispace0)(i)?;

    Ok((remaining_input, ()))
}

pub fn schema_table_reference(i: &[u8]) -> IResult<&[u8], Table> {
    map(
		tuple((
			opt(pair(sql_identifier, tag("."))),
			sql_identifier,
			opt(as_alias)
		)),
	|tup| Table {
        name: String::from(str::from_utf8(tup.1).unwrap()),
        alias: match tup.2 {
            Some(a) => Some(String::from(a)),
            None => None,
        },
        schema: match tup.0 {
            Some((schema, _)) => Some(String::from(str::from_utf8(schema).unwrap())),
            None => None,
        },
    })(i)
}

pub fn as_alias(i: &[u8]) -> IResult<&[u8], &str> {
    map(
        tuple((
            multispace1,
            opt(pair(tag_no_case("as"), multispace1)),
            sql_identifier,
        )),
        |a| str::from_utf8(a.2).unwrap(),
    )(i)
}

pub fn delim_digit(i: &[u8]) -> IResult<&[u8], &[u8]> {
    delimited(tag("("), digit1, tag(")"))(i)
}

pub fn column_identifier_no_alias(i: &[u8]) -> IResult<&[u8], Column> {
    let table_parser = pair(opt(terminated(sql_identifier, tag("."))), sql_identifier);
    map(table_parser, |tup| Column {
        name: str::from_utf8(tup.1).unwrap().to_string(),
        alias: None,
        table: match tup.0 {
            None => None,
            Some(t) => Some(str::from_utf8(t).unwrap().to_string()),
        },
    })(i)
}

pub fn is_sql_identifier(chr: u8) -> bool {
    is_alphanumeric(chr) || chr == '_' as u8 || chr == '@' as u8
}

pub fn sql_identifier(i: &[u8]) -> IResult<&[u8], &[u8]> {
    let is_not_doublequote = |chr| chr != '"' as u8;
    let is_not_backquote = |chr| chr != '`' as u8;
    alt((
        correct_identifier,
        delimited(tag("`"), take_while1(is_not_backquote), tag("`")),
        delimited(tag("\""), take_while1(is_not_doublequote), tag("\"")),
    ))(i)
}

pub fn correct_identifier(i: &[u8]) -> IResult<&[u8], &[u8]> {
    preceded(not(peek(sql_keyword)), take_while1(is_sql_identifier))(i)
}

pub fn escape_identifier(identifier: &str) -> String {
    if correct_identifier(identifier.as_bytes()).is_ok() {
        identifier.to_owned()
    } else {
        format!("`{}`", identifier)
    }

}



#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum SqlQuery {
    CreateTable(CreateTableStatement),
}
impl fmt::Display for SqlQuery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SqlQuery::CreateTable(ref s) => write!(f, "{}", s),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum TypeSize16 {
    B8,
    B16,
}
impl fmt::Display for TypeSize16 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TypeSize16::B8 => write!(f, "8"),
            TypeSize16::B16 => write!(f, "16"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum TypeSize {
    B8,
    B16,
    B32,
    B64,
}
impl fmt::Display for TypeSize {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TypeSize::B8 => write!(f, "8"),
            TypeSize::B16 => write!(f, "16"),
            TypeSize::B32 => write!(f, "32"),
            TypeSize::B64 => write!(f, "64"),
        }
    }
}


#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum SqlType {
    String,
    Int(TypeSize),
    UnsignedInt(TypeSize),
    Enum(Option<TypeSize16>, Vec<(String, i16)>),
    Date,
    DateTime(Option<String>),
    DateTime64(u8, Option<String>),
    Float32,
    Float64,
    FixedString(usize),
    IPv4,
    IPv6,
    UUID,
    Array(Box<SqlTypeOpts>),
}

impl fmt::Display for SqlType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SqlType::String => write!(f, "String"),
            SqlType::Int(size) => write!(f, "Int{}", size),
            SqlType::UnsignedInt(size) => write!(f, "UInt{}", size),
            SqlType::Enum(size, values) => write!(f, "Enum{}({})",
                size.as_ref().map(|size| format!("{}", size)).unwrap_or("".into()),
                values
                    .iter()
                    .map(|(name, num)| format!("'{}' = {}", name, num)) 
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            SqlType::Date => write!(f, "Date"),
            SqlType::DateTime(None) => write!(f, "DateTime"),
            SqlType::DateTime(Some(timezone)) => write!(f, "DateTime({})", timezone),
            SqlType::DateTime64(precision, None) => write!(f, "DateTime64({})", precision),
            SqlType::DateTime64(precision, Some(timezone)) => write!(f, "DateTime64({}, {})", precision, timezone),
            SqlType::Float32 => write!(f, "Float32"),
            SqlType::Float64 => write!(f, "Float64"),
            SqlType::FixedString(size) => write!(f, "FixedString({})", size),
            SqlType::IPv4 => write!(f, "IPv4"),
            SqlType::IPv6 => write!(f, "IPv6"),
            SqlType::UUID => write!(f, "UUID"),
            SqlType::Array(t) => write!(f, "Array({})", t),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SqlTypeOpts {
    pub ftype: SqlType,
    pub nullable: bool,
    pub lowcardinality: bool,
}

impl fmt::Display for SqlTypeOpts{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match (&self.ftype, &self.lowcardinality, &self.nullable) {
            (t, false, false) => write!(f,"{}", t),
            (t, false, true) => write!(f,"Nullable({})", t),
            (t, true, false) => write!(f,"LowCardinality({})", t),
            (t, true, true) => write!(f,"LowCardinality(Nullable({}))", t),
        }
    }
}


fn ttl_expression(i: &[u8]) -> IResult<&[u8], &[u8]> {
    //date + INTERVAL 1 DAY
    let ttl = map(
        sql_identifier,
        |name| name,
    );
    let ttl_interval = map(
        recognize(tuple((
            multispace0,
            sql_identifier,
            multispace0,
            tag_no_case("INTERVAL"),
            multispace1,
            alt(( tag("+"), tag("-") )),
            multispace1,
            digit1,
            multispace0,
            alt((
                tag_no_case("SECOND"),
                tag_no_case("MINUTE"),
                tag_no_case("HOUR"),
                tag_no_case("DAY"),
                tag_no_case("WEEK"),
                tag_no_case("MONTH"),
                tag_no_case("QUARTER"),
                tag_no_case("YEAR"),
            ))
        ))),
        |interval| interval,
    );

    alt((
            ttl_interval,
            ttl,
    ))(i)
}

fn sql_expression(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        recognize(tuple((
            sql_simple_expression,
            multispace0,
            one_of("+-*/<>"),
            multispace0,
            sql_simple_expression,
        ))),
        sql_simple_expression,
    ))(i)
}
fn sql_simple_expression(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        sql_function,
        sql_cast_function,
        sql_tuple,
        recognize(raw_string_single_quoted),
        recognize(raw_string_double_quoted),
        sql_identifier,
    ))(i)
}
fn sql_function(i: &[u8]) -> IResult<&[u8], &[u8]> {
    recognize(tuple((
        sql_identifier,
        multispace0,
        sql_tuple,
    )))(i)
}

fn sql_tuple(i: &[u8]) -> IResult<&[u8], &[u8]> {
    recognize(tuple((
        tag("("),
        separated_list(ws_sep_comma, sql_expression),
        tag(")"),
    )))(i)
}

fn sql_cast_function(i: &[u8]) -> IResult<&[u8], &[u8]> {
    recognize(tuple((
        tag_no_case("CAST"),
        multispace0,
        tag("("),
        sql_expression,
        multispace0,
        alt((tag(","), tag_no_case("AS"))),
        multispace0,
        sql_expression,
        multispace0,
        tag(")"),
    )))(i)
}

fn type_size_suffix64(i: &[u8]) -> IResult<&[u8], TypeSize> {
    alt((
        map(tag_no_case("8"), |_| TypeSize::B8),
        map(tag_no_case("16"), |_| TypeSize::B16),
        map(tag_no_case("32"), |_| TypeSize::B32),
        map(tag_no_case("64"), |_| TypeSize::B64),
    ))(i)
}

fn type_size_suffix16(i: &[u8]) -> IResult<&[u8], TypeSize16> {
    alt((
        map(tag_no_case("8"), |_| TypeSize16::B8),
        map(tag_no_case("16"), |_| TypeSize16::B16),
    ))(i)
}

/// String literal value
fn raw_string_quoted(input: &[u8], is_single_quote: bool) -> IResult<&[u8], Vec<u8>> {
    let quote_slice: &[u8] = if is_single_quote { b"\'" } else { b"\"" };
    let double_quote_slice: &[u8] = if is_single_quote { b"\'\'" } else { b"\"\"" };
    let backslash_quote: &[u8] = if is_single_quote { b"\\\'" } else { b"\\\"" };
    delimited(
        tag(quote_slice),
        fold_many0(
            alt((
                is_not(backslash_quote),
                map(tag(double_quote_slice), |_| -> &[u8] {
                    if is_single_quote {
                        b"\'"
                    } else {
                        b"\""
                    }
                }),
                map(tag("\\\\"), |_| &b"\\"[..]),
                map(tag("\\b"), |_| &b"\x7f"[..]),
                map(tag("\\r"), |_| &b"\r"[..]),
                map(tag("\\n"), |_| &b"\n"[..]),
                map(tag("\\t"), |_| &b"\t"[..]),
                map(tag("\\0"), |_| &b"\0"[..]),
                map(tag("\\Z"), |_| &b"\x1A"[..]),
                preceded(tag("\\"), take(1usize)),
            )),
            Vec::new(),
            |mut acc: Vec<u8>, bytes: &[u8]| {
                acc.extend(bytes);
                acc
            },
        ),
        tag(quote_slice),
    )(input)
}

fn raw_string_single_quoted(i: &[u8]) -> IResult<&[u8], Vec<u8>> {
    raw_string_quoted(i, true)
}

fn raw_string_double_quoted(i: &[u8]) -> IResult<&[u8], Vec<u8>> {
    raw_string_quoted(i, false)
}

// A SQL type specifier.
fn type_identifier(i: &[u8]) -> IResult<&[u8], SqlType> {
    let enum_value = map(
        tuple((
            multispace0,
            map(
                delimited(tag("'"), take_until("'"), tag("'")),
                |s: &[u8]| {
                    String::from_utf8(s.to_vec()).unwrap()
                },
            ),
            multispace0,
            tag("="),
            multispace0,
            digit1,
        )),
        |(_, name, _, _, _, num)| (name.to_string(), i16::from_str(str::from_utf8(num).unwrap()).unwrap())
    );

    alt((
        map(
            tuple((
                    tag_no_case("int"),
                    type_size_suffix64,
            )),
            |t| SqlType::Int(t.1)
        ),
        map(
            tuple((
                    tag_no_case("uint"),
                    type_size_suffix64,
            )),
            |t| SqlType::UnsignedInt(t.1)
        ),
        map(
            tuple((
                    tag_no_case("enum"),
                    opt(type_size_suffix16),
                    tag("("),
                    many1(terminated(enum_value, opt(ws_sep_comma))),
                    tag(")"),
            )),
            |(_,size,_,values,_)| SqlType::Enum(size, values)
        ),
        map(tag_no_case("string"), |_| SqlType::String),
        map(tag_no_case("float32"), |_| SqlType::Float32),
        map(tag_no_case("float64"), |_| SqlType::Float64),
        map(
            tuple((
                tag_no_case("datetime64"),
                multispace0,
                tag("("),
                multispace0,
                one_of("0123456789"),
                multispace0,
                opt(map(
                    tuple((
                        tag(","),
                        multispace0,
                        delimited(tag("'"), take_until("'"), tag("'")),
                    )),
                    |(_, _, timezone)| str::from_utf8(timezone).unwrap().to_string()
                )),
                multispace0,
                tag(")"),
            )),
            |(_, _, _, _, precision, _, timezone, _, _)| SqlType::DateTime64(precision.to_digit(10).unwrap() as u8, timezone)
        ),
        map(
            tuple((
                tag_no_case("datetime"),
                multispace0,
                opt(map(
                    tuple((
                        tag("("),
                        multispace0,
                        delimited(tag("'"), take_until("'"), tag("'")),
                        multispace0,
                        tag(")"),
                    )),
                    |(_, _, timezone, _, _)| str::from_utf8(timezone).unwrap().to_string()
                )),
            )),
            |(_, _, timezone)| SqlType::DateTime(timezone)
        ),
        map(tag_no_case("date"), |_| SqlType::Date),
        map(
            preceded(
                tag_no_case("FixedString"),
                delim_digit,
            ),
            |d| SqlType::FixedString(usize::from_str(str::from_utf8(d).unwrap()).unwrap())
        ),
        map(tag_no_case("ipv4"), |_| SqlType::IPv4),
        map(tag_no_case("ipv6"), |_| SqlType::IPv6),
        map(tag_no_case("uuid"), |_| SqlType::UUID),
        map(
            tuple((
                tag_no_case("array"),
                tag("("),
                field_specification_opts,
                tag(")"),
            )),
            |(_,_,t,_)| SqlType::Array(Box::new(t))
        ),
    ))(i)
}

pub fn sql_query(i: &[u8]) -> IResult<&[u8], SqlQuery> {
    map(creation, |c| SqlQuery::CreateTable(c))(i)
}

pub fn parse_query_bytes<T>(input: T) -> Result<SqlQuery, &'static str>
where
    T: AsRef<[u8]>,
{
    match sql_query(input.as_ref()) {
        Ok((_, o)) => Ok(o),
        Err(_) => Err("failed to parse query"),
    }
}

pub fn parse_query<T>(input: T) -> Result<SqlQuery, &'static str>
where
    T: AsRef<str>,
{
    parse_query_bytes(input.as_ref().trim().as_bytes())
}

#[cfg(test)]
fn parse_set_for_test<'a, T, F>(f: F, patterns: Vec<(&'a str, T)>)
    where
        T: std::fmt::Display + PartialEq,
        F: Fn(&[u8]) -> IResult<&[u8], T>
{

    let mut success = true;
    for (pattern, res) in patterns {
        print!( "* {}: ", pattern);

        match f(pattern.as_bytes()) {
            Ok((_, r)) if r == res => println!("OK"),
            Ok((_, r)) => {
                success = false;
                println!("WARN");
                println!("   expected: {}", res);
                println!("      found: {}", r);
            },
            Err(e) => {
                success = false;
                println!("FAIL ({})",e);
            },
        }
    }
    assert!(success);
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn t_type_identifier() {
        fn t(nullable: bool, lowcardinality: bool, t: SqlType) -> Box<SqlTypeOpts> {
            Box::new(SqlTypeOpts {
                ftype: t,
                nullable: nullable,
                lowcardinality: lowcardinality,
            })
        }
        let patterns = vec![
            ( "Int32", SqlType::Int(TypeSize::B32)),
            ( "UInt32", SqlType::UnsignedInt(TypeSize::B32)),
            (
                "Enum8('a' = 1, 'b' = 2)",
                SqlType::Enum(Some(TypeSize16::B8), vec![("a".into(), 1), ("b".into(), 2)])
            ),
            ( "String", SqlType::String ),
            ( "Float32", SqlType::Float32 ),
            ( "Float64", SqlType::Float64 ),

            ( "DateTime64(9)", SqlType::DateTime64(9, None) ),
            ( "DateTime64( 3 ,'Etc/UTC'  )", SqlType::DateTime64(3, Some("Etc/UTC".into())) ),

            ( "DateTime", SqlType::DateTime(None) ),
            ( "DateTime('Cont/City')", SqlType::DateTime(Some("Cont/City".into())) ),
            ( "DateTime ( 'Cont/City')", SqlType::DateTime(Some("Cont/City".into())) ),

            ( "FixedString(3)", SqlType::FixedString(3) ),

            ( "UUID", SqlType::UUID ),
            ( "Array(FixedString(2))", SqlType::Array(t(false, false, SqlType::FixedString(2))) ),
            ( "Array(Nullable(Int32))", SqlType::Array(t(true, false, SqlType::Int(TypeSize::B32))) ),
            ( "Array(LowCardinality(String))", SqlType::Array(t(false, true, SqlType::String)) ),
            ( "Array(Array(Array(Int64)))", SqlType::Array(t(false, false,
                SqlType::Array(t(false, false,
                    SqlType::Array(t(false, false,
                        SqlType::Int(TypeSize::B64),
                    )),
                )),
            )) ),
        ];
        parse_set_for_test(type_identifier, patterns);
    }
 
    #[test]
    fn t_sql_expression() {
        let patterns = vec![
            ( "rand()", "rand()".to_string() ),
            ( "toDate(requestedAt)", "toDate(requestedAt)".to_string() ),
            ( "(col1, coln2, rand())", "(col1, coln2, rand())".to_string() ),
            ( "func(col)", "func(col)".to_string() ),
            ( "func('col')", "func('col')".to_string() ),
            ( "func('col','df')", "func('col','df')".to_string() ),
            ( "cast('val' as Date)", "cast('val' as Date)".to_string() ),
            (
                r#"CAST('captcha', 'Enum8(\'captcha\' = 1, \'ban\' = 2)')"#,
                r#"CAST('captcha', 'Enum8(\'captcha\' = 1, \'ban\' = 2)')"#.to_string()
            ),
            ( "z>1", "z>1".to_string() ),
            (
                "assumeNotNull(if(1>1, murmurHash3_64(d), rand()))",
                "assumeNotNull(if(1>1, murmurHash3_64(d), rand()))".to_string()
            ),
            (
                "assumeNotNull(if(length(deviceId) > 1, murmurHash3_64(deviceId), rand()))",
                "assumeNotNull(if(length(deviceId) > 1, murmurHash3_64(deviceId), rand()))".to_string()
            ),
        ];
        parse_set_for_test(|i| sql_expression(i)
                .map(|(_, o)| ("".as_bytes(), str::from_utf8(o).unwrap().to_string())),
            patterns);
    }

    #[test]
    fn t_ttl_expression() {
        let patterns = vec![
            ( "col", "col".to_string() ),
            ( "col INTERVAL + 1 day", "col INTERVAL + 1 day".to_string() ),
            ( "col INTERVAL - 15 year", "col INTERVAL - 15 year".to_string() ),
        ];
        parse_set_for_test(|i| ttl_expression(i)
                .map(|(_, o)| ("".as_bytes(), str::from_utf8(o).unwrap().to_string())),
            patterns);
    }

    #[test]
    fn t_schema_table_reference() {
        let patterns = vec![
            ( 
                r#"cluster_shard1.`.inner.api_path_time_view`"#,
                r#"cluster_shard1.`.inner.api_path_time_view`"#.to_string()
            ),
            ( 
                r#"cluster_shard1.".inner.api_path_time_view""#,
                r#"cluster_shard1.`.inner.api_path_time_view`"#.to_string()
            ),
        ];
        parse_set_for_test(|i| schema_table_reference(i)
                .map(|(_, o)| ("".as_bytes(), format!("{}", o))),
            patterns);
    }

    #[test]
    fn t_sql_identifier() {
        let patterns = vec![
            ( 
                r#"`.inner.api_path_time_view`"#,
                ".inner.api_path_time_view".to_string()
            ),
            ( 
                r#"".inner.api_path_time_view""#,
                ".inner.api_path_time_view".to_string()
            ),
        ];
        parse_set_for_test(|i| sql_identifier(i)
                .map(|(_, o)| ("".as_bytes(), str::from_utf8(o).unwrap().to_string())),
            patterns);
    }

    #[test]
    fn t_sql_identifier_incorrect() {
        match sql_identifier(r#"'.inner.api_path_time_view'"#.as_bytes()) {
            Ok(_) => assert!(false),
            Err(_) => assert!(true),
        }
    }

}
