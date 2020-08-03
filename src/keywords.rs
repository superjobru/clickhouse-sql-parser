// vim: set expandtab ts=4 sw=4:

use super::eof;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::combinator::peek;
use nom::sequence::terminated;
use nom::IResult;


fn keyword_follow_char(i: &[u8]) -> IResult<&[u8], &[u8]> {
    peek(alt((
        tag(" "),
        tag("\n"),
        tag(";"),
        tag("("),
        tag(")"),
        tag("\t"),
        tag(","),
        tag("="),
        eof,
    )))(i)
}

fn keyword_a_to_c(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        terminated(tag_no_case("AND"), keyword_follow_char),
        terminated(tag_no_case("AS"), keyword_follow_char),
        terminated(tag_no_case("ASC"), keyword_follow_char),
        terminated(tag_no_case("BETWEEN"), keyword_follow_char),
        terminated(tag_no_case("BY"), keyword_follow_char),
    ))(i)
}

fn keyword_c_to_e(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        terminated(tag_no_case("COLUMN"), keyword_follow_char),
        terminated(tag_no_case("CREATE"), keyword_follow_char),
        terminated(tag_no_case("DATABASE"), keyword_follow_char),
        terminated(tag_no_case("DEFAULT"), keyword_follow_char),
        terminated(tag_no_case("DESC"), keyword_follow_char),
        terminated(tag_no_case("DETACH"), keyword_follow_char),
        terminated(tag_no_case("DISTINCT"), keyword_follow_char),
        terminated(tag_no_case("DROP"), keyword_follow_char),
    ))(i)
}

fn keyword_e_to_i(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        terminated(tag_no_case("EXISTS"), keyword_follow_char),
        terminated(tag_no_case("FROM"), keyword_follow_char),
        terminated(tag_no_case("GROUP"), keyword_follow_char),
        terminated(tag_no_case("HAVING"), keyword_follow_char),
        terminated(tag_no_case("IGNORE"), keyword_follow_char),
        terminated(tag_no_case("IN"), keyword_follow_char),
        terminated(tag_no_case("INDEX"), keyword_follow_char),
    ))(i)
}

fn keyword_i_to_o(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        terminated(tag_no_case("INSERT"), keyword_follow_char),
        terminated(tag_no_case("INTO"), keyword_follow_char),
        terminated(tag_no_case("ORDER"), keyword_follow_char),
        terminated(tag_no_case("JOIN"), keyword_follow_char),
        terminated(tag_no_case("LEFT"), keyword_follow_char),
        terminated(tag_no_case("LIKE"), keyword_follow_char),
        terminated(tag_no_case("LIMIT"), keyword_follow_char),
        terminated(tag_no_case("NOT"), keyword_follow_char),
        terminated(tag_no_case("NULL"), keyword_follow_char),
        terminated(tag_no_case("OFFSET"), keyword_follow_char),
    ))(i)
}

fn keyword_o_to_s(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        terminated(tag_no_case("ON"), keyword_follow_char),
        terminated(tag_no_case("OR"), keyword_follow_char),
        terminated(tag_no_case("PRIMARY"), keyword_follow_char),
        terminated(tag_no_case("SELECT"), keyword_follow_char),
    ))(i)
}

fn keyword_s_to_z(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        terminated(tag_no_case("SET"), keyword_follow_char),
        terminated(tag_no_case("TABLE"), keyword_follow_char),
        terminated(tag_no_case("TEMPORARY"), keyword_follow_char),
        terminated(tag_no_case("TO"), keyword_follow_char),
        terminated(tag_no_case("UNIQUE"), keyword_follow_char),
        terminated(tag_no_case("VALUES"), keyword_follow_char),
        terminated(tag_no_case("VIEW"), keyword_follow_char),
        terminated(tag_no_case("WHERE"), keyword_follow_char),
    ))(i)
}

// Matches any SQL reserved keyword
pub fn sql_keyword(i: &[u8]) -> IResult<&[u8], &[u8]> {
    alt((
        keyword_a_to_c,
        keyword_c_to_e,
        keyword_e_to_i,
        keyword_i_to_o,
        keyword_o_to_s,
        keyword_s_to_z,
    ))(i)
}

pub fn escape_if_keyword(s: &str) -> String {
    if sql_keyword(s.as_bytes()).is_ok() {
        format!("`{}`", s)
    } else {
        s.to_owned()
    }
}
