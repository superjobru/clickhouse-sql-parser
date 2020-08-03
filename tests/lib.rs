// vim: set expandtab ts=4 sw=4:

extern crate clickhouse_sql_parser;

use clickhouse_sql_parser::{
    self as parser,
    sql_query,
};

use std::fs::File;
use std::io::Read;
use std::path::Path;

fn parse_queryset(queries: Vec<String>) -> (i32, i32) {
    let mut parsed_ok = Vec::new();
    let mut parsed_err = 0;
    for query in queries.iter() {
        println!("# Trying to parse '{}': ", &query);
        match sql_query(query.trim().as_bytes()) {
            Ok(_) => {
                println!("ok");
                parsed_ok.push(query);
                //continue;
            }
            Err(parser::NomErr::Incomplete(e)) => {
                println!("======");
                println!("Incomplete({:?})", e);
                println!("======");
                parsed_err += 1;
            },
            Err(parser::NomErr::Error((rest, e))) => {
                println!("======");
                println!("Error: {:?}: {}", e, std::str::from_utf8(rest).unwrap());
                println!("======");
                parsed_err += 1;
            },
            Err(parser::NomErr::Failure((rest, e))) => {
                println!("======");
                println!("Failure: {:?}: {}", e, std::str::from_utf8(rest).unwrap());
                println!("======");
                parsed_err += 1;
            },
        }
        //panic!("stop");
    }

    println!("\nParsing failed: {} queries", parsed_err);
    println!("Parsed successfully: {} queries", parsed_ok.len());
    println!("\nSuccessfully parsed queries:");
    for q in parsed_ok.iter() {
        println!("{:?}", q);
    }

    (parsed_ok.len() as i32, parsed_err)
}


fn _test_queries_from_file(file: &Path) -> Result<i32, i32> {
    let mut f = File::open(file).unwrap();
    let mut s = String::new();

    // Load queries
    f.read_to_string(&mut s).unwrap();
    let lines: Vec<String> = s
        .lines()
        .filter(|l| {
            !l.is_empty()
                && !l.starts_with("#")
                && !l.starts_with("--")
                && !(l.starts_with("/*") && l.ends_with("*/;"))
        })
        .map(|l| {
            if !(l.ends_with("\n") || l.ends_with(";")) {
                String::from(l) + "\n"
            } else {
                String::from(l)
            }
        })
        .collect();
    println!("\nLoaded {} queries from {}", lines.len(), file.display());

    // Try parsing them all
    let (ok, err) = parse_queryset(lines);

    if err > 0 {
        return Err(err);
    }
    Ok(ok)
}

fn parse_file(path: &str) -> (i32, i32) {
    let mut f = File::open(Path::new(path)).unwrap();
    let mut s = String::new();

    // Load queries
    f.read_to_string(&mut s).unwrap();
    let lines: Vec<&str> = s
        .lines()
        .map(str::trim)
        .filter(|l| {
            !l.is_empty()
                && !l.starts_with("#")
                && !l.starts_with("--")
                && !(l.starts_with("/*") && l.ends_with("*/;"))
        })
        .collect();
    let mut q = String::new();
    let mut queries = Vec::new();
    for l in lines {
        if !l.ends_with(";") {
            q.push_str(l);
            q.push_str(" ".into());
        } else {
            // end of query
            q.push_str(l);
            queries.push(q.clone());
            q = String::new();
        }
    }
    println!("Loaded {} table definitions", queries.len());

    // Try parsing them all
    parse_queryset(queries)
}

#[test]
fn tables() {
    let (_ok, fail) = parse_file("tests/tables.sql");
    let (_ok, fail) = parse_file("tests/tables2.sql");

	assert_eq!(0, fail);
}


