// vim: set expandtab ts=4 sw=4:
extern crate clickhouse_sql_parser;

pub use nom::{
    self,
    Err as NomErr,
    error::ErrorKind,
};

#[derive(Debug, PartialEq)]
pub enum Error {
    Incomplete(nom::Needed),
    Error(ErrorKind),
    Failure(ErrorKind),
}

impl<I> From<NomErr<(I, ErrorKind)>> for Error {
    fn from(e: NomErr<(I, ErrorKind)>) -> Error {
        match e {
            NomErr::Incomplete(n) => Error::Incomplete(n),
            NomErr::Error((_, k)) => Error::Error(k),
            NomErr::Failure((_, k)) => Error::Failure(k),
        }
    }
}

fn test() {
    match clickhouse_sql_parser::sql_query("create table a ( i uint8 ) ENGINE=memory".as_bytes()) {
        Ok((_, _)) => println!("Ok"),
        Err(e) => println!("Error: {:?}", Error::from(e)),
    }
}

fn main() {
    test();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        test();
        assert!(true);
    }
}
