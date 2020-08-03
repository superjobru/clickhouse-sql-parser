// vim: set expandtab ts=4 sw=4:

use std::fmt; 
use crate::keywords::{escape_if_keyword};
use crate::{
    SqlType,
    create::{
        CodecList,
        ColumnTTL,
    },
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Column {
    pub name: String,
    pub alias: Option<String>,
    pub table: Option<String>,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref table) = self.table {
            write!(
                f,
                "{}.{}",
                escape_if_keyword(table),
                escape_if_keyword(&self.name)
            )?;
        } else {
            write!(f, "{}", escape_if_keyword(&self.name))?;
        }
        if let Some(ref alias) = self.alias {
            write!(f, " AS {}", escape_if_keyword(alias))?;
        }
        Ok(())
    }
}

impl<'a> From<&'a str> for Column {
    fn from(c: &str) -> Column {
        match c.find(".") {
            None => Column {
                name: String::from(c),
                alias: None,
                table: None,
            },
            Some(i) => Column {
                name: String::from(&c[i + 1..]),
                alias: None,
                table: Some(String::from(&c[0..i])),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ColumnOption {
    DefaultValue(String),
    Materialized(String),
}

impl fmt::Display for ColumnOption {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ColumnOption::DefaultValue(ref literal) => {
                write!(f, "DEFAULT {}", literal.to_string())
            }
            ColumnOption::Materialized(ref literal) => {
                write!(f, "MATERIALIZED {}", literal.to_string())
            }
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ColumnSpecification {
    pub column: Column,
    pub sql_type: SqlType,
    pub codec: Option<CodecList>,
    pub ttl: Option<ColumnTTL>,
    pub nullable: bool,
    pub option: Option<ColumnOption>,
    pub comment: Option<String>,
    pub lowcardinality: bool,
}

impl fmt::Display for ColumnSpecification {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "`{}` ", escape_if_keyword(&self.column.name))?;
        match (self.lowcardinality, self.nullable) {
            (false,false) => write!(f, "{}", self.sql_type),
            (true,false) => write!(f, "LowCardinality({})", self.sql_type),
            (false,true) => write!(f, "Nullable({})", self.sql_type),
            (true,true) => write!(f, "LowCardinality(Nullable({}))", self.sql_type),
        }?;
        if let Some(ref opt) = self.option {
            write!(f, " {}", opt)?;
        }
        if let Some(ref comment) = self.comment {
            write!(f, " COMMENT '{}'", comment)?;
        }
        if let Some(ref codec) = self.codec {
            write!(f, " CODEC({})",
                codec.0
                    .iter()
                    .map(|c| format!("{}", c)) 
                    .collect::<Vec<String>>()
                    .join(", ")
            )?;
        }
        if let Some(ref ttl) = self.ttl {
            write!(f, " {}", ttl)?;
        }
        Ok(())
    }
}

impl ColumnSpecification {
    pub fn new(column: Column, sql_type: SqlType) -> ColumnSpecification {
        ColumnSpecification {
            column,
            sql_type,
            codec: None,
            ttl: None,
            nullable: false,
            option: None,
            comment: None,
            lowcardinality: false,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        TypeSize16,
    };

    #[test]
    fn t_column_display() {
        let cs = ColumnSpecification::new(
            "time_local".into(),
            SqlType::DateTime(None)
        );

        let exp = "`time_local` DateTime";
        assert_eq!(exp, format!("{}", cs).as_str());
    }

    #[test]
    fn t_column_display_enum() {
        let cs = ColumnSpecification::new(
            "device".into(),
            SqlType::Enum(Some(TypeSize16::B8), vec![("desktop".into(), 1), ("mobile".into(),2)]),
        );

        let exp = "`device` Enum8('desktop' = 1, 'mobile' = 2)";
        assert_eq!(exp, format!("{}", cs).as_str());
    }

}
