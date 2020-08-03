// vim: set expandtab ts=4 sw=4:

use std::fmt; 
use crate::escape_identifier;

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct Table {
    pub name: String,
    pub alias: Option<String>,
    pub schema: Option<String>,
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref schema) = self.schema {
            write!(f, "{}.", escape_identifier(schema))?;
        }
        write!(f, "{}", escape_identifier(&self.name))?;
        if let Some(ref alias) = self.alias {
            write!(f, " AS {}", escape_identifier(alias))?;
        }
        Ok(())
    }
}

impl<'a> From<&'a str> for Table {
    fn from(t: &str) -> Table {
        Table {
            name: String::from(t),
            alias: None,
            schema: None,
        }
    }
}
impl<'a> From<(&'a str, &'a str)> for Table {
    fn from(t: (&str, &str)) -> Table {
        Table {
            name: String::from(t.1),
            alias: None,
            schema: Some(String::from(t.0)),
        }
    }
}

