use std::clone;

use serde::{Deserialize, Serialize};

type Key = String;
type Value = String;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Commmand {
    None,
    set(Set),
    delete(Delete),
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Set {
    pub key: Key,
    pub value: Value,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Delete {
    pub key: Value,
}

impl clone::Clone for Delete {
    fn clone(&self) -> Self {
        Delete {
            key: self.key.clone(),
        }
    }
}
impl clone::Clone for Set {
    fn clone(&self) -> Self {
        Set {
            key: self.key.clone(),
            value: self.value.clone(),
        }
    }
}
impl Commmand {
    pub fn new() -> Self {
        Commmand::None
    }
    pub fn new_set(key: impl ToString, value: impl ToString) -> Self {
        Commmand::set(Set {
            key: key.to_string(),
            value: value.to_string(),
        })
    }

    pub fn new_delete(key: impl ToString) -> Self {
        Commmand::delete(Delete {
            key: key.to_string(),
        })
    }
}

impl Set {
    pub fn new(key: impl ToString, value: impl ToString) -> Set {
        Set {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    fn key(&self) -> String {
        self.key.clone()
    }
    pub fn value(&self) -> String {
        self.value.clone()
    }
}

impl Delete {
    fn new(key: impl ToString) -> Delete {
        Delete {
            key: key.to_string(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}
