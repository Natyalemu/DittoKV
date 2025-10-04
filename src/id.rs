use core::fmt;
use serde::{Deserialize, Serialize};
use std::{fmt::Formatter, ops};

use crate::error::Error;

#[derive(Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Id(u64);

impl Id {
    pub fn new(id: impl Into<u64>) -> Self {
        let id = id.into();
        Id(id)
    }
    pub fn get_id(self) -> u64 {
        self.0
    }
}
impl fmt::Debug for Id {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Id").field("NewType", &self.0).finish()
    }
}

impl ops::Add<usize> for Id {
    type Output = Id;
    fn add(self, rhs: usize) -> Self::Output {
        let rhs = match u64::try_from(rhs) {
            Ok(v) => v,
            Err(_) => return Id(self.0),
        };
        Id(self.0.saturating_add(rhs))
    }
}

impl ops::Add<u32> for Id {
    type Output = Id;
    fn add(self, rhs: u32) -> Self::Output {
        let rhs = rhs as u64;
        Id(self.0.saturating_add(rhs))
    }
}

impl ops::Add<u64> for Id {
    type Output = Id;
    fn add(self, rhs: u64) -> Self::Output {
        Id(self.0.saturating_add(rhs))
    }
}

impl ops::Sub<usize> for Id {
    type Output = Id;

    fn sub(self, rhs: usize) -> Self::Output {
        let rhs = match u64::try_from(rhs) {
            Ok(v) => v,
            Err(_) => return Id(self.0),
        };
        Id(self.0.saturating_sub(rhs))
    }
}

impl ops::Sub<u32> for Id {
    type Output = Id;
    fn sub(self, rhs: u32) -> Self::Output {
        let rhs = rhs as u64;
        Id(self.0.saturating_sub(rhs))
    }
}

impl ops::Sub<u64> for Id {
    type Output = Id;
    fn sub(self, rhs: u64) -> Self::Output {
        Id(self.0.saturating_sub(rhs))
    }
}
