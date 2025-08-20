use core::fmt;
use serde::{Deserialize, Serialize};
use std::{fmt::Formatter, ops};

use crate::error::Error;

#[derive(Deserialize, Serialize)]
pub struct Id(u64);

impl Id {
    fn new(id: impl Into<u64>) -> Self {
        let id = id.into();
        Id(id)
    }
}
impl fmt::Debug for Id {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Id").field("NewType", &self.0);
        Ok(())
    }
}

impl ops::Add<usize> for Id {
    type Output = Id;
    fn add(self, rhs: usize) -> Self::Output {
        let rhs = u64::try_from(rhs).unwrap();
        Id(self.0.checked_add(rhs).unwrap())
    }
}

impl ops::Add<u32> for Id {
    type Output = Id;
    fn add(self, rhs: u32) -> Self::Output {
        let rhs = u64::try_from(rhs).unwrap();
        Id(self.0.checked_add(rhs).unwrap())
    }
}

impl ops::Add<u64> for Id {
    type Output = Id;
    fn add(self, rhs: u64) -> Self::Output {
        Id(self.0 + rhs)
    }
}

impl ops::Sub<usize> for Id {
    type Output = Id;

    fn sub(self, rhs: usize) -> Self::Output {
        let rhs = u64::try_from(rhs).unwrap();
        Id(self.0.checked_sub(rhs).unwrap())
    }
}

impl ops::Sub<u32> for Id {
    type Output = Id;
    fn sub(self, rhs: u32) -> Self::Output {
        let rhs = u64::try_from(rhs).unwrap();
        Id(self.0.checked_sub(rhs).unwrap())
    }
}

impl ops::Sub<u64> for Id {
    type Output = Id;
    fn sub(self, rhs: u64) -> Self::Output {
        Id(self.0 - rhs)
    }
}
