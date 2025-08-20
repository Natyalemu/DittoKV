use serde::{Deserialize, Serialize};

use crate::log::cmd::Commmand;
use core::fmt;
use std::clone;
use std::collections::vec_deque;
use std::process::Command;
use std::sync::Arc;
use std::{
    collections::VecDeque,
    sync::{atomic::AtomicU64, atomic::Ordering, Mutex},
};

use super::cmd::{self, Delete};

pub struct Log {
    pub inner: Arc<Mutex<SharedLog>>,
    pub atomic_commit_index: AtomicU64,
    pub atomic_last_applied: AtomicU64,
}
pub struct SharedLog {
    pub entries: VecDeque<LogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
}
#[derive(Deserialize, Serialize)]
pub struct LogEntry {
    pub term: u64,
    pub command: Commmand,
}
impl clone::Clone for LogEntry {
    fn clone(&self) -> Self {
        match self.command {
            Commmand::delete(ref Delete) => {
                let delete = Delete.clone();
                let command = Commmand::delete(Delete.clone());
                Self {
                    term: self.term.clone(),
                    command: command,
                }
            }

            Commmand::set(ref set) => {
                let set = set.clone();
                let Commmand = Commmand::set(set);
                Self {
                    term: self.term.clone(),
                    command: Commmand,
                }
            }

            Commmand::None => Self {
                term: self.term.clone(),
                command: Commmand::None,
            },
        }
    }
}
impl fmt::Debug for LogEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogEntry")
            .field("temr", &self.term)
            .field("Commmand", &self.command);
        Ok(())
    }
}

impl LogEntry {
    pub fn new() -> Self {
        Self {
            term: 1,
            command: Commmand::new(),
        }
    }

    pub fn entry(&mut self, cmd: Commmand) -> Self {
        Self {
            term: self.term,
            command: cmd,
        }
    }
    pub fn new_set(term: u64, key: impl ToString, value: impl ToString) -> Self {
        Self {
            term,
            command: Commmand::new_set(key, value),
        }
    }
}

impl SharedLog {
    pub fn new() -> SharedLog {
        SharedLog {
            entries: VecDeque::new(),
            commit_index: 0,
            last_applied: 0,
        }
    }

    pub fn new_log(&mut self, entry: LogEntry) {
        self.entries.push_back(entry);
    }

    pub fn push_back(&mut self, entry: LogEntry) {
        self.entries.push_back(entry);
    }
    pub fn pop_front(&mut self) {
        self.entries.pop_front();
    }
    pub fn increment_commit_index(&mut self) {
        self.commit_index + 1;
    }
    pub fn increment_last_applied(&mut self) {
        self.last_applied + 1;
    }
}

impl Log {
    pub fn new() -> Self {
        let shared_log = SharedLog::new();
        Self {
            inner: Arc::new(Mutex::new(shared_log)),
            atomic_commit_index: AtomicU64::new(1),
            atomic_last_applied: AtomicU64::new(1),
        }
    }

    pub fn new_log(&mut self, entry: LogEntry) {
        let mut guard = self.inner.lock().unwrap();
        guard.new_log(entry);
    }

    pub fn increment_atomic_commit_index(&mut self) {
        self.atomic_commit_index.fetch_add(1, Ordering::Acquire);
    }
    pub fn increment_atomic_last_applied(&mut self) {
        self.atomic_last_applied.fetch_add(1, Ordering::Acquire);
    }

    pub fn increment_commit_index(&mut self) {
        self.increment_atomic_commit_index();
        self.inner.lock().unwrap().increment_commit_index();
    }
    pub fn increment_last_applied(&mut self) {
        self.increment_atomic_last_applied();
        self.inner.lock().unwrap().increment_last_applied();
    }
}
