use crate::log::cmd::Commmand;
use std::collections::vec_deque;
use std::process::Command;
use std::sync::Arc;
use std::{
    collections::VecDeque,
    sync::{atomic::AtomicU64, atomic::Ordering, Mutex},
};

use super::cmd;

struct log {
    inner: Arc<Mutex<SharedLog>>,
}
struct SharedLog {
    entries: VecDeque<LogEntry>,
    commit_index: u64,
    last_applied: u64,
}

struct LogEntry {
    term: u64,
    command: Commmand,
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
            term: term,
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
}

impl log {
    pub fn new() -> Self {
        let shared_log = SharedLog::new();
        Self {
            inner: Arc::new(Mutex::new(shared_log)),
        }
    }

    pub fn new_log(&mut self, entry: LogEntry) {
        let mut guard = self.inner.lock().unwrap();
        guard.new_log(entry);
    }
}
