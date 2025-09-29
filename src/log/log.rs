use crate::log::cmd::Commmand;
use core::fmt;
use serde::{Deserialize, Serialize};
use std::clone;
use std::cmp;
use std::collections::vec_deque;
use std::process::Command;
use std::sync::Arc;
use std::{
    collections::VecDeque,
    sync::{atomic::AtomicU64, atomic::Ordering, Mutex},
};

use super::cmd::{self, Delete};

pub struct SharedLog {
    pub entries: VecDeque<LogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
}

impl SharedLog {
    pub fn new() -> SharedLog {
        SharedLog {
            entries: VecDeque::new(),
            commit_index: 0,
            last_applied: 0,
        }
    }

    pub fn push_back(&mut self, entry: LogEntry) {
        self.entries.push_back(entry);
    }

    pub fn pop_front(&mut self) -> Option<LogEntry> {
        self.entries.pop_front()
    }

    pub fn increment_commit_index(&mut self) {
        self.commit_index = self.commit_index.saturating_add(1);
    }

    pub fn increment_last_applied(&mut self) {
        self.last_applied = self.last_applied.saturating_add(1);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub command: Commmand,
}

impl LogEntry {
    pub fn new(term: u64, command: Commmand) -> Self {
        Self { term, command }
    }
}

pub struct Log {
    pub log_base_index: u64,

    pub inner: Arc<Mutex<SharedLog>>,

    pub atomic_commit_index: AtomicU64,
    pub atomic_last_applied: AtomicU64,
}

impl Log {
    pub fn new() -> Self {
        let shared_log = SharedLog::new();
        Self {
            log_base_index: 0,
            inner: Arc::new(Mutex::new(shared_log)),
            atomic_commit_index: AtomicU64::new(0),
            atomic_last_applied: AtomicU64::new(0),
        }
    }
    /// Tracks the commited log by comparing the leader's commited index and the server runnning on
    /// the machine.
    pub fn update_commit_index(&mut self, leader_commit_index: u64) {
        let mut shared_log = self.inner.lock().unwrap();
        let last_log_index = if shared_log.entries.is_empty() {
            self.log_base_index.saturating_sub(1)
        } else {
            self.log_base_index + shared_log.entries.len() as u64 - 1
        };
        let new_commit = last_log_index.min(leader_commit_index);

        if new_commit >= shared_log.commit_index {
            shared_log.commit_index = new_commit;
        }
    }
    pub fn commit_index(&self) -> u64 {
        let guard = self.inner.lock().unwrap();
        guard.commit_index
    }

    pub fn append_entry(&mut self, entry: LogEntry) -> u64 {
        let mut guard = self.inner.lock().unwrap();
        guard.push_back(entry);
        let last_index = self.log_base_index + guard.entries.len() as u64 - 1;
        drop(guard);
        last_index
    }

    pub fn last_log_index(&self) -> Option<u64> {
        let guard = self.inner.lock().unwrap();
        if guard.entries.is_empty() {
            None
        } else {
            Some(self.log_base_index + guard.entries.len() as u64 - 1)
        }
    }

    pub fn last_log_term(&self) -> Option<u64> {
        let guard = self.inner.lock().unwrap();
        guard.entries.back().map(|e| e.term)
    }

    pub fn increment_atomic_commit_index(&self) -> u64 {
        self.atomic_commit_index.fetch_add(1, Ordering::SeqCst)
    }

    pub fn increment_atomic_last_applied(&self) -> u64 {
        self.atomic_last_applied.fetch_add(1, Ordering::SeqCst)
    }

    pub fn increment_commit_index(&self) {
        let mut guard = self.inner.lock().unwrap();
        guard.increment_commit_index();
        let new_commit = guard.commit_index;
        drop(guard);
        self.atomic_commit_index.store(new_commit, Ordering::SeqCst);
    }

    pub fn increment_last_applied(&self) {
        let mut guard = self.inner.lock().unwrap();
        guard.increment_last_applied();
        let new_last_applied = guard.last_applied;
        drop(guard);
        self.atomic_last_applied
            .store(new_last_applied, Ordering::SeqCst);
    }
}

