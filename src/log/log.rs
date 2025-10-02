use crate::log::cmd::Command;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub command: Command,
}

impl LogEntry {
    pub fn new(term: u64, command: Command) -> Self {
        Self { term, command }
    }
}

pub struct SharedLog {
    pub entries: VecDeque<LogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
}

impl SharedLog {
    pub fn new() -> Self {
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
}

pub struct Log {
    pub log_base_index: u64,
    pub inner: Arc<Mutex<SharedLog>>,

    pub atomic_commit_index: AtomicU64,
    pub atomic_last_applied: AtomicU64,
}

impl Log {
    pub fn new() -> Self {
        let s = SharedLog::new();
        Self {
            log_base_index: 0,
            inner: Arc::new(Mutex::new(s)),
            atomic_commit_index: AtomicU64::new(0),
            atomic_last_applied: AtomicU64::new(0),
        }
    }

    pub fn append_entry(&self, entry: LogEntry) -> u64 {
        let mut guard = self.inner.lock().unwrap();
        guard.push_back(entry);
        let last_index = self.log_base_index + guard.entries.len() as u64;
        last_index
    }

    pub fn last_log_index(&self) -> Option<u64> {
        let guard = self.inner.lock().unwrap();
        if guard.entries.is_empty() {
            None
        } else {
            Some(self.log_base_index + guard.entries.len() as u64)
        }
    }

    pub fn entry_term(&self, index: u64) -> Option<u64> {
        let guard = self.inner.lock().unwrap();
        if index <= self.log_base_index {
            return None;
        }
        let pos = (index - self.log_base_index - 1) as usize;
        guard.entries.get(pos).map(|e| e.term)
    }

    pub fn get_entry(&self, index: u64) -> Option<LogEntry> {
        let guard = self.inner.lock().unwrap();
        if index <= self.log_base_index {
            return None;
        }
        let pos = (index - self.log_base_index - 1) as usize;
        guard.entries.get(pos).cloned()
    }

    pub fn last_log_term(&self) -> Option<u64> {
        let guard = self.inner.lock().unwrap();
        guard.entries.back().map(|e| e.term)
    }

    pub fn update_commit_index(&self, leader_commit: u64) {
        let mut guard = self.inner.lock().unwrap();
        let last_idx = if guard.entries.is_empty() {
            self.log_base_index
        } else {
            self.log_base_index + guard.entries.len() as u64
        };
        let new_commit = std::cmp::min(leader_commit, last_idx);
        if new_commit > guard.commit_index {
            guard.commit_index = new_commit;
            self.atomic_commit_index.store(new_commit, Ordering::SeqCst);
        }
    }

    pub fn commit_index(&self) -> u64 {
        self.atomic_commit_index.load(Ordering::SeqCst)
    }

    pub fn set_last_applied(&self, new_last_applied: u64) {
        let mut guard = self.inner.lock().unwrap();
        if new_last_applied > guard.last_applied {
            guard.last_applied = new_last_applied;
            self.atomic_last_applied
                .store(new_last_applied, Ordering::SeqCst);
        }
    }

    pub fn last_applied(&self) -> u64 {
        self.atomic_last_applied.load(Ordering::SeqCst)
    }

    pub fn compact_up_to(&mut self, up_to_index: u64) {
        let mut guard = self.inner.lock().unwrap();
        if up_to_index <= self.log_base_index {
            return;
        }
        let to_remove_abs = (up_to_index - self.log_base_index) as usize;
        let actually_have = guard.entries.len();
        let actually_remove = std::cmp::min(to_remove_abs, actually_have);
        for _ in 0..actually_remove {
            guard.entries.pop_front();
        }
        self.log_base_index = self.log_base_index.saturating_add(actually_remove as u64);
    }
}
