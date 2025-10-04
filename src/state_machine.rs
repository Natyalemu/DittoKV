use crate::error::Error;
use crate::log::cmd::{Command, Delete, Set};
use crate::log::log::{Log, LogEntry};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

// 1) Locks on log
// 2) If commit_index > last_applied pass the command to the state machine
pub struct StateMachine {
    log: Log,
    key_value: BTreeMap<String, String>,
}

pub enum StateMachineMsg {
    ShutDown,
    Append(LogEntry),
    CommitTo(u64),
}

impl StateMachine {
    pub fn new() -> Self {
        Self {
            log: Log::new(),
            key_value: BTreeMap::new(),
        }
    }

    pub fn store(&mut self) {
        loop {
            let (commit_idx, last_applied) = {
                let inner = self.log.inner.lock().unwrap();
                (inner.commit_index, inner.last_applied)
            };

            if last_applied >= commit_idx {
                return;
            }

            let next_index = last_applied + 1;

            let maybe_entry = {
                let inner = self.log.inner.lock().unwrap();
                if next_index <= self.log.log_base_index {
                    None
                } else {
                    let pos = (next_index - self.log.log_base_index - 1) as usize;
                    inner.entries.get(pos).cloned()
                }
            };

            let entry = match maybe_entry {
                Some(e) => e,
                None => return, // missing entry, stop until replication catches up
            };

            self.process(entry);

            self.log.set_last_applied(next_index);
        }
    }

    pub fn update_commit_index(&mut self, leader_commit_index: u64) {
        self.log.update_commit_index(leader_commit_index);
    }

    pub fn last_log_term(&self) -> Option<u64> {
        self.log.last_log_term()
    }

    pub fn last_log_index(&self) -> Option<u64> {
        self.log.last_log_index()
    }

    pub fn ready_to_apply(&self) -> bool {
        self.log.atomic_commit_index.load(Ordering::Acquire)
            > self.log.atomic_last_applied.load(Ordering::Acquire)
    }

    pub fn process(&mut self, log_entry: LogEntry) {
        match log_entry.command {
            Command::Delete(delete) => {
                self.key_value.remove(&delete.key);
            }
            Command::Set(set) => {
                self.key_value.insert(set.key, set.value);
            }
            Command::None => {
                // No-op
            }
        }
    }

    pub fn commit_index(&self) -> u64 {
        self.log.atomic_commit_index.load(Ordering::Acquire)
    }

    pub fn log(&mut self, log_entry: LogEntry) {
        let _ = self.log.append_entry(log_entry);
    }

    pub fn entry_term(&self, index: u64) -> Option<u64> {
        self.log.entry_term(index)
    }

    pub fn get_entry(&self, index: u64) -> Option<LogEntry> {
        self.log.get_entry(index)
    }
}

