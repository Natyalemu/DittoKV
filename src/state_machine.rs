use crate::log::cmd::{Commmand, Delete};
use crate::log::log::{Log, LogEntry};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
//1) Locks on log
//2) If commit_index > last applied pass the command to the state machine
//note: whether to use atomic types at the outer log or lock to check the log commit_index and
//last_applied should be consider and benchmarked
//
pub struct StateMachine {
    log: Log,
    key_value: BTreeMap<String, String>,
}

impl StateMachine {
    pub fn new() -> Self {
        Self {
            log: Log::new(),
            key_value: BTreeMap::new(),
        }
    }
    // Loop over the log to process the remaining entries

    pub fn store(&mut self) {
        loop {
            if self.log.atomic_commit_index.load(Ordering::Acquire)
                > self.log.atomic_last_applied.load(Ordering::Acquire)
            {
                self.log.increment_last_applied();
                let capacity = self.log.inner.lock().unwrap().entries.capacity() as u64;
                let last_applied =
                    (self.log.atomic_last_applied.load(Ordering::Acquire) % capacity) as usize;

                let log_entry = self.log.inner.lock().unwrap().entries[last_applied].clone();
                self.process(log_entry);
            }
            if self.log.atomic_commit_index.load(Ordering::Acquire)
                == self.log.atomic_last_applied.load(Ordering::Acquire)
            {
                return;
            }
        }
    }

    pub fn update_commit_index(&mut self, leader_commit_index: u64) {
        self.log.update_commit_index(leader_commit_index);
    }
    pub fn last_log_term(&self) -> Option<u64> {
        if let Some(last_log_term) = self.log.last_log_term() {
            return Some(last_log_term);
        }
        None
    }
    pub fn last_log_index(&self) -> Option<u64> {
        if let Some(last_log_index) = self.log.last_log_index() {
            return Some(last_log_index);
        }
        None
    }

    pub fn ready_to_apply(&self) -> bool {
        if self.log.atomic_commit_index.load(Ordering::Acquire)
            > self.log.atomic_last_applied.load(Ordering::Acquire)
        {
            true;
        }
        return false;
    }

    // Consumes a LogEntry and processes the command
    pub fn process(&mut self, log_entry: LogEntry) {
        match log_entry.command {
            Commmand::delete(delete) => {
                self.key_value.remove(&delete.key);
            }
            Commmand::set(set) => {
                self.key_value.insert(set.key, set.value);
            }
            Commmand::None => {
                return;
            }
        }
    }
    pub fn commit_index(&self) -> u64 {
        self.log.atomic_commit_index.load(Ordering::Acquire)
    }

    pub fn log(&mut self, log_entry: LogEntry) {
        let _ = self.log.append_entry(log_entry);
    }
    /*pub fn last_log_index(&self) -> u64 {
        if let Some(last_log_index) = self.log.last_log_index() {
            return last_log_index;
        }
        return 0;
    }*/
    pub fn entry_term(&self, index: u64) -> Option<u64> {
        let guard = self.log.inner.lock().unwrap();

        if index < self.log.log_base_index {
            return None;
        }
        let idx = (index - self.log.log_base_index) as usize;
        guard.entries.get(idx).map(|e| e.term)
    }
    pub fn get_entry(&self, index: u64) -> Option<LogEntry> {
        let guard = self.log.inner.lock().unwrap();
        if index < self.log.log_base_index {
            return None;
        }
        let idx = (index - self.log.log_base_index) as usize;
        guard.entries.get(idx).cloned()
    }
}
