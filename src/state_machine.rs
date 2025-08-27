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
    pub fn log(&mut self, log_entry: LogEntry) {
        self.log.new_log(log_entry);
    }
}
