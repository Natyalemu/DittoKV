use crate::log::cmd::{Commmand, Delete};
use crate::log::log::{Log, LogEntry};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
//1) Locks on log
//2) If commit_index > last applied pass the command to the state machine
//note: whether to use atomic types at the outer log or lock to check the log commit_index and
//last_applied should be consider and benchmarked
//
struct StateMachine<'a> {
    log: &'a mut Log,

    key_value: BTreeMap<String, String>,
}

impl<'a> StateMachine<'a> {
    fn new(log: &'a mut Log) -> Self {
        Self {
            log: log,
            key_value: BTreeMap::new(),
        }
    }
    // Loop over the log to process the remaining entries

    fn store(&mut self) {
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
        }
    }

    // Consumes a LogEntry and processes the command
    fn process(&mut self, log_entry: LogEntry) {
        match log_entry.command {
            Commmand::delete(Delete) => {
                self.key_value.remove(&Delete.key);
            }
            Commmand::set(set) => {
                self.key_value.insert(set.key, set.value);
            }
            Commmand::None => {
                return;
            }
        }
    }
}
