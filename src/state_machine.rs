use crate::error::Error;
use crate::log::cmd::{Command, Delete};
use crate::log::log::{Log, LogEntry};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
//1) Locks on log
//2) If commit_index > last applied pass the command to the state machine
//note: whether to use atomic types at the outer log or lock to check the log commit_index and
//last_applied should be consider and benchmarked
pub struct StateMachine {
    log: Log,
    key_value: BTreeMap<String, String>,
}

unsafe impl Send for StateMachine {}
unsafe impl Sync for StateMachine {}

pub enum StateMachineMsg {
    shut_down,
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
    // Loop over the log to process the remaining entries

    pub fn store(&mut self) {
        loop {
            // 1) snapshot current commit_index and last_applied under the lock
            let (commit_idx, last_applied) = {
                let inner = self.log.inner.lock().unwrap();
                (inner.commit_index, inner.last_applied)
            };

            // nothing to do
            if last_applied >= commit_idx {
                return;
            }

            let next_index = last_applied + 1;

            // 2) try to fetch the entry for next_index
            // we clone the entry so we can drop the lock before applying
            let maybe_entry = {
                let inner = self.log.inner.lock().unwrap();
                // compute position relative to log_base_index
                if next_index <= self.log.log_base_index {
                    None
                } else {
                    let pos = (next_index - self.log.log_base_index - 1) as usize;
                    inner.entries.get(pos).cloned()
                }
            };

            let entry = match maybe_entry {
                Some(e) => e,
                None => {
                    // The entry hasn't arrived in the log yet — stop and wait for replication.
                    return;
                }
            };

            // 3) apply the command to the key_value store
            //    (we do this outside the log lock to avoid holding the mutex during application)
            self.process(entry);

            // 4) advance last_applied under the lock/atomics
            self.log.set_last_applied(next_index);
            // loop will continue until last_applied == commit_idx (or missing entries)
        }
    }

    // A message is passed through this method after a listener listens to a channel tasked with
    // listening to state machine changes
    /* pub fn state_machine_update(&mut self, msg: StateMachineMsg) {
        match msg {
            StateMachineMsg::update => {
                self.store();
            }
            StateMachineMsg::shut_down => {
                todo!()
            }

            StateMachineMsg::Append(entry) => {
                self.log(entry);
            }
            StateMachineMsg::CommitTo(idx) => {
                self.update_commit_index(idx);
            }
        }
    } */

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
            Command::Delete(delete) => {
                self.key_value.remove(&delete.key);
            }
            Command::Set(set) => {
                self.key_value.insert(set.key, set.value);
            }
            Command::None => {
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
