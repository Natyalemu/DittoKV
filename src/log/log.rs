use std::collections::vec_deque;
use std::sync::Arc;
use std::{
    collections::VecDeque,
    sync::{atomic::AtomicU64, atomic::Ordering, Mutex},
};

struct log {
    inner: Arc<Mutex<SharedLog>>,
}
struct SharedLog {
    entries: VecDeque<LogEntry>,
    commit_index: AtomicU64,
    last_applied: AtomicU64,
}

struct LogEntry {
    term: AtomicU64,
    record: Record,
}

struct Record {
    key: String,
    value: String,
}

impl Record {
    fn new() -> Record {
        Record {
            key: String::new(),
            value: String::new(),
        }
    }

    fn key(&self) -> &str {
        &self.key
    }
    fn value(&self) -> &str {
        &self.value
    }
}

impl LogEntry {
    fn new() -> LogEntry {
        LogEntry {
            term: AtomicU64::new(0),
            record: Record::new(),
        }
    }

    fn next_id(&self) -> u64 {
        self.term.fetch_add(1, Ordering::Relaxed)
    }
}

impl SharedLog {
    fn new() -> SharedLog {
        SharedLog {
            entries: VecDeque::new(),
            commit_index: AtomicU64::new(0),
            last_applied: AtomicU64::new(0),
        }
    }

    fn inc_commit_index(&self) -> u64 {
        self.commit_index.fetch_add(1, Ordering::Relaxed)
    }
    fn inc_last_applied(&self) -> u64 {
        self.last_applied.fetch_add(1, Ordering::Relaxed)
    }

    fn insert(&mut self, value: LogEntry) {
        self.entries.push_back(value);
    }
    fn append(&mut self, mut entries: VecDeque<LogEntry>) {
        self.entries.append(&mut entries);
    }
}
