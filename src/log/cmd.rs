type Key = String;
type Value = String;

enum Command {
    set(Set),
    delete(Delete),
}

struct Set {
    key: Key,
    value: Value,
}

struct Delete {
    key: Value,
}

impl Set {
    fn new(key: impl ToString, value: impl ToString) -> Set {
        Set {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    fn key(&self) -> String {
        self.key.clone()
    }
    fn value(&self) -> String {
        self.value.clone()
    }
}

impl Delete {
    fn new(key: impl ToString) -> Delete {
        Delete {
            key: key.to_string(),
        }
    }

    fn key(&self) -> &str {
        &self.key
    }
}
