use std::{collections::HashMap, time::{Duration, SystemTime}};

#[derive(Debug, Clone)]
pub struct Entry {
    pub value: String,
    pub expiry: Option<SystemTime>,
}

impl Entry {
    pub fn new(value: String, expiry: Option<Duration>) -> Self {
        if expiry.is_some() {
            let current_time = SystemTime::now();
            let expiry_time = current_time + expiry.unwrap();

            Self { value, expiry: Some(expiry_time) }
        } else {
            Self { value, expiry: None }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Store {
    data: HashMap<String, Entry>,
}

impl Store {
    pub fn new() -> Self {
        Self { data: HashMap::new() }
    }

    pub fn set(&mut self, key: String, value: Entry) {
        self.data.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<&Entry> {
        let entry = self.data.get(&key);
        entry?;

        let entry = entry.unwrap();

        if let Some(expiry_date_time) = entry.expiry {
            if SystemTime::now() > expiry_date_time { return None }
        }

        Some(entry)
    }
}
