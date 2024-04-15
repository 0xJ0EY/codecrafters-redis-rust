use std::{collections::HashMap, time::{Duration, SystemTime}};

use crate::util::decode_hex;

#[derive(Debug, Clone)]
pub struct Entry {
    pub value: String,
    pub expiry_time: Option<Duration>,
    pub expiry_at: Option<SystemTime>,
}

impl Entry {
    pub fn new(value: String, expiry: Option<Duration>) -> Self {
        if expiry.is_some() {
            let current_time = SystemTime::now();
            let expiry_time = current_time + expiry.unwrap();

            Self {
                value,
                expiry_time: expiry,
                expiry_at: Some(expiry_time),
             }
        } else {
            Self { value, expiry_time: None, expiry_at: None }
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

        if let Some(expiry_date_time) = entry.expiry_at {
            if SystemTime::now() > expiry_date_time { return None }
        }

        Some(entry)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

pub fn full_resync_rdb() -> Vec<u8> {
    const EMPTY_RDB: &str = include_str!("empty_rdb.hex");
    let content = decode_hex(EMPTY_RDB).unwrap_or_default();
    let header = format!("${}\r\n", content.len()).as_bytes().to_vec();
    
    [header, content].concat()
}