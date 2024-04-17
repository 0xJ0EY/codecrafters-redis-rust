use std::{
    collections::HashMap,
    env,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Result};
use tokio::{
    fs::{metadata, File},
    io::AsyncReadExt,
};

use crate::{configuration::ServerInformation, util::decode_hex};

#[derive(Debug, Clone)]
pub struct Entry {
    pub value: String,
    pub expiry_at: Option<SystemTime>,
}

impl Entry {
    pub fn new(value: String, expiry: Option<Duration>) -> Self {
        if expiry.is_some() {
            let current_time = SystemTime::now();
            let expiry_time = current_time + expiry.unwrap();

            Self {
                value,
                expiry_at: Some(expiry_time),
            }
        } else {
            Self {
                value,
                expiry_at: None,
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Store {
    data: HashMap<String, Entry>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn keys(&self) -> Vec<String> {
        self.data.keys().cloned().collect()
    }

    pub fn set(&mut self, key: String, value: Entry) {
        self.data.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<&Entry> {
        let entry = self.data.get(&key);
        entry?;

        let entry = entry.unwrap();

        if let Some(expiry_date_time) = entry.expiry_at {
            if SystemTime::now() > expiry_date_time {
                return None;
            }
        }

        Some(entry)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn import(&mut self, data: &Vec<u8>) {
        parse_rdb(self, data)
    }
}

pub fn full_resync_rdb() -> Vec<u8> {
    const EMPTY_RDB: &str = include_str!("empty_rdb.hex");
    let content = decode_hex(EMPTY_RDB).unwrap_or_default();
    let header = format!("${}\r\n", content.len()).as_bytes().to_vec();

    [header, content].concat()
}

pub async fn read_rdb_from_file(information: &Arc<ServerInformation>) -> Option<Vec<u8>> {
    let config = information.config.lock().await;

    let directory = config.dir.clone();
    let filename = config.dbfilename.clone();

    let file_name = if let Some(filename) = filename {
        filename
    } else {
        return None;
    };

    let current_dir = env::current_dir()
        .unwrap()
        .into_os_string()
        .into_string()
        .unwrap();

    let directory = directory.unwrap_or(current_dir);

    let path = Path::new(&directory).join(Path::new(&file_name));

    let mut f = if let Ok(file) = File::open(&path).await {
        file
    } else {
        return None;
    };

    let metadata = if let Ok(metadata) = metadata(&path).await {
        metadata
    } else {
        return None;
    };

    let mut buffer = vec![0; metadata.len() as usize];

    _ = f.read(&mut buffer).await;

    Some(buffer)
}

fn parse_magic_number(data: &Vec<u8>, marker: &mut usize) -> bool {
    let magic_number = b"REDIS";

    *marker += magic_number.len();

    magic_number == &data[0..magic_number.len()]
}

fn find_database_selector(data: &Vec<u8>, database: u8, marker: &mut usize) -> bool {
    let start = *marker + 1;

    for i in start..data.len() {
        if data[i - 1] == 0xFE && data[i] == database {
            *marker = i + 1;
            return true;
        }
    }

    false
}

fn read_resizedb_field(data: &Vec<u8>, marker: &mut usize) -> bool {
    if data[*marker] != 0xFB {
        return false;
    }

    *marker += 1;

    // TODO: I don't know how to really parse the length of the hash table, so we hardcode it for now
    *marker += 1; // Skip the 0 - 1 - 2
    *marker += 1; // Skip the 0

    true
}

fn read_length_prefixed_string(data: &Vec<u8>, marker: &mut usize) -> Option<String> {
    let length = data[*marker] as usize;
    *marker += 1;

    let start = *marker;
    let end = start + length;

    let slice: &[u8] = &data[start..end];
    let value = std::str::from_utf8(slice).unwrap().to_string();

    *marker = end;

    Some(value)
}

fn read_normal_string_value(data: &Vec<u8>, marker: &mut usize) -> Result<(String, String)> {
    let key = if let Some(key) = read_length_prefixed_string(data, marker) {
        key
    } else {
        bail!("Unable to read key from the entry");
    };

    let value = if let Some(value) = read_length_prefixed_string(data, marker) {
        value
    } else {
        bail!("Unable to read value from the entry");
    };

    Ok((key, value))
}

fn read_entry(data: &Vec<u8>, marker: &mut usize) -> Result<(String, Entry)> {
    let mut offset = *marker;

    match data[offset] {
        0xFC => {
            offset += 1; // Skip the tag

            let expiry_time = u64::from_le_bytes([
                data[offset + 0],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);

            offset += 8;

            let value_type = data[offset];
            offset += 1;
            if value_type != 0x00 {
                bail!("Unsupported value");
            }

            let (key, value) = read_normal_string_value(data, &mut offset)?;

            let expiry = UNIX_EPOCH + Duration::from_millis(expiry_time);
            let current = SystemTime::now();

            let duration = expiry
                .duration_since(current)
                .unwrap_or_else(|_| Duration::from_secs(0));

            *marker = offset;

            Ok((key, Entry::new(value, Some(duration))))
        }
        _ => {
            let value_type = data[offset];
            offset += 1;

            if value_type != 0x00 {
                bail!("Unsupported value");
            }

            let (key, value) = read_normal_string_value(data, &mut offset)?;

            *marker = offset;

            Ok((key, Entry::new(value, None)))
        }
    }
}

fn parse_rdb(store: &mut Store, data: &Vec<u8>) {
    let mut marker = 0;

    if !parse_magic_number(data, &mut marker) {
        return;
    }
    if !find_database_selector(data, 0x00, &mut marker) {
        return;
    }
    if !read_resizedb_field(data, &mut marker) {
        return;
    }

    while data[marker] != 0xFF {
        let result = read_entry(data, &mut marker);

        if let Ok((key, entry)) = result {
            println!("loaded {}", &key);
            store.set(key, entry);
        }
    }
}
