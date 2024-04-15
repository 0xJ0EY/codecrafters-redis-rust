use std::{collections::HashMap, env, path::Path, sync::Arc, time::{Duration, SystemTime}};

use tokio::{fs::{metadata, File}, io::AsyncReadExt};

use crate::{configuration::ServerInformation, util::decode_hex};

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

    let file_name = if let Some(filename) = filename { filename } else { return None };

    let current_dir = env::current_dir().unwrap().into_os_string().into_string().unwrap();
    let directory = directory.unwrap_or(current_dir);

    let path = Path::new(&directory).join(Path::new(&file_name));

    let mut f = if let Ok(file) = File::open(&path).await { file } else { return None; };
    let metadata = if let Ok(metadata) = metadata(&path).await { metadata } else { return None; };
    let mut buffer = vec![0; metadata.len() as usize];

    _ = f.read(&mut buffer).await;

    Some(buffer)
}


fn parse_magic_number(data: &Vec<u8>, marker: &mut usize) -> bool {
    let magic_number = b"REDIS";

    *marker += magic_number.len();

    magic_number == &data[0..magic_number.len()]
}

fn parse_rdb(store: &mut Store, data: &Vec<u8>) {
    let mut marker = 0;

    if !parse_magic_number(data, &mut marker) { return; }
    


    dbg!(marker);
}