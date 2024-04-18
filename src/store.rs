use std::{
    collections::HashMap,
    env,
    fmt::Display,
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

pub trait EntryValue {
    fn value_type(&self) -> String;
}

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

impl EntryValue for Entry {
    fn value_type(&self) -> String {
        "string".to_string()
    }
}

#[derive(Debug, Clone)]
pub struct StreamData {
    pub data: HashMap<String, String>,
}

impl StreamData {
    pub fn flatten(&self) -> Vec<String> {
        let mut result = Vec::with_capacity(self.data.len() * 2);

        for (key, value) in self.data.iter() {
            result.push(key.clone());
            result.push(value.clone());
        }

        result
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl From<&String> for StreamId {
    fn from(value: &String) -> Self {
        let (ms, seq) = value
            .split_once('-')
            .expect("Unable to parse the string into two parts");

        let ms = ms
            .parse::<u64>()
            .expect("Unable to parse ms value from string");
        let seq = seq
            .parse::<u64>()
            .expect("Unable to parse seq value from string");

        Self { ms, seq }
    }
}

impl Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

// stream_key   | 1526919030474-0   | temperature 36 humidity 95
// store key    | id                | stream data
#[derive(Debug)]
pub struct Stream {
    pub entries: Vec<(StreamId, StreamData)>,
}

impl Stream {
    pub fn empty() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn new(entries: Vec<(StreamId, StreamData)>) -> Self {
        Self { entries }
    }
}

#[derive(Debug)]
pub enum StoreItem {
    KeyValueEntry(Entry),
    Stream(Stream),
}

impl EntryValue for StoreItem {
    fn value_type(&self) -> String {
        match self {
            Self::KeyValueEntry(x) => x.value_type(),
            Self::Stream(_) => "stream".to_string(),
        }
    }
}

#[derive(Debug)]
pub struct Store {
    data: HashMap<String, StoreItem>,
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

    pub fn set_kv_value(&mut self, key: String, value: Entry) {
        let entry = StoreItem::KeyValueEntry(value);
        self.data.insert(key, entry);
    }

    pub fn auto_generate_stream_id(&self, key: &String, id_pattern: &String) -> Option<String> {
        if let Some(stream) = self.get_stream(key) {
            let last_entry = if let Some((last_entry, _)) = stream.entries.last() {
                Some(last_entry)
            } else {
                None
            };

            build_stream_id(id_pattern, last_entry)
        } else {
            build_stream_id(id_pattern, None)
        }
    }

    pub fn get_lastest_stream_id(&self, key: &String) -> Option<&StreamId> {
        let stream = self.get_stream(key)?;
        let last_entry = stream.entries.last()?;

        let (last_id, _) = last_entry;

        Some(last_id)
    }

    pub fn validate_stream_id(&self, key: &String, id: &String) -> Result<()> {
        let stream = if let Some(stream) = self.get_stream(key) {
            stream
        } else {
            return Ok(());
        };

        if stream.entries.len() == 0 {
            return Ok(());
        }

        let (last_id, _) = stream.entries.last().unwrap();

        let (last_id_ms, last_id_seq) = (last_id.ms, last_id.seq);
        let (cur_id_ms, cur_id_seq) = id.split_once('-').unwrap();

        let cur_id_ms = cur_id_ms.parse::<u64>().unwrap();
        let cur_id_seq = cur_id_seq.parse::<u64>().unwrap();

        if cur_id_ms == 0 && cur_id_seq == 0 {
            bail!("ERR The ID specified in XADD must be greater than 0-0");
        }

        if cur_id_ms < last_id_ms {
            bail!(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            );
        }

        if cur_id_ms == last_id_ms && cur_id_seq <= last_id_seq {
            bail!(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            );
        }

        Ok(())
    }

    pub fn append_stream_value(
        &mut self,
        key: &String,
        id: &String,
        stream_data: StreamData,
    ) -> Result<()> {
        let stream = if let Some(stream) = self.get_mut_stream(key) {
            stream
        } else {
            // Create new stream
            self.data
                .insert(key.clone(), StoreItem::Stream(Stream::empty()));

            self.get_mut_stream(key).unwrap()
        };

        let stream_id = StreamId::from(id);

        stream.entries.push((stream_id, stream_data));

        Ok(())
    }

    pub fn get_kv_value(&self, key: &String) -> Option<&Entry> {
        let store_entry = self.data.get(key)?;

        let key_val_entry = if let StoreItem::KeyValueEntry(key_val_entry) = store_entry {
            key_val_entry
        } else {
            return None;
        };

        if let Some(expiry_date_time) = key_val_entry.expiry_at {
            if SystemTime::now() > expiry_date_time {
                return None;
            }
        }

        Some(key_val_entry)
    }

    pub fn get_value(&self, key: &String) -> Option<&StoreItem> {
        self.data.get(key)
    }

    pub fn get_mut_stream(&mut self, key: &String) -> Option<&mut Stream> {
        let store_entry = self.data.get_mut(key)?;

        if let StoreItem::Stream(stream) = store_entry {
            Some(stream)
        } else {
            None
        }
    }

    pub fn get_stream(&self, key: &String) -> Option<&Stream> {
        let store_entry = self.data.get(key)?;

        if let StoreItem::Stream(stream) = store_entry {
            Some(stream)
        } else {
            None
        }
    }

    pub fn get_stream_read(&self, key: &String, id: &StreamId) -> Option<Stream> {
        let stream = self.get_stream(key)?;

        let mut read_entries: Vec<(StreamId, StreamData)> = Vec::new();

        for (entry_id, entry_data) in stream.entries.iter() {
            if entry_id <= id {
                continue;
            }

            read_entries.push((entry_id.clone(), entry_data.clone()));
        }

        if read_entries.len() == 0 {
            return None;
        }

        Some(Stream::new(read_entries))
    }

    pub fn get_stream_range(
        &self,
        key: &String,
        start: Option<&StreamId>,
        end: Option<&StreamId>,
    ) -> Option<Stream> {
        let stream = self.get_stream(key)?;

        let mut range_entries: Vec<(StreamId, StreamData)> = Vec::new();

        for i in 0..stream.entries.len() {
            let cur_stream_entry = stream.entries.get(i)?;
            let (cur_stream_id, _) = &cur_stream_entry;

            if let Some(start) = start {
                if cur_stream_id < start {
                    continue;
                }
            }

            if let Some(end) = end {
                if cur_stream_id > end {
                    break;
                }
            }

            range_entries.push(cur_stream_entry.clone());
        }

        Some(Stream::new(range_entries))
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
            store.set_kv_value(key, entry);
        }
    }
}

fn build_stream_id(pattern: &String, last_stream_entry: Option<&StreamId>) -> Option<String> {
    let pattern = if pattern.len() < 3 { "*-*" } else { pattern };

    let (cur_id_ms, cur_id_seq) = pattern.split_once('-').unwrap();

    let mut id_ms: String = cur_id_ms.to_string();
    let mut id_seq: String = cur_id_seq.to_string();

    let auto_generate_ms = cur_id_ms == "*";

    if auto_generate_ms {
        let now = SystemTime::now();
        let time_since_unix_time = now.duration_since(UNIX_EPOCH).unwrap();

        id_ms = time_since_unix_time.as_millis().to_string();
    }

    let relevant_stream_entry = if let Some(last_id) = last_stream_entry {
        if last_id.ms.to_string() == id_ms {
            Some(last_id)
        } else {
            None
        }
    } else {
        None
    };

    let auto_generate_seq = cur_id_seq == "*";

    if let Some(last_id) = relevant_stream_entry {
        if auto_generate_seq {
            let next_seq = last_id.seq + 1;
            id_seq = next_seq.to_string();
        }
    } else {
        if auto_generate_seq {
            id_seq = if id_ms == "0" {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
    }

    Some(format!("{}-{}", id_ms, id_seq))
}

pub fn get_start_of_xrange_id(id: &String) -> Option<StreamId> {
    if id == "-" {
        return None;
    }

    let (ms, seq) = if let Some(split) = id.split_once('-') {
        split
    } else {
        (id.as_str(), "0")
    };

    let ms = ms.parse::<u64>().expect("Unable to parse ms");
    let seq = seq.parse::<u64>().expect("Unable to parse seq");

    Some(StreamId { ms, seq })
}

pub fn get_end_of_xrange_id(id: &String, key: &String, store: &Store) -> Option<StreamId> {
    if id == "+" {
        return None;
    }

    let template = if id.contains("-") {
        id.clone()
    } else {
        format!("{}-*", id)
    };

    let id = store.auto_generate_stream_id(key, &template)?;

    Some(StreamId::from(&id))
}
