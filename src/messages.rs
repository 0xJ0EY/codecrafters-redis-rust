use std::vec;
use anyhow::{anyhow, Result};
use bytes::BytesMut;

#[derive(Debug, Clone)]
pub enum Message {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Message>)
}

impl Message {
    pub fn serialize(self) -> Result<String> {
        match self {
            Message::SimpleString(s) => Ok(format!("+{}\r\n", s)),
            Message::BulkString(s) => Ok(format!("${}\r\n{}", s.chars().count(), s)),
            Message::Array(items) => {
                let mut parts = Vec::with_capacity(items.len());

                for item in items {
                    parts.push(item.serialize()?);
                }

                Ok(format!("*{}\r\n{}", parts.len(), parts.join("")))
            },
            _ => { Err(anyhow!("impl the rest of the values"))}
        }
    }

    pub fn parse(bytes: &BytesMut) -> Result<Self> {
        let (message, _) = parse_message(bytes)?;

        Ok(message)
    }
}

fn parse_message(bytes: &BytesMut) -> Result<(Message, usize)> {
   match bytes[0] as char {
        '*' => parse_array(bytes),
        '+' => parse_simple_string(bytes),
        '$' => parse_bulk_string(bytes),
        _ => { Err(anyhow!("Unsupported message type. {:?}", bytes))}
    }
}

fn parse_array(bytes: &BytesMut) -> Result<(Message, usize)> {
    let (array_items, mut bytes_consumed) = if let Some((line, len)) = read_until_crlf(&bytes[1..]) {
        let array_items = parse_int(line).unwrap();

        (array_items, len + 1)
    } else {
        return Err(anyhow!("Invalid array, {:?}", bytes));
    };

    let mut items = vec![];

    for _ in 0..array_items {
        let (array_item, len) = parse_message(&BytesMut::from(&bytes[bytes_consumed..]))?;

        items.push(array_item);
        bytes_consumed += len;
    }

    Ok((Message::Array(items), bytes_consumed))
}

fn parse_simple_string(bytes: &BytesMut) -> Result<(Message, usize)> {
    if let Some((line, len)) = read_until_crlf(&bytes[1..]) {
        let string = String::from_utf8(line.to_vec())?;

        return Ok((Message::SimpleString(string), len + 1));
    }

    Err(anyhow!("Invalid simple string, {:?}", bytes))
}

fn parse_bulk_string(bytes: &BytesMut) -> Result<(Message, usize)> {
    let (str_len, bytes_consumed) = if let Some((line, len)) = read_until_crlf(&bytes[1..]) {
        let str_len = parse_int(line).unwrap();

        (str_len, len + 1)
    } else {
        return Err(anyhow!("Invalid bulk string, {:?}", bytes));
    };

    let end_of_bulk_str = bytes_consumed + str_len as usize;
    let total_length = end_of_bulk_str + 2;

    let string = String::from_utf8(bytes[bytes_consumed..end_of_bulk_str].to_vec())?;

    Ok((Message::BulkString(string), total_length))
}

fn parse_int(buffer: &[u8]) -> Result<i64> {
    Ok(String::from_utf8(buffer.to_vec())?.parse::<i64>()?)
}

fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..(i - 1)], i + 1));
        }
    }

    None
}

pub fn unpack_string(message: &Message) -> Result<String> {
    match message {
        Message::SimpleString(s) => Ok(s.clone()),
        Message::BulkString(s) => Ok(s.clone()),
        _ => Err(anyhow!("Unexpected value to unpack from message"))
    }
}