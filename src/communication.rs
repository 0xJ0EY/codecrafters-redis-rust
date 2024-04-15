use std::collections::VecDeque;

use anyhow::Result;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};

use crate::messages::Message;

pub const NULL_BULK_STRING: &[u8] = b"$-1\r\n";

pub struct MessageStream {
    pub stream: TcpStream,
    pub read_cache: VecDeque<Message>,
}

impl MessageStream {
    pub fn bind(stream: TcpStream) -> Self {
        Self { stream, read_cache: VecDeque::new() }
    }

    pub async fn write_raw(&mut self, data: &[u8]) -> Result<()> {
        self.stream.write_all(data).await?;
        self.stream.flush().await?;

        Ok(())
    }

    pub async fn write(&mut self, message: Message) -> Result<()> {
        let serialized = message.serialize()?;
        self.write_raw(serialized.as_bytes()).await
    }

    pub async fn read_message(&mut self) -> Option<Message> {
        if self.read_cache.is_empty() {
            self.read_stream().await;
        }

        self.read_cache.pop_front()
    }

    async fn read_stream(&mut self) {
        let mut buffer = [0; 512];

        if let Ok(length) = self.stream.read(&mut buffer).await {
            let mut index = 0;

            while index < length {
                let data = &buffer[index..length];

                if let Ok((message, offset)) = Message::parse(data) {
                    self.read_cache.push_back(message);
                    index += offset;
                } else {
                    println!("Invalid data structure");
                    break;
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum ReplicaMessage {
    RdbFile(String),
    Response(Message)
}

impl ReplicaMessage {
    pub fn is_rdb_file(&self) -> bool {
        match self {
            Self::RdbFile(_) => true,
            _ => false
        }
    }

    pub fn is_response(&self) -> bool {
        match self {
            Self::Response(_) => true,
            _ => false
        }
    }
}

#[derive(Debug)]
pub struct ReplicaStream {
    pub stream: TcpStream,
    pub read_cache: VecDeque<ReplicaMessage>
}

impl ReplicaStream {
    pub fn bind(stream: TcpStream) -> Self {
        Self { stream, read_cache: VecDeque::new() }
    }

    pub async fn write_raw(&mut self, data: &[u8]) -> Result<()> {
        self.stream.write_all(data).await?;
        self.stream.flush().await?;

        Ok(())
    }

    pub async fn write(&mut self, message: Message) -> Result<()> {
        let serialized = message.serialize()?;
        self.write_raw(serialized.as_bytes()).await
    }

    pub async fn get_rdb(&mut self) -> Option<String> {
        if self.read_cache.is_empty() && !self.read_stream().await { return None; }

        let index = self.read_cache.iter().position(|x| x.is_rdb_file());

        if let Some(index) = index {
            if let ReplicaMessage::RdbFile(rdb) = self.read_cache.remove(index).unwrap() {
                return Some(rdb);
            }
        }

        None
    }

    pub async fn get_response(&mut self) -> Option<Message> {
        if self.read_cache.is_empty() && !self.read_stream().await { return None; }

        let index = self.read_cache.iter().position(|x| x.is_response());

        if let Some(index) = index {
            if let ReplicaMessage::Response(message) = self.read_cache.remove(index).unwrap() {
                return Some(message);
            }
        }

        None
    }

    async fn read_stream(&mut self) -> bool {
        let mut buffer = [0; 512];

        if let Ok(length) = self.stream.read(&mut buffer).await {
            let mut index = 0;

            while index < length {
                let data: &[u8] = &buffer[index..length];

                let message = match &data[0] {
                    b'$' => {
                        index += 93; // hardcoded the empty rdb file length
                        ReplicaMessage::RdbFile(String::from("foobar"))
                    },
                    b'+' |b'*' => { 
                        let (message, offset) = Message::parse(data).unwrap();

                        index += offset;

                        ReplicaMessage::Response(message)
                    },
                    _ => {
                        todo!("unsupported file format");
                    }
                };

                self.read_cache.push_back(message);
            }

            true
        } else {
            false
        }
    }
}