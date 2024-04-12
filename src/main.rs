use std::{net::IpAddr, sync::Arc, vec};

mod communication;
mod messages;
mod configuration;
mod commands;
mod info;
mod store;
mod replication;
mod util;

use anyhow::Result;
use clap::Parser;
use communication::{parse_client_command, write_bulk_string, write_message, write_null_bulk_string, write_rdb_file, MessageStream, ReplicaStream, NULL_BULK_STRING};
use configuration::ServerConfiguration;
use info::build_replication_response;
use messages::Message;
use replication::{replication_channel, ReplicaCommand};
use store::{full_resync_rdb, Entry, Store};
use tokio::{net::{TcpListener, TcpStream}, sync::Mutex};

use crate::{communication::write_simple_string, replication::{handle_handshake_with_master, needs_to_replicate}};

#[derive(Debug, Clone)]
enum Command {
    Echo(String),
    Ping,
    Quit,
    Set(String, Entry),
    Get(String),
    Info(String),
    Replconf(Vec<String>),
    Psync(Vec<String>),
}

impl Command {
    pub fn to_replica_command(&self) -> Option<ReplicaCommand> {
        match self {
            Self::Set(key, entry) => {
                let message = if entry.expiry_at.is_some() {
                    Message::Array(vec![
                        Message::BulkString("set".to_string()),
                        Message::BulkString(key.clone()),
                        Message::BulkString(entry.value.clone()),
                        Message::BulkString("px".to_string()),
                        Message::BulkString(entry.expiry_time.unwrap().as_millis().to_string())
                    ])
                } else {
                    Message::Array(vec![
                        Message::BulkString("set".to_string()),
                        Message::BulkString(key.clone()),
                        Message::BulkString(entry.value.clone()),
                    ])
                };
                
                Some(ReplicaCommand { message })
            }
            _ => { None }
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[clap(about, long_about = None)]
struct CommandLineArgs {
    #[arg(default_value = "127.0.0.1")]
    #[clap(short, long)]
    address: IpAddr,

    #[arg(default_value = "6379")]
    #[clap(short, long)]
    port: u16,

    #[arg(value_delimiter = ' ', num_args = 2)]
    #[clap(long)]
    replicaof: Option<Vec<String>>
}

async fn handle_master(
    mut message_stream: ReplicaStream,
    store: Arc<Mutex<Store>>,
    _configuration: Arc<Mutex<ServerConfiguration>>
) {
    let mut bytes_received = 0;

    loop {
        if let Some(message) = message_stream.get_response().await {
            // Not the quickest way of doing things, but very easy and accurate
            let message_len = message.serialize().unwrap().as_bytes().len(); 
            bytes_received += message_len;

            let command = if let Ok(command) = parse_client_command(&message) {
                command
            } else {
                continue;
            };

            match command {
                Command::Set(key, value) => {
                    store.lock().await.set(key, value);
                },
                Command::Replconf(args) => {
                    let command = args.first().expect("Replconf args is required").to_lowercase();

                    if command == "getack" {
                        let message = Message::Array(vec![
                            Message::BulkString("REPLCONF".to_string()),
                            Message::BulkString("ACK".to_string()),
                            Message::BulkString(bytes_received.to_string()),
                        ]);

                        _ = message_stream.write(message).await;
                    }
                }
                Command::Ping => {},
                Command::Quit => {
                    break;
                }
                _ => {}
            }
        }
    }
}

async fn handle_client(
    mut message_stream: MessageStream,
    store: Arc<Mutex<Store>>,
    configuration: Arc<Mutex<ServerConfiguration>>
) {
    let mut full_resync = false;

    loop {
        if full_resync {
            {
                let rdb = full_resync_rdb();
                _ = message_stream.write_raw(&rdb).await;
            }

            let (replication_handle, handle) = replication_channel(message_stream.stream);

            { // Block scope is needed for RAII, due to handle.await leaving the scope *alive*
                let config = configuration.lock().await;
                config.replication_handles.lock().await.push(replication_handle);
            }

            _ = handle.await;
            return;
        }

        if let Some(message) = message_stream.read_message().await {

            let command = if let Ok(command) = parse_client_command(&message) {
                command
            } else {
                _ = message_stream.write(Message::simple_string_from_str("Invalid command")).await;
                continue;
            };

            match command {
                Command::Ping => {
                    _ = message_stream.write(Message::simple_string_from_str("PONG")).await;
                },
                Command::Echo(value) => {
                    _ = message_stream.write(Message::bulk_string(value)).await;
                },
                Command::Set(key, value) => {
                    store.lock().await.set(key, value);

                    for replication in configuration.lock().await.replication_handles.lock().await.iter_mut() {
                        _ = replication.tx.send(ReplicaCommand { message: message.clone() }).await;
                    }

                    _ = message_stream.write(Message::simple_string_from_str("OK")).await;
                },
                Command::Get(key) => {
                    if let Some(entry) = store.lock().await.get(key) {
                        _ = message_stream.write(Message::bulk_string(entry.value.clone())).await;
                    } else {
                        _ = message_stream.write_raw(NULL_BULK_STRING).await;
                    }
                },
                Command::Info(section) => {
                    match section.to_ascii_lowercase().as_str() {
                        "replication" => {
                            let config = configuration.lock().await;
                            _ = message_stream.write(Message::bulk_string(build_replication_response(&config))).await;
                        },
                        "" => {
                            let config = configuration.lock().await;
                            _ = message_stream.write(Message::bulk_string(build_replication_response(&config))).await;
                        },
                        _ => {
                            _ = message_stream.write(Message::simple_string_from_str("Invalid replication")).await;
                        }
                    }
                }
                Command::Replconf(_) => {
                    _ = message_stream.write(Message::simple_string_from_str("OK")).await;
                }
                Command::Psync(_) => {
                    let config = configuration.lock().await;
                    _ = message_stream.write(Message::simple_string(format!("FULLRESYNC {} 0", &config.repl_id).to_string())).await;

                    full_resync = true;
                }
                Command::Quit => {
                    break;
                }
            }
        } else {
            _ = message_stream.write(Message::simple_string_from_str("Invalid message")).await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = CommandLineArgs::parse();
    let store = Arc::new(Mutex::new(Store::new()));

    let configuration = Arc::new(Mutex::new(ServerConfiguration::new(&args)));

    let socket_address = {
        if needs_to_replicate(&configuration).await {
            let config = configuration.clone();

            let store = store.clone();
            let configuration = configuration.clone();

            tokio::spawn(async move {
                let mut replica_stream = handle_handshake_with_master(config)
                    .await
                    .expect("Failed the handshake with the master");

                // TODO: handle rdb message
                let _ = replica_stream.get_rdb();
                
                handle_master(replica_stream, store, configuration).await;
            });
        }

        configuration.lock().await.socket_address
    };

    let listener = TcpListener::bind(socket_address).await?;
    
    loop {
        let (socket, _) = listener.accept().await?;
        let message_stream = MessageStream::bind(socket);

        let store = store.clone();
        let configuration = configuration.clone();

        tokio::spawn(async move {
            handle_client(message_stream, store, configuration).await;
        });
    }
}

