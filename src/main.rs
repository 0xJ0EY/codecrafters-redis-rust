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
use communication::{read_command, write_bulk_string, write_message, write_null_bulk_string, write_rdb_file};
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
    mut socket: TcpStream,
    store: Arc<Mutex<Store>>,
    _configuration: Arc<Mutex<ServerConfiguration>>
) {
    loop {
        if let Ok(command) = read_command(&mut socket).await {
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
                            Message::BulkString("0".to_string()),
                        ]);

                        write_message(&mut socket, &message).await;
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
    mut socket: TcpStream,
    store: Arc<Mutex<Store>>,
    configuration: Arc<Mutex<ServerConfiguration>>
) {
    let mut full_resync = false;

    loop {
        if full_resync {
            {
                let rdb = full_resync_rdb();
                write_rdb_file(&mut socket, &rdb).await;
            }

            let (replication_handle, handle) = replication_channel(socket);

            { // Block scope is needed for RAII, due to handle.await leaving the scope *alive*
                let config = configuration.lock().await;
                config.replication_handles.lock().await.push(replication_handle);
            }

            _ = handle.await;
            return;
        }

        let command = read_command(&mut socket).await;

        if let Ok(command) = command {
            let cloned_command = command.clone();

            match command {
                Command::Ping => {
                    write_simple_string(&mut socket, &"PONG".to_string()).await;
                },
                Command::Echo(value) => {
                    write_bulk_string(&mut socket, &value).await;
                },
                Command::Set(key, value) => {
                    store.lock().await.set(key, value);

                    for replication in configuration.lock().await.replication_handles.lock().await.iter_mut() {
                        if let Some(replica_command) = cloned_command.to_replica_command() {
                            _ = replication.tx.send(replica_command).await;
                        }
                    }

                    write_simple_string(&mut socket, &"OK".to_string()).await;
                },
                Command::Get(key) => {
                    if let Some(entry) = store.lock().await.get(key) {
                        write_bulk_string(&mut socket, &entry.value).await;
                    } else {
                        write_null_bulk_string(&mut socket).await;
                    }
                },
                Command::Info(section) => {
                    match section.to_ascii_lowercase().as_str() {
                        "replication" => {
                            let config = configuration.lock().await;

                            write_bulk_string(&mut socket, &build_replication_response(&config)).await;
                        },
                        "" => {
                            let config = configuration.lock().await;

                            write_bulk_string(&mut socket, &build_replication_response(&config)).await;
                        },
                        _ => {
                            write_simple_string(&mut socket, &"Invalid replication".to_string()).await;
                        }
                    }
                }
                Command::Replconf(_) => {
                    write_simple_string(&mut socket, &"OK".to_string()).await;
                }
                Command::Psync(_) => {
                    let config = configuration.lock().await;
                    write_simple_string(&mut socket, &format!("FULLRESYNC {} 0", &config.repl_id).to_string()).await;

                    full_resync = true;
                }
                Command::Quit => {
                    break;
                }
            }
        } else {
            dbg!(&command);
            write_simple_string(&mut socket, &"Invalid command".to_string()).await;
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
                let socket = handle_handshake_with_master(config)
                    .await
                    .expect("Failed the handshake with the master");

                handle_master(socket, store, configuration).await;
            });
        }

        configuration.lock().await.socket_address
    };

    let listener = TcpListener::bind(socket_address).await?;
    
    loop {
        let (socket, _) = listener.accept().await?;
        let store = store.clone();
        let configuration = configuration.clone();

        tokio::spawn(async move {
            handle_client(socket, store, configuration).await;
        });
    }
}

