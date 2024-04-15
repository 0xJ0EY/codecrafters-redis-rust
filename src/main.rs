use std::{net::IpAddr, sync::Arc, time::Duration, vec};

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
use communication::{parse_client_command, MessageStream, ReplicaStream, NULL_BULK_STRING};
use configuration::ServerInformation;
use info::build_replication_response;
use messages::Message;
use replication::{replication_channel, ReplicaCommand};
use store::{full_resync_rdb, Entry, Store};
use tokio::{net::TcpListener, sync::Mutex};

use crate::replication::{handle_handshake_with_master, needs_to_replicate};

#[derive(Debug, Clone)]
enum Command {
    Echo(String),
    Ping,
    Set(String, Entry),
    Get(String),
    Info(String),
    Replconf(Vec<String>),
    Psync(Vec<String>),
    Wait(usize, u64)
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
    replicaof: Option<Vec<String>>,

    #[clap(long)]
    dir: Option<String>,

    #[clap(long)]
    dbfilename: Option<String>
}

async fn handle_master(
    mut message_stream: ReplicaStream,
    store: Arc<Mutex<Store>>
) {
    let mut bytes_received = 0;

    loop {
        if let Some(message) = message_stream.get_response().await {

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
                _ => {}
            }

            // Not the quickest way of doing things, but very easy and accurate
            let message_len = message.serialize().unwrap().as_bytes().len();
            bytes_received += message_len;
        } else {
            println!("Unable to get a message from the stream");
            break;
        }
    }
}

async fn handle_client(
    mut message_stream: MessageStream,
    store: Arc<Mutex<Store>>,
    information: Arc<ServerInformation>
) {
    let mut full_resync = false;

    loop {
        if full_resync {
            {
                let rdb = full_resync_rdb();

                _ = message_stream.write_raw(&rdb).await;
            }

            let (replication_handle, handle) = replication_channel(message_stream);

            { // Block scope is needed for RAII, due to handle.await leaving the scope *alive*
                information.replication_handles.lock().await.push(replication_handle);
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
                    for replication in information.replication_handles.lock().await.iter_mut() {
                        _ = replication.tx.send(ReplicaCommand::new(message.clone())).await;
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
                            _ = message_stream.write(Message::bulk_string(build_replication_response(&information).await)).await;
                        },
                        "" => {
                            _ = message_stream.write(Message::bulk_string(build_replication_response(&information).await)).await;
                        },
                        _ => {
                            _ = message_stream.write(Message::simple_string_from_str("Invalid replication")).await;
                        }
                    }
                }
                Command::Replconf(params) => {
                    _ = message_stream.write(Message::simple_string_from_str("OK")).await;
                }
                Command::Psync(params) => {
                    _ = message_stream.write(Message::simple_string(format!("FULLRESYNC {} 0", &information.repl_id).to_string())).await;

                    full_resync = true;
                }
                Command::Wait(_replications, wait_time) => {
                    // No clue where the replications are needed
                    // But if we have replications available, and set commands have been called (store length != 0)
                    // We query all replications, and wait if they respond to the ack command.
                    // Otherwise, return all the replications we know about

                    if store.lock().await.len() == 0 {
                        let num_replicas = information.replication_handles.lock().await.len();
                        _ = message_stream.write(Message::Integer(num_replicas as isize)).await;
                    } else {
                        let mut count = 0;
                        
                        for replication in information.replication_handles.lock().await.iter_mut() {
                            let ack_message = Message::Array(vec![
                                Message::BulkString("REPLCONF".to_string()),
                                Message::BulkString("GETACK".to_string()),
                                Message::BulkString("*".to_string()),
                            ]);

                            _ = replication.tx.send(ReplicaCommand::with_timeout(ack_message, Duration::from_millis(wait_time))).await;
                        }

                        for replication in information.replication_handles.lock().await.iter_mut() {
                            let response = replication.rx.recv().await.unwrap();

                            if !response.expired { count += 1; }
                        }

                        _ = message_stream.write(Message::Integer(count)).await;
                    }
                }
            }
        } else {
            _ = message_stream.write(Message::simple_string_from_str("Invalid message")).await;
            break;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = CommandLineArgs::parse();
    let store = Arc::new(Mutex::new(Store::new()));
    let information = Arc::new(ServerInformation::new(&args));

    let socket_address = {
        if needs_to_replicate(&information).await {
            let store = store.clone();
            let information = information.clone();

            tokio::spawn(async move {
                let mut replica_stream = handle_handshake_with_master(information)
                    .await
                    .expect("Failed the handshake with the master");

                // TODO: handle rdb message
                let _ = replica_stream.get_rdb().await;
                
                handle_master(replica_stream, store).await;
            });
        }

        information.socket_address
    };

    let listener = TcpListener::bind(socket_address).await?;
    
    loop {
        let (socket, _) = listener.accept().await?;
        let message_stream = MessageStream::bind(socket);

        let store = store.clone();
        let information = information.clone();

        tokio::spawn(async move {
            handle_client(message_stream, store, information).await;
        });
    }
}

