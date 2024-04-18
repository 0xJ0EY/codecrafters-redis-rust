use std::{net::IpAddr, sync::Arc, time::Duration, vec};

mod commands;
mod communication;
mod configuration;
mod info;
mod messages;
mod replication;
mod store;
mod util;

use anyhow::Result;
use clap::Parser;
use commands::parse_client_command;
use communication::{MessageStream, ReplicaStream, NULL_BULK_STRING};
use configuration::ServerInformation;
use info::build_replication_response;
use messages::{stream_to_message, Message};
use replication::{replication_channel, ReplicaCommand};
use store::{
    full_resync_rdb, get_end_of_xrange_id, get_start_of_xrange_id, read_rdb_from_file, Entry,
    EntryValue, Store, StreamData, StreamId,
};
use tokio::{net::TcpListener, sync::Mutex};

use crate::replication::{handle_handshake_with_master, needs_to_replicate};

#[derive(Debug)]
pub struct XADDParams {
    pub key: String,
    pub id: String,
    pub values: StreamData,
}

#[derive(Debug)]
pub struct XRANGEParams {
    pub key: String,
    pub start: String,
    pub end: String,
}

#[derive(Debug)]
pub struct XREADParams {
    pub requests: Vec<(String, StreamId)>,
}

#[derive(Debug)]
enum Command {
    Echo(String),
    Ping,
    Set(String, Entry),
    Get(String),
    Info(String),
    Replconf(Vec<String>),
    Psync(Vec<String>),
    Wait(usize, u64),
    Config(String, String),
    Keys(String),
    Type(String),
    XADD(XADDParams),
    XRANGE(XRANGEParams),
    XREAD(XREADParams),
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
    dbfilename: Option<String>,
}

async fn handle_master(mut message_stream: ReplicaStream, store: Arc<Mutex<Store>>) {
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
                    store.lock().await.set_kv_value(key, value);
                }
                Command::Replconf(args) => {
                    let command = args
                        .first()
                        .expect("Replconf args is required")
                        .to_lowercase();

                    if command == "getack" {
                        let message = Message::Array(vec![
                            Message::BulkString("REPLCONF".to_string()),
                            Message::BulkString("ACK".to_string()),
                            Message::BulkString(bytes_received.to_string()),
                        ]);

                        _ = message_stream.write(message).await;
                    }
                }
                Command::Ping => {}
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
    information: Arc<ServerInformation>,
) {
    let mut full_resync = false;

    loop {
        if full_resync {
            {
                let rdb = full_resync_rdb();

                _ = message_stream.write_raw(&rdb).await;
            }

            let (replication_handle, handle) = replication_channel(message_stream);

            {
                // Block scope is needed for RAII, due to handle.await leaving the scope *alive*
                information
                    .replication_handles
                    .lock()
                    .await
                    .push(replication_handle);
            }

            _ = handle.await;
            return;
        }

        if let Some(message) = message_stream.read_message().await {
            let command = if let Ok(command) = parse_client_command(&message) {
                command
            } else {
                _ = message_stream
                    .write(Message::simple_string_from_str("Invalid command"))
                    .await;
                continue;
            };

            match command {
                Command::Ping => {
                    _ = message_stream
                        .write(Message::simple_string_from_str("PONG"))
                        .await;
                }
                Command::Echo(value) => {
                    _ = message_stream.write(Message::bulk_string(value)).await;
                }
                Command::Set(key, value) => {
                    store.lock().await.set_kv_value(key, value);
                    for replication in information.replication_handles.lock().await.iter_mut() {
                        _ = replication
                            .tx
                            .send(ReplicaCommand::new(message.clone()))
                            .await;
                    }

                    _ = message_stream
                        .write(Message::simple_string_from_str("OK"))
                        .await;
                }
                Command::Get(key) => {
                    if let Some(entry) = store.lock().await.get_kv_value(&key) {
                        _ = message_stream
                            .write(Message::bulk_string(entry.value.clone()))
                            .await;
                    } else {
                        _ = message_stream.write_raw(NULL_BULK_STRING).await;
                    }
                }
                Command::Info(section) => match section.to_ascii_lowercase().as_str() {
                    "replication" => {
                        _ = message_stream
                            .write(Message::bulk_string(
                                build_replication_response(&information).await,
                            ))
                            .await;
                    }
                    "" => {
                        _ = message_stream
                            .write(Message::bulk_string(
                                build_replication_response(&information).await,
                            ))
                            .await;
                    }
                    _ => {
                        _ = message_stream
                            .write(Message::simple_string_from_str("Invalid replication"))
                            .await;
                    }
                },
                Command::Replconf(_params) => {
                    _ = message_stream
                        .write(Message::simple_string_from_str("OK"))
                        .await;
                }
                Command::Psync(_params) => {
                    _ = message_stream
                        .write(Message::simple_string(
                            format!("FULLRESYNC {} 0", &information.repl_id).to_string(),
                        ))
                        .await;

                    full_resync = true;
                }
                Command::Wait(_replications, wait_time) => {
                    // No clue where the replications are needed
                    // But if we have replications available, and set commands have been called (store length != 0)
                    // We query all replications, and wait if they respond to the ack command.
                    // Otherwise, return all the replications we know about

                    if store.lock().await.len() == 0 {
                        let num_replicas = information.replication_handles.lock().await.len();
                        _ = message_stream
                            .write(Message::Integer(num_replicas as isize))
                            .await;
                    } else {
                        let mut count = 0;

                        for replication in information.replication_handles.lock().await.iter_mut() {
                            let ack_message = Message::Array(vec![
                                Message::BulkString("REPLCONF".to_string()),
                                Message::BulkString("GETACK".to_string()),
                                Message::BulkString("*".to_string()),
                            ]);

                            _ = replication
                                .tx
                                .send(ReplicaCommand::with_timeout(
                                    ack_message,
                                    Duration::from_millis(wait_time),
                                ))
                                .await;
                        }

                        for replication in information.replication_handles.lock().await.iter_mut() {
                            let response = replication.rx.recv().await.unwrap();

                            if !response.expired {
                                count += 1;
                            }
                        }

                        _ = message_stream.write(Message::Integer(count)).await;
                    }
                }
                Command::Config(action, key) => match action.to_lowercase().as_str() {
                    "get" => {
                        let config_value = information.config.lock().await.get_value(&key);

                        if let Some(value) = config_value {
                            let message = Message::Array(vec![
                                Message::BulkString(key),
                                Message::BulkString(value),
                            ]);

                            _ = message_stream.write(message).await;
                        } else {
                            _ = message_stream
                                .write(Message::simple_string_from_str("Value not found"))
                                .await;
                        }
                    }
                    _ => {
                        _ = message_stream
                            .write(Message::simple_string_from_str("Unsupported config action"))
                            .await;
                    }
                },
                Command::Keys(pattern) => {
                    if pattern == "*" {
                        let keys = store.lock().await.keys();
                        let keys = keys
                            .into_iter()
                            .map(|x| Message::BulkString(x))
                            .collect::<Vec<_>>();

                        let message = Message::Array(keys);

                        _ = message_stream.write(message).await;
                    } else {
                        _ = message_stream
                            .write(Message::simple_string_from_str("Unsupported keys pattern"))
                            .await;
                    }
                }
                Command::Type(key) => {
                    if key.len() == 0 {
                        _ = send_simple_str(&mut message_stream, "Need a key to fetch the type")
                            .await;

                        continue;
                    }

                    let store = store.lock().await;
                    let value = store.get_value(&key);

                    let value_type = if let Some(x) = value {
                        x.value_type()
                    } else {
                        String::from("none")
                    };

                    _ = send_simple_str(&mut message_stream, value_type.as_str()).await;
                }
                Command::XADD(params) => {
                    let mut store = store.lock().await;

                    let id = store
                        .auto_generate_stream_id(&params.key, &params.id)
                        .unwrap();

                    if let Err(err) = store.validate_stream_id(&params.key, &id) {
                        _ = send_error_string(&mut message_stream, err.to_string()).await;
                        continue;
                    }

                    _ = store.append_stream_value(&params.key, &id, params.values);

                    _ = send_bulk_string(&mut message_stream, id).await
                }
                Command::XRANGE(params) => {
                    let store = store.lock().await;

                    let start = get_start_of_xrange_id(&params.start);
                    let end = get_end_of_xrange_id(&params.end, &params.key, &store);

                    let stream = store.get_stream_range(&params.key, start.as_ref(), end.as_ref());

                    if stream.is_none() {
                        _ = send_error_string(
                            &mut message_stream,
                            "ERR Unable to read stream".to_string(),
                        )
                        .await;

                        continue;
                    }

                    let stream = stream.unwrap();
                    let message = stream_to_message(&stream);

                    _ = message_stream.write(message).await;
                }
                Command::XREAD(params) => {
                    let store = store.lock().await;

                    let mut messages: Vec<Message> = Vec::new();

                    for request in params.requests.iter() {
                        let (key, id) = request;
                        let stream = store.get_stream_read(key, id);

                        if stream.is_none() {
                            _ = send_error_string(
                                &mut message_stream,
                                "ERR Unable to read stream".to_string(),
                            )
                            .await;

                            continue;
                        }

                        let stream = stream.unwrap();

                        messages.push(Message::Array(vec![
                            Message::BulkString(key.clone()),
                            stream_to_message(&stream),
                        ]));
                    }

                    _ = message_stream.write(Message::Array(messages)).await;
                }
            }
        } else {
            _ = send_simple_str(&mut message_stream, "Need a key to fetch the type").await;
            break;
        }
    }
}

async fn send_error_string(message_stream: &mut MessageStream, error: String) -> Result<()> {
    message_stream.write(Message::Error(error)).await
}

async fn send_simple_str(message_stream: &mut MessageStream, message: &str) -> Result<()> {
    message_stream
        .write(Message::simple_string_from_str(message))
        .await
}

async fn send_bulk_string(message_stream: &mut MessageStream, message: String) -> Result<()> {
    message_stream.write(Message::bulk_string(message)).await
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = CommandLineArgs::parse();
    let store = Arc::new(Mutex::new(Store::new()));
    let information = Arc::new(ServerInformation::new(&args));

    // Load config values from param
    if let Some(dir) = args.dir {
        information.config.lock().await.dir = Some(dir);
    }
    if let Some(dbfilename) = args.dbfilename {
        information.config.lock().await.dbfilename = Some(dbfilename);
    }

    {
        let rdb_content = read_rdb_from_file(&information).await;

        if let Some(data) = rdb_content {
            store.lock().await.import(&data);
        }
    }

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
