use std::{net::IpAddr, sync::Arc};

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
use communication::{read_command, write_bulk_string, write_null_bulk_string, write_rdb_file};
use configuration::ServerConfiguration;
use info::build_replication_response;
use store::{Entry, Store};
use tokio::{net::TcpListener, sync::Mutex};
use util::decode_hex;

use crate::{communication::write_simple_string, replication::{handle_handshake_with_master, needs_to_replicate}};

const empty_rdb: &str = include_str!("empty_rdb.hex");

#[derive(Debug)]
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

#[tokio::main]
async fn main() -> Result<()> {
    let args = CommandLineArgs::parse();
    let store = Arc::new(Mutex::new(Store::new()));

    let configuration = Arc::new(Mutex::new(ServerConfiguration::new(&args)));

    let socket_address = {
        let config = configuration.lock().await;

        if needs_to_replicate(&config) {
            let config = config.clone();

            tokio::spawn(async move {
                _ = handle_handshake_with_master(&config).await;
            });
        }

        config.socket_address
    };

    let listener = TcpListener::bind(socket_address).await?;

    loop {
        let (mut socket, _) = listener.accept().await?;
        let store = store.clone();
        let configuration = configuration.clone();

        tokio::spawn(async move {
            loop {
                if let Ok(command) = read_command(&mut socket).await {
                    match command {
                        Command::Ping => {
                            write_simple_string(&mut socket, &"PONG".to_string()).await;
                        },
                        Command::Echo(value) => {
                            write_bulk_string(&mut socket, &value).await;
                        },
                        Command::Set(key, value) => {
                            store.lock().await.set(key, value);

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
                            let result = decode_hex(empty_rdb).expect("Invalid RDB file");

                            write_simple_string(&mut socket, &format!("FULLRESYNC {} 0", &config.repl_id).to_string()).await;
                            write_rdb_file(&mut socket, &result).await;
                        }
                        Command::Quit => {
                            break;
                        }
                    }
                } else {
                    dbg!("invalid command");
                    write_simple_string(&mut socket, &"Invalid command".to_string()).await;
                }
            }
        });
    }
}

