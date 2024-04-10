use std::{net::{IpAddr, SocketAddr, ToSocketAddrs}, sync::Arc};

mod communication;
mod messages;
mod configuration;
mod commands;
mod info;
mod store;
mod replication;

use anyhow::Result;
use clap::Parser;
use communication::{read_command, write_bulk_string, write_null_bulk_string};
use configuration::ServerConfiguration;
use info::build_replication_response;
use store::{Entry, Store};
use tokio::{net::TcpListener, sync::Mutex};

use crate::{communication::write_simple_string, replication::{handle_handshake_with_master, needs_to_replicate}};

#[derive(Debug)]
enum Command {
    Echo(String),
    Ping,
    Quit,
    Set(String, Entry),
    Get(String),
    Info(String),
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

    let replication_addr = parse_replication_addr(&args);
    let configuration = Arc::new(Mutex::new(ServerConfiguration::new(replication_addr)));

    {
        let config = configuration.lock().await;

        if needs_to_replicate(&config) {
            handle_handshake_with_master(&config).await?;
        }
    }

    let address = (&args.address).clone();
    let port = (&args.port).clone();

    let socket_address = SocketAddr::new(address, port);

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

fn parse_replication_addr(args: &CommandLineArgs) -> Option<SocketAddr> {
    if args.replicaof.is_some() {
        let arg = args.replicaof.clone().unwrap();
        let raw_addr = arg.get(0).unwrap();
        let raw_port = arg.get(1).unwrap();

        let port = raw_port.parse::<u16>().expect("Port is not a valid number");
        let server = format!("{}:{}", raw_addr, port);

        if let Ok(socket) = server.to_socket_addrs() {
            let server: Vec<_> = socket.collect();

            let addr = server.last().expect("No valid addresses found");

            return Some(addr.clone());
        } else {
            return None;
        }
    };

    return None;
}