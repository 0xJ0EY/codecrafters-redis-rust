use std::{net::{IpAddr, SocketAddr, ToSocketAddrs}, sync::Arc};

mod messages;
mod configuration;
mod commands;
mod info;
mod store;
mod replication;

use anyhow::{anyhow, Result};
use bytes::BytesMut;
use clap::Parser;
use commands::{get_expiry_from_args, get_key_value_from_args};
use configuration::{ReplicationRole, ServerConfiguration};
use info::build_replication_response;
use messages::unpack_string;
use store::{Entry, Store};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::Mutex};

use crate::{messages::Message, replication::{handle_handshake_with_master, needs_to_replicate}};

#[derive(Debug)]
enum Command {
    Echo(String),
    Ping,
    Quit,
    Set(String, Entry),
    Get(String),
    Info(String),
}

async fn read_command(stream: &mut TcpStream) -> Result<Command> {
    let mut buffer = BytesMut::with_capacity(512);
    let bytes_to_read = stream.read_buf(&mut buffer).await?;

    if bytes_to_read == 0 { return Ok(Command::Quit); }
    let (command, args) = parse_command(&buffer)?;
    let command = command.to_lowercase();

    match command.as_str() {
        "ping" => { Ok(Command::Ping) },
        "echo" => { Ok(Command::Echo(unpack_string(args.first().unwrap())?)) },
        "set" => {
            let (key, value) = get_key_value_from_args(&args)?;
            let expiry = get_expiry_from_args(&args);

            let entry = Entry::new(value, expiry);

            Ok(Command::Set(key, entry))
        },
        "get" => {
            let key: String = unpack_string(args.get(0).unwrap())?;
            Ok(Command::Get(key))
        }
        "info" => {
            let section = if args.len() > 0 {
                unpack_string(args.get(0).unwrap())?
            } else {
                String::new()
            };

            Ok(Command::Info(section))
        },
        _ => Err(anyhow!("Unsupported command"))
    }
}

fn parse_command(buffer: &BytesMut) -> Result<(String, Vec<Message>)> {
    let message = Message::parse(buffer)?;

    match message {
        Message::Array(x) => { 
            let command = unpack_string(x.first().unwrap())?;
            let args = x.into_iter().skip(1).collect();

            Ok((command, args))
        },
        _ => Err(anyhow!("Unexpected command format"))
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

#[tokio::main]
async fn main() -> Result<()> {
    let args = CommandLineArgs::parse();
    let store = Arc::new(Mutex::new(Store::new()));

    let replication_addr = if args.replicaof.is_some() {
        let arg = args.replicaof.unwrap();
        let raw_addr = arg.get(0).unwrap();
        let raw_port = arg.get(1).unwrap();

        let port = raw_port.parse::<u16>().expect("Port is not a valid number");
        let server = format!("{}:{}", raw_addr, port);

        if let Ok(socket) = server.to_socket_addrs() {
            let server: Vec<_> = socket.collect();

            let addr = server.last().expect("No valid addresses found");

            Some(addr.clone())
        } else {
            None
        }
    } else {
        None
    };

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
                                write_bulk_string(&mut socket, &entry.value).await
                            } else {
                                write_null_bulk_string(&mut socket).await
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
                    write_simple_string(&mut socket, &"Invalid command".to_string()).await
                }
            }
        });
    }
}

async fn write_simple_string(socket: &mut TcpStream, value: &String) {
    if let Ok(serialized) = Message::SimpleString(String::from(value)).serialize() {
        socket.write(serialized.as_bytes())
            .await
            .expect("Unable to write to socket");
    }
}

async fn write_bulk_string(socket: &mut TcpStream, value: &String) {
    if let Ok(serialized) = Message::BulkString(String::from(value)).serialize() {
        socket.write(serialized.as_bytes())
            .await
            .expect("Unable to write to socket");
    }
}

async fn write_null_bulk_string(socket: &mut TcpStream) {
    socket.write(b"$-1\r\n").await.expect("Unable to write to socket");
}
