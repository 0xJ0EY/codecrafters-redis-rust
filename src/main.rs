use std::{net::{IpAddr, Ipv4Addr, SocketAddr}, sync::Arc};

mod messages;
mod commands;
mod store;

use anyhow::{anyhow, Result};
use bytes::BytesMut;
use clap::Parser;
use commands::{get_expiry_from_args, get_key_value_from_args};
use messages::unpack_string;
use store::{Entry, Store};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::Mutex};

use crate::messages::Message;

#[derive(Debug)]
enum Command {
    Echo(String),
    Ping,
    Quit,
    Set(String, Entry),
    Get(String)
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = CommandLineArgs::parse();
    let store = Arc::new(Mutex::new(Store::new()));

    let address = (&args.address).clone();
    let port = (&args.port).clone();

    let socket_address = SocketAddr::new(address, port);

    let listener = TcpListener::bind(socket_address).await?;

    loop {
        let (mut socket, _) = listener.accept().await?;
        let store = store.clone();

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
