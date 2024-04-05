use std::{f32::consts::E, sync::Arc};

use anyhow::{anyhow, Context, Result};
use bytes::BytesMut;
use messages::unpack_string;
use store::Store;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::Mutex};

use crate::messages::Message;

mod messages;
mod store;

#[derive(Debug)]
enum Command {
    Echo(String),
    Ping,
    Quit,
    Set(String, String),
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
            if args.len() < 2 { return Err(anyhow!("Incomplete command for set")) }

            let key = unpack_string(args.get(0).unwrap())?;
            let value = unpack_string(args.get(1).unwrap())?;

            Ok(Command::Set(key, value))
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

#[tokio::main]
async fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    // println!("Logs from your program will appear here!");
    let store = Arc::new(Mutex::new(Store::new()));
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut socket, _) = listener.accept().await?;
        let store = store.clone();

        tokio::spawn(async move {
            loop {
                if let Ok(command) = read_command(&mut socket).await {
                    match command {
                        Command::Ping => {
                            write_simple_string(&mut socket, &"PONG".to_string())
                                .await
                                .expect("Unable to write to socket");
                        },
                        Command::Echo(value) => {
                            write_bulk_string(&mut socket, &value)
                                .await
                                .expect("Unable to write to socket");
                        },
                        Command::Set(key, value) => {
                            store.lock().await.set(key, value);

                            write_simple_string(&mut socket, &"OK".to_string())
                                .await
                                .expect("Unable to write to socket");
                        },
                        Command::Get(key) => {

                            if let Some(value) = store.lock().await.get(key) {
                                write_bulk_string(&mut socket, value)
                                    .await
                                    .expect("Unable to write to socket");
                            } else {
                                write_bulk_string(&mut socket, &"-1".to_string())
                                    .await
                                    .expect("Unable to write to socket");
                            }
                        },
                        Command::Quit => {
                            break;
                        }
                    }
                } else {
                    dbg!("invalid command");
                    write_simple_string(&mut socket, &"Invalid command".to_string())
                        .await
                        .expect("Unable to write to socket");
                }
            }
        });
    }
}

async fn write_simple_string(socket: &mut TcpStream, value: &String) -> Result<()> {
    if let Ok(serialized) = Message::SimpleString(String::from(value)).serialize() {
        socket.write(serialized.as_bytes()).await?;
    } else {
        return Err(anyhow!("Unable to write simple string"));
    }

    Ok(())
}

async fn write_bulk_string(socket: &mut TcpStream, value: &String) -> Result<()> {
    if let Ok(serialized) = Message::BulkString(String::from(value)).serialize() {
        socket.write(serialized.as_bytes()).await?;
    } else {
        return Err(anyhow!("Unable to write bulk string"));
    }

    Ok(())
}