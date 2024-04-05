use anyhow::{anyhow, Result};
use bytes::BytesMut;
use messages::unpack_string;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};

use crate::messages::Message;

mod messages;

#[derive(Debug)]
enum Command {
    Echo(Vec<Message>),
    Ping,
    Quit
}

async fn read_command(stream: &mut TcpStream) -> Result<Command> {
    let mut buffer = BytesMut::with_capacity(512);
    let bytes_to_read = stream.read_buf(&mut buffer).await?;

    if bytes_to_read == 0 { return Ok(Command::Quit); }
    let (command, args) = parse_command(&buffer)?;
    let command = command.to_lowercase();

    match command.as_str() {
        "ping" => { Ok(Command::Ping) },
        "echo" => { Ok(Command::Echo(args)) },
        _ => Err(anyhow!("Unsupported command"))
    }

    // Ok(Command::Ping)
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
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            loop {
                if let Ok(command) = read_command(&mut socket).await {
                    match command {
                        Command::Ping => {
                            if let Ok(serialized) = Message::SimpleString(String::from("PONG")).serialize() {
                                socket.write(serialized.as_bytes()).await.unwrap();
                            }
                        },
                        Command::Echo(values) => {
                            if let Some(message) = values.first() {
                                if let Ok(serialized) = message.clone().serialize() {
                                    socket.write(serialized.as_bytes()).await.unwrap();
                                }
                            }
                        },
                        Command::Quit => {
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
        });
    }
}
