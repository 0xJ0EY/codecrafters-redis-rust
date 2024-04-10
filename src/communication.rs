use anyhow::{anyhow, Result};
use bytes::BytesMut;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};

use crate::{commands::{get_expiry_from_args, get_key_value_from_args}, messages::{unpack_string, Message}, store::Entry, Command};

pub async fn read_command(stream: &mut TcpStream) -> Result<Command> {
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
            let key: String = unpack_string(args.first().unwrap())?;
            Ok(Command::Get(key))
        }
        "info" => {
            let section = if !args.is_empty() {
                unpack_string(args.first().unwrap())?
            } else {
                String::new()
            };

            Ok(Command::Info(section))
        },
        "replconf" => {
            let repl_args: Vec<_> = args.iter()
                .map(|x| unpack_string(x).unwrap())
                .collect();

            Ok(Command::Replconf(repl_args))
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

pub async fn read_response(stream: &mut TcpStream) -> Result<Message> {
    let mut buffer = BytesMut::with_capacity(512);
    let _bytes_to_read = stream.read_buf(&mut buffer).await?;

    // if bytes_to_read == 0 { bail!("No bytes to read") }

    Message::parse(&buffer)
}

pub async fn write_simple_string(socket: &mut TcpStream, value: &String) {
    write_message(socket, &Message::SimpleString(value.clone())).await
}

pub async fn write_bulk_string(socket: &mut TcpStream, value: &String) {
    write_message(socket, &Message::BulkString(value.clone())).await
}

pub async fn write_null_bulk_string(socket: &mut TcpStream) {
    socket.write(b"$-1\r\n").await.expect("Unable to write to socket");
}

pub async fn write_message(socket: &mut TcpStream, message: &Message) {
    if let Ok(serialized) = message.serialize() {
        socket.write(serialized.as_bytes()).await.expect("Unable to write to socket");
    }
}