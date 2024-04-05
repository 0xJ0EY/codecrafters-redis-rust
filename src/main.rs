use anyhow::{anyhow, Result};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};

#[derive(Debug)]
enum Command {
    Ping,
    Quit
}

async fn read_command(stream: &mut TcpStream) -> Result<Command> {
    let mut buffer = [0; 512];
    let bytes_to_read = stream.read(&mut buffer).await.unwrap();

    if bytes_to_read == 0 { return Ok(Command::Quit); }

    // let command = std::str::from_utf8(&buffer[0..bytes_to_read])?;

    Ok(Command::Ping)
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
                            socket.write(b"+PONG\r\n").await.unwrap();
                        },
                        Command::Quit => {
                            break;
                        }
                    }
                }
            }
        });
    }
}
