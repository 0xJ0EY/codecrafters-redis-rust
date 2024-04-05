use anyhow::{anyhow, Result};
use tokio::{io::{AsyncWriteExt}, net::{TcpListener, TcpStream}};

#[derive(Debug)]
enum Command {
    Ping,
    Quit
}

fn read_command(stream: &TcpStream) -> Result<Command> {
    let mut buffer = [0; 512];
    let bytes_to_read = stream.try_read(&mut buffer)?;

    if bytes_to_read == 0 { return Ok(Command::Quit); }

    let command = std::str::from_utf8(&buffer[0..bytes_to_read])?;

    match command {
        "*1\r\n$4\r\nPING\r\n" => Ok(Command::Ping),
        "quit\n" => Ok(Command::Quit),
        _ => Err(anyhow!("Unknown command"))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    // println!("Logs from your program will appear here!");
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let command = read_command(&socket).unwrap();

            match command {
                Command::Ping => {
                    socket.write(b"+PONG\r\n").await.unwrap();
                },
                Command::Quit => { }
            }
        });
    }
}
