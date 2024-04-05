use std::{io::{Read, Write}, net::{TcpListener, TcpStream}};
use anyhow::{anyhow, Result};

enum Command {
    Ping,
    Quit
}

fn read_command(mut stream: &mut TcpStream) -> Result<Command> {
    let mut buffer = [0; 1024];

    let bytes_to_read = stream.read(&mut buffer)?;
    if bytes_to_read == 0 { return Ok(Command::Quit); }

    let command = std::str::from_utf8(&buffer[0..bytes_to_read])?;

    match command {
        "*1\r\n$4\r\nping\r\n" => Ok(Command::Ping),
        "quit\n" => Ok(Command::Quit),
        _ => Err(anyhow!("Unknown command"))
    }
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    // println!("Logs from your program will appear here!");
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {

                loop {
                    if let Ok(command) = read_command(&mut stream) {
                        match command {
                            Command::Ping => {
                                stream.write(b"+PONG\r\n").unwrap();
                            },
                            Command::Quit => {
                                break;
                            }
                        }    
                    }
                    stream.write(b"+PONG\r\n").unwrap();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
