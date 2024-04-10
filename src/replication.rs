use std::{net::SocketAddr, vec};

use anyhow::{bail, Result};
use tokio::{net::TcpStream};

use crate::{communication::{block_until_response, read_response, write_message}, configuration::{ReplicationRole, ServerConfiguration}, messages::Message};

pub fn needs_to_replicate(configuration: &ServerConfiguration) -> bool {
    match configuration.role {
        ReplicationRole::Master => false,
        ReplicationRole::Slave(_) => true
    }
}

fn get_master_socket_addr(configuration: &ServerConfiguration) -> Option<SocketAddr> {
    match configuration.role {
        ReplicationRole::Master => None,
        ReplicationRole::Slave(socket) => Some(socket)
    }
}

pub async fn handle_handshake_with_master(configuration: &ServerConfiguration) -> Result<()> {
    // Open socket, send a ping
    let socket_addr = get_master_socket_addr(configuration);
    if socket_addr.is_none() { bail!("invalid socket address") }

    let socket_addr = socket_addr.unwrap();

    let mut stream = TcpStream::connect(socket_addr).await?;

    {
        let ping_command = Message::Array(vec![Message::BulkString("ping".to_string())]);

        write_message(&mut stream, &ping_command).await;
    
        let ping_resp = block_until_response(&mut stream).await?;
        dbg!(ping_resp);
    }

    {
        // Just send the replconf messages
        let listening_port_command = Message::Array(vec![
            Message::BulkString("REPLCONF".to_string()),
            Message::BulkString("listening-port".to_string()),
            Message::BulkString(socket_addr.port().to_string())
        ]);

        write_message(&mut stream, &listening_port_command).await;
    
        let listening_port_resp: Message = block_until_response(&mut stream).await?;
        dbg!(listening_port_resp);
    }

    {
        // And send the other replconf message
        let capability_command = Message::Array(vec![
            Message::BulkString("REPLCONF".to_string()),
            Message::BulkString("capa".to_string()),
            Message::BulkString("psync2".to_string())
        ]);

        write_message(&mut stream, &capability_command).await;

        let capability_command_resp = block_until_response(&mut stream).await?;
        dbg!(capability_command_resp);
    }

    Ok(())
}