use std::net::SocketAddr;

use anyhow::{bail, Result};
use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::{configuration::{ReplicationRole, ServerConfiguration}, messages::Message};

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
    let ping_command = Message::Array(vec![Message::BulkString("ping".to_string())]);

    stream.write_all(ping_command.serialize()?.as_bytes()).await?;

    return Ok(());
}