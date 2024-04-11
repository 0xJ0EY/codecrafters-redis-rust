use std::{net::SocketAddr, sync::Arc, vec};

use anyhow::{bail, Result};
use tokio::{net::TcpStream, sync::{mpsc::{self, Receiver, Sender}, Mutex}, task::JoinHandle};

use crate::{communication::{block_until_response, write_message}, configuration::{ReplicationRole, ServerConfiguration}, messages::Message};

pub async fn needs_to_replicate(configuration: &Arc<Mutex<ServerConfiguration>>) -> bool {
    let configuration = configuration.lock().await;

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

pub async fn handle_handshake_with_master(configuration: Arc<Mutex<ServerConfiguration>>) -> Result<TcpStream> {   
    let configuration = configuration.lock().await;

    let socket_addr = get_master_socket_addr(&configuration);
    if socket_addr.is_none() { bail!("invalid socket address") }

    let socket_addr = socket_addr.unwrap();

    let mut stream = TcpStream::connect(socket_addr).await?;

    { // 1. Ping command
        let ping_command = Message::Array(vec![Message::BulkString("ping".to_string())]);

        write_message(&mut stream, &ping_command).await;
    
        let ping_resp = block_until_response(&mut stream).await?;
        dbg!(ping_resp);
    }

    { // 2.1 REPLCONF listening port
        let listening_port_command = Message::Array(vec![
            Message::BulkString("REPLCONF".to_string()),
            Message::BulkString("listening-port".to_string()),
            Message::BulkString(configuration.socket_address.port().to_string())
        ]);

        write_message(&mut stream, &listening_port_command).await;
    
        let listening_port_resp: Message = block_until_response(&mut stream).await?;
        dbg!(listening_port_resp);
    }

    { // 2.2 REPLCONF capabilities
        let capability_command = Message::Array(vec![
            Message::BulkString("REPLCONF".to_string()),
            Message::BulkString("capa".to_string()),
            Message::BulkString("psync2".to_string())
        ]);

        write_message(&mut stream, &capability_command).await;

        let capability_command_resp = block_until_response(&mut stream).await?;
        dbg!(capability_command_resp);
    }

    { // 3. PSYNC
        let psync_command = Message::Array(vec![
            Message::BulkString("PSYNC".to_string()),
            Message::BulkString("?".to_string()), // replication id
            Message::BulkString("-1".to_string()) // replication offset
        ]);

        write_message(&mut stream, &psync_command).await;

        let psync_command_resp = block_until_response(&mut stream).await?;
        dbg!(psync_command_resp);
    }

    Ok(stream)
}

#[derive(Debug)]
pub struct ReplicaCommand {
    pub message: Message
}
#[derive(Debug)]
pub struct ReplicaHandle {
    pub tx: Sender<ReplicaCommand>,
    pub rx: Receiver<ReplicaCommand>,
}

pub fn replication_channel(mut socket: TcpStream) -> (ReplicaHandle, JoinHandle<()>) {
    let (tx_res, mut rx) = mpsc::channel::<ReplicaCommand>(32);
    let (_tx, rx_res) = mpsc::channel::<ReplicaCommand>(32);

    let handle = tokio::spawn(async move {
        loop {
            dbg!("wait for response");

            while let Some(replica_command) = rx.recv().await {

                dbg!(&socket);
                write_message(&mut socket, &replica_command.message).await;
            }

        }
    });

    (
        ReplicaHandle {
            tx: tx_res,
            rx: rx_res
        },
        handle
    )
}