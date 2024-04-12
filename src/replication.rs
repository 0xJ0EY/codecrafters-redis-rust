use std::{net::SocketAddr, sync::Arc, vec};

use anyhow::{bail, Result};
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::{mpsc::{self, Receiver, Sender}, Mutex}, task::JoinHandle};

use crate::{communication::{write_message, MessageStream, ReplicaStream}, configuration::{self, ReplicationRole, ServerConfiguration}, messages::Message, Command};

pub async fn needs_to_replicate(configuration: &Arc<Mutex<ServerConfiguration>>) -> bool {
    let configuration = configuration.lock().await;

    match configuration.role {
        ReplicationRole::Master => false,
        ReplicationRole::Replication(_) => true
    }
}

fn get_master_socket_addr(configuration: &ServerConfiguration) -> Option<SocketAddr> {
    match configuration.role {
        ReplicationRole::Master => None,
        ReplicationRole::Replication(socket) => Some(socket)
    }
}

pub async fn handle_handshake_with_master(configuration: Arc<Mutex<ServerConfiguration>>) -> Result<ReplicaStream> {   
    let configuration = configuration.lock().await;

    let socket_addr = get_master_socket_addr(&configuration);
    if socket_addr.is_none() { bail!("invalid socket address") }

    let socket_addr = socket_addr.unwrap();

    let stream = TcpStream::connect(socket_addr).await?;
    let mut replica_stream = ReplicaStream::bind(stream);

    { // 1. Ping
        let ping_command = Message::Array(vec![Message::BulkString("ping".to_string())]);
        _ = replica_stream.write(ping_command).await;
        _ = replica_stream.read_message().await;

        dbg!("ping send/received");
    }

    { // 2.1 REPLCONF listening port
        let listening_port_command = Message::Array(vec![
            Message::BulkString("REPLCONF".to_string()),
            Message::BulkString("listening-port".to_string()),
            Message::BulkString(configuration.socket_address.port().to_string())
        ]);

        _ = replica_stream.write(listening_port_command).await;
        _ = replica_stream.read_message().await;

        dbg!("replconf port send/received");
    }

    { // 2.2 REPLCONF capabilities
        let capability_command = Message::Array(vec![
            Message::BulkString("REPLCONF".to_string()),
            Message::BulkString("capa".to_string()),
            Message::BulkString("psync2".to_string())
        ]);

        _ = replica_stream.write(capability_command).await;
        _ = replica_stream.read_message().await;

        dbg!("replconf capa send/received");
    }

    { // 3. PSYNC
        let psync_command = Message::Array(vec![
            Message::BulkString("PSYNC".to_string()),
            Message::BulkString("?".to_string()), // replication id
            Message::BulkString("-1".to_string()) // replication offset
        ]);   

        _ = replica_stream.write(psync_command).await;
        _ = replica_stream.read_message().await;
        dbg!("psync received");
    }

    Ok(replica_stream)
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
    let (tx, rx_res) = mpsc::channel::<ReplicaCommand>(32);

    let handle = tokio::spawn(async move {
        loop {
            while let Some(replica_command) = rx.recv().await {
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