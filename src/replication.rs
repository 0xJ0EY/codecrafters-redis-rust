use std::{net::SocketAddr, sync::Arc, time::Duration, vec};

use anyhow::{bail, Result};
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::mpsc::{self, Receiver, Sender}, task::JoinHandle, time::timeout};

use crate::{communication::{MessageStream, ReplicaStream}, configuration::{ReplicationRole, ServerInformation}, messages::Message};

pub async fn needs_to_replicate(info: &Arc<ServerInformation>) -> bool {
    let info = info;

    match info.role {
        ReplicationRole::Master => false,
        ReplicationRole::Replication(_) => true
    }
}

fn get_master_socket_addr(info: &ServerInformation) -> Option<SocketAddr> {
    match info.role {
        ReplicationRole::Master => None,
        ReplicationRole::Replication(socket) => Some(socket)
    }
}

pub async fn handle_handshake_with_master(info: Arc<ServerInformation>) -> Result<ReplicaStream> {   
    let info = info;

    let socket_addr = get_master_socket_addr(&info);
    if socket_addr.is_none() { bail!("invalid socket address") }

    let socket_addr = socket_addr.unwrap();

    let stream = TcpStream::connect(socket_addr).await?;
    let mut replica_stream = ReplicaStream::bind(stream);

    { // 1. Ping
        println!("Replication: ping");

        let ping_command = Message::Array(vec![Message::BulkString("ping".to_string())]);
        _ = replica_stream.write(ping_command).await;
        _ = replica_stream.get_response().await;
    }

    { // 2.1 REPLCONF listening port
        println!("Replication: replconf port");

        let listening_port_command = Message::Array(vec![
            Message::BulkString("REPLCONF".to_string()),
            Message::BulkString("listening-port".to_string()),
            Message::BulkString(info.socket_address.port().to_string())
        ]);

        _ = replica_stream.write(listening_port_command).await;
        _ = replica_stream.get_response().await;
    }

    { // 2.2 REPLCONF capabilities
        println!("Replication: replconf capa");

        let capability_command = Message::Array(vec![
            Message::BulkString("REPLCONF".to_string()),
            Message::BulkString("capa".to_string()),
            Message::BulkString("psync2".to_string())
        ]);

        _ = replica_stream.write(capability_command).await;
        _ = replica_stream.get_response().await;
    }

    { // 3. PSYNC
        println!("Replication: psync");

        let psync_command = Message::Array(vec![
            Message::BulkString("PSYNC".to_string()),
            Message::BulkString("?".to_string()), // replication id
            Message::BulkString("-1".to_string()) // replication offset
        ]);   

        _ = replica_stream.write(psync_command).await;
        _ = replica_stream.get_response().await;
    }

    Ok(replica_stream)
}

#[derive(Debug)]
pub struct ReplicaCommand {
    pub message: Message,
    pub timeout: Option<Duration>
}

impl ReplicaCommand {
    pub fn new(message: Message) -> Self {
        Self { message, timeout: None }
    }

    pub fn with_timeout(message: Message, duration : Duration) -> Self {
        Self { message, timeout: Some(duration) }
    }
}

#[derive(Debug)]
pub struct ReplicaResponse {
    pub expired: bool
}

impl ReplicaResponse {
    pub fn received() -> Self {
        Self { expired: false }
    }

    pub fn expired() -> Self {
        Self { expired: true }
    }
}

#[derive(Debug)]
pub struct ReplicaHandle {
    pub tx: Sender<ReplicaCommand>,
    pub rx: Receiver<ReplicaResponse>,
}

pub fn replication_channel(mut message_stream: MessageStream) -> (ReplicaHandle, JoinHandle<()>) {
    let (tx_res, mut rx) = mpsc::channel::<ReplicaCommand>(32);
    let (tx, rx_res) = mpsc::channel::<ReplicaResponse>(32);

    let handle = tokio::spawn(async move {
        loop {
            while let Some(replica_command) = rx.recv().await {
                write_message(&mut message_stream.stream, &replica_command.message).await;

                if let Some(duration) = replica_command.timeout {
                    let timed_out = timeout(duration, message_stream.read_message()).await.is_err();
                    let response = if timed_out { ReplicaResponse::expired() } else { ReplicaResponse::received() };

                    _ = tx.send(response).await;
                }
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

async fn write_message(socket: &mut TcpStream, message: &Message) {
    if let Ok(serialized) = message.serialize() {
        socket.write(serialized.as_bytes()).await.expect("Unable to write to socket");
    }
}
