use std::{fmt, net::{SocketAddr, ToSocketAddrs}};

use tokio::sync::Mutex;

use crate::{replication::ReplicaHandle, CommandLineArgs};

#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationRole {
    Master,
    Replication(SocketAddr)
}

impl fmt::Display for ReplicationRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = match self {
            Self::Master => { "master" },
            Self::Replication(_) => { "slave" } // Needed for tests 
        };

        write!(f, "{}", str)
    }
}

#[derive(Debug)]
pub struct ServerInformation {
    pub role: ReplicationRole,
    pub repl_id: String,
    pub repl_offset: usize,
    
    pub socket_address: SocketAddr,
    pub replication_handles: Mutex<Vec<ReplicaHandle>>,
}

impl ServerInformation {
    pub fn new(args: &CommandLineArgs) -> Self {
        let role = if let Some(addr) = parse_replication_addr(args) {
            ReplicationRole::Replication(addr)
        } else {
            ReplicationRole::Master
        };

        let address = args.address;
        let port = args.port;

        let socket_address: SocketAddr = SocketAddr::new(address, port);

        Self {
            role,
            repl_id: String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"),
            repl_offset: 0,
            socket_address,
            replication_handles: Mutex::new(Vec::new())
        }
    }
}

fn parse_replication_addr(args: &CommandLineArgs) -> Option<SocketAddr> {
    if args.replicaof.is_some() {
        let arg = args.replicaof.clone().unwrap();
        let raw_addr = arg.first().unwrap();
        let raw_port = arg.get(1).unwrap();

        let port = raw_port.parse::<u16>().expect("Port is not a valid number");
        let server = format!("{}:{}", raw_addr, port);

        if let Ok(socket) = server.to_socket_addrs() {
            let server: Vec<_> = socket.collect();

            let addr = server.last().expect("No valid addresses found");

            return Some(*addr);
        } else {
            return None;
        }
    };

    None
}