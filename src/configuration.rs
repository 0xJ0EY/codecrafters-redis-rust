use std::{fmt, net::{SocketAddr, ToSocketAddrs}};

use crate::CommandLineArgs;

#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationRole {
    Master,
    Slave(SocketAddr)
}

impl fmt::Display for ReplicationRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str = match self {
            Self::Master => { "master" },
            Self::Slave(_) => { "slave" }
        };

        write!(f, "{}", str)
    }
}

#[derive(Debug, Clone)]
pub struct ServerConfiguration {
    pub role: ReplicationRole,
    pub connect_clients: usize,
    pub replid: String,
    pub repl_offset: usize,
    
    pub socket_address: SocketAddr,
}

impl ServerConfiguration {
    pub fn new(args: &CommandLineArgs) -> Self {
        let role = if let Some(addr) = parse_replication_addr(args) {
            ReplicationRole::Slave(addr.clone())  
        } else {
            ReplicationRole::Master
        };

        let address = (&args.address).clone();
        let port = (&args.port).clone();

        let socket_address: SocketAddr = SocketAddr::new(address, port);

        
        Self {
            role,
            connect_clients: 0,
            replid: String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"),
            repl_offset: 0,
            socket_address
        }
    }
}

fn parse_replication_addr(args: &CommandLineArgs) -> Option<SocketAddr> {
    if args.replicaof.is_some() {
        let arg = args.replicaof.clone().unwrap();
        let raw_addr = arg.get(0).unwrap();
        let raw_port = arg.get(1).unwrap();

        let port = raw_port.parse::<u16>().expect("Port is not a valid number");
        let server = format!("{}:{}", raw_addr, port);

        if let Ok(socket) = server.to_socket_addrs() {
            let server: Vec<_> = socket.collect();

            let addr = server.last().expect("No valid addresses found");

            return Some(addr.clone());
        } else {
            return None;
        }
    };

    return None;
}