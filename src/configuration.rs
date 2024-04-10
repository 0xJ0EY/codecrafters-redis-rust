use std::{fmt, net::SocketAddr};

#[derive(Debug, Clone, )]
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
    pub repl_offset: usize
}

impl ServerConfiguration {
    pub fn new(replica_addr: Option<SocketAddr>) -> Self {
        let role = if let Some(addr) = replica_addr {
            ReplicationRole::Slave(addr.clone())  
        } else {
            ReplicationRole::Master
        };
        
        Self {
            role,
            connect_clients: 0,
            replid: String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"),
            repl_offset: 0,
        }
    }
}
