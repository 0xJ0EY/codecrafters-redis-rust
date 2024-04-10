use crate::configuration::ServerConfiguration;

pub fn build_replication_response(config: &ServerConfiguration) -> String {
    format!("# Replication\n\
        role:{}\n\
        connected_clients:{}\n\
        master_replid:0\n\
        master_repl_offset:0\n\
    ", config.role, config.connect_clients)
}
