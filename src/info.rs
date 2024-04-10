use crate::configuration::ServerConfiguration;

pub fn build_replication_response(config: &ServerConfiguration) -> String {
    format!("# Replication\n\
        role:{}\n\
        connected_clients:{}\n\
        master_replid:{}\n\
        master_repl_offset:{}\n",
        config.role,
        config.connect_clients,
        config.repl_id,
        config.repl_offset
    )
}
