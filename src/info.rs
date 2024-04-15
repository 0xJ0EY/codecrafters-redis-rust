use crate::configuration::ServerInformation;

pub async fn build_replication_response(info: &ServerInformation) -> String {
    format!("# Replication\n\
        role:{}\n\
        connected_clients:{}\n\
        master_replid:{}\n\
        master_repl_offset:{}\n",
        info.role,
        info.replication_handles.lock().await.len(),
        info.repl_id,
        info.repl_offset
    )
}
