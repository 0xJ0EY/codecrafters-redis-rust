use std::time::Duration;

use crate::messages::{unpack_string, Message};
use anyhow::{bail, Ok, Result};

pub fn get_key_value_from_args(args: &Vec<Message>) -> Result<(String, String)> {
    if args.len() < 2 { bail!("Incomplete command for set") }

    let key = unpack_string(args.first().unwrap())?;
    let value = unpack_string(args.get(1).unwrap())?;

    Ok((key, value))
}

pub fn get_expiry_from_args(args: &Vec<Message>) -> Option<Duration> {
    if args.len() < 4 { return None; }

    if let Some(px) = args.get(2) {
        let tag = unpack_string(px).unwrap_or_default();
        if tag != "px" { return None }        
    }

    if let Some(duration) = args.get(3) {
        let time = unpack_string(duration).unwrap_or_default();
        let duration_time = time.parse::<u64>().unwrap_or_default();

        return Some(Duration::from_millis(duration_time));
    }

    None
}

pub fn get_wait_args(args: &Vec<Message>) -> Result<(usize, u64)> {
    if args.len() < 2 { bail!("Incomplete command for wait") }

    let num_replicas = unpack_string(args.get(0)
        .unwrap())?
        .parse::<usize>()?;

    let timeout = unpack_string(args.get(1)
        .unwrap())?
        .parse::<u64>()?;

    Ok((num_replicas, timeout))
}