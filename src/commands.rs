use std::time::Duration;

use crate::messages::{unpack_string, Message};
use anyhow::{anyhow, Ok, Result};

pub fn get_key_value_from_args(args: &Vec<Message>) -> Result<(String, String)> {
    if args.len() < 2 { return Err(anyhow!("Incomplete command for set")) }

    let key = unpack_string(args.get(0).unwrap())?;
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
