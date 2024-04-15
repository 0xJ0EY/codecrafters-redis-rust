use std::time::Duration;

use crate::{messages::{unpack_string, Message}, store::Entry, Command};
use anyhow::{anyhow, bail, Ok, Result};

pub fn get_key_value_from_args(args: &Vec<Message>) -> Result<(String, String)> {
    if args.len() < 2 { bail!("Incomplete command for set") }

    let key = unpack_string(args.first().unwrap())?;
    let value = unpack_string(args.get(1).unwrap())?;

    Ok((key, value))
}

pub fn get_config_params_from_args(args: &Vec<Message>) -> Result<(String, String)> {
    if args.len() < 2 { bail!("Incomplete command for args") }

    let action = unpack_string(args.get(0).unwrap())?;
    let key = unpack_string(args.get(1).unwrap())?;

    Ok((action, key))
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


pub fn parse_client_command(message: &Message) -> Result<Command> {
    let (command, args) = parse_command(message)?;
    let command = command.to_lowercase();

    match command.as_str() {
        "ping" => { Ok(Command::Ping) },
        "echo" => { Ok(Command::Echo(unpack_string(args.first().unwrap())?)) },
        "set" => {
            let (key, value) = get_key_value_from_args(&args)?;
            let expiry = get_expiry_from_args(&args);

            let entry = Entry::new(value, expiry);

            Ok(Command::Set(key, entry))
        },
        "get" => {
            let key: String = unpack_string(args.first().unwrap())?;
            Ok(Command::Get(key))
        }
        "info" => {
            let section = if !args.is_empty() {
                unpack_string(args.first().unwrap())?
            } else {
                String::new()
            };

            Ok(Command::Info(section))
        },
        "replconf" => {
            let repl_args: Vec<_> = args.iter()
                .map(|x| unpack_string(x).unwrap())
                .collect();

            Ok(Command::Replconf(repl_args))
        },
        "psync" => {
            let psync_args: Vec<_> = args.iter()
                .map(|x| unpack_string(x).unwrap())
                .collect();

            Ok(Command::Psync(psync_args))
        },
        "wait" => {
            let (num_replicas, timeout) = get_wait_args(&args)?;

            Ok(Command::Wait(num_replicas, timeout))
        },
        "config" => {
            let (action, key) = get_config_params_from_args(&args)?;

            Ok(Command::Config(action, key))
        }
        _ => Err(anyhow!(format!("Unsupported command, {}", command)))
    }
}

fn parse_command(message: &Message) -> Result<(String, Vec<Message>)> {
    match message {
        Message::Array(x) => { 
            let command = unpack_string(x.first().unwrap())?;
            let args = x.clone().into_iter().skip(1).collect();

            Ok((command, args))
        },
        _ => Err(anyhow!("Unexpected command format"))
    }
}
