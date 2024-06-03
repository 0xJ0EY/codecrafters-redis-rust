# Codecrafters Redis clone
This project is an implementation of the ["Build Your Own Redis" challenge by codecrafters.io](https://app.codecrafters.io/courses/redis) in Rust.
As of June 3rd, it passes all the tests within the Codecrafters project.

Please do not use this as an actual replacement for Redis.

# Build
Its a simple Rust project, so you run it by using cargo, like: `cargo run`
And use the official Redis client to connect to the server.

# Features
- Basic key store functionality
- Replication
- RDB persistence
- Streams
