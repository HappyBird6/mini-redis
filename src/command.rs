//! Redis command parsing and execution will be implemented here.

#![allow(dead_code)]

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Ping,
    Get { key: String },
    Set { key: String, value: Vec<u8> },
}
