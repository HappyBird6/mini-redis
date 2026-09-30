mod command;
mod connection;
mod db;
mod resp;
mod server;
mod executor;

use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    server::run("0.0.0.0:6379").await
}
