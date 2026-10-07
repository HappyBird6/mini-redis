mod command;
mod connection;
mod db;
mod executor;
mod logging;
mod resp;
mod server;

use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    logging::init();

    if let Err(error) = server::run("0.0.0.0:6379").await {
        tracing::error!(%error, "server failed");
        return Err(error);
    }

    tracing::info!("server stopped");
    Ok(())
}
