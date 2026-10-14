mod command;
mod config;
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
    let server_config = config::from_env().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("config read error: {error:?}"),
        )
    })?;

    if let Err(error) = server::run(server_config).await {
        tracing::error!(%error, "server failed");
        return Err(error);
    }

    tracing::info!("server stopped");
    Ok(())
}
