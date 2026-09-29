use std::io;
use tokio::net::TcpListener;

use crate::connection;

pub async fn run(address: &str) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;
    println!("mini-redis echo server listening on {address}");

    loop {
        let (socket, peer) = listener.accept().await?;
        println!("client connected: {peer}");
        // peer -> 접속한 클라이언트 주소 ex)127.0.0.1:52431

        tokio::spawn(async move {
            if let Err(error) = connection::handle(socket).await {
                eprintln!("connection error ({peer}): {error}");
            }
        });
    }
}

