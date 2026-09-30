use std::io;
use tokio::net::TcpListener;

use crate::{connection, db::Db};

pub async fn run(address: &str) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;
    println!("mini-redis echo server listening on {address}");

    // 클라이언트 모두 같은 DB
    let db = Db::default();

    loop {
        let (socket, peer) = listener.accept().await?;
        println!("client connected: {peer}");
        // peer -> 접속한 클라이언트 주소 ex)127.0.0.1:52431

        // 각 task가 db의 소유권 하나씩을 가지게 함
        let db = db.clone();

        tokio::spawn(async move {
            if let Err(error) = connection::handle(socket,db).await {
                eprintln!("connection error ({peer}): {error}");
            }
        });
    }
}
