use std::io;
use tokio::net::TcpListener;
use tokio::task::JoinSet;

use crate::{connection, db::Db};

pub async fn run(address: &str) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;
    println!("mini-redis echo server listening on {address}");

    // 클라이언트 모두 같은 DB
    let db = Db::default();

    // 비동기 태스크 관리 컨테이너
    let mut connections: JoinSet<()> = JoinSet::new();

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (socket, peer) = result?;

                // peer -> 접속한 클라이언트 주소 ex)127.0.0.1:52431
                println!("client connected: {peer}");

                // 각 task가 db의 소유권 하나씩을 가지게 함
                let db = db.clone();

                connections.spawn(async move {
                    if let Err(error) = connection::handle(socket, db).await {
                        eprintln!("connection error ({peer}): {error}");
                    }
                });
            }
            _ = tokio::signal::ctrl_c() => {
                // 컨트롤 c로 강제종료
                println!("shutdown signal received");
                break;
            }

            // 끝난 태스크를 JoinSet에서 제거   
            // join_next()의 결과가 Some()일때의 분기 + 근데 connections가 비어있지 않아야 join_next를 기다림
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    eprintln!("connection task failed: {error}");
                }
            }
        }
    }

    // 기존 연결 태스크가 끝날 때까지 대기
    // 강제 종료되었다면 connections.join_next()에 값이 남아있기때문에 이거 마저 처리하고 서버 종료
    // graceful shutdown : 요청 처리와 응답 전송이 끝날 시간을 보장하는 것
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            eprintln!("connection task failed: {error}");
        }
    }

    Ok(())
}
