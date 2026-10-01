use std::net::SocketAddr;
use std::time::Duration;
use std::{io, sync::Arc};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::{connection, db::Db};

const REJECTION_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const SERVER_WAITING_TIMEOUT: Duration = Duration::from_secs(20); // 서버 종료 시 기존 연결 기다리는 시간
const MAXIMUM_CONNECTIONS: usize = 100;

pub async fn run(address: &str) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;
    println!("mini-redis echo server listening on {address}");

    // 클라이언트 모두 같은 DB
    let db = Db::default();

    // 비동기 태스크 관리 컨테이너
    let mut connections: JoinSet<()> = JoinSet::new();

    // 연결 수 제한. Semaphore는 동시에 작업할 수 있는 수 제한하는 도구
    let semaphore = Arc::new(Semaphore::new(MAXIMUM_CONNECTIONS));

    // watch 채널 생성 -> 종료 신호 전달하기 위함 (Sender, Receiver), false는 안닫혔다는 의미, 인자로 bool 받음
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);

    let result: io::Result<()> = loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (socket, peer) = match accepted {
                    Ok(connection) => connection,
                    Err(error) => break Err(error),
                };

                let outcome = match accept_connection(
                    socket, peer, &semaphore, &mut connections, &db, &shutdown_receiver,
                ).await {
                    Ok(outcome) => outcome,
                    Err(error) => break Err(error),
                };
                // Shutdown 이면 루프 종료
                if matches!(outcome,AcceptOutcome::Shutdown) {
                    break Ok(());
                }
            }
            signal = tokio::signal::ctrl_c() => {
                // 컨트롤 c로 강제종료
                match signal {
                    Ok(()) => {
                        println!("shutdown signal received");
                        break Ok(());
                    }
                    Err(error) => break Err(error),
                }
            }

            // 끝난 태스크를 JoinSet에서 제거
            // join_next()의 결과가 Some()일때의 분기 + 근데 connections가 비어있지 않아야 join_next를 기다림
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    eprintln!("connection task failed: {error}");
                }
            }
        }
    };

    // 신규 연결을 받지 않도록 리스닝 소켓 닫기
    drop(listener);

    // 종료를 알리는 true를 send
    let _ = shutdown_sender.send(true);

    graceful_shutdown(connections).await;

    // 루프 결과를 반환
    result
}
enum AcceptOutcome {
    Continue,
    Shutdown,
}
async fn reject_connection(
    socket: &mut TcpStream,
    signal: impl std::future::Future<Output = io::Result<()>>,
) -> io::Result<AcceptOutcome> {
    tokio::select! {
        biased;

        result = signal => {
            result?;
            println!("shutdown signal received");
            Ok(AcceptOutcome::Shutdown)
        }

        _ = timeout(
            REJECTION_WRITE_TIMEOUT,
            socket.write_all(b"-ERR max number of clients reached\r\n"),
        ) => {
            Ok(AcceptOutcome::Continue)
        }
    }
}
async fn accept_connection(
    mut socket: TcpStream,
    peer: SocketAddr,
    semaphore: &Arc<Semaphore>,
    connections: &mut JoinSet<()>,
    db: &Db,
    shutdown: &watch::Receiver<bool>,
) -> io::Result<AcceptOutcome> {
    // 퍼밋 받아옴
    let permit = match Arc::clone(semaphore).try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return reject_connection(&mut socket, tokio::signal::ctrl_c()).await;
        }
    };

    println!("client connected: {peer}");

    let db = db.clone();
    let shutdown = shutdown.clone();

    connections.spawn(async move {
        // 퍼밋하나 할당. 블록끝나면 자동 드랍으로 퍼밋 보충
        let _permit = permit;

        if let Err(error) = connection::handle(socket, db, shutdown).await {
            eprintln!("connection error ({peer}): {error}");
        }
    });

    Ok(AcceptOutcome::Continue)
}
async fn graceful_shutdown(mut connections: JoinSet<()>) {
    // 기존 연결 태스크가 끝날 때까지 대기
    // 강제 종료되었다면 connections.join_next()에 값이 남아있기때문에 이거 마저 처리하고 서버 종료
    // graceful shutdown : 요청 처리와 응답 전송이 끝날 시간을 보장하는 것
    let drained = timeout(SERVER_WAITING_TIMEOUT, async {
        while let Some(result) = connections.join_next().await {
            if let Err(error) = result {
                eprintln!("connection task failed: {error}");
            }
        }
    })
    .await;

    if drained.is_err() {
        // 모든 태스크에 취소 요청
        connections.abort_all();
        // 태스트 끝날때까지 기다리고 connections에서 제거
        while connections.join_next().await.is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    async fn socket_pair() -> (TcpStream, TcpStream, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, peer) = listener.accept().await.unwrap();
        (client, server, peer)
    }

    #[tokio::test]
    async fn rejects_excess_connection_and_reuses_permit_after_shutdown() {
        let semaphore = Arc::new(Semaphore::new(1));
        let mut connections = JoinSet::new();
        let db = Db::default();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut client, socket, peer) = socket_pair().await;
        assert!(matches!(
            accept_connection(
                socket,
                peer,
                &semaphore,
                &mut connections,
                &db,
                &shutdown_rx
            )
            .await
            .unwrap(),
            AcceptOutcome::Continue
        ));
        assert_eq!(semaphore.available_permits(), 0);
        client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        let mut pong = [0; 7];
        timeout(Duration::from_secs(2), client.read_exact(&mut pong))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&pong, b"+PONG\r\n");

        let (mut rejected, socket, peer) = socket_pair().await;
        assert!(matches!(
            accept_connection(
                socket,
                peer,
                &semaphore,
                &mut connections,
                &db,
                &shutdown_rx
            )
            .await
            .unwrap(),
            AcceptOutcome::Continue
        ));
        let mut response = Vec::new();
        timeout(Duration::from_secs(2), rejected.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response, b"-ERR max number of clients reached\r\n");
        assert_eq!(connections.len(), 1);

        shutdown_tx.send(true).unwrap();
        timeout(Duration::from_secs(2), graceful_shutdown(connections))
            .await
            .unwrap();
        assert_eq!(semaphore.available_permits(), 1);
        let mut byte = [0];
        assert_eq!(
            timeout(Duration::from_secs(2), client.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );

        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut client, socket, peer) = socket_pair().await;
        let mut connections = JoinSet::new();
        accept_connection(
            socket,
            peer,
            &semaphore,
            &mut connections,
            &db,
            &shutdown_rx,
        )
        .await
        .unwrap();
        client.write_all(b"*1\r\n$4\r\nQUIT\r\n").await.unwrap();
        let mut response = Vec::new();
        timeout(Duration::from_secs(2), client.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response, b"+OK\r\n");
        timeout(Duration::from_secs(2), graceful_shutdown(connections))
            .await
            .unwrap();
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[tokio::test]
    async fn rejection_prioritizes_shutdown_and_propagates_signal_errors() {
        let (_client, mut socket, _) = socket_pair().await;
        assert!(matches!(
            reject_connection(&mut socket, std::future::ready(Ok(())))
                .await
                .unwrap(),
            AcceptOutcome::Shutdown
        ));
        let error = reject_connection(
            &mut socket,
            std::future::ready(Err(io::Error::other("signal registration failed"))),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(error.to_string(), "signal registration failed");
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_aborts_stuck_tasks_after_deadline_and_releases_permits() {
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let mut connections = JoinSet::new();
        connections.spawn(async move {
            let _permit = permit;
            std::future::pending::<()>().await;
        });
        let started = tokio::time::Instant::now();
        graceful_shutdown(connections).await;
        assert_eq!(started.elapsed(), SERVER_WAITING_TIMEOUT);
        assert_eq!(semaphore.available_permits(), 1);
    }

    // 신규 연결을 받지 않도록 리스닝 소켓 닫기
    drop(listener);

    // 종료를 알리는 true를 send
    let _ = shutdown_sender.send(true);

    // 기존 연결 태스크가 끝날 때까지 대기
    // 강제 종료되었다면 connections.join_next()에 값이 남아있기때문에 이거 마저 처리하고 서버 종료
    // graceful shutdown : 요청 처리와 응답 전송이 끝날 시간을 보장하는 것
    let drained = timeout(SERVER_WAITING_TIMEOUT, async {
        while let Some(result) = connections.join_next().await {
            if let Err(error) = result {
                eprintln!("connection task failed: {error}");
            }
        }
    })
    .await;

    if drained.is_err() {
        // 모든 태스크에 취소 요청
        connections.abort_all(); 
        // 태스트 끝날때까지 기다리고 connections에서 제거
        while connections.join_next().await.is_some() {} 
    }

    Ok(())
}
