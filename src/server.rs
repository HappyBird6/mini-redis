use std::net::SocketAddr;
use std::time::Duration;
use std::{io, sync::Arc};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::config::ServerConfig;
use crate::{connection, db::Db};

const PURGE_INTERVAL: Duration = Duration::from_secs(1);
const PURGE_BATCH_SIZE: usize = 2000;

const REJECTION_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const SERVER_WAITING_TIMEOUT: Duration = Duration::from_secs(20); // 서버 종료 시 기존 연결 기다리는 시간

pub async fn run(server_config: ServerConfig) -> io::Result<()> {
    let address = server_config.address;
    let listener = TcpListener::bind(&address).await?;
    info!(%address, "server listening");
    // 클라이언트 모두 같은 DB
    let db = Db::default();

    // 비동기 태스크 관리 컨테이너
    let mut connections: JoinSet<()> = JoinSet::new();

    // 연결 수 제한. Semaphore는 동시에 작업할 수 있는 수 제한하는 도구
    let semaphore = Arc::new(Semaphore::new(server_config.max_connections));

    // watch 채널 생성 -> 종료 신호 전달하기 위함 (Sender, Receiver), false는 안닫혔다는 의미, 인자로 bool 받음
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);

    // 만료 키 삭제 타이머 추가
    let mut purge_timer = tokio::time::interval(PURGE_INTERVAL);
    purge_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let result: io::Result<()> = loop {
        tokio::select! {
            _ = purge_timer.tick() => {
                let removed = db.purge_expired(PURGE_BATCH_SIZE);
                if removed > 0 {
                    debug!(removed, "expired keys purged");
                }
            }
            accepted = listener.accept() => {
                let (socket, peer) = match accepted {
                    Ok(connection) => connection,
                    Err(error) => break Err(error),
                };

                let outcome = match accept_connection(
                    socket, peer, &semaphore, &mut connections, &db, &shutdown_receiver,server_config.max_connections
                ).await {
                    Ok(outcome) => outcome,
                    Err(error) => break Err(error),
                };
                // Shutdown 이면 루프 종료
                if matches!(outcome,AcceptOutcome::Shutdown) {
                    break Ok(());
                }
            }
            signal = shutdown_signal() => {
                // Ctrl+C 또는 SIGTERM을 받으면 정상 종료 시작
                match signal {
                    Ok(()) => {
                        info!("shutdown signal received");
                        break Ok(());
                    }
                    Err(error) => break Err(error),
                }
            }

            // 끝난 태스크를 JoinSet에서 제거
            // join_next()의 결과가 Some()일때의 분기 + 근데 connections가 비어있지 않아야 join_next를 기다림
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    error!(%error, "connection task failed");
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
            info!("shutdown signal received");
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
    max_connections: usize,
) -> io::Result<AcceptOutcome> {
    // 퍼밋 받아옴
    let permit = match Arc::clone(semaphore).try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            warn!(%peer, limit = max_connections, "connection limit reached");
            return reject_connection(&mut socket, shutdown_signal()).await;
        }
    };

    let db = db.clone();
    let shutdown = shutdown.clone();

    connections.spawn(async move {
        // 퍼밋하나 할당. 블록끝나면 자동 드랍으로 퍼밋 보충
        let _permit = permit;

        if let Err(error) = connection::handle(peer, socket, db, shutdown).await {
            warn!(%peer, %error, "connection failed");
        }
    });

    Ok(AcceptOutcome::Continue)
}
async fn graceful_shutdown(mut connections: JoinSet<()>) {
    info!(
        remaining = connections.len(),
        "waiting for connections to close"
    );

    // 기존 연결 태스크가 끝날 때까지 대기
    // 강제 종료되었다면 connections.join_next()에 값이 남아있기때문에 이거 마저 처리하고 서버 종료
    // graceful shutdown : 요청 처리와 응답 전송이 끝날 시간을 보장하는 것
    let drained = timeout(SERVER_WAITING_TIMEOUT, async {
        while let Some(result) = connections.join_next().await {
            if let Err(error) = result {
                error!(%error,"connection task failed");
            }
        }
    })
    .await;

    if drained.is_err() {
        warn!(
            remaining = connections.len(),
            "shutdown timed out; aborting connections"
        );

        // 모든 태스크에 취소 요청
        connections.abort_all();
        // 태스트 끝날때까지 기다리고 connections에서 제거
        while connections.join_next().await.is_some() {}
    }

    info!("connection cleanup completed");
}
async fn shutdown_signal() -> io::Result<()> {
    // Unix 계열일때 여기 컴파일
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut terminate = signal(SignalKind::terminate())?;

        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    // Unix 계열이 아니면 컴파일
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    // 종료 신호는 프로세스 전체에 영향을 주므로 별도 테스트 프로세스에서 검증한다.
    #[cfg(unix)]
    #[test]
    fn shutdown_signal_receives_sigterm_and_sigint() {
        const SIGNAL_ENV: &str = "MINI_REDIS_TEST_SHUTDOWN_SIGNAL";
        if let Ok(signal_name) = std::env::var(SIGNAL_ENV) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let signal = shutdown_signal();
                tokio::pin!(signal);

                // 먼저 poll하여 두 종료 신호의 핸들러를 등록한다.
                tokio::select! {
                    biased;
                    result = &mut signal => panic!("signal completed before delivery: {result:?}"),
                    _ = std::future::ready(()) => {}
                }

                let status = std::process::Command::new("kill")
                    .arg(format!("-{signal_name}"))
                    .arg(std::process::id().to_string())
                    .status()
                    .unwrap();
                assert!(status.success());
                timeout(Duration::from_secs(2), &mut signal)
                    .await
                    .expect("shutdown signal was not received")
                    .expect("shutdown signal returned an error");
            });
            return;
        }

        for signal_name in ["TERM", "INT"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "server::tests::shutdown_signal_receives_sigterm_and_sigint",
                    "--nocapture",
                ])
                .env(SIGNAL_ENV, signal_name)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{signal_name} subprocess failed: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
    }

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
        let max_connections = 1;
        let semaphore = Arc::new(Semaphore::new(max_connections));
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
                &shutdown_rx,
                max_connections,
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
                &shutdown_rx,
                max_connections,
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
            max_connections,
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
}
