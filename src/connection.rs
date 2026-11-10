use std::collections::HashSet;
use std::io::{self};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;
use tracing::info;

use crate::client::{ClientGuard, Clients};
use crate::command::{Command, PubSubCommand};
use crate::db::Db;
use crate::executor;
use crate::pubsub::{PubSub, PubSubMessage, Subscriber, SubscriptionGuard};
use crate::resp;
const MAX_PENDING_READ_BYTES: usize = 2 * 1024 * 1024; // 2 MiB
const MAX_PENDING_WRITE_BYTES: usize = 4 * 1024; // 4 KiB 묶음 전송 기준 (엄격한 메모리 상한 아님)
const READ_TIMEOUT: Duration = Duration::from_secs(60); // 읽기 타임아웃 60초
const WRITE_TIMEOUT: Duration = Duration::from_secs(10); // 쓰기 타임아웃 10초
const MAX_PENDING_PUBSUB_MESSAGES: usize = 128; // 메세지 최대 용량

// 함수 실행 자체를 추적, socket, db, shutdown, clients는 로그필드에서 제외
#[tracing::instrument(skip(socket, db, shutdown, clients, pubsub), fields(peer = %peer))]
pub async fn handle(
    peer: SocketAddr,
    mut socket: TcpStream,
    db: Db,
    clients: Clients,
    mut shutdown: watch::Receiver<bool>,
    client_id: u64,
    pubsub: &PubSub,
) -> io::Result<()> {
    info!("client connected");

    socket.set_nodelay(true)?;

    // 종료시 clients에서 삭제를 하기 위한 client_guard 생성, 동시에 clients에 현재 client 등록
    // handle 끝나면 clientguard 구조체에서 구현한 drop이 실행됨
    let _client_guard = ClientGuard::register(Arc::clone(&clients), client_id, peer);
    /*
    PUBSUB 시작
     */
    // pubsub_guard 생성
    let _subscription_guard = SubscriptionGuard::register(Arc::clone(pubsub), client_id);
    // 메세지 큐 사이즈 포함해 리시버 생성
    let (sender, mut receiver) = mpsc::channel::<PubSubMessage>(MAX_PENDING_PUBSUB_MESSAGES);
    // 연결종료 리시버 생성
    let (disconnect, mut disconnect_receiver) = watch::channel(false);

    let subscriber = Subscriber { sender, disconnect };
    let mut subscriptions = HashSet::new();
    /*
    PUBSUB 끝
    */

    let mut buffer = [0_u8; 4096];
    let mut pending: Vec<u8> = Vec::new();
    loop {
        /*
        shutdwon receiver로 종료 체크
        *로 역참조해서 bool값 가져옴
         */
        if *shutdown.borrow() {
            info!(reason = "shutdown", "connection closing");
            return Ok(());
        }

        if *disconnect_receiver.borrow() {
            info!(reason = "pubsub_disconnect", "connection closing");
            return Ok(());
        }

        // 읽기 한계 측정
        let read_limit = buffer.len().min(MAX_PENDING_READ_BYTES - pending.len());
        if read_limit == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "max pending bytes exceeded",
            ));
        }

        /*
        소켓에서 데이터 읽기, 타임아웃 처리. 중첩된 Result를 반환하기때문에 마지막에 물음표 두개로 처리
        타임아웃되면 에러처리됨
        select로 이벤트 선택 대기
        biased; 로 위쪽 분기에 우선순위 주는 설정

        1. shutdown의 값이 바껴서 연결 처리 종료거나
        2. 읽거나 타임아웃이거나
         */
        let bytes_read = tokio::select! {

            _ = shutdown.changed() =>{
                info!(reason = "shutdown", "connection closing");
                return Ok(());
            }
            // PUBSUB 에 의한 종료
            _ = disconnect_receiver.changed() => {
                info!(reason = "pubsub_disconnect", "connection closing");
                return Ok(());
            }
            // PUBSUB 메세지 수신 이벤트
            event = receiver.recv()=>{
                // 값이 None 일때 Ok
                let Some(event) = event else{
                    return Ok(());
                };

                // 내부 메시지를 RESP 응답으로 변환
                let frame = resp::Frame::Array(vec![
                    resp::Frame::BulkString(b"message".to_vec()),
                    resp::Frame::BulkString(event.channel),
                    resp::Frame::BulkString(event.message),
                ]);

                // Frame을 전송할 바이트로 인코딩
                let mut output = resp::encode(frame).map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("failed to encode pubsub message: {error:?}"),
                    )
                })?;

                // 기존 쓰기 함수 사용: 쓰기 타임아웃도 적용됨
                write_pending(&mut output, &mut socket).await?;

                // 소켓 입력 처리로 내려가지 않고 다시 이벤트 대기
                continue;
            }
            result = async {
                if subscriptions.is_empty() {
                    // 일반 연결: 기존 읽기 타임아웃 적용
                    timeout(
                        READ_TIMEOUT,
                        socket.read(&mut buffer[..read_limit]),
                    )
                    .await
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::TimedOut,
                            "read timeout",
                        )
                    })?
                } else {
                    // 구독 연결: 명령을 보내지 않고 기다릴 수 있음
                    socket.read(&mut buffer[..read_limit]).await
                }
            } => {
                result?
            }
        };

        if bytes_read == 0 {
            // 연결종료 & 펜딩에 남은 데이터 없음
            return if pending.is_empty() {
                info!(reason = "client_eof", "connection closing");
                Ok(())
            } else {
                // 남은 데이터 있는데 연결종료가 됨
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "incomplete RESP frame",
                ))
            };
        }
        // 펜딩에 읽은 버퍼 데이터 붙임
        pending.extend_from_slice(&buffer[..bytes_read]);

        let mut offset: usize = 0;
        let mut pending_resp: Vec<u8> = Vec::new();
        let mut exit_quit: bool = false;
        let mut pending_error: Option<io::Error> = None;

        'commands: loop {
            if *disconnect_receiver.borrow() {
                return Ok(());
            }
            match resp::parse(&pending[offset..]) {
                Ok(Some((frame, consumed))) => {
                    // 처리한 바이트 수를 누적
                    offset += consumed;

                    /*
                    frame을 command로 변경후 execute 함수로 넘기고 응답 frame 받아옴
                     */
                    let responses: Vec<resp::Frame> = match Command::from_frame(frame) {
                        // 구독 모드에서 허용되지않은 커맨드 들어옴 -> 에러
                        Ok(command)
                            if !subscriptions.is_empty() && !allowed_while_subscribed(&command) =>
                        {
                            vec![resp::Frame::Error(
                                "ERR command is not allowed in subscribed mode".to_owned(),
                            )]
                        }
                        // 구독 모드에서 Ping은 배열 응답이라 따로 빼줌
                        Ok(Command::Ping) if !subscriptions.is_empty() => {
                            vec![resp::Frame::Array(vec![
                                resp::Frame::BulkString(b"pong".to_vec()),
                                resp::Frame::BulkString(Vec::new()),
                            ])]
                        }
                        Ok(command) => match command {
                            Command::Quit => {
                                // 쓰기 펜딩에 Quit OK 작성
                                pending_resp.extend_from_slice(b"+OK\r\n");
                                exit_quit = true;
                                break 'commands;
                            }
                            // Client 명령어
                            Command::Client(client_command) => {
                                vec![executor::execute_client(
                                    client_command,
                                    client_id,
                                    &clients,
                                )]
                            }
                            // PubSub
                            Command::PubSub(pubsub_command) => executor::execute_pubsub(
                                pubsub_command,
                                client_id,
                                pubsub,
                                &subscriber,
                                &mut subscriptions,
                                &mut receiver,
                            ),
                            other => vec![executor::execute(other, &db)],
                        },
                        Err(error) => {
                            tracing::debug!(?error, "invalid command");
                            vec![resp::Frame::Error(error.message().to_owned())]
                        }
                    };

                    // 받은 frame을 encoder로 byte로 변환
                    for response in responses {
                        let response_bytes = match resp::encode(response) {
                            Ok(bytes) => bytes,
                            Err(err) => {
                                pending_error = Some(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!("failed to encode RESP response: {err:?}"),
                                ));
                                break 'commands;
                            }
                        };
                        // 응답이 너무 길어지지 않게 도중에 펜딩 비우기
                        if pending_resp.len() + response_bytes.len() > MAX_PENDING_WRITE_BYTES {
                            write_pending(&mut pending_resp, &mut socket).await?;
                        }
                        // 응답 펜딩에 응답 작성
                        pending_resp.extend_from_slice(&response_bytes);

                        // 응답 하나가 맥스 넘어감. -> 펜딩 비우기
                        if pending_resp.len() >= MAX_PENDING_WRITE_BYTES {
                            write_pending(&mut pending_resp, &mut socket).await?;
                        }
                    }
                }
                Ok(None) => {
                    // 데이터 부족 read에서 데이터를 더 받아와야함
                    break;
                }
                Err(err) => {
                    pending_error = Some(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid RESP frame: {err:?}"),
                    ));
                    break;
                }
            }
        }

        // 소켓에 response_pending 읽어서 작성
        write_pending(&mut pending_resp, &mut socket).await?;

        // 루프 도중에러 떴으면 종료
        if let Some(error) = pending_error {
            return Err(error);
        }

        // Quit 이있으면 루프 종료
        if exit_quit {
            info!(reason = "quit", "connection closing");
            return Ok(());
        }
        pending.drain(..offset);
    }
}
async fn write_pending(pending: &mut Vec<u8>, socket: &mut TcpStream) -> io::Result<()> {
    if pending.is_empty() {
        return Ok(());
    }

    timeout(WRITE_TIMEOUT, socket.write_all(pending.as_slice()))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "write timeout"))??;

    pending.clear();
    Ok(())
}

fn allowed_while_subscribed(command: &Command) -> bool {
    matches!(
        command,
        Command::Ping
            | Command::Quit
            | Command::PubSub(PubSubCommand::Subscribe { .. })
            | Command::PubSub(PubSubCommand::Unsubscribe { .. })
    )
}

#[cfg(test)]
mod tests {
    use super::{handle, write_pending, MAX_PENDING_WRITE_BYTES, WRITE_TIMEOUT};
    use crate::client::{ClientGuard, Clients};
    use crate::command::PubSubCommand;
    use crate::db::Db;
    use crate::executor;
    use crate::pubsub::{PubSub, PubSubMessage, Subscriber};
    use crate::resp::{self, Frame};
    use std::collections::HashSet;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::{mpsc, watch};
    use tokio::time::timeout;

    #[tokio::test]
    async fn client_commands_preserve_pipeline_order_and_name_on_error() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut client, server) = start_connection(Db::default(), shutdown_rx).await;
        let commands: &[&[&[u8]]] = &[
            &[b"CLIENT", b"ID"],
            &[b"CLIENT", b"GETNAME"],
            &[b"CLIENT", b"SETNAME", b"worker-1"],
            &[b"CLIENT", b"GETNAME"],
            &[b"CLIENT", b"SETNAME", b"invalid name"],
            &[b"CLIENT", b"GETNAME"],
            &[b"CLIENT", b"UNKNOWN"],
            &[b"CLIENT", b"LIST"],
            &[b"CLIENT", b"ID", b"extra"],
            &[b"CLIENT", b"SETNAME", b""],
            &[b"CLIENT", b"GETNAME"],
            &[b"CLIENT", b"ID"],
            &[b"PING"],
            &[b"QUIT"],
        ];
        let mut requests = Vec::new();
        for parts in commands {
            let frame = Frame::Array(
                parts
                    .iter()
                    .map(|part| Frame::BulkString(part.to_vec()))
                    .collect(),
            );
            requests.extend(resp::encode(frame).unwrap());
        }
        client.write_all(&requests).await.unwrap();
        let list = format!("id=1 addr={} name=worker-1\n", client.local_addr().unwrap());
        let list_response = format!("${}\r\n{}\r\n", list.len(), list);
        expect_closed(
            &mut client,
            server,
            format!(
                concat!(
                    ":1\r\n",
                    "$-1\r\n",
                    "+OK\r\n",
                    "$8\r\nworker-1\r\n",
                    "-ERR invalid client name\r\n",
                    "$8\r\nworker-1\r\n",
                    "-ERR unknown command\r\n",
                    "{}",
                    "-ERR wrong number of arguments\r\n",
                    "+OK\r\n",
                    "$-1\r\n",
                    ":1\r\n",
                    "+PONG\r\n",
                    "+OK\r\n",
                ),
                list_response
            )
            .as_bytes(),
        )
        .await;
    }
    /*
    파이프라이닝 테스트
    파이프라이닝 : 클라이언트가 응답을 기다리지 않고 여러 명령을 연속으로 보내는 방식
     */
    #[tokio::test]
    async fn handles_pipelined_commands() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let db = Db::default();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let server = tokio::spawn(async move {
            let (socket, peer) = listener.accept().await.unwrap();
            handle(
                peer,
                socket,
                db,
                Clients::default(),
                shutdown_rx,
                1,
                &PubSub::default(),
            )
            .await
            .unwrap();
        });

        let mut client = TcpStream::connect(address).await.unwrap();

        let requests = concat!(
            "*1\r\n$4\r\nPING\r\n",
            "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$3\r\nkim\r\n",
            "*2\r\n$3\r\nGET\r\n$4\r\nname\r\n",
        );

        client.write_all(requests.as_bytes()).await.unwrap();

        let expected = b"+PONG\r\n+OK\r\n$3\r\nkim\r\n";
        let mut response = vec![0; expected.len()];

        timeout(Duration::from_secs(2), client.read_exact(&mut response))
            .await
            .expect("response timed out")
            .unwrap();

        assert_eq!(response, expected);

        drop(client);
        timeout(Duration::from_secs(2), server)
            .await
            .expect("connection did not close")
            .unwrap();
    }

    /*
    분할 수신 테스트
    분할 수신 : 하나의 명령이 여러 TCP 패킷으로 들어오는 경우
     */
    #[tokio::test]
    async fn handles_command_received_in_chunks() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let db = Db::default();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let server = tokio::spawn(async move {
            let (socket, peer) = listener.accept().await.unwrap();
            handle(
                peer,
                socket,
                db,
                Clients::default(),
                shutdown_rx,
                1,
                &PubSub::default(),
            )
            .await
            .unwrap();
        });

        let mut client = TcpStream::connect(address).await.unwrap();

        // 하나의 SET 명령을 여러 조각으로 전송
        client.write_all(b"*3\r\n$3\r\nSE").await.unwrap();
        tokio::task::yield_now().await;

        client.write_all(b"T\r\n$4\r\nna").await.unwrap();
        tokio::task::yield_now().await;

        client.write_all(b"me\r\n$3\r\nkim\r").await.unwrap();
        tokio::task::yield_now().await;

        client.write_all(b"\n").await.unwrap();

        let mut response = [0; 5];
        timeout(Duration::from_secs(2), client.read_exact(&mut response))
            .await
            .expect("response timed out")
            .unwrap();

        assert_eq!(&response, b"+OK\r\n");

        drop(client);
        timeout(Duration::from_secs(2), server)
            .await
            .expect("connection did not close")
            .unwrap();
    }

    #[tokio::test]
    async fn preserves_partial_frame_after_processed_pipeline() {
        let db = Db::default();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut client, server) = start_connection(db.clone(), shutdown_rx).await;

        // 완성된 두 요청 뒤에 SET의 일부를 붙여 보낸다.
        client
            .write_all(
                concat!(
                    "*1\r\n$4\r\nPING\r\n",
                    "*1\r\n$4\r\nPING\r\n",
                    "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$3\r\nki",
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = [0; 14];
        timeout(Duration::from_secs(2), client.read_exact(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&response, b"+PONG\r\n+PONG\r\n");

        // 앞부분 제거 후 남은 미완성 프레임이 다음 수신과 결합되어야 한다.
        client
            .write_all(
                concat!(
                    "m\r\n",
                    "*2\r\n$3\r\nGET\r\n$4\r\nname\r\n",
                    "*1\r\n$4\r\nQUIT\r\n",
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        expect_closed(&mut client, server, b"+OK\r\n$3\r\nkim\r\n+OK\r\n").await;
        assert_eq!(db.get("name"), Some(b"kim".to_vec()));
    }

    // 실제 TCP 연결로 핸들러를 실행한다.
    async fn start_connection(
        db: Db,
        shutdown: watch::Receiver<bool>,
    ) -> (TcpStream, tokio::task::JoinHandle<std::io::Result<()>>) {
        start_connection_with_clients(db, Clients::default(), shutdown, 1).await
    }

    async fn start_connection_with_clients(
        db: Db,
        clients: Clients,
        shutdown: watch::Receiver<bool>,
        client_id: u64,
    ) -> (TcpStream, tokio::task::JoinHandle<std::io::Result<()>>) {
        start_connection_with_pubsub(db, clients, PubSub::default(), shutdown, client_id).await
    }

    async fn start_connection_with_pubsub(
        db: Db,
        clients: Clients,
        pubsub: PubSub,
        shutdown: watch::Receiver<bool>,
        client_id: u64,
    ) -> (TcpStream, tokio::task::JoinHandle<std::io::Result<()>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = TcpStream::connect(address).await.unwrap();
        let (socket, peer) = listener.accept().await.unwrap();
        let server = tokio::spawn(async move {
            handle(peer, socket, db, clients, shutdown, client_id, &pubsub).await
        });
        (client, server)
    }

    fn request(parts: &[&[u8]]) -> Vec<u8> {
        resp::encode(Frame::Array(
            parts
                .iter()
                .map(|part| Frame::BulkString(part.to_vec()))
                .collect(),
        ))
        .unwrap()
    }

    fn pubsub_acknowledgement(kind: &[u8], channel: &[u8], count: i64) -> Vec<u8> {
        resp::encode(Frame::Array(vec![
            Frame::BulkString(kind.to_vec()),
            Frame::BulkString(channel.to_vec()),
            Frame::Integer(count),
        ]))
        .unwrap()
    }

    async fn subscribe_news(client: &mut TcpStream) {
        client
            .write_all(&request(&[b"SUBSCRIBE", b"news"]))
            .await
            .unwrap();
        let expected = pubsub_acknowledgement(b"subscribe", b"news", 1);
        let mut actual = vec![0; expected.len()];
        timeout(Duration::from_secs(2), client.read_exact(&mut actual))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn pubsub_mode_restricts_commands_and_returns_to_normal_after_unsubscribe() {
        let db = Db::default();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut client, server) = start_connection(db.clone(), shutdown_rx).await;
        let commands: &[&[&[u8]]] = &[
            &[b"SUBSCRIBE", b"news"],
            &[b"SUBSCRIBE", b"news"],
            &[b"PING"],
            &[b"GET", b"name"],
            &[b"SET", b"name", b"changed"],
            &[b"CLIENT", b"ID"],
            &[b"PUBLISH", b"news", b"forbidden"],
            &[b"UNSUBSCRIBE", b"missing"],
            &[b"UNSUBSCRIBE"],
            &[b"SET", b"name", b"allowed"],
            &[b"PING"],
            &[b"QUIT"],
        ];
        let requests: Vec<u8> = commands.iter().flat_map(|parts| request(parts)).collect();
        client.write_all(&requests).await.unwrap();
        let mut expected = pubsub_acknowledgement(b"subscribe", b"news", 1);
        expected.extend(pubsub_acknowledgement(b"subscribe", b"news", 1));
        expected.extend(
            resp::encode(Frame::Array(vec![
                Frame::BulkString(b"pong".to_vec()),
                Frame::BulkString(vec![]),
            ]))
            .unwrap(),
        );
        for _ in 0..4 {
            expected.extend_from_slice(b"-ERR command is not allowed in subscribed mode\r\n");
        }
        expected.extend(pubsub_acknowledgement(b"unsubscribe", b"missing", 1));
        expected.extend(pubsub_acknowledgement(b"unsubscribe", b"news", 0));
        expected.extend_from_slice(b"+OK\r\n+PONG\r\n+OK\r\n");
        expect_closed(&mut client, server, &expected).await;
        assert_eq!(db.get("name"), Some(b"allowed".to_vec()));
    }

    #[tokio::test]
    async fn pubsub_guard_cleans_up_on_eof_quit_protocol_error_abort_and_shutdown() {
        for exit in ["eof", "quit", "protocol_error", "abort", "shutdown"] {
            let pubsub = PubSub::default();
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let (mut client, server) = start_connection_with_pubsub(
                Db::default(),
                Clients::default(),
                pubsub.clone(),
                shutdown_rx,
                1,
            )
            .await;
            subscribe_news(&mut client).await;
            assert!(pubsub
                .lock()
                .unwrap()
                .get(b"news".as_slice())
                .unwrap()
                .contains_key(&1));
            match exit {
                "eof" => {
                    drop(client);
                    timeout(Duration::from_secs(2), server)
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                }
                "quit" => {
                    client.write_all(&request(&[b"QUIT"])).await.unwrap();
                    expect_closed(&mut client, server, b"+OK\r\n").await;
                }
                "protocol_error" => {
                    client.write_all(b"?").await.unwrap();
                    let error = timeout(Duration::from_secs(2), server)
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap_err();
                    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
                }
                "abort" => {
                    server.abort();
                    assert!(timeout(Duration::from_secs(2), server)
                        .await
                        .unwrap()
                        .unwrap_err()
                        .is_cancelled());
                }
                "shutdown" => {
                    shutdown_tx.send(true).unwrap();
                    timeout(Duration::from_secs(2), server)
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                }
                _ => unreachable!(),
            }
            assert!(pubsub.lock().unwrap().is_empty(), "exit: {exit}");
        }
    }

    #[tokio::test]
    async fn subscribed_connection_survives_read_idle_timeout() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut client, server) = start_connection(Db::default(), shutdown_rx).await;
        subscribe_news(&mut client).await;
        tokio::time::pause();
        tokio::time::advance(super::READ_TIMEOUT + Duration::from_secs(1)).await;
        assert!(
            !server.is_finished(),
            "idle subscriber must remain connected"
        );
        tokio::time::resume();
        client.write_all(&request(&[b"PING"])).await.unwrap();
        let expected = resp::encode(Frame::Array(vec![
            Frame::BulkString(b"pong".to_vec()),
            Frame::BulkString(vec![]),
        ]))
        .unwrap();
        let mut actual = vec![0; expected.len()];
        timeout(Duration::from_secs(2), client.read_exact(&mut actual))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual, expected);
        client.write_all(&request(&[b"QUIT"])).await.unwrap();
        expect_closed(&mut client, server, b"+OK\r\n").await;
    }

    #[tokio::test]
    async fn full_pubsub_queue_closes_only_the_slow_connection() {
        let pubsub = PubSub::default();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut slow, slow_server) = start_connection_with_pubsub(
            Db::default(),
            Clients::default(),
            pubsub.clone(),
            shutdown_rx.clone(),
            1,
        )
        .await;
        let (mut fast, fast_server) = start_connection_with_pubsub(
            Db::default(),
            Clients::default(),
            pubsub.clone(),
            shutdown_rx,
            2,
        )
        .await;
        subscribe_news(&mut slow).await;
        subscribe_news(&mut fast).await;
        let slow_subscriber = pubsub
            .lock()
            .unwrap()
            .get(b"news".as_slice())
            .unwrap()
            .get(&1)
            .unwrap()
            .clone();

        // 단일 스레드에서 await 없이 큐를 채워 느린 구독자를 재현한다.
        for _ in 0..super::MAX_PENDING_PUBSUB_MESSAGES {
            slow_subscriber
                .sender
                .try_send(PubSubMessage {
                    channel: b"news".to_vec(),
                    message: b"queued".to_vec(),
                })
                .unwrap();
        }
        let (sender, mut receiver) = mpsc::channel(1);
        let (disconnect, _) = watch::channel(false);
        let publisher = Subscriber { sender, disconnect };
        let response = executor::execute_pubsub(
            PubSubCommand::Publish {
                channel: b"news".to_vec(),
                message: b"live".to_vec(),
            },
            3,
            &pubsub,
            &publisher,
            &mut HashSet::new(),
            &mut receiver,
        );
        assert_eq!(response, vec![Frame::Integer(1)]);
        assert!(*slow_subscriber.disconnect.borrow());
        expect_closed(&mut slow, slow_server, b"").await;

        let expected = resp::encode(Frame::Array(vec![
            Frame::BulkString(b"message".to_vec()),
            Frame::BulkString(b"news".to_vec()),
            Frame::BulkString(b"live".to_vec()),
        ]))
        .unwrap();
        let mut actual = vec![0; expected.len()];
        timeout(Duration::from_secs(2), fast.read_exact(&mut actual))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual, expected);
        assert!(!fast_server.is_finished());
        assert!(!*publisher.disconnect.borrow());
        fast.write_all(&request(&[b"QUIT"])).await.unwrap();
        expect_closed(&mut fast, fast_server, b"+OK\r\n").await;
        assert!(pubsub.lock().unwrap().is_empty());
    }

    async fn expect_closed(
        client: &mut TcpStream,
        server: tokio::task::JoinHandle<std::io::Result<()>>,
        expected: &[u8],
    ) {
        let mut response = Vec::new();
        timeout(Duration::from_secs(2), client.read_to_end(&mut response))
            .await
            .expect("connection did not send EOF")
            .unwrap();
        assert_eq!(response, expected);
        timeout(Duration::from_secs(2), server)
            .await
            .expect("connection task did not finish")
            .expect("connection task panicked")
            .expect("connection returned an error");
    }

    #[tokio::test]
    async fn registry_cleans_up_on_eof_protocol_error_abort_and_shutdown() {
        for exit in ["eof", "protocol_error", "abort", "shutdown"] {
            let clients = Clients::default();
            let _other =
                ClientGuard::register(clients.clone(), 99, "127.0.0.1:50000".parse().unwrap());
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let (mut client, server) =
                start_connection_with_clients(Db::default(), clients.clone(), shutdown_rx, 1).await;

            // 응답을 기다려 핸들러가 등록을 마친 상태임을 보장한다.
            client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
            let mut response = [0; 7];
            timeout(Duration::from_secs(2), client.read_exact(&mut response))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(&response, b"+PONG\r\n");
            assert!(clients.lock().unwrap().contains_key(&1));

            match exit {
                "eof" => {
                    drop(client);
                    timeout(Duration::from_secs(2), server)
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                }
                "protocol_error" => {
                    client.write_all(b"?").await.unwrap();
                    let error = timeout(Duration::from_secs(2), server)
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap_err();
                    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
                }
                "abort" => {
                    server.abort();
                    let error = timeout(Duration::from_secs(2), server)
                        .await
                        .unwrap()
                        .unwrap_err();
                    assert!(error.is_cancelled());
                }
                "shutdown" => {
                    shutdown_tx.send(true).unwrap();
                    timeout(Duration::from_secs(2), server)
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                }
                _ => unreachable!(),
            }
            assert_eq!(
                clients.lock().unwrap().keys().copied().collect::<Vec<_>>(),
                vec![99],
                "exit: {exit}"
            );
        }
    }

    #[tokio::test]
    async fn closes_when_shutdown_is_already_true() {
        // 초기값 true는 변경 알림이 없어도 borrow()로 확인해야 한다.
        let (_shutdown_tx, shutdown_rx) = watch::channel(true);
        let (mut client, server) = start_connection(Db::default(), shutdown_rx).await;
        expect_closed(&mut client, server, b"").await;
    }

    #[tokio::test]
    async fn shutdown_interrupts_pending_read() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (socket, peer) = listener.accept().await.unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let pubsub = PubSub::default();
        let handler = handle(
            peer,
            socket,
            Db::default(),
            Clients::default(),
            shutdown_rx,
            1,
            &pubsub,
        );
        tokio::pin!(handler);

        // sleep 없이 핸들러를 먼저 poll해서 읽기 대기 상태로 만든다.
        tokio::select! {
            biased;
            result = &mut handler => panic!("connection closed before shutdown: {result:?}"),
            _ = std::future::ready(()) => {}
        }
        shutdown_tx.send(true).unwrap();
        timeout(Duration::from_secs(2), &mut handler)
            .await
            .expect("shutdown did not interrupt the pending read")
            .unwrap();
        let mut byte = [0];
        assert_eq!(
            timeout(Duration::from_secs(2), client.read(&mut byte))
                .await
                .expect("connection did not send EOF")
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn quit_replies_ok_and_skips_following_pipelined_commands() {
        let db = Db::default();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut client, server) = start_connection(db.clone(), shutdown_rx).await;
        client
            .write_all(
                concat!(
                    "*1\r\n$4\r\nPING\r\n",
                    "*1\r\n$4\r\nqUiT\r\n",
                    "*3\r\n$3\r\nSET\r\n$5\r\nafter\r\n$1\r\nx\r\n",
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        expect_closed(&mut client, server, b"+PONG\r\n+OK\r\n").await;
        assert_eq!(db.get("after"), None);
    }

    #[tokio::test]
    async fn quit_with_extra_argument_does_not_close_connection() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut client, server) = start_connection(Db::default(), shutdown_rx).await;
        client
            .write_all(
                concat!(
                    "*2\r\n$4\r\nQUIT\r\n$5\r\nextra\r\n",
                    "*1\r\n$4\r\nPING\r\n",
                    "*1\r\n$4\r\nQUIT\r\n",
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        expect_closed(
            &mut client,
            server,
            b"-ERR wrong number of arguments\r\n+PONG\r\n+OK\r\n",
        )
        .await;
    }

    #[tokio::test]
    async fn write_timeout_closes_connection_when_client_does_not_read() {
        let db = Db::default();
        // TCP 버퍼에 모두 들어가지 않는 큰 응답으로 쓰기 대기를 유발한다.
        db.set("large".to_owned(), vec![b'x'; 32 * 1024 * 1024]);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut client, server) = start_connection(db, shutdown_rx).await;

        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nlarge\r\n")
            .await
            .unwrap();

        // client를 유지하되 응답을 읽지 않아 서버의 전송을 막는다.
        let error = timeout(WRITE_TIMEOUT + Duration::from_secs(5), server)
            .await
            .expect("connection did not finish after write timeout")
            .expect("connection task panicked")
            .expect_err("unread response should cause a write timeout");

        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        assert_eq!(error.to_string(), "write timeout");

        // 일부 전송된 응답 이후 EOF가 도착해야 한다.
        let received = timeout(
            Duration::from_secs(2),
            tokio::io::copy(&mut client, &mut tokio::io::sink()),
        )
        .await
        .expect("connection did not close after write timeout")
        .unwrap();
        assert!(received < (32 * 1024 * 1024) as u64);
    }

    #[tokio::test]
    async fn flushes_large_and_batched_responses_in_order() {
        let db = Db::default();
        let mut request = Vec::new();
        let mut expected = Vec::new();
        // 작은 응답의 합산과 기준 초과 단일 응답을 함께 검사한다.
        for (index, size) in [2000, 2200, MAX_PENDING_WRITE_BYTES + 1, 3]
            .into_iter()
            .enumerate()
        {
            let key = format!("key{index}");
            let value = vec![b'a' + index as u8; size];
            db.set(key.clone(), value.clone());
            request.extend_from_slice(
                format!("*2\r\n$3\r\nGET\r\n${}\r\n{key}\r\n", key.len()).as_bytes(),
            );
            expected.extend_from_slice(format!("${size}\r\n").as_bytes());
            expected.extend_from_slice(&value);
            expected.extend_from_slice(b"\r\n");
        }
        request.extend_from_slice(b"*1\r\n$4\r\nQUIT\r\n");
        expected.extend_from_slice(b"+OK\r\n");
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut client, server) = start_connection(db, shutdown_rx).await;
        client.write_all(&request).await.unwrap();
        expect_closed(&mut client, server, &expected).await;
    }

    #[tokio::test]
    async fn sends_queued_responses_before_protocol_error() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut client, server) = start_connection(Db::default(), shutdown_rx).await;
        client
            .write_all(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n?invalid\r\n")
            .await
            .unwrap();

        let mut response = Vec::new();
        timeout(Duration::from_secs(2), client.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response, b"+PONG\r\n+PONG\r\n");
        let error = timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .expect_err("invalid RESP must close the connection");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn write_pending_handles_empty_buffer_and_reuses_cleared_buffer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut pending = Vec::new();
        write_pending(&mut pending, &mut socket).await.unwrap();
        pending.extend_from_slice(b"+OK\r\n");
        write_pending(&mut pending, &mut socket).await.unwrap();
        assert!(pending.is_empty());
        pending.extend_from_slice(b"+PONG\r\n");
        write_pending(&mut pending, &mut socket).await.unwrap();
        assert!(pending.is_empty());
        let mut response = [0; 12];
        timeout(Duration::from_secs(2), client.read_exact(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&response, b"+OK\r\n+PONG\r\n");
    }
}
