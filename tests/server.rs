use redis::Commands;
use std::io::Read;
use std::io::Write;
use std::process::Child;
use std::process::{Command, Stdio};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
struct ProcessGuard(Child);

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// 추가 테스트는 실제 서버에 RESP를 직접 보내 연결별 동작을 확인한다.
async fn start_test_server() -> (ProcessGuard, std::net::SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);

    let mut server = ProcessGuard(
        Command::new(env!("CARGO_BIN_EXE_mini-redis"))
            .env("MINI_REDIS_ADDRESS", address.to_string())
            .env("MINI_REDIS_MAX_CONNECTIONS", "100")
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );
    timeout(Duration::from_secs(5), async {
        loop {
            assert!(
                server.0.try_wait().unwrap().is_none(),
                "server exited during startup"
            );
            if let Ok(connection) = TcpStream::connect(address).await {
                drop(connection);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("server did not start within 5 seconds");
    (server, address)
}

async fn assert_response(socket: &mut TcpStream, request: &[u8], expected: &[u8]) {
    timeout(Duration::from_secs(2), async {
        socket.write_all(request).await.unwrap();
        let mut response = vec![0; expected.len()];
        socket.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);
    })
    .await
    .expect("server response timed out");
}

// 서버의 RESP 파서를 재사용하지 않고 외부 redis-rs 클라이언트로 호환성을 검증한다.
fn redis_connection(address: std::net::SocketAddr) -> redis::Connection {
    let client = redis::Client::open(format!("redis://{address}/?protocol=resp2")).unwrap();
    let connection = client
        .get_connection_with_timeout(Duration::from_secs(2))
        .unwrap();
    connection
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    connection
        .set_write_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    connection
}

fn assert_pubsub_message(pubsub: &mut redis::PubSub<'_>, channel: &[u8], payload: &[u8]) {
    let message = pubsub.get_message().unwrap();
    assert_eq!(message.get_channel::<Vec<u8>>().unwrap(), channel);
    assert_eq!(message.get_payload::<Vec<u8>>().unwrap(), payload);
    assert!(message.get_pattern::<Option<String>>().unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_rs_pubsub_fans_out_and_unsubscribe_preserves_other_channels() {
    let (_server, address) = start_test_server().await;
    tokio::task::spawn_blocking(move || {
        let mut publisher = redis_connection(address);
        let mut connection_a = redis_connection(address);
        let mut connection_b = redis_connection(address);
        let mut a = connection_a.as_pubsub();
        let mut b = connection_b.as_pubsub();
        a.subscribe("news").unwrap();
        a.subscribe("chat").unwrap();
        b.subscribe("news").unwrap();

        let count: i64 = publisher.publish("news", "hello").unwrap();
        assert_eq!(count, 2);
        assert_pubsub_message(&mut a, b"news", b"hello");
        assert_pubsub_message(&mut b, b"news", b"hello");
        assert_eq!(a.ping::<Vec<String>>().unwrap(), vec!["pong", ""]);

        a.unsubscribe("news").unwrap();
        let count: i64 = publisher.publish("news", "only-b").unwrap();
        assert_eq!(count, 1);
        assert_pubsub_message(&mut b, b"news", b"only-b");
        let count: i64 = publisher.publish("chat", "only-a").unwrap();
        assert_eq!(count, 1);
        // news 해제가 실패했다면 여기서 news 메시지가 먼저 나와 실패한다.
        assert_pubsub_message(&mut a, b"chat", b"only-a");

        a.unsubscribe("chat").unwrap();
        b.unsubscribe("news").unwrap();
        // 인자 없는 전체 해제는 구독이 없는 경우에도 확인 응답을 반환해야 한다.
        a.unsubscribe(Vec::<String>::new()).unwrap();
        assert_eq!(a.ping::<String>().unwrap(), "PONG");
        let count: i64 = publisher.publish("news", "nobody").unwrap();
        assert_eq!(count, 0);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_rs_unsubscribe_preserves_published_messages_and_allows_resubscribe() {
    let (_server, address) = start_test_server().await;
    tokio::task::spawn_blocking(move || {
        let mut publisher = redis_connection(address);
        let mut connection = redis_connection(address);
        let mut subscriber = connection.as_pubsub();
        subscriber.subscribe("news").unwrap();
        subscriber.subscribe("news").unwrap();
        for payload in ["first", "second"] {
            let count: i64 = publisher.publish("news", payload).unwrap();
            assert_eq!(
                count, 1,
                "duplicate subscriptions must not duplicate delivery"
            );
        }
        // redis-rs는 해제 확인 응답보다 앞에 도착한 메시지를 보관한다.
        subscriber.unsubscribe("news").unwrap();
        assert_pubsub_message(&mut subscriber, b"news", b"first");
        assert_pubsub_message(&mut subscriber, b"news", b"second");
        let count: i64 = publisher.publish("news", "after-unsubscribe").unwrap();
        assert_eq!(count, 0);

        subscriber.subscribe("news").unwrap();
        let count: i64 = publisher.publish("news", "new-subscription").unwrap();
        assert_eq!(count, 1);
        assert_pubsub_message(&mut subscriber, b"news", b"new-subscription");
        subscriber.unsubscribe(Vec::<String>::new()).unwrap();
        assert_eq!(subscriber.ping::<String>().unwrap(), "PONG");
        drop(subscriber);
        drop(connection);

        let mut reconnect = redis_connection(address);
        let mut subscriber = reconnect.as_pubsub();
        subscriber.subscribe("news").unwrap();
        let count: i64 = publisher.publish("news", "reconnected").unwrap();
        assert_eq!(count, 1);
        assert_pubsub_message(&mut subscriber, b"news", b"reconnected");
        subscriber.unsubscribe("news").unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_rs_pubsub_preserves_binary_channels_and_large_and_empty_payloads() {
    let (_server, address) = start_test_server().await;
    tokio::task::spawn_blocking(move || {
        let mut publisher = redis_connection(address);
        let mut connection = redis_connection(address);
        let mut subscriber = connection.as_pubsub();
        let channel = b"\xff\x00news";
        let payload: Vec<u8> = (0..16384).map(|index| (index % 256) as u8).collect();
        subscriber.subscribe(channel.as_slice()).unwrap();
        subscriber.subscribe(b"".as_slice()).unwrap();
        let count: i64 = publisher
            .publish(channel.as_slice(), payload.as_slice())
            .unwrap();
        assert_eq!(count, 1);
        assert_pubsub_message(&mut subscriber, channel, &payload);
        let count: i64 = publisher.publish(b"".as_slice(), b"".as_slice()).unwrap();
        assert_eq!(count, 1);
        assert_pubsub_message(&mut subscriber, b"", b"");
        subscriber.unsubscribe(channel.as_slice()).unwrap();
        subscriber.unsubscribe(b"".as_slice()).unwrap();
        assert_eq!(subscriber.ping::<String>().unwrap(), "PONG");
    })
    .await
    .unwrap();
}

async fn read_client_id(socket: &mut TcpStream) -> i64 {
    timeout(Duration::from_secs(2), async {
        socket
            .write_all(b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        while !response.ends_with(b"\r\n") {
            assert!(response.len() < 32, "invalid CLIENT ID response");
            response.push(socket.read_u8().await.unwrap());
        }
        assert_eq!(response[0], b':');
        let id = std::str::from_utf8(&response[1..response.len() - 2])
            .unwrap()
            .parse::<i64>()
            .unwrap();
        assert!(id > 0);
        id
    })
    .await
    .expect("CLIENT ID response timed out")
}

async fn read_client_list(socket: &mut TcpStream) -> String {
    timeout(Duration::from_secs(2), async {
        socket
            .write_all(b"*2\r\n$6\r\nCLIENT\r\n$4\r\nLIST\r\n")
            .await
            .unwrap();
        let mut pending = Vec::new();
        loop {
            let mut buffer = [0; 1024];
            let length = socket.read(&mut buffer).await.unwrap();
            assert_ne!(length, 0, "server closed before CLIENT LIST response");
            pending.extend_from_slice(&buffer[..length]);
            assert!(pending.len() <= 1024 * 1024);
            if let Some((frame, consumed)) = mini_redis::resp::parse(&pending).unwrap() {
                assert_eq!(consumed, pending.len());
                let mini_redis::resp::Frame::BulkString(bytes) = frame else {
                    panic!("CLIENT LIST must return a bulk string");
                };
                return String::from_utf8(bytes).unwrap();
            }
        }
    })
    .await
    .expect("CLIENT LIST response timed out")
}

async fn expect_client_list(socket: &mut TcpStream, expected: &str) {
    // EOF/QUIT 처리와 준비 확인용 연결의 정리가 스케줄링될 시간을 허용한다.
    timeout(Duration::from_secs(2), async {
        loop {
            let actual = read_client_list(socket).await;
            if actual == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("CLIENT LIST did not converge to the expected live connections");
}

#[tokio::test]
async fn client_list_tracks_live_connections_and_name_changes() {
    let (_server, address) = start_test_server().await;
    let mut client_a = TcpStream::connect(address).await.unwrap();
    let mut client_b = TcpStream::connect(address).await.unwrap();
    let id_a = read_client_id(&mut client_a).await;
    let id_b = read_client_id(&mut client_b).await;
    let addr_a = client_a.local_addr().unwrap();
    let addr_b = client_b.local_addr().unwrap();
    let expected = |name: &str| {
        let rows = std::collections::BTreeMap::from([
            (id_a, format!("id={id_a} addr={addr_a} name={name}\n")),
            (id_b, format!("id={id_b} addr={addr_b} name=\n")),
        ]);
        rows.into_values().collect::<String>()
    };
    // 요청자 자신과 다른 연결이 모두 포함되고, 아직 이름이 없으면 name=이다.
    expect_client_list(&mut client_b, &expected("")).await;
    assert_response(
        &mut client_a,
        b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$8\r\nworker-a\r\n",
        b"+OK\r\n",
    )
    .await;
    expect_client_list(&mut client_b, &expected("worker-a")).await;
    assert_response(
        &mut client_a,
        b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$0\r\n\r\n",
        b"+OK\r\n",
    )
    .await;
    expect_client_list(&mut client_b, &expected("")).await;

    assert_response(&mut client_a, b"*1\r\n$4\r\nQUIT\r\n", b"+OK\r\n").await;
    expect_client_list(&mut client_b, &format!("id={id_b} addr={addr_b} name=\n")).await;
    drop(client_a);
    drop(client_b);

    let mut client_c = TcpStream::connect(address).await.unwrap();
    let id_c = read_client_id(&mut client_c).await;
    let addr_c = client_c.local_addr().unwrap();
    expect_client_list(&mut client_c, &format!("id={id_c} addr={addr_c} name=\n")).await;
}

#[tokio::test]
async fn client_ids_are_unique_and_names_are_connection_local() {
    let (_server, address) = start_test_server().await;
    let mut client_a = TcpStream::connect(address).await.unwrap();
    let mut client_b = TcpStream::connect(address).await.unwrap();
    let id_a = read_client_id(&mut client_a).await;
    let id_b = read_client_id(&mut client_b).await;
    assert_ne!(id_a, id_b);

    let getname = b"*2\r\n$6\r\nCLIENT\r\n$7\r\nGETNAME\r\n";
    assert_response(&mut client_a, getname, b"$-1\r\n").await;
    assert_response(
        &mut client_a,
        b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$8\r\nworker-a\r\n",
        b"+OK\r\n",
    )
    .await;
    assert_response(&mut client_a, getname, b"$8\r\nworker-a\r\n").await;
    assert_response(&mut client_b, getname, b"$-1\r\n").await;
    assert_response(
        &mut client_b,
        b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$8\r\nworker-b\r\n",
        b"+OK\r\n",
    )
    .await;
    assert_response(&mut client_a, getname, b"$8\r\nworker-a\r\n").await;
    assert_response(&mut client_b, getname, b"$8\r\nworker-b\r\n").await;
    assert_eq!(read_client_id(&mut client_a).await, id_a);

    assert_response(
        &mut client_a,
        b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$0\r\n\r\n",
        b"+OK\r\n",
    )
    .await;
    assert_response(&mut client_a, getname, b"$-1\r\n").await;
    assert_response(&mut client_b, getname, b"$8\r\nworker-b\r\n").await;
    assert_response(&mut client_a, b"*1\r\n$4\r\nQUIT\r\n", b"+OK\r\n").await;
    drop(client_a);

    let mut client_c = TcpStream::connect(address).await.unwrap();
    let id_c = read_client_id(&mut client_c).await;
    assert!(
        id_c > id_a && id_c > id_b,
        "new connections must not reuse an old ID"
    );
    assert_response(&mut client_c, getname, b"$-1\r\n").await;
    assert_eq!(read_client_id(&mut client_b).await, id_b);
}

#[tokio::test]
async fn multiple_clients_share_database() {
    let (_server, address) = start_test_server().await;
    // 두 연결을 동시에 유지하여 같은 DB를 공유하는지 확인한다.
    let mut client_a = TcpStream::connect(address).await.unwrap();
    let mut client_b = TcpStream::connect(address).await.unwrap();

    assert_response(
        &mut client_a,
        b"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$3\r\nkim\r\n",
        b"+OK\r\n",
    )
    .await;
    assert_response(
        &mut client_b,
        b"*2\r\n$3\r\nGET\r\n$4\r\nname\r\n",
        b"$3\r\nkim\r\n",
    )
    .await;
    assert_response(
        &mut client_b,
        b"*2\r\n$3\r\nDEL\r\n$4\r\nname\r\n",
        b":1\r\n",
    )
    .await;
    assert_response(
        &mut client_a,
        b"*2\r\n$3\r\nGET\r\n$4\r\nname\r\n",
        b"$-1\r\n",
    )
    .await;

    // 한 클라이언트의 QUIT이 다른 연결을 종료시키지 않아야 한다.
    assert_response(&mut client_a, b"*1\r\n$4\r\nQUIT\r\n", b"+OK\r\n").await;
    assert_response(&mut client_b, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n").await;
}

#[cfg(unix)]
#[tokio::test]
async fn sigterm_closes_connections_and_exits_successfully() {
    let (mut server, address) = start_test_server().await;
    let mut client_a = TcpStream::connect(address).await.unwrap();
    let mut client_b = TcpStream::connect(address).await.unwrap();
    // 신호 전 두 연결이 실제 핸들러에서 처리되고 있음을 확인한다.
    for client in [&mut client_a, &mut client_b] {
        assert_response(client, b"*1\r\n$4\r\nPING\r\n", b"+PONG\r\n").await;
    }

    // kill()의 강제 종료 대신 실제 SIGTERM을 보내 정상 종료 경로를 검증한다.
    assert!(Command::new("kill")
        .args(["-TERM", &server.0.id().to_string()])
        .status()
        .unwrap()
        .success());

    timeout(Duration::from_secs(5), async {
        for client in [&mut client_a, &mut client_b] {
            let mut byte = [0];
            assert_eq!(
                client.read(&mut byte).await.unwrap(),
                0,
                "connection must receive EOF"
            );
        }
        loop {
            if let Some(status) = server.0.try_wait().unwrap() {
                assert!(status.success(), "server did not exit normally: {status}");
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("graceful shutdown did not finish within 5 seconds");

    assert!(
        TcpStream::connect(address).await.is_err(),
        "listener must be closed"
    );
}

#[tokio::test]
async fn integrated_server_test() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    // 이후 포트 충돌 막기위해 리스너 드랍
    drop(listener);

    // 레디스 서버를 바이너리로 실행
    // 현재 서버와 포트 충돌 막기위해서 env읽어온 값에 address 덧씌우기
    // server는 자식 프로세스. .spawn() 실제 실행. Result<Child>고, Child로 프로세스 컨트롤 가능. tokio::spawn과 다름
    let _redis = ProcessGuard(
        std::process::Command::new(env!("CARGO_BIN_EXE_mini-redis"))
            .env("MINI_REDIS_ADDRESS", address.to_string())
            .spawn()
            .unwrap(),
    );

    // cli 서버 열기전에 redis 서버 열렸는지 확인
    let connection = timeout(Duration::from_secs(5), async {
        loop {
            match TcpStream::connect(address).await {
                Ok(connection) => break connection,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    })
    .await
    .expect("server did not start within 5 seconds");

    // 연결되었으면 드랍
    drop(connection);
    // 히스토리 저장요 임시 작업 디렉토리
    let temp_dir = tempfile::tempdir().unwrap();
    // 레디스 cli 서버를 바이너리로 실행
    let mut cli = ProcessGuard(
        Command::new(env!("CARGO_BIN_EXE_mini-redis-cli"))
            .current_dir(temp_dir.path())
            // 옵션 전달
            .args(["-h", "127.0.0.1", "-p", &address.port().to_string()])
            // 테스트 코드가 CLI에 입력을 전달할 수 있도록 파이프 생성
            .stdin(Stdio::piped())
            // CLI 출력을 테스트 코드에서 읽을 수 있도록 파이프 생성
            .stdout(Stdio::piped())
            // CLI 에러 출력도 수집
            .stderr(Stdio::piped())
            // CLI 프로세스 실행. 실패하면 unwrap()에서 panic
            .spawn()
            .unwrap(),
    );

    {
        // cli 안의 표준입력 핸들을 꺼내 소유권을 가져옴
        let mut stdin = cli.0.stdin.take().unwrap();

        // 줄바꿈으로 구분한 명령들을 CLI에 입력
        stdin
            .write_all(b"PING\nSET name kim\nGET name\nQUIT\n")
            .unwrap();
    } // 입력 핸들을 드롭하여 파이프를 닫음. CLI는 남은 입력 후 EOF를 받음

    // CLI 종료까지 기다리고 표준출력·에러 및 종료 상태를 수집
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if cli.0.try_wait().unwrap().is_some() {
                break;
            }

            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("CLI did not finish within 5 seconds");

    // 출력 읽기
    let mut stdout = String::new();
    cli.0
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .unwrap();

    // 에러 읽기
    let mut stderr = String::new();
    cli.0
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();

    let status = cli.0.wait().unwrap();
    assert!(status.success(), "CLI failed: {stderr}");
    // 출력에 PING 응답과 GET 결과가 포함됐는지 확인
    assert!(stdout.contains("PONG"));
    assert!(stdout.contains("kim"));
    assert_eq!(stdout.lines().filter(|line| line.trim() == "OK").count(), 2,);
}
