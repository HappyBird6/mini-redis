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
