use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::time::Duration;

// CLI의 접속 주소가 고정되어 있으므로 하나의 테스트 안에서 순차 실행한다.
#[test]
fn cli_requests_responses_and_eof() {
    run_case(
        "SET name \"unclosed\nSET name \"hello world\"\n\"qUiT\"\nPING\n",
        |mut socket| {
            expect_request(
                &mut socket,
                b"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$11\r\nhello world\r\n",
            );
            socket.write_all(b"+OK\r\n").unwrap();
            expect_request(&mut socket, b"*1\r\n$4\r\nqUiT\r\n");
            socket.write_all(b"+OK\r\n").unwrap();
            let mut byte = [0];
            assert_eq!(
                socket.read(&mut byte).unwrap(),
                0,
                "quoted QUIT must stop input"
            );
        },
        true,
        "OK",
        Some("invalid quotes"),
    );
    run_case(
        "\n \t\nPING\nQUIT\nSET after x\n",
        |mut socket| {
            expect_request(&mut socket, b"*1\r\n$4\r\nPING\r\n");
            socket.write_all(b"+PONG\r\n").unwrap();
            expect_request(&mut socket, b"*1\r\n$4\r\nQUIT\r\n");
            socket.write_all(b"+OK\r\n").unwrap();
            let mut byte = [0];
            assert_eq!(socket.read(&mut byte).unwrap(), 0, "QUIT must stop input");
        },
        true,
        "PONG",
        None,
    );

    let value = "x".repeat(5000);
    let expected = value.clone();
    run_case(
        "GET large\n",
        move |mut socket| {
            expect_request(&mut socket, b"*2\r\n$3\r\nGET\r\n$5\r\nlarge\r\n");
            // 헤더만 먼저 보내면 아직 프레임이 완성되지 않는다.
            socket.write_all(b"$5000\r\n").unwrap();
            std::thread::sleep(Duration::from_millis(30));
            socket.write_all(value.as_bytes()).unwrap();
            socket.write_all(b"\r\n").unwrap();
        },
        true,
        &expected,
        None,
    );

    run_case(
        "UNKNOWN\nPING\n",
        |mut socket| {
            expect_request(&mut socket, b"*1\r\n$7\r\nUNKNOWN\r\n");
            socket.write_all(b"-ERR unknown command\r\n").unwrap();
            expect_request(&mut socket, b"*1\r\n$4\r\nPING\r\n");
            socket.write_all(b"+PONG\r\n").unwrap();
        },
        true,
        "(error) ERR unknown command",
        None,
    );

    for (response, message) in [
        (b"".as_slice(), "before sending a response"),
        (b"$5\r\nhel".as_slice(), "incomplete RESP frame"),
    ] {
        run_case(
            "PING\n",
            move |mut socket| {
                expect_request(&mut socket, b"*1\r\n$4\r\nPING\r\n");
                socket.write_all(response).unwrap();
            },
            false,
            "",
            Some(message),
        );
    }
}

fn expect_request(socket: &mut TcpStream, expected: &[u8]) {
    let mut received = vec![0; expected.len()];
    socket.read_exact(&mut received).unwrap();
    assert_eq!(received, expected);
}

fn run_case(
    input: &str,
    respond: impl FnOnce(TcpStream) + Send + 'static,
    success: bool,
    expected_stdout: &str,
    expected_stderr: Option<&str>,
) {
    let listener = TcpListener::bind("127.0.0.1:6379")
        .expect("CLI test requires port 6379 to be free; stop mini-redis first");
    listener.set_nonblocking(true).unwrap();
    let server = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(std::time::Instant::now() < deadline, "CLI did not connect");
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("accept failed: {error}"),
            }
        };
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        respond(socket);
    });
    let mut child = Command::new(env!("CARGO_BIN_EXE_mini-redis-cli"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    // EOF를 포함한 무한 루프 회귀에서도 테스트가 무한히 대기하지 않게 한다.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("CLI did not finish within 5 seconds");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let output = child.wait_with_output().unwrap();
    server.join().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        output.status.success(),
        success,
        "stdout: {stdout}; stderr: {stderr}"
    );
    assert!(stdout.contains(expected_stdout), "stdout: {stdout}");
    if let Some(message) = expected_stderr {
        if !success {
            assert!(stderr.contains("UnexpectedEof"), "stderr: {stderr}");
        }
        assert!(stderr.contains(message), "stderr: {stderr}");
    }
}
