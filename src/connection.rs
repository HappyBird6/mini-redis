use std::io::{self};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::timeout;

use crate::command::Command;
use crate::db::Db;
use crate::executor;
use crate::resp;

const MAX_PENDING_BYTES: usize = 2 * 1024 * 1024; // 2 MiB
const READ_TIMEOUT: Duration = Duration::from_secs(60); // 타임아웃 60초

pub async fn handle(
    mut socket: TcpStream,
    db: Db,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut buffer = [0_u8; 4096];
    let mut pending: Vec<u8> = Vec::new();
    loop {
        /*
        shutdwon receiver로 종료 체크
        *로 역참조해서 bool값 가져옴
         */
        if *shutdown.borrow() {
            return Ok(());
        }

        // 읽기 한계 측정
        let read_limit = buffer.len().min(MAX_PENDING_BYTES - pending.len());
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
            biased;

            _ = shutdown.changed() =>{
                return Ok(());
            }
            result = timeout(
                READ_TIMEOUT,
                socket.read(&mut buffer[..read_limit]),
            ) =>{
                result.map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "read timeout")
                })??
            }
        };

        if bytes_read == 0 {
            // 연결종료 & 펜딩에 남은 데이터 없음
            return if pending.is_empty() {
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
        loop {
            match resp::parse(&pending) {
                Ok(Some((frame, consumed))) => {
                    // 쓴 데이터 길이만큼 펜딩에서 제거
                    // TODO : 이후 처리량 많아지면 오프셋으로 처리한뒤 한번에 제거
                    pending.drain(..consumed);

                    /*
                    frame을 command로 변경후 execute 함수로 넘기고 응답 frame 받아옴
                     */
                    let response = match Command::from_frame(frame) {
                        Ok(command) => {
                            if command == Command::Quit {
                                socket.write_all(b"+OK\r\n").await?;
                                return Ok(());
                            }
                            executor::execute(command, &db)
                        }
                        Err(error) => resp::Frame::Error(error.message().to_owned()),
                    };

                    // 받은 frame을 encoder로 byte로 변환
                    let response_bytes = resp::encode(response).map_err(|err| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("failed to encode RESP response: {err:?}"),
                        )
                    })?;

                    // 응답 전송
                    socket.write_all(&response_bytes).await?;
                }
                Ok(None) => {
                    // 데이터 부족 read에서 데이터를 더 받아와야함
                    break;
                }
                Err(err) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid RESP frame: {err:?}"),
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::handle;
    use crate::db::Db;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::watch;
    use tokio::time::timeout;
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
            let (socket, _) = listener.accept().await.unwrap();
            handle(socket, db, shutdown_rx).await.unwrap();
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
            let (socket, _) = listener.accept().await.unwrap();
            handle(socket, db, shutdown_rx).await.unwrap();
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

    // 실제 TCP 연결로 핸들러를 실행한다.
    async fn start_connection(
        db: Db,
        shutdown: watch::Receiver<bool>,
    ) -> (TcpStream, tokio::task::JoinHandle<std::io::Result<()>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = TcpStream::connect(address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let server = tokio::spawn(handle(socket, db, shutdown));
        (client, server)
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
        let (socket, _) = listener.accept().await.unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handler = handle(socket, Db::default(), shutdown_rx);
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
}
