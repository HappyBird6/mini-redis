use std::io::{self};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::command::Command;
use crate::db::Db;
use crate::executor;
use crate::resp;

const MAX_PENDING_BYTES: usize = 2 * 1024 * 1024; // 2 MiB

pub async fn handle(mut socket: TcpStream, db: Db) -> io::Result<()> {
    let mut buffer = [0_u8; 4096];
    let mut pending: Vec<u8> = Vec::new();
    loop {
        // 읽기 한계 측정
        let read_limit = buffer.len().min(MAX_PENDING_BYTES - pending.len());
        if read_limit == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "max pending bytes exceeded",
            ));
        }

        // 소켓에서 데이터 읽기
        let bytes_read: usize = socket.read(&mut buffer[..read_limit]).await?;
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
                        Ok(command) => executor::execute(command, &db),
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /*
    파이프라이닝 테스트
    파이프라이닝 : 클라이언트가 응답을 기다리지 않고 여러 명령을 연속으로 보내는 방식
     */
    #[tokio::test]
    async fn handles_pipelined_commands() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let db = Db::default();

        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle(socket, db).await.unwrap();
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

        client.read_exact(&mut response).await.unwrap();

        assert_eq!(response, expected);

        drop(client);
        server.await.unwrap();
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

        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            handle(socket, db).await.unwrap();
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
        client.read_exact(&mut response).await.unwrap();

        assert_eq!(&response, b"+OK\r\n");

        drop(client);
        server.await.unwrap();
    }
}
