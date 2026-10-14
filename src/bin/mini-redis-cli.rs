use mini_redis::logging;
use mini_redis::resp::{self, Frame};

use std::io::{self, Write};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::info;

const READ_TIMEOUT: Duration = Duration::from_secs(60); // 읽기 타임아웃 60초
const WRITE_TIMEOUT: Duration = Duration::from_secs(10); // 쓰기 타임아웃 10초

#[tokio::main]
async fn main() -> io::Result<()> {
    logging::init();

    // 서버에 접속해서 소켓 획득
    let mut socket = TcpStream::connect("127.0.0.1:6379").await?;
    info!("server connected");

    let mut input = String::new();
    'input: loop {
        input.clear();
        // 사용자 입력 → RESP 인코딩 → socket에 전송
        print!("mini-redis> ");

        io::stdout().flush()?; // 프롬프트를 즉시 출력
                               // EOF 처리
        if io::stdin().read_line(&mut input)? == 0 {
            break;
        }

        let command = input.trim();
        if command.is_empty() {
            continue;
        }

        // 이제 받은 raw command를 띄워쓰기 단위로 나누고 벌크스트링화 시킨후 array에 담아서 보낸다
        let parts = command.split_whitespace();

        let mut frames: Vec<Frame> = Vec::new();
        for part in parts {
            let bytes_vec = part.as_bytes().to_vec();
            frames.push(Frame::BulkString(bytes_vec));
        }

        // 벌크스트링 벡터 넣어서 Frame::Array화
        let request = Frame::Array(frames);

        // Frame::Array를 bytes로 인코딩
        match resp::encode(request) {
            Ok(request_bytes) => {
                // socket 응답 수신 → RESP 파싱 → 출력

                timeout(WRITE_TIMEOUT, socket.write_all(&request_bytes))
                    .await
                    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "write timeout"))??;

                let mut buffer = [0_u8; 4096];
                let mut pending: Vec<u8> = Vec::new();

                loop {
                    // 읽는 처리
                    let bytes_read = timeout(READ_TIMEOUT, socket.read(&mut buffer))
                        .await
                        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "read timeout"))??;

                    if bytes_read == 0 {
                        // 연결 종료
                        let message = if pending.is_empty() {
                            "server closed connection before sending a response"
                        } else {
                            "server closed connection with an incomplete RESP frame"
                        };

                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, message));
                    }
                    // 펜딩에 읽은 버퍼 데이터 붙임
                    pending.extend_from_slice(&buffer[..bytes_read]);
                    match resp::parse(&pending) {
                        Ok(Some((frame, consumed))) => {
                            // 쓴 데이터 길이만큼 펜딩에서 제거
                            pending.drain(..consumed);

                            // frame에 따라 출력
                            match frame {
                                Frame::SimpleString(text) => {
                                    println!("{text}");

                                    if command.eq_ignore_ascii_case("QUIT") && text == "OK" {
                                        break 'input;
                                    }
                                }
                                Frame::Error(text) => println!("(error) {text}"),
                                Frame::Integer(value) => println!("(integer) {value}"),
                                Frame::BulkString(bytes) => {
                                    println!("{}", String::from_utf8_lossy(&bytes));
                                }
                                Frame::Null => println!("(nil)"),
                                Frame::Array(items) => println!("{items:?}"),
                            }
                            break;
                        }
                        Ok(None) => {
                            // 데이터 부족 read에서 데이터를 더 받아와야함
                            continue;
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
            Err(err) => {
                tracing::debug!(?err, "invalid command");
            }
        };
    }

    Ok(())
}
