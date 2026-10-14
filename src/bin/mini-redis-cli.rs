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
        // let parts = command.split_whitespace(); -> Set name "hello word" 처럼 따옴표안 공백있는 명령 처리 불가
        // -> 문자열 순회하면서 따로 계산
        let args = match parse_args(command) {
            Ok(args) => args,
            Err(error) => {
                // 에러 출력하고 새 입력받음
                eprintln!("invalid quotes: {error:?}");
                continue;
            }
        };

        // 하단 for arg in args에서 소유권 소비되니까 quit 종료 명령어 미리 판단
        let is_quit = args.len() == 1 && args[0].eq_ignore_ascii_case("QUIT");

        let mut frames: Vec<Frame> = Vec::new();
        for arg in args {
            frames.push(Frame::BulkString(arg.into_bytes()));
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

                                    if is_quit && text == "OK" {
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
fn parse_args(input: &str) -> Result<Vec<String>, InputError> {
    let mut current = String::new();
    let mut args = Vec::new();
    // 따옴표 안 인지
    let mut in_quotes = false;
    // 인자 시작인지
    let mut start = false;
    for ch in input.chars() {
        // " 만나면 in_quotes 토글
        if ch == '"' {
            in_quotes = !in_quotes;
            start = true;
            continue;
        }
        // 공백 만남
        if ch.is_whitespace() && !in_quotes {
            if start {
                // args.push(current.clone());
                // current = String::new();
                // std::mem::take를 써서 불필요한 복사를 줄임.
                // 현재 current 문자열 소유권을 넘김과 동시에 current는 빈 문자열을 소유함
                args.push(std::mem::take(&mut current));

                start = false;
            }
            continue;
        }
        start = true;
        current.push(ch);
    }

    if in_quotes {
        return Err(InputError::InvalidQuotes);
    }

    if start {
        args.push(current);
    }
    Ok(args)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputError {
    InvalidQuotes,
}

#[cfg(test)]
mod tests {
    use super::{parse_args, InputError};

    #[test]
    fn parses_whitespace_separated_arguments() {
        assert_eq!(
            parse_args("  SET\tname   kim  ").unwrap(),
            ["SET", "name", "kim"]
        );
    }

    #[test]
    fn preserves_spaces_and_unicode_inside_quotes() {
        assert_eq!(
            parse_args("SET name \"안녕 world\"").unwrap(),
            ["SET", "name", "안녕 world"]
        );
        assert_eq!(
            parse_args("SET name \" a\tb \"").unwrap(),
            ["SET", "name", " a\tb "]
        );
    }

    #[test]
    fn preserves_empty_quoted_arguments() {
        assert_eq!(parse_args("SET \"\" \"\"").unwrap(), ["SET", "", ""]);
        assert_eq!(parse_args("\"\"").unwrap(), [""]);
    }

    #[test]
    fn empty_input_has_no_arguments() {
        assert!(parse_args("").unwrap().is_empty());
        assert!(parse_args(" \t\r\n").unwrap().is_empty());
    }

    #[test]
    fn rejects_unclosed_quotes() {
        for input in ["\"", "SET name \"hello", "SET name hello\""] {
            assert_eq!(parse_args(input), Err(InputError::InvalidQuotes));
        }
    }

    #[test]
    fn joins_quoted_and_unquoted_parts_of_one_argument() {
        assert_eq!(
            parse_args("SET name pre\"hello world\"post").unwrap(),
            ["SET", "name", "prehello worldpost"]
        );
    }
}
