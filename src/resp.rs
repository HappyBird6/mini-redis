/*
RESP는 Redis 클라이언트와 서버가 통신할 때 사용하는 텍스트 기반 프로토콜입니다.
connection.rs에서 읽은 buffer에 담긴 내용을
1. 파싱 : Frame으로 변환하고
2. 인코딩 : Frame을 소켓에 보낼 바이트로 변환
*/
#![allow(dead_code)]

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<Frame>),
    Null,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodeError {
    InvalidFormat,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    InvalidFormat,
    TooDeep,
    TooLarge,
}
const MAX_ARRAY_DEPTH: usize = 64;
const MAX_BULK_LENGTH: usize = 1024 * 1024; // 1 MiB
const MAX_ARRAY_LENGTH: usize = 1024; // 원소 1,024개

pub fn parse(buffer: &[u8]) -> Result<Option<(Frame, usize)>, ParseError> {
    // 재귀호출이 있어서 스택 오버플로 방지용 depth
    parse_inner(buffer, 0)
}
fn parse_inner(buffer: &[u8], depth: usize) -> Result<Option<(Frame, usize)>, ParseError> {
    // 바이트 읽기 buffer에 바이트가 담기고 bytes_read는 길이
    if buffer.is_empty() {
        return Ok(None);
    }
    match buffer[0] {
        b'+' | b'-' | b':' => {
            /*
            + 일 경우 SimpleString
            - 일 경우 Error
            : 일 경우 Integer
            */
            // CRLF 인덱스 찾기
            let Some(line_end) = buffer.windows(2).position(|p| p == b"\r\n") else {
                // 못찾으면 데이터 부족
                return Ok(None);
            };
            // 찾은 인덱스로 text 분리 시도.
            let text =
                std::str::from_utf8(&buffer[1..line_end]).map_err(|_| ParseError::InvalidFormat)?;

            // SimpleString 내용에는 CRLF가 들어갈 수 없음
            if text.contains('\r') || text.contains('\n') {
                return Err(ParseError::InvalidFormat);
            }
            // buffer[0]에 따라 Frame 형식 결정
            let frame = match buffer[0] {
                b'+' => Frame::SimpleString(text.to_owned()),
                b'-' => Frame::Error(text.to_owned()),
                b':' => Frame::Integer(text.parse::<i64>().map_err(|_| ParseError::InvalidFormat)?),
                _ => unreachable!(),
            };

            Ok(Some((frame, line_end + 2)))
        }
        b'$' => {
            /*
            BulkString
            */
            // CRLF 인덱스 찾기 -> 헤더에 적힌 벌크스트링 사이즈 구하기 위해
            let Some(line_end) = buffer.windows(2).position(|p| p == b"\r\n") else {
                // 못찾으면 데이터 부족
                return Ok(None);
            };
            // 찾은 인덱스로 text 분리 시도.
            let text =
                std::str::from_utf8(&buffer[1..line_end]).map_err(|_| ParseError::InvalidFormat)?;
            let bulk_length = text.parse::<i64>().map_err(|_| ParseError::InvalidFormat)?;

            // 벌크 사이즈가 -1인경우 Null Frame
            if bulk_length == -1 {
                return Ok(Some((Frame::Null, line_end + 2)));
            }

            // i64 형식을 usize로 변경
            let bulk_length =
                usize::try_from(bulk_length).map_err(|_| ParseError::InvalidFormat)?;

            // 벌크사이즈가 한계 보다 큼
            if bulk_length > MAX_BULK_LENGTH {
                return Err(ParseError::TooLarge);
            }
            let data_start = line_end + 2;
            let data_end = data_start
                .checked_add(bulk_length)
                .ok_or(ParseError::InvalidFormat)?;
            let consumed = data_end.checked_add(2).ok_or(ParseError::InvalidFormat)?;

            // 읽은 벌크 사이즈보다 작음. -> 데이터 부족
            if buffer.len() < consumed {
                return Ok(None);
            }

            // 마지막 CRLF 체크.
            if &buffer[data_end..consumed] != b"\r\n" {
                return Err(ParseError::InvalidFormat);
            }

            let data = buffer[data_start..data_end].to_vec();
            Ok(Some((Frame::BulkString(data), consumed)))
        }
        b'*' => {
            /*
            Array
             */
            // TODO: 불완전한 배열의 경우 원소들을 파싱하지만 사용하지않음. 이미 파싱한 원소 재사용 방법 강구

            // 재귀로 인한 스택오버플로 체크
            if depth >= MAX_ARRAY_DEPTH {
                return Err(ParseError::TooDeep);
            }
            // CRLF 인덱스 찾기 -> 헤더에 적힌 어레이 사이즈 구하기 위해
            let Some(line_end) = buffer.windows(2).position(|p| p == b"\r\n") else {
                // 못찾으면 데이터 부족
                return Ok(None);
            };
            // 찾은 인덱스로 text 분리 시도.
            let text =
                std::str::from_utf8(&buffer[1..line_end]).map_err(|_| ParseError::InvalidFormat)?;

            let array_length = text.parse::<i64>().map_err(|_| ParseError::InvalidFormat)?;

            if array_length == -1 {
                return Ok(Some((Frame::Null, line_end + 2)));
            }

            // -1 이외의 음수는 잘못된 길이
            let array_length =
                usize::try_from(array_length).map_err(|_| ParseError::InvalidFormat)?;

            // Array길이가 한계 보다 큼
            if array_length > MAX_ARRAY_LENGTH {
                return Err(ParseError::TooLarge);
            }

            let mut frames = Vec::new();
            let mut consumed = line_end + 2;

            for _ in 0..array_length {
                // 다음 원소의 데이터 없음
                if consumed == buffer.len() {
                    return Ok(None);
                }

                match parse_inner(&buffer[consumed..], depth + 1)? {
                    Some((frame, size)) => {
                        frames.push(frame);
                        consumed += size;
                    }
                    None => return Ok(None),
                }
            }

            Ok(Some((Frame::Array(frames), consumed)))
        }
        _ => Err(ParseError::InvalidFormat),
    }
}

pub fn encode(frame: Frame) -> Result<Vec<u8>, EncodeError> {
    let mut response: Vec<u8> = Vec::new();
    match frame {
        Frame::SimpleString(item) => {
            if item.contains('\r') || item.contains('\n') {
                return Err(EncodeError::InvalidFormat);
            }
            response.extend_from_slice(b"+");
            response.extend_from_slice(item.as_bytes());
            response.extend_from_slice(b"\r\n");

            Ok(response)
        }
        Frame::Error(item) => {
            if item.contains('\r') || item.contains('\n') {
                return Err(EncodeError::InvalidFormat);
            }
            response.extend_from_slice(b"-");
            response.extend_from_slice(item.as_bytes());
            response.extend_from_slice(b"\r\n");

            Ok(response)
        }
        Frame::Integer(item) => {
            response.extend_from_slice(b":");
            response.extend_from_slice(item.to_string().as_bytes());
            response.extend_from_slice(b"\r\n");

            Ok(response)
        }
        Frame::BulkString(items) => {
            response.extend_from_slice(b"$");
            response.extend_from_slice(items.len().to_string().as_bytes());
            response.extend_from_slice(b"\r\n");
            response.extend_from_slice(&items);
            response.extend_from_slice(b"\r\n");
            Ok(response)
        }
        Frame::Array(items) => {
            response.extend_from_slice(b"*");
            response.extend_from_slice(items.len().to_string().as_bytes());
            response.extend_from_slice(b"\r\n");

            for item in items {
                let bytes = encode(item)?;
                response.extend_from_slice(&bytes);
            }

            Ok(response)
        }
        Frame::Null => {
            response.extend_from_slice("$-1".as_bytes());
            response.extend_from_slice(b"\r\n");

            Ok(response)
        }
    }
}

#[cfg(test)]
mod parse_tests {
    use super::{parse, Frame, ParseError, MAX_ARRAY_DEPTH, MAX_ARRAY_LENGTH, MAX_BULK_LENGTH};

    #[test]
    fn parses_simple_string() {
        assert_eq!(
            parse(b"+OK\r\n"),
            Ok(Some((Frame::SimpleString("OK".to_owned()), 5)))
        );
    }

    #[test]
    fn returns_none_for_incomplete_frame() {
        assert_eq!(parse(b"+OK\r"), Ok(None));
    }

    #[test]
    fn parses_one_frame_at_a_time() {
        let input = b"+OK\r\n:123\r\n";

        let (frame, consumed) = parse(input).unwrap().unwrap();
        assert_eq!(frame, Frame::SimpleString("OK".to_owned()));
        assert_eq!(consumed, 5);

        // connection.rs에서 consumed만큼 제거하는 것과 같은 효과
        assert_eq!(
            parse(&input[consumed..]),
            Ok(Some((Frame::Integer(123), 6)))
        );
    }
    #[test]
    fn parses_bulk_string() {
        assert_eq!(
            parse(b"$3\r\nabc\r\n"),
            Ok(Some((Frame::BulkString(b"abc".to_vec()), 9)))
        );
    }

    #[test]
    fn parses_array() {
        let input = b"*2\r\n+OK\r\n:123\r\n";

        assert_eq!(
            parse(input),
            Ok(Some((
                Frame::Array(vec![
                    Frame::SimpleString("OK".to_owned()),
                    Frame::Integer(123),
                ]),
                input.len(),
            )))
        );
    }

    #[test]
    fn returns_none_for_incomplete_array() {
        // 두 번째 원소의 마지막 LF가 아직 도착하지 않음
        assert_eq!(parse(b"*2\r\n+OK\r\n:123\r"), Ok(None));
    }

    #[test]
    fn rejects_invalid_bulk_terminator() {
        // 데이터 abc 뒤에는 반드시 CRLF가 와야 함
        assert_eq!(parse(b"$3\r\nabcXX"), Err(ParseError::InvalidFormat));
    }

    #[test]
    fn rejects_bulk_length_over_limit() {
        let input = format!("${}\r\n", MAX_BULK_LENGTH + 1);

        assert_eq!(parse(input.as_bytes()), Err(ParseError::TooLarge));
    }

    #[test]
    fn rejects_array_length_over_limit() {
        let input = format!("*{}\r\n", MAX_ARRAY_LENGTH + 1);

        assert_eq!(parse(input.as_bytes()), Err(ParseError::TooLarge));
    }

    #[test]
    fn rejects_array_nesting_over_limit() {
        // 원소가 하나인 배열을 제한보다 한 단계 더 중첩
        let input = "*1\r\n".repeat(MAX_ARRAY_DEPTH + 1) + "+OK\r\n";

        assert_eq!(parse(input.as_bytes()), Err(ParseError::TooDeep));
    }
}

#[cfg(test)]
mod encode_tests {
    use super::{encode, parse, EncodeError, Frame};

    #[test]
    fn encodes_simple_string() {
        assert_eq!(
            encode(Frame::SimpleString("OK".into())),
            Ok(b"+OK\r\n".to_vec())
        );
    }

    #[test]
    fn encodes_error_with_minus_prefix() {
        assert_eq!(
            encode(Frame::Error("ERR unknown command".into())),
            Ok(b"-ERR unknown command\r\n".to_vec())
        );
    }

    #[test]
    fn encodes_integer_boundaries() {
        for (value, expected) in [
            (0, ":0\r\n"),
            (-1, ":-1\r\n"),
            (i64::MIN, ":-9223372036854775808\r\n"),
            (i64::MAX, ":9223372036854775807\r\n"),
        ] {
            assert_eq!(
                encode(Frame::Integer(value)),
                Ok(expected.as_bytes().to_vec())
            );
        }
    }

    #[test]
    fn encodes_bulk_string() {
        assert_eq!(
            encode(Frame::BulkString(b"kim".to_vec())),
            Ok(b"$3\r\nkim\r\n".to_vec())
        );
    }

    #[test]
    fn preserves_binary_bulk_data() {
        assert_eq!(
            encode(Frame::BulkString(vec![0xff, 0, b'\r', b'\n'])),
            Ok(b"$4\r\n\xff\x00\r\n\r\n".to_vec())
        );
    }

    #[test]
    fn counts_utf8_bytes_not_characters() {
        assert_eq!(
            encode(Frame::BulkString("한글".as_bytes().to_vec())),
            Ok("$6\r\n한글\r\n".as_bytes().to_vec())
        );
    }

    #[test]
    fn encodes_empty_bulk_string() {
        assert_eq!(
            encode(Frame::BulkString(vec![])),
            Ok(b"$0\r\n\r\n".to_vec())
        );
    }

    #[test]
    fn encodes_null() {
        assert_eq!(encode(Frame::Null), Ok(b"$-1\r\n".to_vec()));
    }

    #[test]
    fn encodes_empty_array() {
        assert_eq!(encode(Frame::Array(vec![])), Ok(b"*0\r\n".to_vec()));
    }

    #[test]
    fn encodes_nested_array() {
        let frame = Frame::Array(vec![
            Frame::SimpleString("OK".into()),
            Frame::Array(vec![Frame::Integer(-1), Frame::Null]),
            Frame::BulkString(b"x".to_vec()),
        ]);
        assert_eq!(
            encode(frame),
            Ok(b"*3\r\n+OK\r\n*2\r\n:-1\r\n$-1\r\n$1\r\nx\r\n".to_vec())
        );
    }

    #[test]
    fn rejects_line_breaks_in_simple_strings_and_errors() {
        for text in ["a\rb", "a\nb", "a\r\nb"] {
            for frame in [Frame::SimpleString(text.into()), Frame::Error(text.into())] {
                assert_eq!(encode(frame), Err(EncodeError::InvalidFormat));
            }
        }
    }

    #[test]
    fn propagates_nested_encoding_errors() {
        for invalid in [
            Frame::SimpleString("bad\r".into()),
            Frame::Error("bad\n".into()),
        ] {
            let frame = Frame::Array(vec![Frame::Integer(1), Frame::Array(vec![invalid])]);
            assert_eq!(encode(frame), Err(EncodeError::InvalidFormat));
        }
    }

    #[test]
    fn round_trips_all_frame_types() {
        let frames = vec![
            Frame::SimpleString("OK".into()),
            Frame::Error("ERR failed".into()),
            Frame::Integer(-42),
            Frame::BulkString(vec![0xff, 0, b'\r', b'\n']),
            Frame::Null,
            Frame::Array(vec![Frame::Array(vec![]), Frame::Integer(1)]),
        ];
        for frame in frames {
            let bytes = encode(frame.clone()).unwrap();
            assert_eq!(parse(&bytes), Ok(Some((frame, bytes.len()))));
        }
    }
}
