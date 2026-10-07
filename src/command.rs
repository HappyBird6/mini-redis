//! Redis command parsing and execution will be implemented here.

#![allow(dead_code)]
#![allow(unused)]

use crate::{db::Db, resp::Frame};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Ping,
    Get { key: String },
    Set { key: String, value: Vec<u8> },
    Del { key: String }, // TODO : 여러키 삭제 아직 미구현
    Expire { key: String, seconds: i64 },
    Ttl { key: String },
    Quit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandError {
    ExpectedArray,
    EmptyCommand,
    ExpectedBulkString,
    InvalidUtf8Key,
    UnknownCommand,
    WrongArity,
    InvalidInteger,
}
impl CommandError {
    pub fn message(&self) -> &'static str {
        match self {
            Self::ExpectedArray => "ERR expected an array",
            Self::EmptyCommand => "ERR empty command",
            Self::ExpectedBulkString => "ERR expected a bulk string argument",
            Self::InvalidUtf8Key => "ERR key must be valid UTF-8",
            Self::UnknownCommand => "ERR unknown command",
            Self::WrongArity => "ERR wrong number of arguments",
            Self::InvalidInteger => "ERR invalid integer",
        }
    }
}
/*
프레임을 bytes 벡터로 변경
*/
pub fn into_bytes(frame: Frame) -> Result<Vec<u8>, CommandError> {
    match frame {
        Frame::BulkString(bytes) => Ok(bytes),
        _ => Err(CommandError::ExpectedBulkString),
    }
}

/*
Vec<Frame> 에서 into_iter()로 돌면서 받은 인자 다음 frame을 받아서 bytes 벡터로 반환
*/
fn next_bytes(args: &mut std::vec::IntoIter<Frame>) -> Result<Vec<u8>, CommandError> {
    let frame = args.next().ok_or(CommandError::WrongArity)?;
    into_bytes(frame)
}

/*
next_bytes 와 마찬가지로 key는 String으로 받아야하니까 bytes벡터가 아니라 String 으로 반환
*/
fn next_key(args: &mut std::vec::IntoIter<Frame>) -> Result<String, CommandError> {
    String::from_utf8(next_bytes(args)?).map_err(|_| CommandError::InvalidUtf8Key)
}
impl Command {
    pub fn from_frame(frame: Frame) -> Result<Self, CommandError> {
        let Frame::Array(items) = frame else {
            return Err(CommandError::ExpectedArray);
        };

        // iter()는 소유권 안넘김. into_iter()가 소유권까지 넘기는거
        // let mut args = items.iter();
        let mut args = items.into_iter();

        let command = into_bytes(args.next().ok_or(CommandError::EmptyCommand)?)?;

        //args는 next() 호출후 len()이 1씩 감소함 -> next()로 command 꺼내고 남은 요소 체크
        if command.eq_ignore_ascii_case(b"PING") {
            /*
            @@@ Ping @@@
             */
            if args.len() != 0 {
                return Err(CommandError::WrongArity);
            }

            Ok(Self::Ping)
        } else if command.eq_ignore_ascii_case(b"GET") {
            /*
            @@@ GET @@@
             */
            if args.len() != 1 {
                return Err(CommandError::WrongArity);
            }

            let key = next_key(&mut args)?;
            Ok(Self::Get { key })
        } else if command.eq_ignore_ascii_case(b"SET") {
            /*
            @@@ Set @@@
             */
            if args.len() != 2 {
                return Err(CommandError::WrongArity);
            }

            let key = next_key(&mut args)?;
            let value = next_bytes(&mut args)?;
            Ok(Self::Set { key, value })
        } else if command.eq_ignore_ascii_case(b"DEL") {
            /*
            @@@ Del @@@
             */
            if args.len() != 1 {
                return Err(CommandError::WrongArity);
            }

            let key = next_key(&mut args)?;
            Ok(Self::Del { key })
        } else if command.eq_ignore_ascii_case(b"EXPIRE") {
            /*
            @@@ EXPIRE @@@
             */
            if args.len() != 2 {
                return Err(CommandError::WrongArity);
            }

            let key = next_key(&mut args)?;
            let bytes = next_bytes(&mut args)?;
            let seconds = std::str::from_utf8(&bytes)
                .map_err(|_| CommandError::InvalidInteger)?
                .parse::<i64>()
                .map_err(|_| CommandError::InvalidInteger)?;
            Ok(Self::Expire { key, seconds })
        } else if command.eq_ignore_ascii_case(b"QUIT") {
            /*
            @@@ Quit @@@
             */
            if args.len() != 0 {
                return Err(CommandError::WrongArity);
            }
            Ok(Self::Quit)
        } else {
            Err(CommandError::UnknownCommand)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Command, CommandError};
    use crate::resp::Frame;

    // 명령 이름과 인자를 BulkString 배열로 구성
    fn request(parts: &[&[u8]]) -> Frame {
        Frame::Array(
            parts
                .iter()
                .map(|part| Frame::BulkString(part.to_vec()))
                .collect(),
        )
    }

    #[test]
    fn parses_ping() {
        assert_eq!(Command::from_frame(request(&[b"PING"])), Ok(Command::Ping));
    }

    #[test]
    fn parses_get() {
        assert_eq!(
            Command::from_frame(request(&[b"GET", b"name"])),
            Ok(Command::Get {
                key: "name".to_owned()
            })
        );
    }

    #[test]
    fn parses_set() {
        assert_eq!(
            Command::from_frame(request(&[b"SET", b"name", b"kim"])),
            Ok(Command::Set {
                key: "name".to_owned(),
                value: b"kim".to_vec()
            })
        );
    }

    #[test]
    fn parses_del() {
        assert_eq!(
            Command::from_frame(request(&[b"DEL", b"name"])),
            Ok(Command::Del {
                key: "name".to_owned()
            })
        );
    }

    #[test]
    fn accepts_mixed_case_command_names() {
        for (parts, expected) in [
            (vec![b"pInG".as_slice()], Command::Ping),
            (
                vec![b"gEt".as_slice(), b"name"],
                Command::Get {
                    key: "name".to_owned(),
                },
            ),
            (
                vec![b"sEt".as_slice(), b"name", b"kim"],
                Command::Set {
                    key: "name".to_owned(),
                    value: b"kim".to_vec(),
                },
            ),
            (
                vec![b"dEl".as_slice(), b"name"],
                Command::Del {
                    key: "name".to_owned(),
                },
            ),
        ] {
            assert_eq!(Command::from_frame(request(&parts)), Ok(expected));
        }
    }

    #[test]
    fn rejects_wrong_argument_counts() {
        let cases: &[&[&[u8]]] = &[
            &[b"PING", b"extra"],
            &[b"GET"],
            &[b"GET", b"name", b"extra"],
            &[b"SET"],
            &[b"SET", b"name"],
            &[b"SET", b"name", b"kim", b"extra"],
            &[b"DEL"],
            &[b"DEL", b"name", b"extra"],
        ];
        for parts in cases {
            assert_eq!(
                Command::from_frame(request(parts)),
                Err(CommandError::WrongArity),
                "input: {parts:?}"
            );
        }
    }

    #[test]
    fn rejects_empty_array() {
        assert_eq!(
            Command::from_frame(request(&[])),
            Err(CommandError::EmptyCommand)
        );
    }

    #[test]
    fn rejects_non_array_frame() {
        assert_eq!(
            Command::from_frame(Frame::Integer(1)),
            Err(CommandError::ExpectedArray)
        );
    }

    #[test]
    fn rejects_non_bulk_command_name() {
        let frame = Frame::Array(vec![Frame::Integer(1)]);
        assert_eq!(
            Command::from_frame(frame),
            Err(CommandError::ExpectedBulkString)
        );
    }

    #[test]
    fn rejects_non_bulk_key() {
        for name in [b"GET".as_slice(), b"SET", b"DEL"] {
            let mut parts = vec![Frame::BulkString(name.to_vec()), Frame::Integer(1)];
            if name == b"SET" {
                parts.push(Frame::BulkString(b"kim".to_vec()));
            }
            assert_eq!(
                Command::from_frame(Frame::Array(parts)),
                Err(CommandError::ExpectedBulkString)
            );
        }
    }

    #[test]
    fn rejects_non_bulk_value() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"SET".to_vec()),
            Frame::BulkString(b"name".to_vec()),
            Frame::Null,
        ]);
        assert_eq!(
            Command::from_frame(frame),
            Err(CommandError::ExpectedBulkString)
        );
    }

    #[test]
    fn rejects_invalid_utf8_key() {
        for name in [b"GET".as_slice(), b"SET", b"DEL"] {
            let mut parts: Vec<&[u8]> = vec![name, &[0xff]];
            if name == b"SET" {
                parts.push(b"kim");
            }
            assert_eq!(
                Command::from_frame(request(&parts)),
                Err(CommandError::InvalidUtf8Key)
            );
        }
    }

    #[test]
    fn preserves_binary_value() {
        assert_eq!(
            Command::from_frame(request(&[b"SET", b"name", &[0xff, 0x00]])),
            Ok(Command::Set {
                key: "name".to_owned(),
                value: vec![0xff, 0x00]
            })
        );
    }

    #[test]
    fn accepts_empty_key_and_value() {
        assert_eq!(
            Command::from_frame(request(&[b"SET", b"", b""])),
            Ok(Command::Set {
                key: String::new(),
                value: Vec::new()
            })
        );
    }

    #[test]
    fn rejects_unknown_command() {
        assert_eq!(
            Command::from_frame(request(&[b"UNKNOWN"])),
            Err(CommandError::UnknownCommand)
        );
    }
    /*
    Expire 테스트
     */
    #[test]
    fn parses_expire() {
        for name in [b"EXPIRE".as_slice(), b"eXpIrE"] {
            assert_eq!(
                Command::from_frame(request(&[name, b"name", b"10"])),
                Ok(Command::Expire {
                    key: "name".to_owned(),
                    seconds: 10,
                })
            );
        }
    }

    #[test]
    fn parses_expire_with_zero_and_negative_seconds() {
        for (seconds, expected) in [(b"0".as_slice(), 0), (b"-10".as_slice(), -10)] {
            assert_eq!(
                Command::from_frame(request(&[b"EXPIRE", b"name", seconds])),
                Ok(Command::Expire {
                    key: "name".to_owned(),
                    seconds: expected,
                })
            );
        }
    }

    #[test]
    fn rejects_expire_with_wrong_argument_count() {
        let cases: &[&[&[u8]]] = &[
            &[b"EXPIRE"],
            &[b"EXPIRE", b"name"],
            &[b"EXPIRE", b"name", b"10", b"extra"],
        ];

        for parts in cases {
            assert_eq!(
                Command::from_frame(request(parts)),
                Err(CommandError::WrongArity)
            );
        }
    }

    #[test]
    fn rejects_expire_with_invalid_integer() {
        let cases: &[&[u8]] = &[
            b"",
            b"abc",
            b"1.5",
            b"9223372036854775808",
            b"-9223372036854775809",
            &[0xff],
        ];

        for seconds in cases {
            assert_eq!(
                Command::from_frame(request(&[b"EXPIRE", b"name", seconds])),
                Err(CommandError::InvalidInteger)
            );
        }
    }

    #[test]
    fn rejects_expire_with_non_bulk_seconds() {
        let frame = Frame::Array(vec![
            Frame::BulkString(b"EXPIRE".to_vec()),
            Frame::BulkString(b"name".to_vec()),
            Frame::Integer(10),
        ]);

        assert_eq!(
            Command::from_frame(frame),
            Err(CommandError::ExpectedBulkString)
        );
    }
    /*
    Expire 테스트 끝
     */
}
