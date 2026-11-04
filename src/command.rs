//! Redis command parsing and execution will be implemented here.

#![allow(dead_code)]
#![allow(unused)]

use crate::{db::Db, resp::Frame};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Ping,
    Get { key: String },
    Set { key: String, value: Vec<u8> },
    Del { keys: Vec<String> },
    Expire { key: String, seconds: i64 },
    Ttl { key: String },
    Quit,
    Client(ClientCommand),
    PubSub(PubSubCommand),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubSubCommand {
    Subscribe { channels: Vec<Vec<u8>> },
    Unsubscribe { channels: Vec<Vec<u8>> },
    Publish { channel: Vec<u8>, message: Vec<u8> },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientCommand {
    Id,
    SetName { name: String },
    GetName,
    List,
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
    InvalidClientName,
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
            Self::InvalidClientName => "ERR invalid client name",
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
            if args.len() < 1 {
                return Err(CommandError::WrongArity);
            }

            let mut keys = Vec::new();
            while args.len() > 0 {
                keys.push(next_key(&mut args)?);
            }
            Ok(Self::Del { keys })
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
        } else if command.eq_ignore_ascii_case(b"TTL") {
            /*
            @@@ TTL @@@
             */
            if args.len() != 1 {
                return Err(CommandError::WrongArity);
            }

            let key = next_key(&mut args)?;
            Ok(Self::Ttl { key })
        } else if command.eq_ignore_ascii_case(b"QUIT") {
            /*
            @@@ Quit @@@
             */
            if args.len() != 0 {
                return Err(CommandError::WrongArity);
            }
            Ok(Self::Quit)
        } else if command.eq_ignore_ascii_case(b"CLIENT") {
            /*
            @@@ Client @@@
             */
            // 인자가 없으면 next_key()가 WrongArity를 반환
            let option = next_key(&mut args)?;
            match option.to_ascii_uppercase().as_str() {
                "ID" => {
                    if args.len() != 0 {
                        return Err(CommandError::WrongArity);
                    }

                    Ok(Self::Client(ClientCommand::Id))
                }
                "GETNAME" => {
                    if args.len() != 0 {
                        return Err(CommandError::WrongArity);
                    }

                    Ok(Self::Client(ClientCommand::GetName))
                }
                "SETNAME" => {
                    if args.len() != 1 {
                        return Err(CommandError::WrongArity);
                    }

                    let name = next_key(&mut args)?;
                    let valid = name.bytes().all(|b| b.is_ascii_graphic());
                    if !valid {
                        return Err(CommandError::InvalidClientName);
                    }
                    Ok(Self::Client(ClientCommand::SetName { name }))
                }
                "LIST" => {
                    if args.len() != 0 {
                        return Err(CommandError::WrongArity);
                    }

                    Ok(Self::Client(ClientCommand::List))
                }
                _ => Err(CommandError::UnknownCommand),
            }
        } else if command.eq_ignore_ascii_case(b"SUBSCRIBE") {
            /*
            @@@ SUBSCRIBE @@@
             */
            if args.len() == 0 {
                return Err(CommandError::WrongArity);
            }
            let channels = args
                .map(into_bytes)
                .collect::<Result<Vec<_>, CommandError>>()?;

            Ok(Self::PubSub(PubSubCommand::Subscribe { channels }))
        } else if command.eq_ignore_ascii_case(b"UNSUBSCRIBE") {
            /*
            @@@ UNSUBSCRIBE @@@
             */
            let channels = args
                .map(into_bytes)
                .collect::<Result<Vec<_>, CommandError>>()?;

            Ok(Self::PubSub(PubSubCommand::Unsubscribe { channels }))
        } else if command.eq_ignore_ascii_case(b"PUBLISH") {
            /*
            @@@ PUBLISH @@@
             */
            if args.len() != 2 {
                return Err(CommandError::WrongArity);
            }
            let channel = next_bytes(&mut args)?;
            let message = next_bytes(&mut args)?;

            Ok(Self::PubSub(PubSubCommand::Publish { channel, message }))
        } else {
            Err(CommandError::UnknownCommand)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ClientCommand, Command, CommandError, PubSubCommand};
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
    fn parses_pubsub_commands_and_preserves_binary_arguments() {
        let channel = b"\xff\x00";
        let payload = b"\x00\xfe\r\n";
        for name in [b"SUBSCRIBE".as_slice(), b"sUbScRiBe"] {
            assert_eq!(
                Command::from_frame(request(&[name, b"news", channel, b""])),
                Ok(Command::PubSub(PubSubCommand::Subscribe {
                    channels: vec![b"news".to_vec(), channel.to_vec(), Vec::new()],
                }))
            );
        }
        assert_eq!(
            Command::from_frame(request(&[b"uNsUbScRiBe", channel, b"news"])),
            Ok(Command::PubSub(PubSubCommand::Unsubscribe {
                channels: vec![channel.to_vec(), b"news".to_vec()],
            }))
        );
        assert_eq!(
            Command::from_frame(request(&[b"UNSUBSCRIBE"])),
            Ok(Command::PubSub(PubSubCommand::Unsubscribe {
                channels: vec![]
            }))
        );
        assert_eq!(
            Command::from_frame(request(&[b"pUbLiSh", channel, payload])),
            Ok(Command::PubSub(PubSubCommand::Publish {
                channel: channel.to_vec(),
                message: payload.to_vec(),
            }))
        );
    }

    #[test]
    fn rejects_pubsub_wrong_arity_and_non_bulk_arguments() {
        for parts in [
            vec![b"SUBSCRIBE".as_slice()],
            vec![b"PUBLISH".as_slice()],
            vec![b"PUBLISH".as_slice(), b"news"],
            vec![b"PUBLISH".as_slice(), b"news", b"hello", b"extra"],
        ] {
            assert_eq!(
                Command::from_frame(request(&parts)),
                Err(CommandError::WrongArity)
            );
        }
        for command in [b"SUBSCRIBE".as_slice(), b"UNSUBSCRIBE", b"PUBLISH"] {
            let mut args = vec![Frame::BulkString(command.to_vec()), Frame::Integer(1)];
            if command == b"PUBLISH" {
                args.push(Frame::BulkString(b"hello".to_vec()));
            }
            assert_eq!(
                Command::from_frame(Frame::Array(args)),
                Err(CommandError::ExpectedBulkString)
            );
        }
        assert_eq!(
            Command::from_frame(Frame::Array(vec![
                Frame::BulkString(b"PUBLISH".to_vec()),
                Frame::BulkString(b"news".to_vec()),
                Frame::Null,
            ])),
            Err(CommandError::ExpectedBulkString)
        );
    }

    #[test]
    fn parses_client_subcommands_case_insensitively() {
        let cases: &[(&[&[u8]], ClientCommand)] = &[
            (&[b"CLIENT", b"ID"], ClientCommand::Id),
            (&[b"cLiEnT", b"iD"], ClientCommand::Id),
            (&[b"CLIENT", b"GETNAME"], ClientCommand::GetName),
            (&[b"client", b"getname"], ClientCommand::GetName),
            (&[b"CLIENT", b"LIST"], ClientCommand::List),
            (&[b"client", b"list"], ClientCommand::List),
            (
                &[b"CLIENT", b"SETNAME", b"worker-1"],
                ClientCommand::SetName {
                    name: "worker-1".to_owned(),
                },
            ),
            (
                &[b"client", b"setname", b""],
                ClientCommand::SetName {
                    name: String::new(),
                },
            ),
        ];
        for (parts, expected) in cases {
            assert_eq!(
                Command::from_frame(request(parts)),
                Ok(Command::Client(expected.clone()))
            );
        }
    }

    #[test]
    fn rejects_client_wrong_argument_counts() {
        let cases: &[&[&[u8]]] = &[
            &[b"CLIENT"],
            &[b"CLIENT", b"ID", b"extra"],
            &[b"CLIENT", b"GETNAME", b"extra"],
            &[b"CLIENT", b"LIST", b"extra"],
            &[b"CLIENT", b"SETNAME"],
            &[b"CLIENT", b"SETNAME", b"worker", b"extra"],
        ];
        for parts in cases {
            assert_eq!(
                Command::from_frame(request(parts)),
                Err(CommandError::WrongArity)
            );
        }
    }

    #[test]
    fn rejects_unknown_client_subcommand() {
        for parts in [
            vec![b"CLIENT".as_slice(), b"UNKNOWN"],
            vec![b"CLIENT".as_slice(), b"UNKNOWN", b"extra"],
        ] {
            assert_eq!(
                Command::from_frame(request(&parts)),
                Err(CommandError::UnknownCommand)
            );
        }
    }

    #[test]
    fn rejects_invalid_client_names() {
        let names: &[&[u8]] = &[
            b"hello world",
            b" ",
            b"tab\tname",
            b"line\r\nname",
            b"null\0name",
            b"\x7f",
            "이름".as_bytes(),
        ];
        for name in names {
            assert_eq!(
                Command::from_frame(request(&[b"CLIENT", b"SETNAME", name])),
                Err(CommandError::InvalidClientName)
            );
        }
    }

    #[test]
    fn rejects_invalid_client_argument_types_and_encoding() {
        for parts in [
            vec![Frame::BulkString(b"CLIENT".to_vec()), Frame::Integer(1)],
            vec![
                Frame::BulkString(b"CLIENT".to_vec()),
                Frame::BulkString(b"SETNAME".to_vec()),
                Frame::Integer(1),
            ],
        ] {
            assert_eq!(
                Command::from_frame(Frame::Array(parts)),
                Err(CommandError::ExpectedBulkString)
            );
        }
        assert_eq!(
            Command::from_frame(request(&[b"CLIENT", b"SETNAME", &[0xff]])),
            Err(CommandError::InvalidUtf8Key)
        );
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
                keys: vec!["name".to_owned()],
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
                    keys: vec!["name".to_owned()],
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
    fn parses_del_with_multiple_keys() {
        assert_eq!(
            Command::from_frame(request(&[b"DEL", b"a", b"b", b"c"])),
            Ok(Command::Del {
                keys: vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
            })
        );
    }

    #[test]
    fn rejects_del_with_invalid_utf8_key_among_many() {
        assert_eq!(
            Command::from_frame(request(&[b"DEL", b"a", &[0xff]])),
            Err(CommandError::InvalidUtf8Key)
        );
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
