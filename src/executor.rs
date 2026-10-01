#![allow(unused)]

use crate::{command::Command, db::Db, resp::Frame};

pub fn execute(command: Command, db: &Db) -> Frame {
    match command {
        Command::Ping => Frame::SimpleString("PONG".to_owned()),
        Command::Set { key, value } => {
            db.set(key, value);
            Frame::SimpleString("OK".to_owned())
        }
        Command::Get { key } => match db.get(&key) {
            Some(value) => Frame::BulkString(value),
            None => Frame::Null,
        },
        Command::Del { key } => {
            let deleted = db.delete(&key);
            //redis 삭제는 삭제한 갯수를 리턴한다고 함
            Frame::Integer(if deleted { 1 } else { 0 })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::execute;
    use crate::{command::Command, db::Db, resp::Frame};

    #[test]
    fn ping_returns_pong() {
        let db = Db::default();
        assert_eq!(
            execute(Command::Ping, &db),
            Frame::SimpleString("PONG".to_owned())
        );
    }

    #[test]
    fn set_get_del_sequence_returns_expected_responses() {
        let db = Db::default();
        assert_eq!(
            execute(
                Command::Set {
                    key: "name".to_owned(),
                    value: b"kim".to_vec(),
                },
                &db,
            ),
            Frame::SimpleString("OK".to_owned())
        );
        assert_eq!(
            execute(
                Command::Get {
                    key: "name".to_owned()
                },
                &db
            ),
            Frame::BulkString(b"kim".to_vec())
        );
        assert_eq!(
            execute(
                Command::Del {
                    key: "name".to_owned()
                },
                &db
            ),
            Frame::Integer(1)
        );
        assert_eq!(
            execute(
                Command::Get {
                    key: "name".to_owned()
                },
                &db
            ),
            Frame::Null
        );
        assert_eq!(
            execute(
                Command::Del {
                    key: "name".to_owned()
                },
                &db
            ),
            Frame::Integer(0)
        );
    }

    #[test]
    fn missing_key_returns_null_for_get_and_zero_for_del() {
        let db = Db::default();
        assert_eq!(
            execute(
                Command::Get {
                    key: "missing".to_owned()
                },
                &db
            ),
            Frame::Null
        );
        assert_eq!(
            execute(
                Command::Del {
                    key: "missing".to_owned()
                },
                &db
            ),
            Frame::Integer(0)
        );
    }

    #[test]
    fn set_overwrites_existing_value() {
        let db = Db::default();
        db.set("name".to_owned(), b"kim".to_vec());
        assert_eq!(
            execute(
                Command::Set {
                    key: "name".to_owned(),
                    value: b"lee".to_vec(),
                },
                &db,
            ),
            Frame::SimpleString("OK".to_owned())
        );
        assert_eq!(
            execute(
                Command::Get {
                    key: "name".to_owned()
                },
                &db
            ),
            Frame::BulkString(b"lee".to_vec())
        );
    }

    #[test]
    fn set_and_get_preserve_binary_and_empty_values() {
        let db = Db::default();
        for value in [vec![0xff, 0x00, b'\r', b'\n'], Vec::new()] {
            assert_eq!(
                execute(
                    Command::Set {
                        key: "data".to_owned(),
                        value: value.clone(),
                    },
                    &db,
                ),
                Frame::SimpleString("OK".to_owned())
            );
            assert_eq!(
                execute(
                    Command::Get {
                        key: "data".to_owned()
                    },
                    &db
                ),
                Frame::BulkString(value)
            );
        }
    }

    #[test]
    fn del_preserves_other_keys() {
        let db = Db::default();
        db.set("name".to_owned(), b"kim".to_vec());
        db.set("other".to_owned(), b"keep".to_vec());
        assert_eq!(
            execute(
                Command::Del {
                    key: "name".to_owned()
                },
                &db
            ),
            Frame::Integer(1)
        );
        assert_eq!(db.get("name"), None);
        assert_eq!(db.get("other"), Some(b"keep".to_vec()));
    }
}
