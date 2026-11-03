#![allow(unused)]

use crate::client::Clients;
use crate::command::ClientCommand;
use crate::db::ExpireError;
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
        Command::Del { keys } => {
            let count = db.delete(&keys) as i64;
            //redis 삭제는 삭제한 갯수를 리턴한다고 함
            Frame::Integer(count)
        }
        Command::Expire { key, seconds } => match db.set_expired_time(&key, seconds) {
            Ok(true) => Frame::Integer(1),
            Ok(false) => Frame::Integer(0),
            Err(ExpireError::InvalidTime) => {
                Frame::Error("ERR expire time must be positive".to_owned())
            }
            Err(ExpireError::OutOfRange) => {
                Frame::Error("ERR expire time is out of range".to_owned())
            }
        },
        Command::Ttl { key } => Frame::Integer(db.ttl(&key)),
        Command::Quit | Command::Client(_) => {
            unreachable!("Command unreachable!")
        }
    }
}
pub fn execute_client(client_command: ClientCommand, client_id: u64, clients: &Clients) -> Frame {
    match client_command {
        ClientCommand::Id => Frame::Integer(client_id as i64),
        ClientCommand::GetName => {
            let name = {
                let registry = clients.lock().expect("clients lock poisoned");

                registry
                    .get(&client_id)
                    .expect("current client must be registered")
                    .name
                    .clone()
            }; // 이름을 복사하고 락 해제

            match name {
                Some(name) => Frame::BulkString(name.into_bytes()),
                None => Frame::Null,
            }
        }
        ClientCommand::SetName { name } => {
            {
                // 쓰기 락 획득
                let mut registry = clients.lock().expect("clients lock poisoned");

                let client = registry
                    .get_mut(&client_id)
                    .expect("current client must be registered");
                client.name = if name.is_empty() { None } else { Some(name) };
            }
            Frame::SimpleString("OK".to_owned())
        }
        ClientCommand::List => {
            let snapshot = {
                let registry = clients.lock().expect("clients lock poisoned");
                registry.clone()
            }; // 목록을 복사한 뒤 락 해제

            let mut output = String::new();
            for (id, client) in snapshot {
                output.push_str(&format!(
                    "id={} addr={} name={}\n",
                    id,
                    client.addr,
                    client.name.as_deref().unwrap_or("")
                ));
            }
            Frame::BulkString(output.into_bytes())
        }
    }
}
#[cfg(test)]
mod tests {
    use super::execute;
    use crate::db::insert_with_expiration;
    use crate::{command::Command, db::Db, resp::Frame};
    use std::time::{Duration, Instant};

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
                    keys: vec!["name".to_owned()],
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
                    keys: vec!["name".to_owned()],
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
                    keys: vec!["missing".to_owned()],
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
                    keys: vec!["name".to_owned()],
                },
                &db
            ),
            Frame::Integer(1)
        );
        assert_eq!(db.get("name"), None);
        assert_eq!(db.get("other"), Some(b"keep".to_vec()));
    }

    #[test]
    fn del_returns_zero_for_expired_key_before_purge() {
        let db = Db::default();
        insert_with_expiration(
            &db,
            "expired",
            b"old",
            Instant::now() - Duration::from_secs(1),
        );
        assert_eq!(
            execute(
                Command::Del {
                    keys: vec!["expired".to_owned()],
                },
                &db
            ),
            Frame::Integer(0)
        );
    }

    #[test]
    fn del_returns_number_of_deleted_keys() {
        let db = Db::default();
        db.set("a".to_owned(), b"1".to_vec());
        db.set("b".to_owned(), b"2".to_vec());
        db.set("keep".to_owned(), b"3".to_vec());
        assert_eq!(
            execute(
                Command::Del {
                    keys: vec!["a".to_owned(), "missing".to_owned(), "b".to_owned()],
                },
                &db
            ),
            Frame::Integer(2)
        );
        assert_eq!(db.get("a"), None);
        assert_eq!(db.get("keep"), Some(b"3".to_vec()));
    }

    #[test]
    fn del_counts_duplicate_key_once() {
        let db = Db::default();
        db.set("a".to_owned(), b"1".to_vec());
        assert_eq!(
            execute(
                Command::Del {
                    keys: vec!["a".to_owned(), "a".to_owned()],
                },
                &db
            ),
            Frame::Integer(1)
        );
    }
}
