#![allow(unused)]

use crate::{command::Command, db::Db, resp::Frame};

pub fn execute(command: Command, db: &Db) -> Frame {
    match command {
        Command::Ping => Frame::SimpleString("PONG".to_owned()),
        Command::Set {key, value} => {
            db.set(key, value);
            Frame::SimpleString("OK".to_owned())
        }
        Command::Get {key} => {
            match db.get(&key) {
                Some(value) => Frame::BulkString(value),
                None => Frame::Null,
            }
        }
        Command::Del {key} => {
            let deleted = db.delete(&key);
            //redis 삭제는 삭제한 갯수를 리턴한다고 함
            Frame::Integer(if deleted { 1 } else { 0 })
        }
    }
}
