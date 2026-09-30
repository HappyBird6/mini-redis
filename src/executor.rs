#![allow(unused)]

use crate::{command::Command, db::Db, resp::Frame};

pub fn execute(command: Command, db: &Db) -> Frame {
    // TODO : 명령 실행 함수
    Frame::Null
}
