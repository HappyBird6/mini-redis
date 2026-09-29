//! RESP protocol types and parsing will be implemented here.

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

#[cfg(test)]
mod tests {
    use super::Frame;

    #[test]
    fn frame_can_represent_a_ping_response() {
        assert_eq!(
            Frame::SimpleString("PONG".to_owned()),
            Frame::SimpleString("PONG".to_owned())
        );
    }
}
