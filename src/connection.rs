#![allow(unused)]

use std::io::{self, ErrorKind};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::command::Command;

const MAX_PENDING_BYTES: usize = 2 * 1024 * 1024; // 2 MiB

pub async fn handle(mut socket: TcpStream) -> io::Result<()> {
    let mut buffer = [0_u8; 4096];
    let mut pending: Vec<u8> = Vec::new();
    loop {
        // 읽기 한계 측정
        let read_limit = buffer.len().min(MAX_PENDING_BYTES - pending.len());
        if read_limit == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "max pending bytes exceeded",
            ));
        }

        // 소켓에서 데이터 읽기
        let bytes_read: usize = socket.read(&mut buffer[..read_limit]).await?;
        if bytes_read == 0 {
            // 연결종료 & 펜딩에 남은 데이터 없음
            return if pending.is_empty() {
                Ok(())
            } else {
                // 남은 데이터 있는데 연결종료가 됨
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "incomplete RESP frame",
                ))
            };
        }
        // 펜딩에 읽은 버퍼 데이터 붙임
        pending.extend_from_slice(&buffer[..bytes_read]);
        loop {
            match crate::resp::parse(&pending) {
                Ok(Some((frame, consumed))) => {
                    let command = Command::from_frame(frame);

                    // 쓴 데이터 길이만큼 펜딩에서 제거
                    // TODO : 이후 처리량 많아지면 오프셋으로 처리한뒤 한번에 제거
                    pending.drain(..consumed);
                }
                Ok(None) => {
                    // 데이터 부족 read에서 데이터를 더 받아와야함
                    break;
                }
                Err(err) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid RESP frame: {err:?}"),
                    ));
                }
            }
        }
        socket.write_all(&buffer[..bytes_read]).await?;
    }
}
