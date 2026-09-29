use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn handle(mut socket: TcpStream) -> io::Result<()> {
    let mut buffer = [0_u8; 4096];

    loop {
        // 바이트 읽기 buffer에 바이트가 담기고 bytes_read는 길이
        let bytes_read = socket.read(&mut buffer).await?;

        if bytes_read == 0 { // 클라이언트의 연결 종료
            return Ok(());
        }

        socket.write_all(&buffer[..bytes_read]).await?;
    }
}

