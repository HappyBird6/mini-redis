use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::test]
async fn tcp_round_trip_echoes_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0_u8; 64];
        let length = socket.read(&mut buffer).await.unwrap();
        socket.write_all(&buffer[..length]).await.unwrap();
    });

    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    client.write_all(b"hello mini redis\r\n").await.unwrap();

    let mut response = vec![0_u8; 18];
    client.read_exact(&mut response).await.unwrap();
    assert_eq!(response, b"hello mini redis\r\n");

    server.await.unwrap();
}

