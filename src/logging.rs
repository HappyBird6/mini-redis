use tracing::Level;

pub fn init() {
    /*
    DEBUG 레벨 이상의 로그를 출력
     */
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();
}
