/*
lib.rs는 Cargo가 라이브러리 진입점으로 자동 인식하는 정해진 이름.
변경하려면 Cargo.toml에서 경로 설정 따로 해줘야함.

bin/mini-redis-cli.rs에서 공통 코드 사용하기 위해 추가
*/
pub mod logging;
pub mod resp;
