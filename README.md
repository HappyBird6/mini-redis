# Mini Redis

현재 애플리케이션은 `0.0.0.0:6379`에서 Tokio TCP 에코 서버로 동작합니다. 받은 바이트를 그대로 돌려주므로 `nc`로 개발 환경을 바로 확인할 수 있습니다.

> 현재 서버는 아직 RESP/Redis 명령을 구현하지 않았습니다. 따라서 `redis-cli`는 공식 Redis 비교 서비스 확인 및 이후 RESP 구현 단계에서 사용합니다.

## 프로젝트 구조

```text
src/
├── main.rs        # 프로그램 시작점
├── server.rs      # TCP 리스너와 연결 태스크
├── connection.rs  # 소켓 읽기/쓰기 (현재 에코 동작)
├── resp.rs        # RESP 프레임과 향후 파서
├── command.rs     # 향후 Redis 명령 파싱/실행
└── db.rs          # 인메모리 저장소 기초
tests/
└── echo_server.rs # TCP 왕복 통합 테스트
```

## 실행

프로젝트 루트에서 두 서비스를 빌드하고 시작합니다.

```bash
docker compose up
```

- 미니 Redis 에코 서버: `localhost:6379`
- 공식 Redis 비교 서버: `localhost:6380`

```bash
docker compose down
```

## 개발 컨테이너에서 명령 실행

서비스를 실행한 상태에서 새 터미널을 열고 다음 명령을 사용합니다.

```bash
docker compose exec mini-redis rustc --version
docker compose exec mini-redis cargo --version
docker compose exec mini-redis cargo build
docker compose exec mini-redis cargo fmt --check
docker compose exec mini-redis cargo clippy --all-targets --all-features -- -D warnings
docker compose exec mini-redis cargo test
```

서비스를 계속 실행하지 않고 일회성으로 검사하려면:

```bash
docker compose run --rm mini-redis cargo fmt --check
docker compose run --rm mini-redis cargo clippy --all-targets --all-features -- -D warnings
docker compose run --rm mini-redis cargo test
```

## nc로 에코 서버 확인

개발 컨테이너 안에서 `nc`로 문자열을 보냅니다. 입력한 내용이 그대로 돌아오면 성공입니다.

```bash
docker compose exec mini-redis bash
printf 'hello mini redis\r\n' | nc -N 127.0.0.1 6379
```

호스트에 `nc`가 설치되어 있다면 바로 다음처럼 실행해도 됩니다.

```bash
printf 'hello mini redis\r\n' | nc 127.0.0.1 6379
```

## redis-cli로 공식 Redis 확인

공식 Redis는 호스트의 `6380` 포트에 연결됩니다.

```bash
docker compose exec mini-redis redis-cli -h official-redis -p 6379 PING
docker compose exec mini-redis redis-cli -h official-redis -p 6379 SET name java-team
docker compose exec mini-redis redis-cli -h official-redis -p 6379 GET name
```

예상 결과는 각각 `PONG`, `OK`, `java-team`입니다.

현재 미니 Redis의 `6379` 포트는 단순 에코 서버이므로 아래 명령은 Redis 응답으로 해석되지 않습니다. RESP와 명령 처리를 구현한 뒤 비교용으로 사용하세요.

```bash
docker compose exec mini-redis redis-cli -h mini-redis -p 6379 PING
```

## Docker 없이 로컬 Rust로 실행 (선택)

호스트에 Rust를 설치한 경우에만 사용할 수 있습니다.

```bash
cargo run
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```
