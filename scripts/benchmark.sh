#!/usr/bin/env bash
# 호스트의 Git Bash에서 실행: bash scripts/benchmark.sh
set -uo pipefail

if [ "$#" -ne 0 ]; then
  echo "사용법: bash scripts/benchmark.sh"
  exit 1
fi

# Docker를 건드리기 전에 설정을 검증한다. redis-benchmark의 정수 범위 안으로 제한한다.
positive_integer() {
  local name=$1 value=$2
  if [[ ! "$value" =~ ^[1-9][0-9]{0,9}$ ]] || (( value > 2147483647 )); then
    printf '%s는 1~2147483647 사이의 정수여야 합니다.\n' "$name" >&2
    exit 1
  fi
}
requests=${BENCH_REQUESTS-50000000}
warmup_requests=${BENCH_WARMUP_REQUESTS-1000000}
measured_runs=${BENCH_RUNS-3}
min_seconds=${BENCH_MIN_SECONDS-5}
positive_integer BENCH_REQUESTS "$requests"
positive_integer BENCH_WARMUP_REQUESTS "$warmup_requests"
positive_integer BENCH_RUNS "$measured_runs"
positive_integer BENCH_MIN_SECONDS "$min_seconds"
if (( measured_runs < 3 || measured_runs > 100 )); then
  echo 'BENCH_RUNS는 3~100 사이여야 합니다.' >&2
  exit 1
fi

# 세미콜론으로 조건을 구분한다. 기본값은 일반/파이프라인/높은 부하의 대표 조건이다.
conditions_text=${BENCH_CONDITIONS-'10 1;10 64;100 256'}
if [[ ! "$conditions_text" =~ ^[1-9][0-9]*[[:blank:]]+[1-9][0-9]*(\;[1-9][0-9]*[[:blank:]]+[1-9][0-9]*)*$ ]]; then
  echo "BENCH_CONDITIONS 형식: '10 1;10 64;100 256'" >&2
  exit 1
fi
IFS=';' read -r -a conditions <<< "$conditions_text"
declare -A seen_conditions=()
for condition in "${conditions[@]}"; do
  read -r clients pipeline <<< "$condition"
  positive_integer clients "$clients"
  positive_integer pipeline "$pipeline"
  key="$clients/$pipeline"
  if [[ -n "${seen_conditions[$key]-}" ]]; then
    echo "중복 측정 조건: $key" >&2
    exit 1
  fi
  seen_conditions[$key]=1
done
runs=(warmup)
for ((run=1; run<=measured_runs; run++)); do runs+=("$run"); done

# 어느 디렉터리에서 실행해도 같은 Compose 설정과 로그 경로를 사용한다.
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd) || exit 1
cd "$script_dir/.." || exit 1
mkdir -p docs/benchmarks/raw || exit 1

session="$(date -u +%Y%m%dT%H%M%SZ)-$$"
commit=$(git rev-parse HEAD) || exit 1
log="docs/benchmarks/raw/${session}-${commit:0:7}.txt"
summary="docs/benchmarks/benchmark-baseline-summary.md"
total_runs=$((${#conditions[@]} * ${#runs[@]}))
completed=0

# 실패한 세션도 완료된 측정으로 오인하지 않도록 종료 상태를 남긴다.
finish() {
  local code=$?
  printf '\n=== Session %s finished ===\nTime: %s\nCompleted runs: %s/%s\nSession exit code: %s\n' \
    "$session" "$(date -Iseconds)" "$completed" "$total_runs" "$code" | tee -a "$log"
  # 실패/중단 세션도 결과를 남기고, 동일 세션의 중복 추가를 막는다.
  if ! grep -Fqx "## 세션 $session" "$summary" 2>/dev/null; then
    local report
    report=$(mktemp) || exit 1
    if awk -v source="raw/$(basename "$log")" -f scripts/summarize-benchmark.awk "$log" > "$report"; then
      if [ ! -s "$summary" ]; then
        printf '# 부하 테스트 세션 요약\n' >> "$summary" || { rm -f "$report"; exit 1; }
      fi
      cat "$report" >> "$summary" || { rm -f "$report"; exit 1; }
      printf 'Summary: %s\n' "$summary"
    else
      rm -f "$report"
      exit 1
    fi
    rm -f "$report"
  fi
  exit "$code"
}
trap finish EXIT

# 메타데이터 명령 실패도 기록하고 중단한다.
record() {
  local label=$1
  shift
  printf '\n%s:\n' "$label" | tee -a "$log" || return 1
  "$@" 2>&1 | tee -a "$log"
}

{
  printf '\n=== Benchmark session %s ===\n' "$session"
  printf 'Time: %s\nCommit: %s\n' "$(date -Iseconds)" "$commit"
  printf 'Protocol: 2\nCommand: PING\nMeasured requests per run: %s\nWarmup requests per run: %s\nWarmup: 1 per condition\nMeasured runs: %s per condition\nMinimum measured seconds: %s\n' "$requests" "$warmup_requests" "$measured_runs" "$min_seconds"
  printf 'Conditions (clients pipeline): %s\n' "${conditions[*]}"
  printf 'Target: stop server, build release, recreate server, then benchmark\n'
  printf 'Commit is the working checkout; uncommitted source changes are included in the build.\n'
} | tee -a "$log" || exit 1

record "Working tree at session start" git status --short || exit 1
record "Uncommitted tracked changes" git diff HEAD -- docker-compose.yml Dockerfile Cargo.toml Cargo.lock src || exit 1
record "Docker version" docker version --format '{{.Server.Version}}' || exit 1
record "Docker VM resources (CPUs / bytes of memory)" docker info --format '{{.NCPU}} / {{.MemTotal}}' || exit 1

# 같은 target 볼륨의 실행 파일을 덮어쓰기 전에 기존 서버를 종료한다.
record "Stop existing server (active connections will close)" docker compose stop mini-redis || exit 1
record "Build release binary" docker compose run --rm --no-deps mini-redis cargo build --locked --release --bin mini-redis || exit 1
record "Recreate release server" env BUILD_PROFILE=release docker compose up -d --no-deps --force-recreate mini-redis || exit 1
record "Start benchmark tools container" docker compose up -d --no-deps official-redis || exit 1

server_id=$(docker compose ps -q mini-redis) || exit 1
if [ -z "$server_id" ]; then
  echo "재생성한 mini-redis 서버를 찾을 수 없습니다." | tee -a "$log"
  exit 1
fi

server_command=$(docker inspect --format '{{.Path}} {{join .Args " "}}' "$server_id") || exit 1
printf '\nActual server command: %s\n' "$server_command" | tee -a "$log" || exit 1
if [[ "$server_command" != *"/target/release/mini-redis"* ]]; then
  echo "release 바이너리 직접 실행을 확인하지 못했습니다. 서버 설정을 확인하세요." | tee -a "$log"
  exit 1
fi

record "Server state" docker inspect --format 'Running={{.State.Running}} StartedAt={{.State.StartedAt}} Image={{.Image}}' "$server_id" || exit 1
# 0은 컨테이너별 명시적 제한 없음이다. Docker VM/상위 cgroup 제한은 별도로 존재할 수 있다.
record "Container resource limits (0 means no explicit limit)" docker inspect --format 'NanoCpus={{.HostConfig.NanoCpus}} CpuQuota={{.HostConfig.CpuQuota}} CpuPeriod={{.HostConfig.CpuPeriod}} CpusetCpus={{.HostConfig.CpusetCpus}} Memory={{.HostConfig.Memory}} MemorySwap={{.HostConfig.MemorySwap}}' "$server_id" || exit 1
record "Runtime server settings" docker compose exec -T mini-redis sh -c 'printf "MINI_REDIS_ADDRESS=%s\nMINI_REDIS_MAX_CONNECTIONS=%s\nRUST_LOG=%s\n" "${MINI_REDIS_ADDRESS-<unset>}" "${MINI_REDIS_MAX_CONNECTIONS-<unset>}" "${RUST_LOG-<unset>}"' || exit 1
# 현재 구현은 RUST_LOG가 아닌 with_max_level(Level::DEBUG)로 로그 수준을 고정한다.
record "Logging configuration source (checkout, not embedded binary metadata)" cat src/logging.rs || exit 1
record "Rust toolchain available in container (not proof of build version)" docker compose exec -T mini-redis rustc --version || exit 1
record "Benchmark tool version" docker compose exec -T official-redis redis-benchmark --version || exit 1
# 컨테이너가 running이어도 아직 bind 전일 수 있다. 연결당 2초, 전체 30초로 제한한다.
reply=$(docker compose exec -T official-redis timeout 30 sh -c '
  while :; do
    reply=$(timeout 2 redis-cli -h mini-redis -p 6379 PING 2>/dev/null)
    if [ "$reply" = "PONG" ]; then
      printf "%s\n" "$reply"
      exit 0
    fi
    sleep 1
  done
')
ready_status=$?
if [ "$ready_status" -ne 0 ]; then
  printf '서버 준비 확인 실패 (최대 30초), exit code: %s\n' "$ready_status" | tee -a "$log"
  record "Server logs after readiness failure" docker compose logs --tail 30 mini-redis
  exit "$ready_status"
fi
printf '\nReadiness response: %s\n' "$reply" | tee -a "$log" || exit 1
if [ "$(printf '%s' "$reply" | tr -d '\r')" != "PONG" ]; then
  echo "서버의 PONG 응답을 확인하지 못했습니다." | tee -a "$log"
  exit 1
fi

for condition in "${conditions[@]}"; do
  read -r clients pipeline <<< "$condition"
  for run in "${runs[@]}"; do
    run_requests=$requests
    if [ "$run" = "warmup" ]; then
      run_requests=$warmup_requests
    fi
    # 측정 중 커밋 변경을 감지한다. 미커밋 소스도 실행 중 수정하지 않아야 한다.
    if [ "$(git rev-parse HEAD)" != "$commit" ]; then
      echo "측정 중 HEAD가 변경되어 중단합니다." | tee -a "$log"
      exit 1
    fi
    cmd=(docker compose exec -T official-redis redis-benchmark
      -h mini-redis -p 6379
      -n "$run_requests" -c "$clients" -P "$pipeline" PING)
    {
      printf '\n=== Session %s / c=%s P=%s / Run %s ===\n' "$session" "$clients" "$pipeline" "$run"
      printf 'Time: %s\nCommit: %s\n' "$(date -Iseconds)" "$commit"
      printf 'Conditions: requests=%s clients=%s pipeline=%s\n' "$run_requests" "$clients" "$pipeline"
      printf 'Command: '
      printf '%q ' "${cmd[@]}"
      printf '\n'
    } | tee -a "$log" || exit 1

    "${cmd[@]}" 2>&1 |
      tr '\r' '\n' |
      sed -u '/PING: rps=/d; /^[[:space:]]*$/d' |
      tee -a "$log"
    statuses=("${PIPESTATUS[@]}")
    status=${statuses[0]}
    printf 'Exit code: %s\n' "$status" | tee -a "$log" || exit 1
    # 벤치마크뿐 아니라 필터와 로그 저장 실패도 실패로 처리한다.
    for code in "${statuses[@]}"; do
      if [ "$code" -ne 0 ]; then
        exit "$code"
      fi
    done
    completed=$((completed + 1))
  done
done
