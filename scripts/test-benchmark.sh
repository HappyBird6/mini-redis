#!/usr/bin/env bash
# Docker 없이 규격 2 집계와 전체 스크립트 흐름을 검증한다.
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf -- "$tmp"' EXIT

fixture() {
  local runs=$1 seconds=$2 protocol=${3-2} requested=${4-1000} completed=${5-1000}
  printf '=== Benchmark session fixture ===\nProtocol: %s\nMeasured runs: %s per condition\nMinimum measured seconds: 5\n' "$protocol" "$runs"
  for ((i=0; i<=runs; i++)); do
    run=$i
    if ((i == 0)); then run=warmup; fi
    printf '=== Session fixture / c=10 P=64 / Run %s ===\nConditions: requests=%s clients=10 pipeline=64\n' "$run" "$requested"
    printf '%s requests completed in %s seconds\n' "$completed" "$seconds"
    printf 'throughput summary: %s requests per second\n' "$((100+i))"
    printf 'latency summary (msec):\navg min p50 p95 p99 max\n1 0 1 2 3 4\nExit code: 0\n'
  done
  printf 'Session exit code: 0\n'
}
summarize() { awk -f "$root/scripts/summarize-benchmark.awk"; }
fixture 5 6 | summarize > "$tmp/valid"
grep -Fq '| 10 | 64 | 1000 | 5 | 6.000 | 103.00 | 101.00–105.00 |' "$tmp/valid"
fixture 4 6 | summarize > "$tmp/even"
grep -Fq '| 10 | 64 | 1000 | 4 | 6.000 | 102.50 |' "$tmp/even"
fixture 3 0.01 | summarize > "$tmp/short"
grep -q '시간 부족' "$tmp/short"
grep -Fq '| 10 | 64 | 1000 | 0 | 미집계 |' "$tmp/short"
fixture 3 6 | sed '/requests completed/d' | summarize > "$tmp/missing"
grep -Fq '| 10 | 64 | 1000 | 0 | 미집계 |' "$tmp/missing"
fixture 3 6 2 1000 999 | summarize > "$tmp/mismatch"
grep -Fq '| 10 | 64 | 1000 | 0 | 미집계 |' "$tmp/mismatch"
fixture 3 6 2 1000 1024 | summarize > "$tmp/pipeline-rounded"
grep -Fq '| 10 | 64 | 1000 | 3 | 6.000 | 102.00 |' "$tmp/pipeline-rounded"
fixture 3 6 2 1000 1064 | summarize > "$tmp/pipeline-overflow"
grep -Fq '| 10 | 64 | 1000 | 0 | 미집계 |' "$tmp/pipeline-overflow"
fixture 3 6 | sed 's/Run 2/Run 1/' | summarize > "$tmp/duplicate"
grep -q '실행 번호 오류' "$tmp/duplicate"
grep -q '미집계' "$tmp/duplicate"
fixture 3 6 | sed 's/Session exit code: 0/Session exit code: 1/' | summarize > "$tmp/failed"
grep -q '미집계' "$tmp/failed"
fixture 3 6 | sed '/Run 3/,$d' | summarize > "$tmp/interrupted"
grep -q '미집계' "$tmp/interrupted"
fixture 3 6 | sed 's/Exit code: 0/Exit code: 1/' | summarize > "$tmp/run-failed"
grep -Fq '| 10 | 64 | 1000 | 0 | 미집계 |' "$tmp/run-failed"
# 예전 형식에는 규격·최소 시간 헤더가 없어도 3회 집계가 가능해야 한다.
fixture 3 0.01 | sed '/^Protocol:/d; /^Measured runs:/d; /^Minimum measured seconds:/d' | summarize > "$tmp/legacy"
grep -Fq '| 10 | 64 | 1000 | 3 | 미기록 | 102.00 |' "$tmp/legacy"

# 임시 저장소와 가짜 docker에서 전체 실행: 실제 컨테이너/과거 기록을 건드리지 않는다.
mkdir -p "$tmp/repo/scripts" "$tmp/repo/src" "$tmp/repo/docs/benchmarks/raw" "$tmp/bin"
cp "$root/scripts/benchmark.sh" "$root/scripts/summarize-benchmark.awk" "$tmp/repo/scripts/"
printf 'with_max_level(Level::DEBUG)\n' > "$tmp/repo/src/logging.rs"
printf 'historical summary\n' > "$tmp/repo/docs/benchmarks/benchmark-summary.md"
printf 'historical raw\n' > "$tmp/repo/docs/benchmarks/raw/history.txt"
git -C "$tmp/repo" init -q
git -C "$tmp/repo" config core.autocrlf false
git -C "$tmp/repo" -c user.name=Test -c user.email=test@example.invalid add .
git -C "$tmp/repo" -c user.name=Test -c user.email=test@example.invalid commit -qm initial
cat > "$tmp/bin/docker" <<'MOCK'
#!/usr/bin/env bash
set -eu
printf '%s\n' "$*" >> "$MOCK_CALLS"
case "$*" in
  *redis-benchmark\ --version) echo 'redis-benchmark 7.4.0' ;;
  *redis-benchmark*)
    while [[ "$1" != -n ]]; do shift; done
    n=$2
    printf '%s requests completed in 6.00 seconds\n' "$n"
    printf 'throughput summary: 100 requests per second\nlatency summary (msec):\navg min p50 p95 p99 max\n1 0 1 2 3 4\n'
    ;;
  *'ps -q mini-redis') echo mock-id ;;
  *'{{.Path}}'*) echo /app/target/release/mini-redis ;;
  *'timeout 30 sh -c'*) echo PONG ;;
  *) echo mock-metadata ;;
esac
MOCK
chmod +x "$tmp/bin/docker"
export PATH="$tmp/bin:$PATH" MOCK_CALLS="$tmp/calls"
unset BENCH_REQUESTS BENCH_WARMUP_REQUESTS BENCH_RUNS BENCH_MIN_SECONDS BENCH_CONDITIONS
for setting in 'BENCH_REQUESTS=0' 'BENCH_REQUESTS=2147483648' 'BENCH_REQUESTS=01' 'BENCH_RUNS=2' 'BENCH_RUNS=101' 'BENCH_MIN_SECONDS=' 'BENCH_CONDITIONS=10 1;10 1' 'BENCH_CONDITIONS=10 1;' 'BENCH_CONDITIONS=10 0'; do
  if env "$setting" bash "$tmp/repo/scripts/benchmark.sh" > "$tmp/invalid" 2>&1; then
    echo "잘못된 설정을 허용함: $setting" >&2; exit 1
  fi
done
[[ ! -e "$MOCK_CALLS" ]]
BENCH_REQUESTS=1000 BENCH_WARMUP_REQUESTS=100 BENCH_RUNS=4 \
  BENCH_CONDITIONS='10 1;10 64' bash "$tmp/repo/scripts/benchmark.sh" > "$tmp/integration"
grep -Fq 'Completed runs: 10/10' "$tmp/integration"
grep -Fq '| 10 | 64 | 1000 | 4 | 6.000 | 100.00 |' "$tmp/repo/docs/benchmarks/benchmark-baseline-summary.md"
[[ $(cat "$tmp/repo/docs/benchmarks/benchmark-summary.md") == 'historical summary' ]]
[[ $(cat "$tmp/repo/docs/benchmarks/raw/history.txt") == 'historical raw' ]]
[[ $(grep -c 'redis-benchmark -h' "$MOCK_CALLS") == 10 ]]
# 기본값도 대표 조건 3개, 워밍업 포함 12회로 실행되는지 확인한다.
bash "$tmp/repo/scripts/benchmark.sh" > "$tmp/defaults"
grep -Fq 'Completed runs: 12/12' "$tmp/defaults"
grep -Fq 'Minimum measured seconds: 5' "$tmp/repo/docs/benchmarks/raw/"*.txt
echo 'benchmark tests passed'
