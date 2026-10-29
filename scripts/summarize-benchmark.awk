# 사용: awk -v source="raw/파일.txt" -f scripts/summarize-benchmark.awk 원본.txt
function median(key, metric,    i,j,n,v,tmp) {
    n = count[key]
    for (i = 1; i <= n; i++) tmp[i] = values[key,metric,i]
    for (i = 2; i <= n; i++) {
        v = tmp[i]; j = i - 1
        while (j >= 1 && tmp[j] > v) { tmp[j+1] = tmp[j]; j-- }
        tmp[j+1] = v
    }
    return n % 2 ? tmp[(n+1)/2] : (tmp[n/2] + tmp[n/2+1])/2
}
BEGIN { expected_runs=3; min_seconds=0; protocol=1 }
{ sub(/\r$/, "") }
/^=== Benchmark session / { session=$4; next }
/^Protocol:/ { protocol=$2 }
/^Measured runs:/ { expected_runs=$3 }
/^Minimum measured seconds:/ { min_seconds=$4 }
/^Warmup requests per run:/ { warmup_requests=$5 }
/^Time:/ && !time { time=substr($0,7) }
/^Commit:/ && !commit { commit=$2 }
/^Actual server command:/ { server=substr($0,24) }
/^Docker VM resources / { getline; sub(/\r$/, ""); resources=$0 }
/^Container resource limits / { getline; sub(/\r$/, ""); limits=$0 }
/^rustc / { rust=$0 }
/^redis-benchmark [0-9]/ { benchmark=$0 }
/with_max_level/ { logging=$0; sub(/^[ \t]+/, "", logging) }
/^=== Session .* \/ Run / {
    run=$9; throughput=""; p50=""; p95=""; p99=""; seconds=""; actual_requests=""; latency=0; bad=0
    active=1
    next
}
/^Conditions: / {
    split($2,a,"="); requests=a[2]
    split($3,a,"="); clients=a[2]
    split($4,a,"="); pipeline=a[2]
    key=clients "/" pipeline
    if (!seen[key]++) { order[++groups]=key; cs[key]=clients; ps[key]=pipeline }
}
/^Error:|^\(error\)|^-ERR/ { if (active) bad=1 }
/^[[:space:]]*[0-9]+ requests completed in [0-9.]+ seconds/ && active {
    actual_requests=$1; seconds=$5
}
/throughput summary:/ { throughput=$3 }
/latency summary \(msec\):/ { latency=1; next }
latency && $1 == "avg" { latency=2; next }
latency == 2 && NF == 6 && $1 ~ /^[0-9.]+$/ {
    p50=$3; p95=$4; p99=$5; latency=0
}
/^Exit code:/ && active {
    valid=($3 == 0 && !bad && throughput != "" && p50 != "" && p95 != "" && p99 != "")
    # redis-benchmark 7.4는 마지막 파이프라인을 모두 소비해 -n보다 최대 P-1개 더 완료할 수 있다.
    # https://github.com/redis/redis/blob/7.4/src/redis-benchmark.c (readHandler/clientDone)
    if (protocol >= 2 && (seconds == "" || actual_requests == "" || actual_requests+0 < requests+0 || actual_requests+0 >= requests+pipeline)) valid=0
    if (run != "warmup") {
        status=valid ? "정상" : "실패/불완전"
        if (valid && seconds+0 < min_seconds) { valid=0; status="시간 부족" }
        if (run_seen[key,run]++ || (run+0 < 1 || run+0 > expected_runs)) {
            valid=0; invalid_group[key]=1; status="실행 번호 오류"
        }
        if (group_requests[key] != "" && group_requests[key] != requests) {
            valid=0; invalid_group[key]=1; status="요청 수 불일치"
        }
        group_requests[key]=requests
        rows=rows sprintf("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n", clients,pipeline,run,requests,actual_requests,seconds != "" ? seconds : "미기록",throughput,p50,p95,p99,status)
        if (valid) {
            n=++count[key]
            values[key,"rps",n]=throughput+0
            values[key,"p50",n]=p50+0
            values[key,"p95",n]=p95+0
            values[key,"p99",n]=p99+0
            values[key,"seconds",n]=seconds+0
            if (n == 1 || throughput+0 < min_rps[key]) min_rps[key]=throughput+0
            if (n == 1 || throughput+0 > max_rps[key]) max_rps[key]=throughput+0
        }
    }
    active=0
}
/^Completed runs:/ { completed=$3 }
/^Session exit code:/ { exitcode=$4 }
END {
    if (!session) { print "세션 헤더가 없는 로그입니다." > "/dev/stderr"; exit 1 }
    printf "\n## 세션 %s\n\n", session
    printf "- 시각: %s\n- 체크아웃 커밋: `%s`\n- 원본: [%s](%s)\n", time,commit,source,source
    printf "- 실행 명령: `%s`\n- 측정 규격: %s / 명령: PING\n",server,protocol
    printf "- 워밍업 요청 수: %s / 조건별 본 측정: %s회 / 최소 측정 시간: %s초\n",warmup_requests,expected_runs,min_seconds
    printf "- Docker VM CPU / 메모리(bytes): `%s`\n- 컨테이너 제한: `%s`\n",resources,limits
    printf "- 도구: `%s`, `%s`\n- 체크아웃 로그 설정: `%s`\n",rust,benchmark,logging
    printf "- 완료 횟수: %s / 세션 종료 코드: %s\n\n",completed ? completed : "미완료",exitcode != "" ? exitcode : "미기록"
    print "체크아웃 커밋·로그 설정·컨테이너 rustc 버전은 바이너리의 빌드 출처를 보증하지 않습니다. 환경 상세와 미커밋 변경은 원본을 참조하세요."
    print "\n### 실행별 결과 (워밍업 제외)\n"
    print "| 연결 | 파이프라인 | 실행 | 요청 수(-n) | 완료 요청 수 | 측정 시간(s) | 처리량(req/s) | p50(ms) | p95(ms) | p99(ms) | 상태 |"
    print "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    printf "%s", rows
    print "\n### 조건별 중앙값\n"
    print "| 연결 | 파이프라인 | 요청 수 | 유효 측정 수 | 측정 시간 중앙값(s) | 처리량 중앙값(req/s) | 처리량 최소–최대(req/s) | p50(ms) | p95(ms) | p99(ms) |"
    print "|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|"
    for (i=1; i<=groups; i++) {
        k=order[i]
        if (count[k] == expected_runs && !invalid_group[k] && exitcode == "0")
            printf "| %s | %s | %s | %d | %s | %.2f | %.2f–%.2f | %.3f | %.3f | %.3f |\n",cs[k],ps[k],group_requests[k],count[k],protocol >= 2 ? sprintf("%.3f",median(k,"seconds")) : "미기록",median(k,"rps"),min_rps[k],max_rps[k],median(k,"p50"),median(k,"p95"),median(k,"p99")
        else
            printf "| %s | %s | %s | %d | 미집계 | 미집계 | 미집계 | 미집계 | 미집계 | 미집계 |\n",cs[k],ps[k],group_requests[k],count[k]
    }
    printf "\n워밍업은 제외합니다. 세션이 정상 종료되고 조건별 유효 측정 %s회가 모두 모였을 때만 집계합니다. 규격 2는 완료 요청 수 검증(마지막 파이프라인의 최대 P-1개 초과 허용)과 최소 %s초를 요구합니다. 시간 부족은 기준값에서 제외하며 요청 수를 늘려 재측정하세요. CONFIG 미지원 경고는 실패로 간주하지 않습니다. 처리량 최소–최대는 관측 범위이며 신뢰구간이 아닙니다.\n",expected_runs,min_seconds
}
