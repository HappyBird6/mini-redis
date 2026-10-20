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
{ sub(/\r$/, "") }
/^=== Benchmark session / { session=$4; next }
/^Time:/ && !time { time=substr($0,7) }
/^Commit:/ && !commit { commit=$2 }
/^Actual server command:/ { server=substr($0,24) }
/^Docker VM resources / { getline; sub(/\r$/, ""); resources=$0 }
/^Container resource limits / { getline; sub(/\r$/, ""); limits=$0 }
/^rustc / { rust=$0 }
/^redis-benchmark [0-9]/ { benchmark=$0 }
/with_max_level/ { logging=$0; sub(/^[ \t]+/, "", logging) }
/^=== Session .* \/ Run / {
    run=$9; throughput=""; p50=""; p95=""; p99=""; latency=0; bad=0
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
/throughput summary:/ { throughput=$3 }
/latency summary \(msec\):/ { latency=1; next }
latency && $1 == "avg" { latency=2; next }
latency == 2 && NF == 6 && $1 ~ /^[0-9.]+$/ {
    p50=$3; p95=$4; p99=$5; latency=0
}
/^Exit code:/ && active {
    valid=($3 == 0 && !bad && throughput != "" && p50 != "" && p95 != "" && p99 != "")
    if (run != "warmup") {
        rows=rows sprintf("| %s | %s | %s | %s | %s | %s | %s | %s |\n", clients,pipeline,run,throughput,p50,p95,p99,valid ? "정상" : "실패/불완전")
        if (valid) {
            n=++count[key]
            values[key,"rps",n]=throughput+0
            values[key,"p50",n]=p50+0
            values[key,"p95",n]=p95+0
            values[key,"p99",n]=p99+0
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
    printf "- 실행 명령: `%s`\n- 요청 수: %s / 명령: PING\n",server,requests
    printf "- Docker VM CPU / 메모리(bytes): `%s`\n- 컨테이너 제한: `%s`\n",resources,limits
    printf "- 도구: `%s`, `%s`\n- 체크아웃 로그 설정: `%s`\n",rust,benchmark,logging
    printf "- 완료 횟수: %s / 세션 종료 코드: %s\n\n",completed ? completed : "미완료",exitcode != "" ? exitcode : "미기록"
    print "체크아웃 커밋·로그 설정·컨테이너 rustc 버전은 바이너리의 빌드 출처를 보증하지 않습니다. 환경 상세와 미커밋 변경은 원본을 참조하세요."
    print "\n### 실행별 결과 (워밍업 제외)\n"
    print "| 연결 | 파이프라인 | 실행 | 처리량(req/s) | p50(ms) | p95(ms) | p99(ms) | 상태 |"
    print "|---:|---:|---:|---:|---:|---:|---:|---|"
    printf "%s", rows
    print "\n### 조건별 중앙값\n"
    print "| 연결 | 파이프라인 | 유효 측정 수 | 처리량(req/s) | p50(ms) | p95(ms) | p99(ms) |"
    print "|---:|---:|---:|---:|---:|---:|---:|"
    for (i=1; i<=groups; i++) {
        k=order[i]
        if (count[k] == 3)
            printf "| %s | %s | 3 | %.2f | %.3f | %.3f | %.3f |\n",cs[k],ps[k],median(k,"rps"),median(k,"p50"),median(k,"p95"),median(k,"p99")
        else
            printf "| %s | %s | %d | 미집계 | 미집계 | 미집계 | 미집계 |\n",cs[k],ps[k],count[k]
    }
    print "\n워밍업은 제외하며 정상 종료 및 Summary가 확인된 측정 3회에 한해 지표별 중앙값을 계산합니다. CONFIG 미지원 경고는 실패로 간주하지 않습니다. 세션이 실패/중단됐다면 전체 비교 기준으로 사용하기 전에 원본을 확인하세요."
}
