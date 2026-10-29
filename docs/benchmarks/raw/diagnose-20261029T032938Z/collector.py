import subprocess, pathlib, datetime, json, re, time, os
out=pathlib.Path('docs/benchmarks/raw')/('diagnose-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
out.mkdir()
script='for f in /proc/uptime /proc/stat /proc/pressure/cpu /proc/net/snmp /sys/fs/cgroup/cpu.stat /sys/fs/cgroup/cpu.pressure /sys/fs/cgroup/memory.events /proc/1/task/*/schedstat; do echo "FILE $f"; cat "$f"; done'
def docker(*args):
    p=subprocess.run(['docker',*args],capture_output=True,text=True,encoding='utf-8',errors='replace')
    if p.returncode: raise RuntimeError(p.stderr)
    return p.stdout
def snapshot(label):
    data={}
    for service in ['mini-redis','official-redis']:
        s=docker('compose','exec','-T',service,'sh','-c',script)
        (out/f'{label}-{service}.txt').write_text(s,encoding='utf-8'); data[service]=s
    return data
def sections(s): return dict(re.findall(r'FILE ([^\n]+)\n(.*?)(?=FILE |\Z)',s,re.S))
def calc(before,after):
    result={}
    for svc in before:
        b=sections(before[svc]); a=sections(after[svc]); metrics={}
        file='/sys/fs/cgroup/cpu.stat'
        bv=dict((k,int(v)) for k,v in re.findall(r'^(\w+) ([0-9]+)$',b[file],re.M))
        av=dict((k,int(v)) for k,v in re.findall(r'^(\w+) ([0-9]+)$',a[file],re.M))
        metrics.update({k:av[k]-bv[k] for k in av})
        for path in ['/sys/fs/cgroup/cpu.pressure','/proc/pressure/cpu']:
            for kind in ['some','full']:
                bb=re.search(kind+r'.*total=(\d+)',b[path]); aa=re.search(kind+r'.*total=(\d+)',a[path])
                if aa and bb: metrics[path+':'+kind+'_usec']=int(aa[1])-int(bb[1])
        sched=[(list(map(int,b[p].split())),list(map(int,a[p].split()))) for p in a if p.endswith('/schedstat') and p in b]
        for i,key in enumerate(['thread_run_ns','thread_queue_ns','thread_slices']):metrics[key]=sum(y[i]-x[i] for x,y in sched)
        def tcp(x):
            lines=[z.split()[1:] for z in x['/proc/net/snmp'].splitlines() if z.startswith('Tcp:')]
            return dict(zip(lines[0],map(int,lines[1])))
        bt=tcp(b); at=tcp(a);metrics.update({'tcp_'+k:at[k]-bt[k] for k in ['RetransSegs','InErrs','OutRsts','InSegs','OutSegs']})
        result[svc]=metrics
    return result
results=[]
for i in range(1,4):
    print('diagnostic run',i,flush=True)
    b=snapshot(f'run{i}-before'); start=time.monotonic()
    extra=['--threads',os.environ['DIAG_CLIENT_THREADS']] if 'DIAG_CLIENT_THREADS' in os.environ else []
    raw=docker('compose','exec','-T','official-redis','redis-benchmark','-h','mini-redis','-p',os.environ.get('DIAG_PORT','6379'),'-n','50000000','-c','10','-P','64',*extra,'PING')
    wall=time.monotonic()-start; a=snapshot(f'run{i}-after');(out/f'run{i}-benchmark.txt').write_text(raw,encoding='utf-8')
    elapsed=float(re.search(r'requests completed in ([\d.]+) seconds',raw)[1]);rps=float(re.search(r'throughput summary: ([\d.]+)',raw)[1])
    lat=re.search(r'latency summary \(msec\):\s*avg\s+min\s+p50\s+p95\s+p99\s+max\s+([^\n]+)',raw)[1].split()
    record={'run':i,'client_threads':os.environ.get('DIAG_CLIENT_THREADS','default-single'),'port':os.environ.get('DIAG_PORT','6379'),'seconds':elapsed,'wall_seconds':wall,'rps':rps,'p99':float(lat[4]),'metrics':calc(b,a)}
    results.append(record);(out/'results.json').write_text(json.dumps(results,indent=2),encoding='utf-8');print(json.dumps(record),flush=True)
print('OUTPUT',out,flush=True)
