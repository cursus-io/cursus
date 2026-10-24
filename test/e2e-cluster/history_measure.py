"""Opt-in, disposable E2E instrumentation. Never target a shared data directory.

The wrapper traces only sync calls and samples the broker process, not tracer I/O.
Its own CPU/I/O and tracing overhead remain in measured wall time. Page eviction
is advisory and limited to the stopped fixture's files; no host drop_caches.
"""
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
import time
import urllib.request

ROOT = Path('/app/cluster-logs')
STATS = Path('/tmp/history-resource.json')
TRACE = '/tmp/history-sync.trace'


def sizes():
    total = retained = temporary = segments = 0
    for path in ROOT.rglob('*'):
        try:
            if path.is_symlink() or not path.is_file():
                continue
            size = path.stat().st_size
            total += size
            if '__consumer_offsets' in path.parts and path.suffix == '.log':
                retained += size
                segments += 1
            if '.tmp' in path.name or 'compact' in path.name or 'rewrite' in path.name:
                temporary += size
        except FileNotFoundError:
            pass
    return {'store_bytes': total, 'retained_metadata_bytes': retained,
            'temporary_bytes': temporary, 'retained_metadata_segments': segments}


def broker_pid():
    for comm in Path('/proc').glob('[0-9]*/comm'):
        try:
            if comm.read_text().strip() == 'broker':
                return comm.parent
        except (FileNotFoundError, ProcessLookupError):
            pass
    return None


def sample():
    data = sizes()
    # cgroup totals include the instrumented broker and sampling/tracing
    # helpers. Process counters below isolate the broker itself. Both are
    # Linux guest accounting, not host-device or storage-controller telemetry.
    cgroup = Path('/sys/fs/cgroup')
    if (cgroup / 'cpu.stat').exists():
        cpu = dict(line.split() for line in (cgroup / 'cpu.stat').read_text().splitlines())
        data['container_cpu_seconds'] = int(cpu['usage_usec']) / 1e6
    elif (cgroup / 'cpuacct/cpuacct.usage').exists():
        data['container_cpu_seconds'] = int((cgroup / 'cpuacct/cpuacct.usage').read_text()) / 1e9
    if (cgroup / 'io.stat').exists():
        reads = writes = 0
        for line in (cgroup / 'io.stat').read_text().splitlines():
            fields = dict(field.split('=') for field in line.split()[1:])
            reads += int(fields.get('rbytes', 0))
            writes += int(fields.get('wbytes', 0))
        data['container_read_bytes'], data['container_write_bytes'] = reads, writes
    if (cgroup / 'memory.peak').exists():
        data['container_peak_memory_bytes'] = int((cgroup / 'memory.peak').read_text())
    elif (cgroup / 'memory/memory.max_usage_in_bytes').exists():
        data['container_peak_memory_bytes'] = int((cgroup / 'memory/memory.max_usage_in_bytes').read_text())
    # Interface counters include replication/control traffic and monitoring;
    # they are distinct from application recovery payload-byte counters.
    received = transmitted = 0
    for line in Path('/proc/net/dev').read_text().splitlines()[2:]:
        interface, values = line.split(':', 1)
        if interface.strip() != 'lo':
            fields = values.split()
            received += int(fields[0])
            transmitted += int(fields[8])
    data['network_received_bytes'], data['network_transmitted_bytes'] = received, transmitted
    pid = broker_pid()
    if pid:
        fields = (pid / 'stat').read_text().split()
        data['cpu_seconds'] = (int(fields[13]) + int(fields[14])) / os.sysconf('SC_CLK_TCK')
        io = dict(line.split(': ') for line in (pid / 'io').read_text().splitlines())
        for name in ('read_bytes', 'write_bytes', 'rchar', 'wchar', 'cancelled_write_bytes'):
            data['process_' + name] = int(io[name])
        status = dict(line.split(':', 1) for line in (pid / 'status').read_text().splitlines())
        data['peak_rss_bytes'] = int(status['VmHWM'].split()[0]) * 1024
    try:
        with urllib.request.urlopen('http://127.0.0.1:9100/metrics', timeout=.5) as response:
            for line in response.read().decode().splitlines():
                if line.startswith('go_memstats_heap_alloc_bytes '):
                    data['heap_bytes'] = int(float(line.split()[1]))
                elif line.startswith('go_memstats_alloc_bytes_total '):
                    data['allocated_bytes_total'] = int(float(line.split()[1]))
                elif line.startswith('go_memstats_mallocs_total '):
                    data['allocations_total'] = int(float(line.split()[1]))
    except (OSError, ValueError):
        pass
    return data


def run():
    # A forced wrapper kill must leave accounting unavailable, never expose a
    # previous process's successful report as this process's resource usage.
    Path('/tmp/history-exit.json').unlink(missing_ok=True)
    process_token = os.urandom(16).hex()
    args = sys.argv[2:]
    if args and args[0] in ('./broker', 'broker'):
        args.pop(0)
    trace_enabled = os.environ.get('HISTORY_TRACE_SYNC', 'true') == 'true'
    command = ['/app/broker', *args]
    if trace_enabled:
        command = ['strace', '-f', '--seccomp-bpf', '-e', 'trace=fsync,fdatasync', '-o', TRACE, *command]
    child = subprocess.Popen(command)
    def stop(sig, _frame):
        pid = broker_pid()
        if pid:
            os.kill(int(pid.name), sig)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    peak_heap = peak_store = peak_temp = 0
    resource_samples = heap_samples = 0
    while child.poll() is None:
        try:
            data = sample()
            resource_samples += 1
            heap_samples += int('heap_bytes' in data)
            peak_heap = max(peak_heap, data.get('heap_bytes', 0))
            peak_store = max(peak_store, data['store_bytes'])
            peak_temp = max(peak_temp, data['temporary_bytes'])
            data.update(sampled_peak_heap_bytes=peak_heap, sampled_peak_store_bytes=peak_store,
                        sampled_peak_temporary_bytes=peak_temp, sample_monotonic=time.monotonic())
            temp = STATS.with_suffix('.tmp')
            temp.write_text(json.dumps(data))
            temp.replace(STATS)
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            pass
        time.sleep(1)
    # Reaped-child accounting survives an abrupt broker exit. It includes the
    # tracing process when enabled, unlike live broker-only /proc counters.
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    wrapper = resource.getrusage(resource.RUSAGE_SELF)
    final = {
        'process_token': process_token,
        'child_returncode': child.returncode,
        'child_cpu_seconds': children.ru_utime + children.ru_stime,
        'wrapper_cpu_seconds_before_report': wrapper.ru_utime + wrapper.ru_stime,
        'child_peak_rss_bytes': children.ru_maxrss * 1024,
        'child_input_blocks': children.ru_inblock,
        'child_output_blocks': children.ru_oublock,
        'resource_samples': resource_samples,
        'heap_samples': heap_samples,
    }
    if resource_samples:
        final.update(sampled_peak_store_bytes=peak_store, sampled_peak_temporary_bytes=peak_temp)
    if heap_samples:
        final['sampled_peak_heap_bytes'] = peak_heap
    if trace_enabled:
        final.update(sync_work())
    try:
        final['resources_after_child_exit'] = sample()
    except OSError as error:
        final['resource_sample_error'] = str(error)
    Path('/tmp/history-exit.json').write_text(json.dumps(final))
    sys.exit(child.returncode)


def sync_work():
    lines = Path(TRACE).read_text().splitlines()
    return {
        'started_sync_calls': sum('fsync(' in line or 'fdatasync(' in line for line in lines),
        'successful_sync_calls': sum(('fsync(' in line or 'fdatasync(' in line or 'resumed>' in line)
                                     and '= 0' in line for line in lines),
    }


def offline(mode, evict):
    # The Go launcher verifies the fixture container is stopped. This helper
    # shares its volume, not its PID namespace, so /proc cannot prove that here.
    index = ROOT / '.consumer-replay-index.json'
    if mode == 'absent':
        index.unlink(missing_ok=True)
    elif mode == 'corrupt':
        index.write_text('{broken')
    elif mode == 'old':
        envelope = json.loads(index.read_text())
        payload = envelope['Payload']
        payload['Version'] = 0
        raw = json.dumps(payload, separators=(',', ':')).encode()
        # Preserve compact payload bytes so its digest is valid but version is not.
        index.write_bytes(b'{"Payload":' + raw + b',"SHA256":"' + hashlib.sha256(raw).hexdigest().encode() + b'"}')
    elif mode != 'valid':
        raise ValueError(mode)
    if evict:
        for path in ROOT.rglob('*'):
            if path.is_file() and not path.is_symlink():
                with path.open('rb') as file:
                    os.fsync(file.fileno())
                    os.posix_fadvise(file.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
    print(json.dumps(sizes()))


if __name__ == '__main__':
    if sys.argv[1] == 'run':
        run()
    elif sys.argv[1] == 'sample':
        data = json.loads(STATS.read_text())
        data.update(sample())
        if os.environ.get('HISTORY_TRACE_SYNC', 'true') == 'true':
            data.update(sync_work())
        print(json.dumps(data))
    elif sys.argv[1] == 'offline':
        offline(sys.argv[2], sys.argv[3] == 'cold')
    elif sys.argv[1] == 'retained-bytes':
        total = 0
        for path in (ROOT / '__consumer_offsets').glob('*.log'):
            try:
                total += path.stat().st_size
            except FileNotFoundError:
                pass
        print(total)
    elif sys.argv[1] == 'transaction-count':
        count = 0
        for path in (ROOT / '__consumer_offsets').glob('*.log'):
            try:
                count += path.read_bytes().count(b'"type":"transactional_offset_snapshot"')
            except FileNotFoundError:
                pass
        print(count)
