#!/usr/bin/env python3
"""Compare prebuilt FSM test binaries on one Linux filesystem, without deployment.

Usage: python3 scripts/measure-checkpoints.py /results
The directory must contain baseline.test and fixed.test built from the same
checkpoint_workload_test.go. Untraced runs are A/B/B/A per load condition.
"""
import collections
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def trace_counts(path, start, end):
    counts = collections.Counter()
    pending = {}
    for raw in path.open():
        match = re.match(r"\s*(\d+)\s+(\d+\.\d+)\s+(.*)", raw)
        if not match:
            continue
        pid, timestamp, call = match.groups()
        if "<unfinished ...>" in call:
            pending[pid] = (float(timestamp), call.split("<unfinished ...>")[0])
            continue
        if call.startswith("<..."):
            if pid not in pending:
                continue
            timestamp, prefix = pending.pop(pid)
            call = prefix + call.split("resumed>", 1)[1].lstrip()
        if not start <= float(timestamp) < end:
            continue
        syscall = re.match(r"(\w+)\(", call)
        result = re.search(r"\)\s+=\s+(-?\d+)", call)
        if not syscall or not result:
            continue
        name, value = syscall.group(1), int(result.group(1))
        counts[name] += 1
        if value < 0:
            counts["errors"] += 1
        checkpoint = bool(re.search(r"partition_\d+\.(hwm|producers)", call))
        if checkpoint:
            counts["checkpoint_" + name] += 1
        if name in ("write", "pwrite64") and value > 0:
            counts["write_bytes"] += value
            if "/data/checkpoint-load/" in call:
                counts["file_write_calls"] += 1
                counts["file_write_bytes"] += value
            if checkpoint:
                counts["checkpoint_write_bytes"] += value
        if name in ("fsync", "fdatasync") and "/data/checkpoint-load>" in call:
            counts["checkpoint_directory_sync"] += 1
    return dict(counts)


def run(root, name, revision, load, seconds, trace=False):
    result = root / (name + ".json")
    env = dict(os.environ, CURSUS_CHECKPOINT_RESULT=str(result),
               CURSUS_CHECKPOINT_LOAD=str(int(load)),
               CURSUS_CHECKPOINT_SECONDS=str(seconds), GOMAXPROCS="4")
    command = [str(root / (revision + ".test")),
               "-test.run=^TestCheckpointWorkload$", "-test.timeout=180s"]
    if trace:
        command = ["strace", "-f", "-ttt", "-yy", "-s", "80", "-o",
                   str(root / (name + ".trace")), "-e",
                   "trace=write,pwrite64,fsync,fdatasync,rename,renameat,renameat2,openat,close"] + command
    print("running", name, flush=True)
    with (root / (name + ".log")).open("w") as log:
        subprocess.run(command, env=env, stdout=log, stderr=log, check=True)
    data = json.loads(result.read_text())
    data.update(revision=revision, traced=trace, run=name)
    elapsed = data["seconds"]
    data["cpu_cores"] = (data["user_seconds"] + data["system_seconds"]) / elapsed
    data["write_syscalls_per_second"] = data["io"]["syscw"] / elapsed
    data["write_bytes_per_second"] = data["io"]["write_bytes"] / elapsed
    before = list(map(int, data["host_cpu_before"].split()[1:]))
    after = list(map(int, data["host_cpu_after"].split()[1:]))
    delta = [b-a for a, b in zip(before, after)]
    # Guest ticks already belong to user/nice. Do not count them twice.
    data["host_iowait_percent"] = 100 * delta[4] / sum(delta[:8])
    data["host_iowait_cpu_seconds"] = delta[4] / os.sysconf("SC_CLK_TCK")
    if trace:
        data["syscalls"] = trace_counts(root / (name + ".trace"), data["start_unix"], data["end_unix"])
    result.write_text(json.dumps(data, indent=2) + "\n")
    return data


def main():
    root = Path(sys.argv[1]).resolve()
    metadata = {"uname": os.uname()._asdict() if hasattr(os.uname(), "_asdict") else list(os.uname()),
                "gomaxprocs": 4, "clock_ticks": os.sysconf("SC_CLK_TCK"),
                "binary_sha256": {r: hashlib.sha256((root / (r+".test")).read_bytes()).hexdigest()
                                  for r in ("baseline", "fixed")}}
    (root / "environment.json").write_text(json.dumps(metadata, indent=2)+"\n")
    results = []
    for load in (False, True):
        mode = "load" if load else "idle"
        for i, revision in enumerate(("baseline", "fixed", "fixed", "baseline")):
            results.append(run(root, f"{mode}-{i}-{revision}", revision, load, 30))
    for load in (False, True):
        for revision in ("baseline", "fixed"):
            mode = "load" if load else "idle"
            results.append(run(root, f"trace-{mode}-{revision}", revision, load, 10, trace=True))
    (root / "summary.json").write_text(json.dumps(results, indent=2)+"\n")
    print("wrote", root / "summary.json", flush=True)


if __name__ == "__main__":
    main()
