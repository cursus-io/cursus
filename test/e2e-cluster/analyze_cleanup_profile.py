"""Summarize one no-restart cleanup profile; retain raw samples for audit."""
import json
import re
import statistics
import sys


def pairs(text):
    result = {}
    for line in text.splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            match = re.match(r"(\d+)(?:\s|$)", value.strip())
            if match:
                result[key] = int(match[1])
    return result


def block(text):
    result = {}
    for line in text.splitlines():
        device, *fields = line.split()
        if len(fields) == 2 and fields[0] in ("Read", "Write", "Sync", "Async", "Total"):
            result[f"{device}/{fields[0].lower()}_bytes"] = int(fields[1])
            continue
        if device == "Total":
            continue
        for field in fields:
            key, value = field.split("=")
            result[f"{device}/{key}"] = int(value)
    return result


def network(text):
    result = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        device, data = line.split(":")
        fields = data.split()
        result[f"{device.strip()}/receive_bytes"] = int(fields[0])
        result[f"{device.strip()}/transmit_bytes"] = int(fields[8])
    return result


def delta(first, last):
    # A missing series is omitted, not a measured zero.
    return {key: last[key] - value for key, value in first.items() if key in last}


def summarize(data):
    result = {key: data[key] for key in ("setup_ns", "operation_start", "intent_ack",
                                        "release_start", "all_materialized", "recovery_ns", "pending")}
    result["nodes"] = []
    for node, (before, after) in enumerate(zip(data["before"], data["after"]), 1):
        assert before["metrics"]["process_start_time_seconds"] == after["metrics"]["process_start_time_seconds"], "counter reset"
        metrics = delta(before["metrics"], after["metrics"])
        # New logical counter label sets are captured separately rather than
        # silently turning their missing baseline into zero.
        appeared = {k: v for k, v in after["metrics"].items() if k not in before["metrics"]}
        calls = {}
        for line in data["sync_trace"][node - 1].splitlines():
            fields = line.split()
            if fields and fields[-1] in ("fsync", "fdatasync"):
                calls[fields[-1]] = {"calls": int(fields[3]), "errors": int(fields[4]) if len(fields) == 6 else 0}
        result["nodes"].append({"node": node, "before_at": before["at"], "after_at": after["at"],
            "before_completed": before["completed"], "after_completed": after["completed"],
            "trace_started": data["trace_started"][node - 1], "trace_stopped": data["trace_stopped"][node - 1],
            "cgroup_sources": after["cgroup_sources"],
            "process_io_delta": delta(pairs(before["proc_io"]), pairs(after["proc_io"])),
            "kernel_block_io_delta": delta(block(before["cgroup_io"]), block(after["cgroup_io"])),
            "network_delta": delta(network(before["network"]), network(after["network"])),
            "cgroup_cpu_delta": delta(dict((k, int(v)) for k, v in (s.split() for s in before["cgroup_cpu"].splitlines())),
                                      dict((k, int(v)) for k, v in (s.split() for s in after["cgroup_cpu"].splitlines()))),
            "counter_deltas": {k: v for k, v in metrics.items() if k.endswith("_total") or "_total{" in k},
            "appeared_series": appeared, "traced_syncs": calls,
            "phase_sampled_peaks": data["sampled_peaks"][node - 1],
            "lifetime_rss_high_water_kib": pairs(after["proc_status"])["VmHWM"]})
    times = sorted(data["traffic"]["publish_commit_heartbeat_ns"])
    result["traffic"] = {"acknowledged": data["traffic"]["acknowledged"], "samples": len(times),
                         "p50_ns": statistics.median(times), "p99_ns": times[(len(times)-1)*99//100], "max_ns": max(times)}
    return result


if __name__ == "__main__":
    with open(sys.argv[1], encoding="utf-8-sig") as source:
        print(json.dumps(summarize(json.load(source)), indent=2))
