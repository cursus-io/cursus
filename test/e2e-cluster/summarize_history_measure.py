"""Summarize raw experiment rows without combining overlapping I/O counters."""
import csv
import json
from pathlib import Path
import sys

source = Path(sys.argv[1])
rows = [json.loads(line) for line in source.read_text(encoding='utf-8-sig').splitlines()]


def work(stage, unit):
    return f'cursus_recovery_work_total{{stage="{stage}",unit="{unit}"}}'


def total(nodes, key):
    reported = [values[key] for values in nodes.values() if key in values]
    return sum(reported) if reported else ''


def delta(nodes, before):
    return {node: {key: value - before.get(node, {}).get(key, 0)
                   for key, value in values.items()} for node, values in nodes.items()}


summary = []
for row in rows:
    if row['phase'] == 'experiment':
        continue
    item = {key: row.get(key, '') for key in ['history', 'variant', 'phase', 'index', 'wall_seconds', 'error']}
    resources = row.get('resources', row.get('after', row.get('resources_last_process_only', row.get('after_survivors', {}))))
    metrics = row.get('metrics', row.get('metrics_survivors', {}))
    if row['phase'] == 'coordinator_failover':
        counters = delta(resources, row['before'])
        metrics = delta(metrics, row['before_metrics'])
        item['resource_scope'] = 'survivor delta; peaks are process lifetime'
    else:
        counters = resources
        item['resource_scope'] = ('last process only' if 'resources_last_process_only' in row else 'current process lifetime')
    for key in ['cpu_seconds', 'container_cpu_seconds', 'container_read_bytes', 'container_write_bytes', 'process_read_bytes', 'process_write_bytes', 'process_rchar', 'process_wchar', 'successful_sync_calls', 'allocated_bytes_total', 'allocations_total', 'network_received_bytes', 'network_transmitted_bytes']:
        item[key] = total(counters, key) if counters else ''
    for key in ['retained_metadata_bytes', 'retained_metadata_segments', 'sampled_peak_heap_bytes', 'peak_rss_bytes', 'container_peak_memory_bytes', 'sampled_peak_temporary_bytes', 'sampled_peak_store_bytes']:
        if key == 'sampled_peak_heap_bytes':
            # The explicit end-of-phase sample can exceed the periodic peak.
            # Older wrappers emitted zero before observing any heap sample.
            observed = [v[k] for v in resources.values()
                        for k in ('sampled_peak_heap_bytes', 'heap_bytes') if v.get(k, 0) > 0]
        else:
            observed = [v[key] for v in resources.values() if key in v]
        item['max_node_' + key] = max(observed, default='')
    for stage, units in {'consumer_frames': ['read_bytes', 'decoded_frames'], 'consumer_replay': ['decoded_bytes', 'decoded_frames', 'pages'], 'consumer_index': ['read_bytes', 'write_bytes', 'cache_hits', 'cache_misses'], 'retained_scan': ['read_bytes', 'decoded_frames'], 'metadata_rpc': ['transfer_bytes'], 'replica_rpc': ['transfer_bytes']}.items():
        for unit in units:
            item[stage + '_' + unit] = total(metrics, work(stage, unit)) if metrics else ''
    for key in ['source_records_total', 'source_payload_bytes_total', 'removed_records_total', 'removed_frame_bytes_total']:
        item['history_' + key] = total(metrics, 'cursus_consumer_history_' + key) if metrics else ''
    summary.append(item)
with source.with_suffix('.summary.csv').open('w', encoding='utf-8', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=list(summary[0]) if summary else [])
    writer.writeheader()
    writer.writerows(summary)
for row in summary:
    print(row['history'], row['variant'], row['phase'], row['index'], round(row['wall_seconds'], 3))
