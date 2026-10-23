# Recovery workload observations

See [the design, configuration and results](../../recovery-scheduling.md).
These JSON files are observations, not golden test assertions or production SLOs.

* `bb7dc59-*` records the initial main baseline and tuning experiments with
  Go 1.25.0. The local harness ran for 40 seconds; the real-TCP cluster harness
  ran for 50 seconds. `scheduler-local` used the initial 16-MiB/s candidate.
* `2260df1-baseline.json` and `2260df1-scheduler.json` are the final comparison
  on the integrated main revision with Go 1.26.2 and the default 1-MiB/s budget.
  Both real-TCP workload tests passed. Their full statistics and tradeoffs are
  reported separately from the earlier Go 1.25 tuning series.
* Baseline run 2 recorded one produce failure; its test correctly failed. Its
  latency and resource observations are retained instead of discarding the run.
* The first 1-MiB/s scheduler run could not read the receiver exporter after the
  workload. Missing work counters are unknown, not zero.
* Each cluster has fresh Docker volumes, three brokers limited to 2 CPUs / 2 GiB
  each, and `GOMAXPROCS=2`. Both variants use the same isolated workload harness.
  Storage uses Docker volumes, not the tmpfs used for deterministic unit tests.
* `/proc` CPU and I/O totals use before/after differences for the two sources and
  the restarted receiver's new-process counters. Process-lifetime `VmHWM` values
  cannot establish a simultaneous cluster memory peak or host physical disk QoS.

Run `scripts/recovery-mixed-load.ps1` as documented to collect new observations.
The baseline contains only the opt-in fault hook, never the changed scheduler.
