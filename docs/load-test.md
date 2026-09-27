# Load-test report

This benchmark measures the core reverse-proxy path with k6. The runner builds
Gatex and a small mock-backend binary, starts two backends with a fixed 2 ms
processing delay, and compares one backend directly with Gatex routing across
both backends. Authentication, rate limiting, and caching are disabled so the
result isolates proxying, balancing, request metadata, metrics, and logging.

## Reproduce

Install Go, k6, and curl, then run:

```sh
make load-test
```

The default workload is a constant arrival rate of 1,000 requests/second for
10 seconds with 100 preallocated k6 virtual users. Useful overrides are:

```sh
RATE=2000 DURATION=30s BACKEND_DELAY=5ms make load-test
PREALLOCATED_VUS=200 MAX_VUS=500 make load-test
RESULT_DIR=/tmp/gatex-load-results make load-test
```

`RESULT_DIR` is optional. When set, k6 JSON summaries are retained as
`direct-backend.json` and `gateway.json`; otherwise all binaries, logs, and
temporary output are removed when the run finishes.

## Baseline

Recorded on 2026-09-27 using Go 1.26.1 and k6 2.3.0 under WSL2 on an
Intel Core i5-1035G1 with four cores/eight logical CPUs. The client, gateway,
and backends shared the same host. Each result completed 10,001 requests with
zero HTTP failures and zero dropped iterations.

| Path | Achieved req/s | p50 | p95 | p99 | Max |
|---|---:|---:|---:|---:|---:|
| Direct backend | 999.77 | 2.88 ms | 3.41 ms | 3.68 ms | 5.40 ms |
| Through Gatex | 999.72 | 3.30 ms | 4.01 ms | 4.32 ms | 7.08 ms |
| Observed difference | — | 0.42 ms | 0.60 ms | 0.64 ms | 1.68 ms |

## Bottleneck and interpretation

At 1,000 requests/second, Gatex sustained the requested rate without errors or
drops. The configured 2 ms backend delay is the largest component of median
latency; Gatex adds less than 1 ms through p99 in this run. The gateway is
therefore not the throughput bottleneck at the recorded baseline.

An exploratory 5,000 requests/second run caused the colocated k6 process to
drop scheduled iterations while competing with Gatex and both backends for the
same CPUs. That run is not reported as Gatex capacity: the single-host load
generator became part of the bottleneck. A defensible saturation result should
run k6 on a separate machine, extend the duration, monitor CPU and allocation
profiles, and compare logging enabled versus disabled because Gatex currently
formats and writes one structured log event per request.

These numbers are a repeatable local baseline, not a production service-level
objective. TLS, real backend work, cross-host networking, and production log
sinks will change the latency distribution.
