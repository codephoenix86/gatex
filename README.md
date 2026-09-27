# Gatex

`gatex` is a concurrent API gateway written in Go. It currently provides a
configuration-driven reverse proxy with path-based routing, concurrent,
health-aware load balancing, per-client rate limiting, circuit breaking, API-key
authentication, CORS, structured request logging, and opt-in in-memory response
caching. It also provides Prometheus request, runtime, and process metrics,
request deadlines and IDs, tuned upstream connection reuse, panic recovery, and
graceful process shutdown. Operational liveness and readiness endpoints are
also available; tracing is added in the remainder of the observability phase.

See [the architecture notes](docs/architecture.md) and
[the example configuration](configs/gateway.example.yaml) to get started.

## Testing

Run the regular test suite:

```sh
make test
```

Run the complete suite with Go's race detector and without cached results:

```sh
make test-race
```

Run the local k6 benchmark against Gatex and its mock backends:

```sh
make load-test
```

The benchmark setup, environment overrides, latest baseline, and bottleneck
analysis are documented in [the load-test report](docs/load-test.md).
