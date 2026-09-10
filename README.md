# Gatex

`gatex` is a concurrent API gateway written in Go. It currently provides a
configuration-driven reverse proxy with path-based routing, concurrent,
health-aware load balancing, per-client rate limiting, circuit breaking, API-key
authentication, CORS, structured request logging, and opt-in in-memory response
caching. It also provides Prometheus request, runtime, and process metrics,
request deadlines and IDs, tuned upstream connection reuse, panic recovery, and
graceful process shutdown. Gateway health endpoints and tracing are added in
the remainder of the observability phase.

See [the architecture notes](docs/architecture.md) and
[the example configuration](configs/gateway.example.yaml) to get started.
