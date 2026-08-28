# Gatex

`gatex` is a concurrent API gateway written in Go. It currently provides a
configuration-driven reverse proxy with path-based routing, request deadlines
and IDs, tuned upstream connection reuse, and graceful process shutdown.
Load balancing, resilience, authentication, and observability are added in
subsequent phases.

See [the architecture notes](docs/architecture.md) and
[the example configuration](configs/gateway.example.yaml) to get started.
