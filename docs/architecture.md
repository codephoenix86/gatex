# Gatex architecture and scope

Gatex owns the gateway-level cross-cutting concerns that should be consistent
for every backend service: configuration-driven route selection, backend-pool
load balancing, per-client rate limiting, protected-route authentication,
response caching for explicitly cacheable GET routes, circuit breaking, and
operational observability. It deliberately does not own application business
logic, service discovery, or a control plane. The gateway uses Go's standard
`net/http` server and `httputil.ReverseProxy` on the request path so the core
behavior remains explicit and easy to reason about; a lightweight router may
later be used only for local admin and health endpoints.

## Request flow

```
client
  |
  v
gateway HTTP server
  |-- recovery -> request ID/logging -> auth -> rate limit -> cache
  |                                                        |
  v                                                        v
route matcher (YAML routes) -------------------------> cache hit -> client
  |
  v
backend pool -> circuit breaker -> load balancer -> healthy backend
  |
  v
ReverseProxy (configured transport, deadline, request ID)
  |
  v
backend response -> response hooks / metrics -> client
```

Routes are configuration-driven, not hardcoded. Changing a prefix, target pool,
or operational limit should require a config change and restart rather than a
code change. This keeps the gateway reusable for multiple services while keeping
runtime configuration reload out of the initial scope.

## Deliberate library choices

- `net/http` and `net/http/httputil.ReverseProxy` provide the server, transport,
  cancellation semantics, and core proxy implementation without concealing the
  request path behind a framework.
- `gopkg.in/yaml.v3` decodes the operator-facing YAML configuration. Its only
  role is configuration parsing and validation.
- No HTTP router dependency is included yet. If administrative endpoints grow
  beyond the standard library's needs, `github.com/go-chi/chi/v5` is the
  preferred thin router, restricted to those endpoints.

## Package boundaries

| Package | Responsibility |
| --- | --- |
| `cmd/gateway` | Process startup and configuration loading. |
| `internal/config` | YAML schema, defaults, and validation. |
| `internal/proxy` | Reverse-proxy handler and outbound transport. |
| `internal/balancer` | Backend selection and health tracking. |
| `internal/ratelimiter` | Per-client request limiting. |
| `internal/breaker` | Backend-pool circuit-breaker state machine. |
| `internal/middleware` | Composable HTTP middleware. |
| `internal/metrics` | Metrics and health/readiness endpoints. |
