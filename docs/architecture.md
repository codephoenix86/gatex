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
panic recovery
  |
  v
structured request/response logging
  |
  v
CORS ------------------------------------> preflight response -> client
  |
  v
route matcher (YAML routes)
  |
  v
API-key auth (protected routes only)
  |
  v
per-client rate limit
  |
  v
request deadline + request ID
  |
  v
pool circuit breaker -> load balancer -> healthy backend
  |
  v
ReverseProxy (configured transport) -> backend -> client
```

Routes are configuration-driven, not hardcoded. Changing a prefix, target pool,
or operational limit should require a config change and restart rather than a
code change. This keeps the gateway reusable for multiple services while keeping
runtime configuration reload out of the initial scope.

## Middleware order

`middleware.Chain` treats the first item as the outermost handler. Gatex uses
two composition points: process-wide middleware wraps the route matcher, and
each matched route wraps its proxy handler with route-specific policy. Together
they produce this order:

1. **Recovery** is outermost so a panic from any later middleware or handler is
   contained and converted to a generic `500` response.
2. **Structured logging** wraps every outcome—including CORS preflights,
   authentication failures, rate-limit rejections, and upstream responses—so
   status and latency describe the whole gateway request.
3. **CORS** runs before route policy so browser preflights do not require an API
   key or consume rate-limit capacity. It also places CORS headers on downstream
   success and error responses.
4. **Route matching** selects the backend pool and the route-specific policy
   before authentication is enforced.
5. **API-key authentication** runs only for protected routes and precedes the
   limiter, so rejected credentials do not consume the quota reserved for
   authenticated traffic. A deployment exposed to credential-guessing floods
   should add a separate coarse IP limiter before authentication.
6. **Rate limiting** rejects excess authenticated or public-route traffic
   before a circuit-breaker permit, backend slot, or upstream connection is
   acquired.
7. **Proxy handling** adds the request deadline and request ID, checks the pool
   circuit breaker, selects a healthy backend, and performs the upstream call.

Responses unwind through the same handlers in reverse order. In particular,
the access logger observes the final status and latency after the inner request
path completes.

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
