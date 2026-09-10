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
route response cache + request ID ----------> HIT -> client
  |
  | MISS
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
7. **Response caching** runs only on explicitly configured routes and after
   authentication and rate limiting. This prevents unauthorized access to a
   cached response and ensures cache hits consume the same client quota as
   misses. A hit avoids the circuit breaker, load balancer, and upstream call.
   The cache layer assigns the current request ID to a hit and passes that same
   ID into proxy handling on a miss.
8. **Proxy handling** adds the request deadline and request ID, checks the pool
   circuit breaker, selects a healthy backend, and performs the upstream call.

Responses unwind through the same handlers in reverse order. In particular,
the access logger observes the final status and latency after the inner request
path completes.

## Structured request logs

The outer request logger emits one JSON completion event for every request,
including preflight and gateway-generated error responses. Each event records
the request ID, method, path, host, remote address, final status, response size,
and total gateway latency. Once load balancing selects an upstream, the event
also records that backend URL without URL credentials or query parameters.
Request IDs are validated at the start of the middleware chain and returned to
the client in `X-Request-ID`, so even requests rejected before proxying can be
correlated with their log event.

## Response cache

Caching is disabled unless a route supplies both `cache.ttl` and
`cache.max_entries`. Each configured route owns an independent, concurrency-safe
LRU cache, so entries cannot collide across routes and one route cannot evict
another route's responses. Entries expire lazily after the configured TTL. The
entry limit bounds the number of stored responses, while a fixed 1 MiB body
limit prevents one response from consuming unbounded temporary or cache memory.

Only bodyless `GET` requests are candidates. The key contains the incoming
scheme, host, escaped path, and raw query. Requests carrying authorization,
cookies, range or conditional headers, upgrade requests, and client no-cache
directives bypass the cache. Gatex stores only complete `200 OK` responses and
does not store responses with `no-cache`, `no-store`, `private`, zero freshness,
`Set-Cookie`, `Vary`, content ranges, content encoding, trailers, or an oversized
body. These conservative rules avoid replaying personalized, partial, encoded,
or otherwise variant content without implementing a full RFC-compliant shared
HTTP cache.

Eligible lookups return `X-Cache: MISS` when the request reaches the proxy and
`X-Cache: HIT` when Gatex replays a stored response. Bypassed requests and
routes without caching omit the header. Request IDs are never stored; every
cache hit returns the current request's ID.

Invalidation is TTL- and eviction-based: there is no active purge endpoint in
the initial scope, and restarting Gatex clears every entry. Each gateway
instance has its own cache, so instances can temporarily hold different values.
A multi-instance deployment that needs coordinated invalidation should use a
shared store such as Redis or an invalidation channel; Gatex intentionally
implements neither in this phase.

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
| `internal/cache` | Bounded, concurrency-safe TTL/LRU response storage. |
| `internal/proxy` | Reverse-proxy handler and outbound transport. |
| `internal/balancer` | Backend selection and health tracking. |
| `internal/ratelimiter` | Per-client request limiting. |
| `internal/breaker` | Backend-pool circuit-breaker state machine. |
| `internal/middleware` | Composable HTTP middleware. |
| `internal/metrics` | Metrics and health/readiness endpoints. |
