// Package proxy implements Gatex's HTTP reverse-proxy request path.
package proxy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codephoenix86/gatex/internal/balancer"
	"github.com/codephoenix86/gatex/internal/config"
	"github.com/codephoenix86/gatex/internal/ratelimiter"
)

const (
	// RequestIDHeader is forwarded to upstreams and returned to callers.
	RequestIDHeader = "X-Request-ID"

	defaultRequestTimeout        = 15 * time.Second
	defaultDialTimeout           = 2 * time.Second
	defaultTLSHandshakeTimeout   = 3 * time.Second
	defaultResponseHeaderTimeout = 5 * time.Second
	defaultIdleConnectionTimeout = 90 * time.Second
)

type requestIDContextKey struct{}

// RequestID returns the request ID attached by Gateway, if present.
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDContextKey{}).(string)
	return id
}

// Gateway routes requests to configured backend pools.
type Gateway struct {
	routes           []route
	pools            map[string]*pool
	requestTimeout   time.Duration
	transport        *http.Transport
	healthCheckOnce  sync.Once
	healthChecksDone sync.WaitGroup
}

type route struct {
	pathPrefix       string
	pool             *pool
	limiter          *ratelimiter.ClientLimiter
	retryAfterHeader string
}

type pool struct {
	balancer      *balancer.Pool
	upstreams     []*upstream
	healthChecker *balancer.HealthChecker
}

type upstream struct {
	backend *balancer.Backend
	target  *url.URL
	proxy   *httputil.ReverseProxy
}

// NewGateway validates cfg and creates a gateway with a tuned shared transport.
func NewGateway(cfg config.Config) (*Gateway, error) {
	return NewGatewayWithTransport(cfg, NewTransport(cfg.Timeouts))
}

// NewGatewayWithTransport is like NewGateway but permits a caller to supply a
// transport, which is useful when embedding Gatex or testing outbound calls.
// The supplied transport must remain usable for the Gateway's lifetime.
func NewGatewayWithTransport(cfg config.Config, transport http.RoundTripper) (*Gateway, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("gateway configuration: %w", err)
	}
	if transport == nil {
		return nil, errors.New("gateway transport cannot be nil")
	}

	gateway := &Gateway{
		pools:          make(map[string]*pool, len(cfg.BackendPools)),
		requestTimeout: withDefault(cfg.Timeouts.Request, defaultRequestTimeout),
	}
	if configuredTransport, ok := transport.(*http.Transport); ok {
		gateway.transport = configuredTransport
	}

	for name, configuredPool := range cfg.BackendPools {
		urls := make([]string, 0, len(configuredPool.Backends))
		for _, configuredBackend := range configuredPool.Backends {
			urls = append(urls, configuredBackend.URL)
		}
		balancingPool, err := balancer.NewPoolWithStrategy(urls, balancer.Strategy(configuredPool.Strategy))
		if err != nil {
			return nil, fmt.Errorf("create balancer pool %q: %w", name, err)
		}
		healthChecker, err := balancer.NewHealthChecker(
			configuredPool.HealthCheck.Interval,
			configuredPool.HealthCheck.Timeout,
			configuredPool.HealthCheck.Path,
			&http.Client{Transport: transport},
		)
		if err != nil {
			return nil, fmt.Errorf("create health checker for pool %q: %w", name, err)
		}
		balancedBackends := balancingPool.Backends()

		backendPool := &pool{
			balancer:      balancingPool,
			upstreams:     make([]*upstream, 0, len(configuredPool.Backends)),
			healthChecker: healthChecker,
		}
		for index, configuredBackend := range configuredPool.Backends {
			target, err := url.Parse(configuredBackend.URL)
			if err != nil {
				return nil, fmt.Errorf("parse backend %q in pool %q: %w", configuredBackend.URL, name, err)
			}
			backendPool.upstreams = append(backendPool.upstreams, &upstream{
				backend: balancedBackends[index],
				target:  target,
				proxy:   newReverseProxy(target, transport),
			})
		}
		gateway.pools[name] = backendPool
	}

	for _, configuredRoute := range cfg.Routes {
		gatewayRoute := route{
			pathPrefix: configuredRoute.PathPrefix,
			pool:       gateway.pools[configuredRoute.BackendPool],
		}
		limit := cfg.RateLimit
		if configuredRoute.RateLimit != nil {
			limit = *configuredRoute.RateLimit
		}
		if limit.RequestsPerSecond > 0 {
			limiter, err := ratelimiter.NewClientLimiter(limit.RequestsPerSecond, limit.Burst)
			if err != nil {
				return nil, fmt.Errorf("create rate limiter for route %q: %w", configuredRoute.PathPrefix, err)
			}
			gatewayRoute.limiter = limiter
			gatewayRoute.retryAfterHeader = retryAfterValue(limit.RequestsPerSecond)
		}
		gateway.routes = append(gateway.routes, gatewayRoute)
	}

	return gateway, nil
}

// StartHealthChecks starts one health-check loop for every backend pool. The
// supplied context controls their lifetime; subsequent calls have no effect.
func (g *Gateway) StartHealthChecks(ctx context.Context) {
	g.healthCheckOnce.Do(func() {
		for _, backendPool := range g.pools {
			done := backendPool.healthChecker.Start(ctx, backendPool.balancer)
			g.healthChecksDone.Add(1)
			go func() {
				defer g.healthChecksDone.Done()
				<-done
			}()
		}
	})
}

// WaitForHealthChecks waits for health-check loops started by
// StartHealthChecks to exit.
func (g *Gateway) WaitForHealthChecks() {
	g.healthChecksDone.Wait()
}

// NewTransport builds the shared upstream transport. Explicit timeouts from
// configuration win; safe defaults ensure the proxy never uses unbounded dial
// or response-header waits. The idle-connection limits prevent a busy gateway
// from retaining an unbounded number of backend connections.
func NewTransport(timeouts config.Timeouts) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   withDefault(timeouts.Dial, defaultDialTimeout),
		KeepAlive: 30 * time.Second,
	}

	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       withDefault(timeouts.IdleConnection, defaultIdleConnectionTimeout),
		TLSHandshakeTimeout:   withDefault(timeouts.TLSHandshake, defaultTLSHandshakeTimeout),
		ResponseHeaderTimeout: withDefault(timeouts.ResponseHeader, defaultResponseHeaderTimeout),
		ExpectContinueTimeout: time.Second,
	}
}

// CloseIdleConnections closes reusable upstream connections during process
// shutdown. It is safe to call after the HTTP server has drained requests.
func (g *Gateway) CloseIdleConnections() {
	if g.transport != nil {
		g.transport.CloseIdleConnections()
	}
}

// ServeHTTP matches the most-specific route prefix, attaches a request ID and
// deadline to the request context, and delegates the backend call to
// httputil.ReverseProxy.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	matchedRoute := g.matchRoute(r.URL.Path)
	if matchedRoute == nil {
		http.Error(w, "no route configured for request path", http.StatusNotFound)
		return
	}

	requestID := incomingOrNewRequestID(r.Header.Get(RequestIDHeader))
	if matchedRoute.limiter != nil && !matchedRoute.limiter.Allow(clientKeyFromRemoteAddr(r.RemoteAddr)) {
		w.Header().Set(RequestIDHeader, requestID)
		w.Header().Set("Retry-After", matchedRoute.retryAfterHeader)
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), g.requestTimeout)
	defer cancel()
	ctx = context.WithValue(ctx, requestIDContextKey{}, requestID)

	request := r.Clone(ctx)
	request.Header.Set(RequestIDHeader, requestID)
	upstream, ok := matchedRoute.pool.acquire()
	if !ok {
		w.Header().Set(RequestIDHeader, requestID)
		http.Error(w, "no healthy backends available", http.StatusServiceUnavailable)
		return
	}
	defer upstream.backend.Release()
	upstream.proxy.ServeHTTP(w, request)
}

func (g *Gateway) matchRoute(path string) *route {
	var matched *route
	for index := range g.routes {
		candidate := &g.routes[index]
		if !prefixMatches(candidate.pathPrefix, path) {
			continue
		}
		if matched == nil || len(candidate.pathPrefix) > len(matched.pathPrefix) {
			matched = candidate
		}
	}
	return matched
}

func (p *pool) acquire() (*upstream, bool) {
	backend, ok := p.balancer.Acquire()
	if !ok {
		return nil, false
	}
	for _, upstream := range p.upstreams {
		if upstream.backend == backend {
			return upstream, true
		}
	}

	// A pool is constructed from matching backend and upstream slices, so this
	// path is unreachable unless that construction invariant is broken.
	backend.Release()
	return nil, false
}

func newReverseProxy(target *url.URL, transport http.RoundTripper) *httputil.ReverseProxy {
	proxy := &httputil.ReverseProxy{
		Director: func(request *http.Request) {
			originalHost := request.Host
			rewriteRequestURL(request.URL, target)
			request.Host = target.Host
			request.Header.Set(RequestIDHeader, RequestID(request.Context()))
			request.Header.Set("X-Forwarded-Host", originalHost)
			if request.TLS != nil {
				request.Header.Set("X-Forwarded-Proto", "https")
			} else {
				request.Header.Set("X-Forwarded-Proto", "http")
			}
		},
		Transport: transport,
		ModifyResponse: func(response *http.Response) error {
			response.Header.Set(RequestIDHeader, RequestID(response.Request.Context()))
			response.Header.Set("X-Gateway", "gatex")
			return nil
		},
		ErrorHandler: func(writer http.ResponseWriter, request *http.Request, err error) {
			status := http.StatusBadGateway
			message := "upstream request failed"
			if errors.Is(request.Context().Err(), context.DeadlineExceeded) {
				status = http.StatusGatewayTimeout
				message = "upstream request timed out"
			}
			writer.Header().Set(RequestIDHeader, RequestID(request.Context()))
			http.Error(writer, message, status)
		},
	}
	return proxy
}

func rewriteRequestURL(requestURL, target *url.URL) {
	requestURL.Scheme = target.Scheme
	requestURL.Host = target.Host
	requestURL.Path = joinPath(target.Path, requestURL.Path)
	requestURL.RawPath = joinPath(target.EscapedPath(), requestURL.EscapedPath())
	if requestURL.RawPath == requestURL.Path {
		requestURL.RawPath = ""
	}
	if target.RawQuery == "" {
		return
	}
	if requestURL.RawQuery == "" {
		requestURL.RawQuery = target.RawQuery
		return
	}
	requestURL.RawQuery = target.RawQuery + "&" + requestURL.RawQuery
}

func joinPath(base, path string) string {
	if base == "" || base == "/" {
		return path
	}
	if path == "" || path == "/" {
		return base
	}
	return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(path, "/")
}

func prefixMatches(prefix, path string) bool {
	return prefix == "/" || path == prefix || strings.HasPrefix(path, strings.TrimRight(prefix, "/")+"/")
}

func clientKeyFromRemoteAddr(remoteAddr string) string {
	// Forwarded headers are intentionally ignored until the gateway can be
	// configured with trusted proxies; otherwise clients could spoof keys.
	if address, err := netip.ParseAddrPort(remoteAddr); err == nil {
		return address.Addr().Unmap().String()
	}
	if address, err := netip.ParseAddr(remoteAddr); err == nil {
		return address.Unmap().String()
	}
	return strings.TrimSpace(remoteAddr)
}

func retryAfterValue(requestsPerSecond float64) string {
	seconds := math.Ceil(1 / requestsPerSecond)
	if seconds < 1 {
		seconds = 1
	}
	if seconds >= float64(math.MaxInt64) {
		return strconv.FormatInt(math.MaxInt64, 10)
	}
	return strconv.FormatInt(int64(seconds), 10)
}

func incomingOrNewRequestID(incoming string) string {
	if isSafeRequestID(incoming) {
		return incoming
	}
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err == nil {
		return hex.EncodeToString(bytes)
	}
	// crypto/rand failures are exceptionally rare. A timestamp still provides
	// an identifier for log correlation without failing a customer request.
	return fmt.Sprintf("%x", time.Now().UnixNano())
}

func isSafeRequestID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if char < 0x21 || char > 0x7e {
			return false
		}
	}
	return true
}

func withDefault(value, fallback time.Duration) time.Duration {
	if value <= 0 {
		return fallback
	}
	return value
}
