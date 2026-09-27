package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codephoenix86/gatex/internal/config"
	"github.com/codephoenix86/gatex/internal/middleware"
	"github.com/codephoenix86/gatex/internal/proxy"
)

func TestGatewayEndToEndRoutesAndRewritesRequests(t *testing.T) {
	t.Parallel()

	observed := make(chan observedUpstreamRequest, 2)
	usersBackend := newMockBackend(t, "users", http.StatusCreated, observed)
	ordersBackend := newMockBackend(t, "orders", http.StatusAccepted, observed)
	cfg := config.Config{
		ListenAddress: ":8080",
		Auth:          config.Auth{APIKeys: []string{"integration-secret"}},
		BackendPools: map[string]config.Pool{
			"users": {
				Strategy: config.RoundRobin,
				Backends: []config.Backend{{URL: usersBackend.URL + "/users-base?source=users"}},
			},
			"orders": {
				Strategy: config.RoundRobin,
				Backends: []config.Backend{{URL: ordersBackend.URL + "/orders-base?source=orders"}},
			},
		},
		Routes: []config.Route{
			{PathPrefix: "/api", BackendPool: "orders"},
			{PathPrefix: "/api/users", BackendPool: "users", Protected: true},
		},
	}
	gatewayServer := newEndToEndGateway(t, cfg)

	usersRequest := newIntegrationRequest(t, http.MethodGet, gatewayServer.URL+"/api/users/42?expand=teams")
	usersRequest.Host = "gateway.example"
	usersRequest.Header.Set(proxy.RequestIDHeader, "integration-users-request")
	usersRequest.Header.Set(middleware.APIKeyHeader, "integration-secret")
	usersResponse := sendIntegrationRequest(t, gatewayServer.Client(), usersRequest)

	if usersResponse.status != http.StatusCreated {
		t.Fatalf("users status = %d, want %d", usersResponse.status, http.StatusCreated)
	}
	if usersResponse.body != "users" {
		t.Errorf("users body = %q, want %q", usersResponse.body, "users")
	}
	if got := usersResponse.header.Get(proxy.RequestIDHeader); got != "integration-users-request" {
		t.Errorf("users request ID = %q, want %q", got, "integration-users-request")
	}
	assertGatewayResponseHeaders(t, usersResponse.header, "users")

	usersUpstream := receiveUpstreamRequest(t, observed)
	if usersUpstream.name != "users" {
		t.Fatalf("users request reached %q backend", usersUpstream.name)
	}
	if usersUpstream.path != "/users-base/api/users/42" {
		t.Errorf("users upstream path = %q", usersUpstream.path)
	}
	if usersUpstream.rawQuery != "source=users&expand=teams" {
		t.Errorf("users upstream query = %q", usersUpstream.rawQuery)
	}
	if usersUpstream.host != strings.TrimPrefix(usersBackend.URL, "http://") {
		t.Errorf("users upstream host = %q, want %q", usersUpstream.host, strings.TrimPrefix(usersBackend.URL, "http://"))
	}
	if usersUpstream.requestID != "integration-users-request" {
		t.Errorf("users upstream request ID = %q", usersUpstream.requestID)
	}
	if usersUpstream.apiKey != "" {
		t.Errorf("users upstream received API key %q", usersUpstream.apiKey)
	}
	if usersUpstream.forwardedHost != "gateway.example" {
		t.Errorf("users forwarded host = %q, want %q", usersUpstream.forwardedHost, "gateway.example")
	}
	if usersUpstream.forwardedProto != "http" {
		t.Errorf("users forwarded proto = %q, want %q", usersUpstream.forwardedProto, "http")
	}

	ordersRequest := newIntegrationRequest(t, http.MethodGet, gatewayServer.URL+"/api/orders/9")
	ordersResponse := sendIntegrationRequest(t, gatewayServer.Client(), ordersRequest)
	if ordersResponse.status != http.StatusAccepted {
		t.Fatalf("orders status = %d, want %d", ordersResponse.status, http.StatusAccepted)
	}
	if ordersResponse.body != "orders" {
		t.Errorf("orders body = %q, want %q", ordersResponse.body, "orders")
	}
	if ordersResponse.header.Get(proxy.RequestIDHeader) == "" {
		t.Error("orders response has no generated request ID")
	}
	assertGatewayResponseHeaders(t, ordersResponse.header, "orders")

	ordersUpstream := receiveUpstreamRequest(t, observed)
	if ordersUpstream.name != "orders" {
		t.Fatalf("orders request reached %q backend", ordersUpstream.name)
	}
	if ordersUpstream.path != "/orders-base/api/orders/9" {
		t.Errorf("orders upstream path = %q", ordersUpstream.path)
	}
	if ordersUpstream.rawQuery != "source=orders" {
		t.Errorf("orders upstream query = %q", ordersUpstream.rawQuery)
	}
	if ordersUpstream.requestID != ordersResponse.header.Get(proxy.RequestIDHeader) {
		t.Errorf("orders upstream request ID = %q, want response ID %q", ordersUpstream.requestID, ordersResponse.header.Get(proxy.RequestIDHeader))
	}
}

func TestGatewayEndToEndAppliesAuthRateLimitAndCache(t *testing.T) {
	t.Parallel()

	var upstreamCalls atomic.Int64
	upstreamAPIKeys := make(chan string, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		call := upstreamCalls.Add(1)
		upstreamAPIKeys <- request.Header.Get(middleware.APIKeyHeader)
		writer.Header().Set("ETag", `"integration-v1"`)
		writer.Header().Set(proxy.CacheStatusHeader, "UPSTREAM")
		_, _ = writer.Write([]byte("payload-" + strconv.FormatInt(call, 10)))
	}))
	t.Cleanup(backend.Close)

	cfg := config.Config{
		ListenAddress: ":8080",
		Auth:          config.Auth{APIKeys: []string{"integration-secret"}},
		RateLimit: config.RateLimit{
			RequestsPerSecond: 0.0001,
			Burst:             2,
		},
		BackendPools: map[string]config.Pool{
			"backend": {
				Strategy: config.RoundRobin,
				Backends: []config.Backend{{URL: backend.URL}},
			},
		},
		Routes: []config.Route{{
			PathPrefix:  "/protected",
			BackendPool: "backend",
			Protected:   true,
			Cache:       &config.Cache{TTL: time.Minute, MaxEntries: 10},
		}},
	}
	gatewayServer := newEndToEndGateway(t, cfg)

	unauthorizedRequest := newIntegrationRequest(t, http.MethodGet, gatewayServer.URL+"/protected/resource")
	unauthorizedRequest.Header.Set(proxy.RequestIDHeader, "integration-unauthorized")
	unauthorized := sendIntegrationRequest(t, gatewayServer.Client(), unauthorizedRequest)
	if unauthorized.status != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d, want %d", unauthorized.status, http.StatusUnauthorized)
	}
	if unauthorized.header.Get("WWW-Authenticate") != "ApiKey" {
		t.Errorf("WWW-Authenticate = %q, want %q", unauthorized.header.Get("WWW-Authenticate"), "ApiKey")
	}
	if unauthorized.header.Get(proxy.CacheStatusHeader) != "" {
		t.Errorf("unauthorized cache status = %q, want empty", unauthorized.header.Get(proxy.CacheStatusHeader))
	}

	missRequest := newIntegrationRequest(t, http.MethodGet, gatewayServer.URL+"/protected/resource")
	missRequest.Header.Set(proxy.RequestIDHeader, "integration-cache-miss")
	missRequest.Header.Set(middleware.APIKeyHeader, "integration-secret")
	miss := sendIntegrationRequest(t, gatewayServer.Client(), missRequest)
	assertIntegrationResponse(t, miss, http.StatusOK, "payload-1", "integration-cache-miss", "MISS")

	hitRequest := newIntegrationRequest(t, http.MethodGet, gatewayServer.URL+"/protected/resource")
	hitRequest.Header.Set(proxy.RequestIDHeader, "integration-cache-hit")
	hitRequest.Header.Set(middleware.APIKeyHeader, "integration-secret")
	hit := sendIntegrationRequest(t, gatewayServer.Client(), hitRequest)
	assertIntegrationResponse(t, hit, http.StatusOK, "payload-1", "integration-cache-hit", "HIT")
	if got := hit.header.Get("ETag"); got != `"integration-v1"` {
		t.Errorf("cached ETag = %q, want %q", got, `"integration-v1"`)
	}

	limitedRequest := newIntegrationRequest(t, http.MethodGet, gatewayServer.URL+"/protected/resource")
	limitedRequest.Header.Set(middleware.APIKeyHeader, "integration-secret")
	limited := sendIntegrationRequest(t, gatewayServer.Client(), limitedRequest)
	if limited.status != http.StatusTooManyRequests {
		t.Errorf("rate-limited status = %d, want %d", limited.status, http.StatusTooManyRequests)
	}
	if limited.header.Get("Retry-After") != "10000" {
		t.Errorf("Retry-After = %q, want %q", limited.header.Get("Retry-After"), "10000")
	}
	if limited.header.Get(proxy.CacheStatusHeader) != "" {
		t.Errorf("rate-limited cache status = %q, want empty", limited.header.Get(proxy.CacheStatusHeader))
	}

	if got := upstreamCalls.Load(); got != 1 {
		t.Errorf("upstream calls = %d, want 1", got)
	}
	if got := <-upstreamAPIKeys; got != "" {
		t.Errorf("upstream received API key %q", got)
	}

	metricsRequest := newIntegrationRequest(t, http.MethodGet, gatewayServer.URL+"/metrics")
	metricsResponse := sendIntegrationRequest(t, gatewayServer.Client(), metricsRequest)
	if metricsResponse.status != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", metricsResponse.status, http.StatusOK)
	}
	for _, metric := range []string{
		`gatex_http_requests_total{method="GET",route="/protected",status="200"} 2`,
		`gatex_http_requests_total{method="GET",route="/protected",status="401"} 1`,
		`gatex_http_requests_total{method="GET",route="/protected",status="429"} 1`,
	} {
		if !strings.Contains(metricsResponse.body, metric) {
			t.Errorf("metrics response does not contain %q", metric)
		}
	}
}

func TestGatewayEndToEndCircuitBreakerUpdatesReadiness(t *testing.T) {
	t.Parallel()

	var upstreamCalls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		upstreamCalls.Add(1)
		http.Error(writer, "backend unavailable", http.StatusBadGateway)
	}))
	t.Cleanup(backend.Close)

	cfg := config.Config{
		ListenAddress: ":8080",
		BackendPools: map[string]config.Pool{
			"critical": {
				Strategy: config.RoundRobin,
				Backends: []config.Backend{{URL: backend.URL}},
				CircuitBreaker: config.CircuitBreaker{
					FailureThreshold: 1,
					OpenTimeout:      time.Minute,
				},
			},
		},
		Routes: []config.Route{{PathPrefix: "/critical", BackendPool: "critical"}},
	}
	gatewayServer := newEndToEndGateway(t, cfg)

	tripRequest := newIntegrationRequest(t, http.MethodGet, gatewayServer.URL+"/critical/trip")
	trip := sendIntegrationRequest(t, gatewayServer.Client(), tripRequest)
	if trip.status != http.StatusBadGateway {
		t.Fatalf("trip status = %d, want %d", trip.status, http.StatusBadGateway)
	}

	blockedRequest := newIntegrationRequest(t, http.MethodGet, gatewayServer.URL+"/critical/blocked")
	blocked := sendIntegrationRequest(t, gatewayServer.Client(), blockedRequest)
	if blocked.status != http.StatusServiceUnavailable {
		t.Errorf("blocked status = %d, want %d", blocked.status, http.StatusServiceUnavailable)
	}
	if !strings.Contains(blocked.body, "backend circuit breaker is open") {
		t.Errorf("blocked body = %q", blocked.body)
	}
	if got := upstreamCalls.Load(); got != 1 {
		t.Errorf("upstream calls = %d, want 1", got)
	}

	readinessRequest := newIntegrationRequest(t, http.MethodGet, gatewayServer.URL+"/readyz")
	readiness := sendIntegrationRequest(t, gatewayServer.Client(), readinessRequest)
	if readiness.status != http.StatusServiceUnavailable {
		t.Errorf("readiness status = %d, want %d", readiness.status, http.StatusServiceUnavailable)
	}
	if got, want := strings.TrimSpace(readiness.body), `{"status":"not_ready","unavailable_pools":["critical"]}`; got != want {
		t.Errorf("readiness body = %q, want %q", got, want)
	}

	metricsRequest := newIntegrationRequest(t, http.MethodGet, gatewayServer.URL+"/metrics")
	metricsResponse := sendIntegrationRequest(t, gatewayServer.Client(), metricsRequest)
	wantMetric := `gatex_circuit_breaker_state{pool="critical",state="open"} 1`
	if !strings.Contains(metricsResponse.body, wantMetric) {
		t.Errorf("metrics response does not contain %q", wantMetric)
	}
}

type observedUpstreamRequest struct {
	name           string
	path           string
	rawQuery       string
	host           string
	requestID      string
	apiKey         string
	forwardedHost  string
	forwardedProto string
}

type integrationResponse struct {
	status int
	header http.Header
	body   string
}

func newMockBackend(t *testing.T, name string, status int, observed chan<- observedUpstreamRequest) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		observed <- observedUpstreamRequest{
			name:           name,
			path:           request.URL.Path,
			rawQuery:       request.URL.RawQuery,
			host:           request.Host,
			requestID:      request.Header.Get(proxy.RequestIDHeader),
			apiKey:         request.Header.Get(middleware.APIKeyHeader),
			forwardedHost:  request.Header.Get("X-Forwarded-Host"),
			forwardedProto: request.Header.Get("X-Forwarded-Proto"),
		}
		writer.Header().Set("X-Upstream", name)
		writer.Header().Set(proxy.CacheStatusHeader, "UPSTREAM")
		writer.WriteHeader(status)
		_, _ = writer.Write([]byte(name))
	}))
	t.Cleanup(server.Close)
	return server
}

func newEndToEndGateway(t *testing.T, cfg config.Config) *httptest.Server {
	t.Helper()
	gateway, err := proxy.NewGateway(cfg)
	if err != nil {
		t.Fatalf("NewGateway() error = %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := httptest.NewServer(newGatewayHandler(cfg, logger, gateway))
	t.Cleanup(gateway.CloseIdleConnections)
	t.Cleanup(server.Close)
	return server
}

func newIntegrationRequest(t *testing.T, method, requestURL string) *http.Request {
	t.Helper()
	request, err := http.NewRequest(method, requestURL, nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	return request
}

func sendIntegrationRequest(t *testing.T, client *http.Client, request *http.Request) integrationResponse {
	t.Helper()
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("Do(%s %s) error = %v", request.Method, request.URL, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read %s %s response: %v", request.Method, request.URL, err)
	}
	return integrationResponse{status: response.StatusCode, header: response.Header.Clone(), body: string(body)}
}

func receiveUpstreamRequest(t *testing.T, observed <-chan observedUpstreamRequest) observedUpstreamRequest {
	t.Helper()
	select {
	case request := <-observed:
		return request
	case <-time.After(time.Second):
		t.Fatal("mock backend did not receive request")
		return observedUpstreamRequest{}
	}
}

func assertGatewayResponseHeaders(t *testing.T, header http.Header, upstream string) {
	t.Helper()
	if got := header.Get("X-Gateway"); got != "gatex" {
		t.Errorf("X-Gateway = %q, want %q", got, "gatex")
	}
	if got := header.Get("X-Upstream"); got != upstream {
		t.Errorf("X-Upstream = %q, want %q", got, upstream)
	}
	if got := header.Get(proxy.CacheStatusHeader); got != "" {
		t.Errorf("unconfigured cache status = %q, want empty", got)
	}
}

func assertIntegrationResponse(t *testing.T, response integrationResponse, status int, body, requestID, cacheStatus string) {
	t.Helper()
	if response.status != status {
		t.Errorf("status = %d, want %d", response.status, status)
	}
	if response.body != body {
		t.Errorf("body = %q, want %q", response.body, body)
	}
	if got := response.header.Get(proxy.RequestIDHeader); got != requestID {
		t.Errorf("request ID = %q, want %q", got, requestID)
	}
	if got := response.header.Get(proxy.CacheStatusHeader); got != cacheStatus {
		t.Errorf("cache status = %q, want %q", got, cacheStatus)
	}
}
