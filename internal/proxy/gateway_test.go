package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codephoenix86/gatex/internal/breaker"
	"github.com/codephoenix86/gatex/internal/config"
)

func TestGatewayCreatesClosedCircuitBreakerPerPool(t *testing.T) {
	t.Parallel()

	cfg := config.Config{
		ListenAddress: ":8080",
		BackendPools: map[string]config.Pool{
			"users": {
				Strategy: config.RoundRobin,
				Backends: []config.Backend{{URL: "http://users.internal"}},
			},
			"orders": {
				Strategy: config.RoundRobin,
				Backends: []config.Backend{{URL: "http://orders.internal"}},
			},
		},
		Routes: []config.Route{
			{PathPrefix: "/users", BackendPool: "users"},
			{PathPrefix: "/orders", BackendPool: "orders"},
		},
	}
	gateway, err := NewGatewayWithTransport(cfg, roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return upstreamResponse(request, http.StatusOK), nil
	}))
	if err != nil {
		t.Fatalf("NewGatewayWithTransport() error = %v", err)
	}

	usersBreaker := gateway.pools["users"].circuitBreaker
	ordersBreaker := gateway.pools["orders"].circuitBreaker
	if usersBreaker == nil || ordersBreaker == nil {
		t.Fatal("gateway pool has nil circuit breaker")
	}
	if usersBreaker == ordersBreaker {
		t.Fatal("backend pools share one circuit breaker")
	}
	if got := usersBreaker.State(); got != breaker.StateClosed {
		t.Errorf("users breaker state = %s, want %s", got, breaker.StateClosed)
	}
	if got := ordersBreaker.State(); got != breaker.StateClosed {
		t.Errorf("orders breaker state = %s, want %s", got, breaker.StateClosed)
	}

	if err := usersBreaker.TransitionTo(breaker.StateOpen); err != nil {
		t.Fatalf("open users breaker: %v", err)
	}
	if got := ordersBreaker.State(); got != breaker.StateClosed {
		t.Errorf("orders breaker state after users transition = %s, want %s", got, breaker.StateClosed)
	}
}

func TestGatewayTripsCircuitBreakerAfterConsecutiveUpstreamFailures(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		call := calls.Add(1)
		if call == 3 {
			return upstreamResponse(request, http.StatusNoContent), nil
		}
		return upstreamResponse(request, http.StatusBadGateway), nil
	})
	gateway, err := NewGatewayWithTransport(config.Config{
		ListenAddress: ":8080",
		BackendPools: map[string]config.Pool{
			"backend": {
				Strategy:       config.RoundRobin,
				Backends:       []config.Backend{{URL: "http://backend.internal"}},
				CircuitBreaker: config.CircuitBreaker{FailureThreshold: 3},
			},
		},
		Routes: []config.Route{{PathPrefix: "/", BackendPool: "backend"}},
	}, transport)
	if err != nil {
		t.Fatalf("NewGatewayWithTransport() error = %v", err)
	}

	wantFailures := []int{1, 2, 0, 1, 2, 3}
	for index, wantFailureCount := range wantFailures {
		response := httptest.NewRecorder()
		gateway.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://gateway.example/work", nil))

		wantStatus := http.StatusBadGateway
		if index == 2 {
			wantStatus = http.StatusNoContent
		}
		if response.Code != wantStatus {
			t.Fatalf("request %d status = %d, want %d", index+1, response.Code, wantStatus)
		}
		if got := gateway.pools["backend"].circuitBreaker.ConsecutiveFailures(); got != wantFailureCount {
			t.Fatalf("failure count after request %d = %d, want %d", index+1, got, wantFailureCount)
		}
	}
	if got := gateway.pools["backend"].circuitBreaker.State(); got != breaker.StateOpen {
		t.Errorf("breaker state = %s, want %s", got, breaker.StateOpen)
	}
}

func TestGatewayRecordsTransportErrorsAsCircuitBreakerFailures(t *testing.T) {
	t.Parallel()

	gateway, err := NewGatewayWithTransport(config.Config{
		ListenAddress: ":8080",
		BackendPools: map[string]config.Pool{
			"backend": {
				Strategy:       config.RoundRobin,
				Backends:       []config.Backend{{URL: "http://backend.internal"}},
				CircuitBreaker: config.CircuitBreaker{FailureThreshold: 1},
			},
		},
		Routes: []config.Route{{PathPrefix: "/", BackendPool: "backend"}},
	}, roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("dial backend: connection refused")
	}))
	if err != nil {
		t.Fatalf("NewGatewayWithTransport() error = %v", err)
	}

	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://gateway.example/work", nil))
	if response.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want %d", response.Code, http.StatusBadGateway)
	}
	if got := gateway.pools["backend"].circuitBreaker.State(); got != breaker.StateOpen {
		t.Errorf("breaker state = %s, want %s", got, breaker.StateOpen)
	}
}

func TestGatewayLimitsHalfOpenTrafficToConfiguredProbeBatch(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	probeStarted := make(chan struct{}, 2)
	releaseProbes := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseProbes) })
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return upstreamResponse(request, http.StatusBadGateway), nil
		}
		probeStarted <- struct{}{}
		<-releaseProbes
		return upstreamResponse(request, http.StatusNoContent), nil
	})
	const openTimeout = 10 * time.Millisecond
	gateway, err := NewGatewayWithTransport(config.Config{
		ListenAddress: ":8080",
		BackendPools: map[string]config.Pool{
			"backend": {
				Strategy: config.RoundRobin,
				Backends: []config.Backend{{URL: "http://backend.internal"}},
				CircuitBreaker: config.CircuitBreaker{
					FailureThreshold:    1,
					OpenTimeout:         openTimeout,
					HalfOpenMaxRequests: 2,
				},
			},
		},
		Routes: []config.Route{{PathPrefix: "/", BackendPool: "backend"}},
	}, transport)
	if err != nil {
		t.Fatalf("NewGatewayWithTransport() error = %v", err)
	}

	tripResponse := serveGatewayRequest(gateway, "/trip", "192.0.2.1:1000")
	if tripResponse.Code != http.StatusBadGateway {
		t.Fatalf("trip status = %d, want %d", tripResponse.Code, http.StatusBadGateway)
	}
	time.Sleep(2 * openTimeout)

	probeResponses := make(chan *httptest.ResponseRecorder, 2)
	for probe := range 2 {
		go func() {
			probeResponses <- serveGatewayRequest(gateway, fmt.Sprintf("/probe/%d", probe), "192.0.2.1:1000")
		}()
	}
	for probe := range 2 {
		select {
		case <-probeStarted:
		case <-time.After(time.Second):
			t.Fatalf("probe %d did not reach upstream", probe+1)
		}
	}

	blockedResponse := serveGatewayRequest(gateway, "/blocked", "192.0.2.1:1000")
	if blockedResponse.Code != http.StatusServiceUnavailable {
		t.Errorf("request beyond probe limit status = %d, want %d", blockedResponse.Code, http.StatusServiceUnavailable)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("upstream calls before probe completion = %d, want 3", got)
	}

	releaseOnce.Do(func() { close(releaseProbes) })
	for probe := range 2 {
		response := <-probeResponses
		if response.Code != http.StatusNoContent {
			t.Errorf("probe %d status = %d, want %d", probe+1, response.Code, http.StatusNoContent)
		}
	}
	if got := gateway.pools["backend"].circuitBreaker.State(); got != breaker.StateClosed {
		t.Errorf("breaker state after successful probes = %s, want %s", got, breaker.StateClosed)
	}
}

func TestGatewayRoutesAndRewritesRequests(t *testing.T) {
	t.Parallel()

	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Host {
		case "users.internal":
			if request.URL.Path != "/base/api/users/42" {
				t.Errorf("users upstream path = %q", request.URL.Path)
			}
			if request.URL.RawQuery != "source=users&expand=teams" {
				t.Errorf("users upstream query = %q", request.URL.RawQuery)
			}
			if request.Header.Get(RequestIDHeader) != "client-request-id" {
				t.Errorf("request ID = %q", request.Header.Get(RequestIDHeader))
			}
			if request.Header.Get("X-Forwarded-Host") != "gateway.example" {
				t.Errorf("forwarded host = %q", request.Header.Get("X-Forwarded-Host"))
			}
			if request.Header.Get("X-Forwarded-Proto") != "http" {
				t.Errorf("forwarded proto = %q", request.Header.Get("X-Forwarded-Proto"))
			}
			return upstreamResponse(request, http.StatusCreated), nil
		case "orders.internal":
			if request.URL.Path != "/api/orders/9" {
				t.Errorf("orders upstream path = %q", request.URL.Path)
			}
			return upstreamResponse(request, http.StatusNoContent), nil
		default:
			t.Errorf("unexpected upstream host %q", request.URL.Host)
			return upstreamResponse(request, http.StatusBadGateway), nil
		}
	})
	gateway, err := NewGatewayWithTransport(config.Config{
		ListenAddress: ":8080",
		BackendPools: map[string]config.Pool{
			"users":  {Strategy: config.RoundRobin, Backends: []config.Backend{{URL: "http://users.internal/base?source=users"}}},
			"orders": {Strategy: config.RoundRobin, Backends: []config.Backend{{URL: "http://orders.internal"}}},
		},
		Routes: []config.Route{
			{PathPrefix: "/api", BackendPool: "orders"},
			{PathPrefix: "/api/users", BackendPool: "users"},
		},
	}, transport)
	if err != nil {
		t.Fatalf("NewGatewayWithTransport() error = %v", err)
	}

	usersRequest := httptest.NewRequest(http.MethodGet, "http://gateway.example/api/users/42?expand=teams", nil)
	usersRequest.Host = "gateway.example"
	usersRequest.Header.Set(RequestIDHeader, "client-request-id")
	usersResponse := httptest.NewRecorder()
	gateway.ServeHTTP(usersResponse, usersRequest)

	if usersResponse.Code != http.StatusCreated {
		t.Fatalf("users status = %d, want %d", usersResponse.Code, http.StatusCreated)
	}
	if usersResponse.Header().Get(RequestIDHeader) != "client-request-id" {
		t.Errorf("response request ID = %q", usersResponse.Header().Get(RequestIDHeader))
	}
	if usersResponse.Header().Get("X-Gateway") != "gatex" {
		t.Errorf("gateway response header = %q", usersResponse.Header().Get("X-Gateway"))
	}

	ordersResponse := httptest.NewRecorder()
	gateway.ServeHTTP(ordersResponse, httptest.NewRequest(http.MethodGet, "http://gateway.example/api/orders/9", nil))
	if ordersResponse.Code != http.StatusNoContent {
		t.Fatalf("orders status = %d, want %d", ordersResponse.Code, http.StatusNoContent)
	}
}

func TestGatewayContextDeadlineAndRequestIDReachTransport(t *testing.T) {
	t.Parallel()

	observed := make(chan *http.Request, 1)
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		observed <- request
		<-request.Context().Done()
		return nil, request.Context().Err()
	})
	gateway, err := NewGatewayWithTransport(config.Config{
		ListenAddress: ":8080",
		Timeouts:      config.Timeouts{Request: 20 * time.Millisecond},
		BackendPools: map[string]config.Pool{
			"backend": {Strategy: config.RoundRobin, Backends: []config.Backend{{URL: "http://backend.example"}}},
		},
		Routes: []config.Route{{PathPrefix: "/", BackendPool: "backend"}},
	}, transport)
	if err != nil {
		t.Fatalf("NewGatewayWithTransport() error = %v", err)
	}

	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://gateway.example/slow", nil))

	request := <-observed
	if RequestID(request.Context()) == "" {
		t.Error("outbound request context has no request ID")
	}
	if request.Header.Get(RequestIDHeader) == "" {
		t.Error("outbound request has no request ID header")
	}
	if _, ok := request.Context().Deadline(); !ok {
		t.Error("outbound request context has no deadline")
	}
	if response.Code != http.StatusGatewayTimeout {
		t.Errorf("status = %d, want %d", response.Code, http.StatusGatewayTimeout)
	}
}

func TestGatewayReturnsNotFoundForUnmatchedPath(t *testing.T) {
	t.Parallel()

	gateway := mustGateway(t, config.Config{
		ListenAddress: ":8080",
		BackendPools: map[string]config.Pool{
			"backend": {Strategy: config.RoundRobin, Backends: []config.Backend{{URL: "http://backend.example"}}},
		},
		Routes: []config.Route{{PathPrefix: "/api", BackendPool: "backend"}},
	})

	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://gateway.example/apiv2", nil))
	if response.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", response.Code, http.StatusNotFound)
	}
	if !strings.Contains(response.Body.String(), "no route configured") {
		t.Errorf("body = %q", response.Body.String())
	}
}

func TestGatewayEnforcesGlobalRateLimitBeforeAcquiringBackend(t *testing.T) {
	t.Parallel()

	var upstreamRequests atomic.Int64
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		upstreamRequests.Add(1)
		return upstreamResponse(request, http.StatusNoContent), nil
	})
	gateway, err := NewGatewayWithTransport(config.Config{
		ListenAddress: ":8080",
		RateLimit: config.RateLimit{
			RequestsPerSecond: 0.001,
			Burst:             1,
		},
		BackendPools: map[string]config.Pool{
			"backend": {Strategy: config.RoundRobin, Backends: []config.Backend{{URL: "http://backend.internal"}}},
		},
		Routes: []config.Route{{PathPrefix: "/", BackendPool: "backend"}},
	}, transport)
	if err != nil {
		t.Fatalf("NewGatewayWithTransport() error = %v", err)
	}

	firstResponse := serveGatewayRequest(gateway, "/first", "192.0.2.10:1000")
	if firstResponse.Code != http.StatusNoContent {
		t.Fatalf("first status = %d, want %d", firstResponse.Code, http.StatusNoContent)
	}

	secondResponse := serveGatewayRequest(gateway, "/second", "192.0.2.10:2000")
	if secondResponse.Code != http.StatusTooManyRequests {
		t.Errorf("second status = %d, want %d", secondResponse.Code, http.StatusTooManyRequests)
	}
	if !strings.Contains(secondResponse.Body.String(), "rate limit exceeded") {
		t.Errorf("second body = %q", secondResponse.Body.String())
	}
	if got := secondResponse.Header().Get("Retry-After"); got != "1000" {
		t.Errorf("Retry-After = %q, want %q", got, "1000")
	}
	if secondResponse.Header().Get(RequestIDHeader) == "" {
		t.Error("rate-limited response has no request ID")
	}
	if got := upstreamRequests.Load(); got != 1 {
		t.Errorf("upstream requests = %d, want 1", got)
	}
	if got := gateway.pools["backend"].balancer.Backends()[0].ActiveConnections(); got != 0 {
		t.Errorf("active backend connections = %d, want 0", got)
	}
}

func TestGatewayRateLimitsClientsIndependently(t *testing.T) {
	t.Parallel()

	gateway := mustSuccessfulGateway(t, rateLimitedConfig(config.RateLimit{RequestsPerSecond: 0.0001, Burst: 1}))

	if response := serveGatewayRequest(gateway, "/first", "192.0.2.10:1000"); response.Code != http.StatusNoContent {
		t.Fatalf("first client status = %d, want %d", response.Code, http.StatusNoContent)
	}
	if response := serveGatewayRequest(gateway, "/second", "192.0.2.11:1000"); response.Code != http.StatusNoContent {
		t.Errorf("second client status = %d, want %d", response.Code, http.StatusNoContent)
	}
	if response := serveGatewayRequest(gateway, "/third", "192.0.2.10:2000"); response.Code != http.StatusTooManyRequests {
		t.Errorf("first client repeat status = %d, want %d", response.Code, http.StatusTooManyRequests)
	}
}

func TestGatewayEnforcesPerClientBurstsUnderConcurrentLoad(t *testing.T) {
	t.Parallel()

	const (
		clients           = 32
		burst             = 10
		requestsPerClient = 40
	)
	var upstreamRequests atomic.Int64
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		upstreamRequests.Add(1)
		return upstreamResponse(request, http.StatusNoContent), nil
	})
	gateway, err := NewGatewayWithTransport(rateLimitedConfig(config.RateLimit{
		RequestsPerSecond: 0.000001,
		Burst:             burst,
	}), transport)
	if err != nil {
		t.Fatalf("NewGatewayWithTransport() error = %v", err)
	}

	start := make(chan struct{})
	var group sync.WaitGroup
	var allowed atomic.Int64
	var rejected atomic.Int64
	var invalidResponses atomic.Int64
	group.Add(clients * requestsPerClient)
	for client := range clients {
		remoteIP := fmt.Sprintf("192.0.2.%d", client+1)
		for requestNumber := range requestsPerClient {
			remoteAddr := fmt.Sprintf("%s:%d", remoteIP, requestNumber+1000)
			go func() {
				defer group.Done()
				<-start
				response := serveGatewayRequest(gateway, "/work", remoteAddr)
				switch response.Code {
				case http.StatusNoContent:
					allowed.Add(1)
				case http.StatusTooManyRequests:
					rejected.Add(1)
					if response.Header().Get(RequestIDHeader) == "" || response.Header().Get("Retry-After") == "" {
						invalidResponses.Add(1)
					}
				default:
					invalidResponses.Add(1)
				}
			}()
		}
	}
	close(start)
	group.Wait()

	wantAllowed := int64(clients * burst)
	wantRejected := int64(clients * (requestsPerClient - burst))
	if got := allowed.Load(); got != wantAllowed {
		t.Errorf("allowed requests = %d, want %d", got, wantAllowed)
	}
	if got := rejected.Load(); got != wantRejected {
		t.Errorf("rejected requests = %d, want %d", got, wantRejected)
	}
	if got := invalidResponses.Load(); got != 0 {
		t.Errorf("invalid responses = %d, want 0", got)
	}
	if got := upstreamRequests.Load(); got != wantAllowed {
		t.Errorf("upstream requests = %d, want %d", got, wantAllowed)
	}
	if got := gateway.pools["backend"].balancer.Backends()[0].ActiveConnections(); got != 0 {
		t.Errorf("active backend connections = %d, want 0", got)
	}
}

func TestGatewayAppliesRouteRateLimitOverride(t *testing.T) {
	t.Parallel()

	strictLimit := config.RateLimit{RequestsPerSecond: 0.0001, Burst: 1}
	gateway := mustSuccessfulGateway(t, config.Config{
		ListenAddress: ":8080",
		RateLimit:     config.RateLimit{RequestsPerSecond: 0.0001, Burst: 2},
		BackendPools: map[string]config.Pool{
			"backend": {Strategy: config.RoundRobin, Backends: []config.Backend{{URL: "http://backend.internal"}}},
		},
		Routes: []config.Route{
			{PathPrefix: "/strict", BackendPool: "backend", RateLimit: &strictLimit},
			{PathPrefix: "/default", BackendPool: "backend"},
		},
	})
	client := "192.0.2.10:1000"

	wantStatuses := []struct {
		path   string
		status int
	}{
		{path: "/strict/one", status: http.StatusNoContent},
		{path: "/strict/two", status: http.StatusTooManyRequests},
		{path: "/default/one", status: http.StatusNoContent},
		{path: "/default/two", status: http.StatusNoContent},
		{path: "/default/three", status: http.StatusTooManyRequests},
	}
	for _, want := range wantStatuses {
		if response := serveGatewayRequest(gateway, want.path, client); response.Code != want.status {
			t.Errorf("%s status = %d, want %d", want.path, response.Code, want.status)
		}
	}
}

func TestGatewayRouteCanDisableGlobalRateLimit(t *testing.T) {
	t.Parallel()

	disabledLimit := config.RateLimit{}
	gateway := mustSuccessfulGateway(t, config.Config{
		ListenAddress: ":8080",
		RateLimit:     config.RateLimit{RequestsPerSecond: 0.0001, Burst: 1},
		BackendPools: map[string]config.Pool{
			"backend": {Strategy: config.RoundRobin, Backends: []config.Backend{{URL: "http://backend.internal"}}},
		},
		Routes: []config.Route{
			{PathPrefix: "/open", BackendPool: "backend", RateLimit: &disabledLimit},
		},
	})

	for requestNumber := range 3 {
		response := serveGatewayRequest(gateway, "/open", "192.0.2.10:1000")
		if response.Code != http.StatusNoContent {
			t.Errorf("request %d status = %d, want %d", requestNumber+1, response.Code, http.StatusNoContent)
		}
	}
}

func TestClientKeyFromRemoteAddr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		remoteAddr string
		want       string
	}{
		{remoteAddr: "192.0.2.10:1234", want: "192.0.2.10"},
		{remoteAddr: "192.0.2.10", want: "192.0.2.10"},
		{remoteAddr: "[2001:0db8::1]:1234", want: "2001:db8::1"},
		{remoteAddr: "[::ffff:192.0.2.10]:1234", want: "192.0.2.10"},
		{remoteAddr: " backend-client ", want: "backend-client"},
		{remoteAddr: "", want: ""},
	}

	for _, test := range tests {
		t.Run(test.remoteAddr, func(t *testing.T) {
			t.Parallel()
			if got := clientKeyFromRemoteAddr(test.remoteAddr); got != test.want {
				t.Errorf("clientKeyFromRemoteAddr(%q) = %q, want %q", test.remoteAddr, got, test.want)
			}
		})
	}
}

func TestGatewaySkipsUnhealthyBackends(t *testing.T) {
	t.Parallel()

	requestedHost := make(chan string, 1)
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		requestedHost <- request.URL.Host
		return upstreamResponse(request, http.StatusNoContent), nil
	})
	gateway, err := NewGatewayWithTransport(config.Config{
		ListenAddress: ":8080",
		BackendPools: map[string]config.Pool{
			"backend": {
				Strategy: config.RoundRobin,
				Backends: []config.Backend{
					{URL: "http://backend-1.internal"},
					{URL: "http://backend-2.internal"},
				},
			},
		},
		Routes: []config.Route{{PathPrefix: "/", BackendPool: "backend"}},
	}, transport)
	if err != nil {
		t.Fatalf("NewGatewayWithTransport() error = %v", err)
	}
	gateway.pools["backend"].balancer.Backends()[0].SetHealthy(false)

	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://gateway.example/users", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
	}
	if got := <-requestedHost; got != "backend-2.internal" {
		t.Errorf("upstream host = %q, want %q", got, "backend-2.internal")
	}
}

func TestGatewayReturnsServiceUnavailableWhenNoBackendIsHealthy(t *testing.T) {
	t.Parallel()

	gateway := mustGateway(t, config.Config{
		ListenAddress: ":8080",
		BackendPools: map[string]config.Pool{
			"backend": {
				Strategy: config.RoundRobin,
				Backends: []config.Backend{{URL: "http://backend.internal"}},
			},
		},
		Routes: []config.Route{{PathPrefix: "/", BackendPool: "backend"}},
	})
	gateway.pools["backend"].balancer.Backends()[0].SetHealthy(false)

	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://gateway.example/users", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	if !strings.Contains(response.Body.String(), "no healthy backends available") {
		t.Errorf("body = %q", response.Body.String())
	}
	if response.Header().Get(RequestIDHeader) == "" {
		t.Error("service-unavailable response has no request ID")
	}
}

func TestGatewayLeastConnectionsReleasesBackendAfterProxying(t *testing.T) {
	t.Parallel()

	firstRequestStarted := make(chan struct{})
	releaseFirstRequest := make(chan struct{})
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Host == "backend-1.internal" {
			close(firstRequestStarted)
			<-releaseFirstRequest
		}
		return upstreamResponse(request, http.StatusNoContent), nil
	})
	gateway, err := NewGatewayWithTransport(config.Config{
		ListenAddress: ":8080",
		BackendPools: map[string]config.Pool{
			"backend": {
				Strategy: config.LeastConnections,
				Backends: []config.Backend{
					{URL: "http://backend-1.internal"},
					{URL: "http://backend-2.internal"},
				},
			},
		},
		Routes: []config.Route{{PathPrefix: "/", BackendPool: "backend"}},
	}, transport)
	if err != nil {
		t.Fatalf("NewGatewayWithTransport() error = %v", err)
	}

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		gateway.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "http://gateway.example/first", nil))
	}()
	<-firstRequestStarted

	secondResponse := httptest.NewRecorder()
	gateway.ServeHTTP(secondResponse, httptest.NewRequest(http.MethodGet, "http://gateway.example/second", nil))
	if secondResponse.Code != http.StatusNoContent {
		t.Errorf("second request status = %d, want %d", secondResponse.Code, http.StatusNoContent)
	}
	backends := gateway.pools["backend"].balancer.Backends()
	if got := backends[0].ActiveConnections(); got != 1 {
		t.Errorf("first backend active connections = %d, want 1", got)
	}
	if got := backends[1].ActiveConnections(); got != 0 {
		t.Errorf("second backend active connections = %d, want 0", got)
	}

	close(releaseFirstRequest)
	<-firstDone
	if got := backends[0].ActiveConnections(); got != 0 {
		t.Errorf("first backend active connections after completion = %d, want 0", got)
	}
}

func TestGatewayStartsHealthChecks(t *testing.T) {
	t.Parallel()

	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/healthz" {
			t.Errorf("unexpected request path %q", request.URL.Path)
		}
		return upstreamResponse(request, http.StatusServiceUnavailable), nil
	})
	gateway, err := NewGatewayWithTransport(config.Config{
		ListenAddress: ":8080",
		BackendPools: map[string]config.Pool{
			"backend": {
				Strategy: config.RoundRobin,
				Backends: []config.Backend{{URL: "http://backend.internal"}},
				HealthCheck: config.HealthCheck{
					Interval: 10 * time.Millisecond,
					Timeout:  time.Second,
				},
			},
		},
		Routes: []config.Route{{PathPrefix: "/", BackendPool: "backend"}},
	}, transport)
	if err != nil {
		t.Fatalf("NewGatewayWithTransport() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	gateway.StartHealthChecks(ctx)
	eventually(t, time.Second, func() bool {
		return !gateway.pools["backend"].balancer.Backends()[0].Healthy()
	})
	cancel()
	gateway.WaitForHealthChecks()
}

func TestGatewayHandlesConcurrentRequestsWhileHealthChecksRun(t *testing.T) {
	t.Parallel()

	var proxiedRequests atomic.Int64
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/healthz" {
			return upstreamResponse(request, http.StatusNoContent), nil
		}
		proxiedRequests.Add(1)
		return upstreamResponse(request, http.StatusOK), nil
	})
	gateway, err := NewGatewayWithTransport(config.Config{
		ListenAddress: ":8080",
		BackendPools: map[string]config.Pool{
			"backend": {
				Strategy: config.LeastConnections,
				Backends: []config.Backend{
					{URL: "http://backend-1.internal"},
					{URL: "http://backend-2.internal"},
					{URL: "http://backend-3.internal"},
				},
				HealthCheck: config.HealthCheck{
					Interval: time.Millisecond,
					Timeout:  100 * time.Millisecond,
				},
			},
		},
		Routes: []config.Route{{PathPrefix: "/", BackendPool: "backend"}},
	}, transport)
	if err != nil {
		t.Fatalf("NewGatewayWithTransport() error = %v", err)
	}

	healthContext, stopHealthChecks := context.WithCancel(context.Background())
	gateway.StartHealthChecks(healthContext)

	const workers = 24
	const requestsPerWorker = 100
	start := make(chan struct{})
	var group sync.WaitGroup
	var failedRequests atomic.Int64
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			<-start
			for range requestsPerWorker {
				response := httptest.NewRecorder()
				request := httptest.NewRequest(http.MethodGet, "http://gateway.example/work", nil)
				gateway.ServeHTTP(response, request)
				if response.Code != http.StatusOK {
					failedRequests.Add(1)
				}
			}
		}()
	}
	close(start)
	group.Wait()
	stopHealthChecks()
	gateway.WaitForHealthChecks()

	if got := failedRequests.Load(); got != 0 {
		t.Errorf("failed requests = %d, want 0", got)
	}
	if got, want := proxiedRequests.Load(), int64(workers*requestsPerWorker); got != want {
		t.Errorf("proxied requests = %d, want %d", got, want)
	}
	for index, backend := range gateway.pools["backend"].balancer.Backends() {
		if got := backend.ActiveConnections(); got != 0 {
			t.Errorf("backend %d active connections = %d, want 0", index, got)
		}
	}
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func mustGateway(t *testing.T, cfg config.Config) *Gateway {
	t.Helper()
	gateway, err := NewGateway(cfg)
	if err != nil {
		t.Fatalf("NewGateway() error = %v", err)
	}
	return gateway
}

func mustSuccessfulGateway(t *testing.T, cfg config.Config) *Gateway {
	t.Helper()
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return upstreamResponse(request, http.StatusNoContent), nil
	})
	gateway, err := NewGatewayWithTransport(cfg, transport)
	if err != nil {
		t.Fatalf("NewGatewayWithTransport() error = %v", err)
	}
	return gateway
}

func rateLimitedConfig(limit config.RateLimit) config.Config {
	return config.Config{
		ListenAddress: ":8080",
		RateLimit:     limit,
		BackendPools: map[string]config.Pool{
			"backend": {Strategy: config.RoundRobin, Backends: []config.Backend{{URL: "http://backend.internal"}}},
		},
		Routes: []config.Route{{PathPrefix: "/", BackendPool: "backend"}},
	}
}

func serveGatewayRequest(gateway *Gateway, path, remoteAddr string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, "http://gateway.example"+path, nil)
	request.RemoteAddr = remoteAddr
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, request)
	return response
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (function roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

var _ http.RoundTripper = roundTripperFunc(nil)

func upstreamResponse(request *http.Request, status int) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("")),
		Request:    request,
	}
}
