package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codephoenix86/gatex/internal/config"
)

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
