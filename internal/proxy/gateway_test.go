package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
