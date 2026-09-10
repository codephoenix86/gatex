package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/codephoenix86/gatex/internal/config"
	"github.com/codephoenix86/gatex/internal/proxy"
)

func TestGatewayHandlerLogsCORSPreflightBeforeRouteHandling(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	handler := newGatewayHandler(config.Config{
		CORS: config.CORS{
			AllowedOrigins: []string{"https://app.example.com"},
			AllowedMethods: []string{http.MethodGet},
			AllowedHeaders: []string{"X-API-Key"},
		},
	}, logger, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("route handler was called for a CORS preflight")
	}))
	request := httptest.NewRequest(http.MethodOptions, "http://gateway.example/protected", nil)
	request.Header.Set("Origin", "https://app.example.com")
	request.Header.Set("Access-Control-Request-Method", http.MethodGet)
	request.Header.Set("Access-Control-Request-Headers", "X-API-Key")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", response.Code, http.StatusNoContent)
	}
	var entry map[string]any
	if err := json.Unmarshal(logs.Bytes(), &entry); err != nil {
		t.Fatalf("decode access log %q: %v", logs.String(), err)
	}
	if got := entry["status"]; got != float64(http.StatusNoContent) {
		t.Errorf("logged status = %v, want %d", got, http.StatusNoContent)
	}
	requestID, ok := entry["request_id"].(string)
	if !ok || requestID == "" {
		t.Errorf("logged request ID = %v, want a non-empty string", entry["request_id"])
	}
	if got := response.Header().Get("X-Request-ID"); got != requestID {
		t.Errorf("response request ID = %q, want logged ID %q", got, requestID)
	}
}

func TestGatewayHandlerExposesPrometheusRequestMetrics(t *testing.T) {
	t.Parallel()

	handler := newGatewayHandler(config.Config{}, slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)), http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusAccepted)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/work", nil))

	scrape := httptest.NewRecorder()
	handler.ServeHTTP(scrape, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if scrape.Code != http.StatusOK {
		t.Fatalf("scrape status = %d, want %d", scrape.Code, http.StatusOK)
	}
	want := `gatex_http_requests_total{method="GET",route="unmatched",status="202"} 1`
	if !strings.Contains(scrape.Body.String(), want) {
		t.Errorf("scrape does not contain %q", want)
	}
}

func TestGatewayHandlerExposesCircuitBreakerStateMetrics(t *testing.T) {
	t.Parallel()

	gateway, err := proxy.NewGateway(config.Config{
		ListenAddress: ":8080",
		BackendPools: map[string]config.Pool{
			"users": {
				Strategy: config.RoundRobin,
				Backends: []config.Backend{{URL: "http://users.internal"}},
			},
		},
		Routes: []config.Route{{PathPrefix: "/users", BackendPool: "users"}},
	})
	if err != nil {
		t.Fatalf("NewGateway() error = %v", err)
	}
	handler := newGatewayHandler(config.Config{}, slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)), gateway)
	scrape := httptest.NewRecorder()

	handler.ServeHTTP(scrape, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	want := `gatex_circuit_breaker_state{pool="users",state="closed"} 1`
	if !strings.Contains(scrape.Body.String(), want) {
		t.Errorf("scrape does not contain %q", want)
	}
}
