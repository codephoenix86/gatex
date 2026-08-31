package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/codephoenix86/gatex/internal/config"
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
}
