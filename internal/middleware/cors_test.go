package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCORSAddsHeadersForAllowedOrigin(t *testing.T) {
	t.Parallel()

	called := false
	handler := CORS(CORSOptions{
		AllowedOrigins:   []string{"https://app.example.com"},
		AllowedMethods:   []string{http.MethodGet},
		ExposedHeaders:   []string{"X-Request-ID"},
		AllowCredentials: true,
	})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusAccepted)
	}))
	request := httptest.NewRequest(http.MethodGet, "/resource", nil)
	request.Header.Set("Origin", "https://app.example.com")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if !called {
		t.Error("next handler was not called")
	}
	if response.Code != http.StatusAccepted {
		t.Errorf("status = %d, want %d", response.Code, http.StatusAccepted)
	}
	assertHeader(t, response.Header(), allowOriginHeader, "https://app.example.com")
	assertHeader(t, response.Header(), allowCredentialsHeader, "true")
	assertHeader(t, response.Header(), exposeHeadersHeader, "X-Request-ID")
	assertVaryContains(t, response.Header(), "Origin")
}

func TestCORSHandlesAllowedPreflightWithoutCallingNext(t *testing.T) {
	t.Parallel()

	handler := CORS(CORSOptions{
		AllowedOrigins: []string{"https://app.example.com"},
		AllowedMethods: []string{http.MethodGet, http.MethodPost},
		AllowedHeaders: []string{"Content-Type", "X-API-Key"},
		MaxAge:         10 * time.Minute,
	})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("next handler was called for preflight")
	}))
	request := httptest.NewRequest(http.MethodOptions, "/resource", nil)
	request.Header.Set("Origin", "https://app.example.com")
	request.Header.Set(requestMethodHeader, http.MethodPost)
	request.Header.Set(requestHeadersHeader, "x-api-key, content-type")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", response.Code, http.StatusNoContent)
	}
	assertHeader(t, response.Header(), allowOriginHeader, "https://app.example.com")
	assertHeader(t, response.Header(), allowMethodsHeader, "GET, POST")
	assertHeader(t, response.Header(), allowHeadersHeader, "Content-Type, X-API-Key")
	assertHeader(t, response.Header(), maxAgeHeader, "600")
	assertVaryContains(t, response.Header(), requestMethodHeader)
	assertVaryContains(t, response.Header(), requestHeadersHeader)
}

func TestCORSRejectsInvalidPreflight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		origin           string
		requestedMethod  string
		requestedHeaders string
	}{
		{name: "origin", origin: "https://attacker.example", requestedMethod: http.MethodPost},
		{name: "method", origin: "https://app.example.com", requestedMethod: http.MethodDelete},
		{name: "header", origin: "https://app.example.com", requestedMethod: http.MethodPost, requestedHeaders: "X-Forbidden"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := CORS(CORSOptions{
				AllowedOrigins: []string{"https://app.example.com"},
				AllowedMethods: []string{http.MethodPost},
				AllowedHeaders: []string{"X-API-Key"},
			})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Error("next handler was called for denied preflight")
			}))
			request := httptest.NewRequest(http.MethodOptions, "/resource", nil)
			request.Header.Set("Origin", test.origin)
			request.Header.Set(requestMethodHeader, test.requestedMethod)
			request.Header.Set(requestHeadersHeader, test.requestedHeaders)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != http.StatusForbidden {
				t.Errorf("status = %d, want %d", response.Code, http.StatusForbidden)
			}
			if got := response.Header().Get(allowOriginHeader); got != "" {
				t.Errorf("%s = %q, want empty", allowOriginHeader, got)
			}
		})
	}
}

func TestCORSLeavesDisallowedSimpleRequestToNextHandler(t *testing.T) {
	t.Parallel()

	handler := CORS(CORSOptions{
		AllowedOrigins: []string{"https://app.example.com"},
		AllowedMethods: []string{http.MethodGet},
	})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	request := httptest.NewRequest(http.MethodGet, "/resource", nil)
	request.Header.Set("Origin", "https://attacker.example")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusTeapot {
		t.Errorf("status = %d, want %d", response.Code, http.StatusTeapot)
	}
	if got := response.Header().Get(allowOriginHeader); got != "" {
		t.Errorf("%s = %q, want empty", allowOriginHeader, got)
	}
}

func TestCORSWithNoOriginsIsDisabled(t *testing.T) {
	t.Parallel()

	called := false
	handler := CORS(CORSOptions{})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	request := httptest.NewRequest(http.MethodOptions, "/resource", nil)
	request.Header.Set("Origin", "https://app.example.com")
	request.Header.Set(requestMethodHeader, http.MethodPost)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if !called {
		t.Error("disabled CORS middleware did not call next handler")
	}
}

func assertHeader(t *testing.T, header http.Header, name, want string) {
	t.Helper()
	if got := header.Get(name); got != want {
		t.Errorf("%s = %q, want %q", name, got, want)
	}
}

func assertVaryContains(t *testing.T, header http.Header, want string) {
	t.Helper()
	for _, value := range header.Values("Vary") {
		for _, item := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(item), want) {
				return
			}
		}
	}
	t.Errorf("Vary = %v, want value %q", header.Values("Vary"), want)
}
