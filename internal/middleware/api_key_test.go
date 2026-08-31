package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAPIKeyAuthAllowsConfiguredKeys(t *testing.T) {
	t.Parallel()

	for _, apiKey := range []string{"current-key", "next-key"} {
		t.Run(apiKey, func(t *testing.T) {
			var receivedAPIKey string
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedAPIKey = r.Header.Get(APIKeyHeader)
				w.WriteHeader(http.StatusNoContent)
			})
			request := httptest.NewRequest(http.MethodGet, "/protected", nil)
			request.Header.Set(APIKeyHeader, apiKey)
			response := httptest.NewRecorder()

			APIKeyAuth("current-key", "next-key")(next).ServeHTTP(response, request)

			if response.Code != http.StatusNoContent {
				t.Errorf("status = %d, want %d", response.Code, http.StatusNoContent)
			}
			if receivedAPIKey != "" {
				t.Errorf("next handler received API key %q", receivedAPIKey)
			}
			if got := request.Header.Get(APIKeyHeader); got != apiKey {
				t.Errorf("original request API key = %q, want %q", got, apiKey)
			}
		})
	}
}

func TestAPIKeyAuthRejectsMissingOrInvalidCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		apiKey string
	}{
		{name: "missing key"},
		{name: "invalid key", apiKey: "wrong-key"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			called := false
			next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				called = true
			})
			request := httptest.NewRequest(http.MethodGet, "/protected", nil)
			if test.apiKey != "" {
				request.Header.Set(APIKeyHeader, test.apiKey)
			}
			response := httptest.NewRecorder()

			APIKeyAuth("valid-key")(next).ServeHTTP(response, request)

			if called {
				t.Error("next handler was called")
			}
			if response.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want %d", response.Code, http.StatusUnauthorized)
			}
			if got := response.Header().Get("WWW-Authenticate"); got != apiKeyChallenge {
				t.Errorf("WWW-Authenticate = %q, want %q", got, apiKeyChallenge)
			}
			if !strings.Contains(response.Body.String(), "unauthorized") {
				t.Errorf("body = %q, want generic unauthorized error", response.Body.String())
			}
		})
	}
}

func TestAPIKeyAuthWithNoConfiguredKeysRejectsEveryRequest(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	request.Header.Set(APIKeyHeader, "some-key")
	response := httptest.NewRecorder()
	APIKeyAuth()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("next handler was called")
	})).ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
}
