package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestHealthHandlerReportsLiveness(t *testing.T) {
	t.Parallel()

	response := httptest.NewRecorder()
	HealthHandler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if response.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want %q", got, "no-store")
	}
	assertHealthResponse(t, response, healthResponse{Status: "ok"})
}

func TestReadinessHandlerReportsUnavailablePools(t *testing.T) {
	t.Parallel()

	response := httptest.NewRecorder()
	ReadinessHandler(staticReadinessSource{"users", "orders"}).ServeHTTP(
		response,
		httptest.NewRequest(http.MethodGet, "/readyz", nil),
	)

	if response.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	assertHealthResponse(t, response, healthResponse{
		Status:           "not_ready",
		UnavailablePools: []string{"orders", "users"},
	})
}

func TestReadinessHandlerReportsReady(t *testing.T) {
	t.Parallel()

	response := httptest.NewRecorder()
	ReadinessHandler(staticReadinessSource{}).ServeHTTP(
		response,
		httptest.NewRequest(http.MethodGet, "/readyz", nil),
	)

	if response.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", response.Code, http.StatusOK)
	}
	assertHealthResponse(t, response, healthResponse{Status: "ready"})
}

func TestReadinessHandlerWithoutSourceIsNotReady(t *testing.T) {
	t.Parallel()

	response := httptest.NewRecorder()
	ReadinessHandler(nil).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if response.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	assertHealthResponse(t, response, healthResponse{Status: "not_ready"})
}

func TestProbeHandlersSupportHeadWithoutResponseBody(t *testing.T) {
	t.Parallel()

	for name, handler := range map[string]http.Handler{
		"health":    HealthHandler(),
		"readiness": ReadinessHandler(staticReadinessSource{}),
	} {
		t.Run(name, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodHead, "/", nil))
			if response.Code != http.StatusOK {
				t.Errorf("status = %d, want %d", response.Code, http.StatusOK)
			}
			if response.Body.Len() != 0 {
				t.Errorf("body = %q, want empty", response.Body.String())
			}
		})
	}
}

func TestProbeHandlersRejectMutationMethods(t *testing.T) {
	t.Parallel()

	for name, handler := range map[string]http.Handler{
		"health":    HealthHandler(),
		"readiness": ReadinessHandler(staticReadinessSource{}),
	} {
		t.Run(name, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/", nil))
			if response.Code != http.StatusMethodNotAllowed {
				t.Errorf("status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
			}
			if got := response.Header().Get("Allow"); got != "GET, HEAD" {
				t.Errorf("Allow = %q, want %q", got, "GET, HEAD")
			}
		})
	}
}

type staticReadinessSource []string

func (source staticReadinessSource) UnavailablePools() []string {
	return source
}

func assertHealthResponse(t *testing.T, recorder *httptest.ResponseRecorder, want healthResponse) {
	t.Helper()
	var got healthResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response %q: %v", recorder.Body.String(), err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("response = %+v, want %+v", got, want)
	}
}
