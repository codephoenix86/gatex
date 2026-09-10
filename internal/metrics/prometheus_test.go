package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/codephoenix86/gatex/internal/requestmeta"
)

func TestRecorderExposesRequestCountLatencyAndErrors(t *testing.T) {
	t.Parallel()

	recorder := New()
	handler := recorder.Instrument(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestmeta.SetRoute(request.Context(), "/api")
		http.Error(writer, "unavailable", http.StatusServiceUnavailable)
	}))
	request := requestmeta.Ensure(httptest.NewRequest(http.MethodPost, "http://gateway.example/api/work", nil))
	handler.ServeHTTP(httptest.NewRecorder(), request)

	scrape := httptest.NewRecorder()
	recorder.Handler().ServeHTTP(scrape, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if scrape.Code != http.StatusOK {
		t.Fatalf("scrape status = %d, want %d", scrape.Code, http.StatusOK)
	}
	for _, metric := range []string{
		`gatex_http_requests_total{method="POST",route="/api",status="503"} 1`,
		`gatex_http_request_errors_total{method="POST",route="/api",status="503"} 1`,
		`gatex_http_request_duration_seconds_count{method="POST",route="/api",status="503"} 1`,
	} {
		if !strings.Contains(scrape.Body.String(), metric) {
			t.Errorf("scrape does not contain %q", metric)
		}
	}
}

func TestRecorderBoundsUnmatchedRouteAndMethodLabels(t *testing.T) {
	t.Parallel()

	recorder := New()
	handler := recorder.Instrument(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("CUSTOM", "/unknown/123", nil))

	scrape := httptest.NewRecorder()
	recorder.Handler().ServeHTTP(scrape, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	metric := `gatex_http_requests_total{method="OTHER",route="unmatched",status="204"} 1`
	if !strings.Contains(scrape.Body.String(), metric) {
		t.Errorf("scrape does not contain %q", metric)
	}
}
