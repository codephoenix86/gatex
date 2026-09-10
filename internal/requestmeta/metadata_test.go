package requestmeta

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEnsurePreservesSafeIncomingRequestID(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set(IDHeader, "client-request-id")

	request = Ensure(request)

	if got := RequestID(request.Context()); got != "client-request-id" {
		t.Errorf("request ID = %q, want %q", got, "client-request-id")
	}
	if got := request.Header.Get(IDHeader); got != "client-request-id" {
		t.Errorf("request header ID = %q, want %q", got, "client-request-id")
	}
}

func TestEnsureReplacesUnsafeRequestIDAndKeepsMetadata(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set(IDHeader, "unsafe\nrequest-id")
	request = Ensure(request)
	firstID := RequestID(request.Context())

	if firstID == "" || firstID == "unsafe\nrequest-id" {
		t.Fatalf("generated request ID = %q", firstID)
	}
	if ensuredAgain := Ensure(request); ensuredAgain != request {
		t.Error("second Ensure call cloned request with existing metadata")
	}
	if got := RequestID(request.Context()); got != firstID {
		t.Errorf("request ID after second Ensure = %q, want %q", got, firstID)
	}
}

func TestBackendMetadata(t *testing.T) {
	t.Parallel()

	request := Ensure(httptest.NewRequest(http.MethodGet, "/", nil))
	SetBackend(request.Context(), "http://backend.internal")

	if got := Backend(request.Context()); got != "http://backend.internal" {
		t.Errorf("backend = %q, want %q", got, "http://backend.internal")
	}
	SetRoute(request.Context(), "/api")
	if got := Route(request.Context()); got != "/api" {
		t.Errorf("route = %q, want %q", got, "/api")
	}
}
