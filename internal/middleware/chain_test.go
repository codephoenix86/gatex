package middleware

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestChainAppliesMiddlewareInDeclaredOrder(t *testing.T) {
	t.Parallel()

	var events []string
	record := func(name string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				events = append(events, name+":before")
				next.ServeHTTP(w, r)
				events = append(events, name+":after")
			})
		}
	}
	terminal := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		events = append(events, "handler")
		w.WriteHeader(http.StatusNoContent)
	})

	handler := Chain(record("first"), record("second"))(terminal)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))

	if response.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", response.Code, http.StatusNoContent)
	}
	wantEvents := []string{
		"first:before",
		"second:before",
		"handler",
		"second:after",
		"first:after",
	}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Errorf("events = %v, want %v", events, wantEvents)
	}
}

func TestEmptyChainDelegatesToNextHandler(t *testing.T) {
	t.Parallel()

	called := false
	terminal := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusAccepted)
	})

	response := httptest.NewRecorder()
	Chain()(terminal).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))

	if !called {
		t.Error("next handler was not called")
	}
	if response.Code != http.StatusAccepted {
		t.Errorf("status = %d, want %d", response.Code, http.StatusAccepted)
	}
}

func TestChainCanWrapMultipleHandlers(t *testing.T) {
	t.Parallel()

	addHeader := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Middleware", "applied")
			next.ServeHTTP(w, r)
		})
	}
	chain := Chain(addHeader)

	for _, status := range []int{http.StatusOK, http.StatusCreated} {
		response := httptest.NewRecorder()
		chain(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
		})).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))

		if response.Code != status {
			t.Errorf("status = %d, want %d", response.Code, status)
		}
		if got := response.Header().Get("X-Middleware"); got != "applied" {
			t.Errorf("X-Middleware = %q, want %q", got, "applied")
		}
	}
}
