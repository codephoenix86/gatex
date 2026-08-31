package middleware

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRecoveryHandlesPanicAndKeepsServing(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	requests := 0
	handler := Recovery(logger)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		if requests == 1 {
			panic("database password must not reach client")
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	panicResponse := httptest.NewRecorder()
	handler.ServeHTTP(panicResponse, httptest.NewRequest(http.MethodGet, "/panic", nil))

	if panicResponse.Code != http.StatusInternalServerError {
		t.Errorf("panic response status = %d, want %d", panicResponse.Code, http.StatusInternalServerError)
	}
	if body := panicResponse.Body.String(); body != recoveryErrorMessage+"\n" {
		t.Errorf("panic response body = %q, want generic error", body)
	}
	if logOutput := logs.String(); !strings.Contains(logOutput, "database password must not reach client") || !strings.Contains(logOutput, `"stack"`) {
		t.Errorf("recovery log does not contain the panic and stack: %s", logOutput)
	}

	normalResponse := httptest.NewRecorder()
	handler.ServeHTTP(normalResponse, httptest.NewRequest(http.MethodGet, "/healthy", nil))
	if normalResponse.Code != http.StatusNoContent {
		t.Errorf("subsequent response status = %d, want %d", normalResponse.Code, http.StatusNoContent)
	}
}

func TestRecoveryPreservesAbortHandlerPanic(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	handler := Recovery(logger)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))

	defer func() {
		recovered := recover()
		recoveredErr, ok := recovered.(error)
		if !ok || !errors.Is(recoveredErr, http.ErrAbortHandler) {
			t.Errorf("recovered panic = %v, want %v", recovered, http.ErrAbortHandler)
		}
		if logs.Len() != 0 {
			t.Errorf("abort-handler panic was logged: %s", logs.String())
		}
	}()

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/abort", nil))
	t.Error("Recovery did not re-panic with http.ErrAbortHandler")
}
