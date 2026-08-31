package middleware

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestLoggerRecordsStructuredResponseDetails(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	handler := RequestLogger(logger)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(requestIDHeader, "request-123")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, "hello")
	}))
	request := httptest.NewRequest(http.MethodPost, "http://gateway.example/resources?secret=hidden", nil)
	request.RemoteAddr = "192.0.2.10:4321"
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	entry := decodeLogEntry(t, logs.Bytes())
	assertLogValue(t, entry, "msg", "request completed")
	assertLogValue(t, entry, "method", http.MethodPost)
	assertLogValue(t, entry, "path", "/resources")
	assertLogValue(t, entry, "host", "gateway.example")
	assertLogValue(t, entry, "remote_addr", "192.0.2.10:4321")
	assertLogValue(t, entry, "status", float64(http.StatusCreated))
	assertLogValue(t, entry, "response_bytes", float64(len("hello")))
	assertLogValue(t, entry, "request_id", "request-123")
	if _, ok := entry["duration"]; !ok {
		t.Error("structured log has no duration")
	}
	if _, ok := entry["secret"]; ok {
		t.Error("structured log contains query-string data")
	}
	if bytes.Contains(logs.Bytes(), []byte("hidden")) {
		t.Error("structured log contains a query-string value")
	}
}

func TestRequestLoggerRecordsRecoveredPanicAsServerError(t *testing.T) {
	t.Parallel()

	var accessLogs bytes.Buffer
	accessLogger := slog.New(slog.NewJSONHandler(&accessLogs, nil))
	recoveryLogger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler := Recovery(recoveryLogger)(RequestLogger(accessLogger)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/panic", nil))

	if response.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", response.Code, http.StatusInternalServerError)
	}
	entry := decodeLogEntry(t, accessLogs.Bytes())
	assertLogValue(t, entry, "level", "ERROR")
	assertLogValue(t, entry, "status", float64(http.StatusInternalServerError))
}

func TestLoggingResponseWriterRetainsFinalStatusAfterInformationalResponse(t *testing.T) {
	t.Parallel()

	underlying := &statusRecordingResponseWriter{header: make(http.Header)}
	response := &loggingResponseWriter{ResponseWriter: underlying}
	response.WriteHeader(http.StatusEarlyHints)
	response.WriteHeader(http.StatusAccepted)

	if got := response.status(); got != http.StatusAccepted {
		t.Errorf("status = %d, want %d", got, http.StatusAccepted)
	}
	if len(underlying.statuses) != 2 || underlying.statuses[0] != http.StatusEarlyHints || underlying.statuses[1] != http.StatusAccepted {
		t.Errorf("underlying statuses = %v, want [%d %d]", underlying.statuses, http.StatusEarlyHints, http.StatusAccepted)
	}
}

type statusRecordingResponseWriter struct {
	header   http.Header
	statuses []int
}

func (w *statusRecordingResponseWriter) Header() http.Header {
	return w.header
}

func (w *statusRecordingResponseWriter) Write(body []byte) (int, error) {
	return len(body), nil
}

func (w *statusRecordingResponseWriter) WriteHeader(status int) {
	w.statuses = append(w.statuses, status)
}

func decodeLogEntry(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var entry map[string]any
	if err := json.Unmarshal(data, &entry); err != nil {
		t.Fatalf("decode structured log %q: %v", data, err)
	}
	return entry
}

func assertLogValue(t *testing.T, entry map[string]any, key string, want any) {
	t.Helper()
	if got := entry[key]; got != want {
		t.Errorf("log %s = %v, want %v", key, got, want)
	}
}
