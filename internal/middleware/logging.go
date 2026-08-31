package middleware

import (
	"log/slog"
	"net/http"
	"time"
)

const requestIDHeader = "X-Request-ID"

// RequestLogger writes one structured completion event for every request. A
// nil logger uses slog.Default.
func RequestLogger(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.Default()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			startedAt := time.Now()
			response := &loggingResponseWriter{ResponseWriter: w}

			defer func() {
				recovered := recover()
				status := response.status()
				if recovered != nil && !response.wroteHeader {
					status = http.StatusInternalServerError
				}
				logCompletedRequest(logger, r, response, status, time.Since(startedAt))
				if recovered != nil {
					panic(recovered)
				}
			}()

			next.ServeHTTP(response, r)
		})
	}
}

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
	wroteHeader  bool
}

func (w *loggingResponseWriter) WriteHeader(statusCode int) {
	if statusCode >= 100 && statusCode < 200 && statusCode != http.StatusSwitchingProtocols {
		w.ResponseWriter.WriteHeader(statusCode)
		return
	}
	if w.wroteHeader {
		return
	}
	w.statusCode = statusCode
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *loggingResponseWriter) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	written, err := w.ResponseWriter.Write(body)
	w.bytesWritten += int64(written)
	return written, err
}

// Unwrap lets http.ResponseController retain streaming, flushing, and
// connection-hijacking capabilities provided by the underlying writer.
func (w *loggingResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *loggingResponseWriter) status() int {
	if !w.wroteHeader {
		return http.StatusOK
	}
	return w.statusCode
}

func logCompletedRequest(logger *slog.Logger, request *http.Request, response *loggingResponseWriter, status int, duration time.Duration) {
	level := slog.LevelInfo
	if status >= http.StatusInternalServerError {
		level = slog.LevelError
	} else if status >= http.StatusBadRequest {
		level = slog.LevelWarn
	}

	requestID := response.Header().Get(requestIDHeader)
	if requestID == "" {
		requestID = request.Header.Get(requestIDHeader)
	}
	attributes := []slog.Attr{
		slog.String("method", request.Method),
		slog.String("path", request.URL.Path),
		slog.String("host", request.Host),
		slog.String("remote_addr", request.RemoteAddr),
		slog.Int("status", status),
		slog.Int64("response_bytes", response.bytesWritten),
		slog.Duration("duration", duration),
	}
	if requestID != "" {
		attributes = append(attributes, slog.String("request_id", requestID))
	}
	logger.LogAttrs(request.Context(), level, "request completed", attributes...)
}
