package middleware

import (
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"
)

const recoveryErrorMessage = "internal server error"

// Recovery prevents unexpected handler panics from escaping the request path.
// It logs the recovered value and stack trace, while callers receive only a
// generic error. A nil logger uses slog.Default.
func Recovery(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.Default()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				recovered := recover()
				if recovered == nil {
					return
				}
				if recoveredErr, ok := recovered.(error); ok && errors.Is(recoveredErr, http.ErrAbortHandler) {
					panic(recovered)
				}

				logger.ErrorContext(
					r.Context(),
					"handler panic recovered",
					slog.Any("panic", recovered),
					slog.String("stack", string(debug.Stack())),
				)
				http.Error(w, recoveryErrorMessage, http.StatusInternalServerError)
			}()

			next.ServeHTTP(w, r)
		})
	}
}
