// Package requestmeta carries request-scoped gateway metadata between layers.
package requestmeta

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"sync"
	"time"
)

const IDHeader = "X-Request-ID"

type contextKey struct{}

type metadata struct {
	requestID string

	mu      sync.RWMutex
	backend string
}

// Ensure returns a request carrying validated request metadata. The incoming
// request ID is retained when safe; otherwise a new one is generated. Calling
// Ensure repeatedly preserves the same metadata instance.
func Ensure(request *http.Request) *http.Request {
	if current(request.Context()) != nil {
		return request
	}

	values := &metadata{requestID: incomingOrNewID(request.Header.Get(IDHeader))}
	request = request.Clone(context.WithValue(request.Context(), contextKey{}, values))
	request.Header.Set(IDHeader, values.requestID)
	return request
}

// RequestID returns the correlation ID associated with ctx, if metadata has
// been installed by Ensure.
func RequestID(ctx context.Context) string {
	values := current(ctx)
	if values == nil {
		return ""
	}
	return values.requestID
}

// SetBackend records the upstream selected for this request. It is a no-op
// when request metadata has not been installed.
func SetBackend(ctx context.Context, backend string) {
	values := current(ctx)
	if values == nil {
		return
	}
	values.mu.Lock()
	values.backend = backend
	values.mu.Unlock()
}

// Backend returns the upstream selected for this request, if any.
func Backend(ctx context.Context) string {
	values := current(ctx)
	if values == nil {
		return ""
	}
	values.mu.RLock()
	defer values.mu.RUnlock()
	return values.backend
}

func current(ctx context.Context) *metadata {
	values, _ := ctx.Value(contextKey{}).(*metadata)
	return values
}

func incomingOrNewID(incoming string) string {
	if isSafeID(incoming) {
		return incoming
	}
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err == nil {
		return hex.EncodeToString(bytes)
	}
	// crypto/rand failures are exceptionally rare. A timestamp still provides
	// an identifier for log correlation without failing a customer request.
	return fmt.Sprintf("%x", time.Now().UnixNano())
}

func isSafeID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if char < 0x21 || char > 0x7e {
			return false
		}
	}
	return true
}
