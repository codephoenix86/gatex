package balancer

import (
	"errors"
	"strings"
	"sync/atomic"
)

// ErrNoBackends indicates that a pool was created without an upstream.
var ErrNoBackends = errors.New("balancer pool requires at least one backend")

// Pool is an immutable collection of backends. The membership does not change
// after construction, so callers can safely obtain a snapshot with Backends.
// Mutable state belongs to each Backend and is stored atomically.
type Pool struct {
	backends []*Backend
}

// Backend represents one upstream and the state used by the balancer to make
// routing decisions. A backend starts healthy; the health checker will update
// that state in a later phase-two change.
type Backend struct {
	url               string
	healthy           atomic.Bool
	activeConnections atomic.Int64
}

// NewPool creates a pool whose backends initially accept traffic.
func NewPool(urls []string) (*Pool, error) {
	if len(urls) == 0 {
		return nil, ErrNoBackends
	}

	pool := &Pool{backends: make([]*Backend, 0, len(urls))}
	for _, url := range urls {
		if strings.TrimSpace(url) == "" {
			return nil, errors.New("balancer backend URL cannot be empty")
		}
		backend := &Backend{url: url}
		backend.healthy.Store(true)
		pool.backends = append(pool.backends, backend)
	}

	return pool, nil
}

// Backends returns a copy of the pool membership. The returned Backend values
// are shared and expose only concurrency-safe state operations.
func (p *Pool) Backends() []*Backend {
	backends := make([]*Backend, len(p.backends))
	copy(backends, p.backends)
	return backends
}

// URL returns the backend's configured upstream URL.
func (b *Backend) URL() string {
	return b.url
}

// Healthy reports whether the backend currently accepts traffic.
func (b *Backend) Healthy() bool {
	return b.healthy.Load()
}

// SetHealthy changes whether the balancer may send traffic to this backend.
func (b *Backend) SetHealthy(healthy bool) {
	b.healthy.Store(healthy)
}

// ActiveConnections reports the number of requests currently assigned to the
// backend. It is safe to call while requests start and finish concurrently.
func (b *Backend) ActiveConnections() int64 {
	return b.activeConnections.Load()
}

// Acquire records that a request has been assigned to the backend.
func (b *Backend) Acquire() {
	b.activeConnections.Add(1)
}

// Release records that an assigned request has completed. Extra releases are
// ignored so malformed accounting cannot produce a negative connection count.
func (b *Backend) Release() {
	for {
		current := b.activeConnections.Load()
		if current == 0 {
			return
		}
		if b.activeConnections.CompareAndSwap(current, current-1) {
			return
		}
	}
}
