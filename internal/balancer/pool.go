package balancer

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	// ErrNoBackends indicates that a pool was created without an upstream.
	ErrNoBackends = errors.New("balancer pool requires at least one backend")

	// ErrEmptyBackendURL indicates that a pool contains a blank upstream URL.
	ErrEmptyBackendURL = errors.New("balancer backend URL cannot be empty")

	// ErrUnsupportedStrategy indicates that a pool was configured with an
	// unknown selection strategy.
	ErrUnsupportedStrategy = errors.New("balancer strategy is unsupported")
)

// Strategy determines how a Pool assigns requests to healthy backends.
type Strategy string

const (
	// RoundRobin rotates evenly through healthy backends.
	RoundRobin Strategy = "round_robin"

	// LeastConnections assigns each request to the healthy backend currently
	// handling the fewest requests.
	LeastConnections Strategy = "least_connections"
)

// Pool is a fixed collection of backends and a selection strategy. Membership
// does not change after construction; selection is serialized so choosing a
// backend and incrementing its active request count happen as one operation.
type Pool struct {
	backends    []*Backend
	strategy    Strategy
	next        atomic.Uint64
	selectionMu sync.Mutex
}

// Backend represents one upstream and the state used by the balancer to make
// routing decisions. A backend starts healthy; the health checker will update
// that state in a later phase-two change.
type Backend struct {
	url               string
	healthy           atomic.Bool
	activeConnections atomic.Int64
}

// NewPool creates a round-robin pool whose backends initially accept traffic.
func NewPool(urls []string) (*Pool, error) {
	return NewPoolWithStrategy(urls, RoundRobin)
}

// NewPoolWithStrategy creates a pool with the requested selection strategy.
func NewPoolWithStrategy(urls []string, strategy Strategy) (*Pool, error) {
	if len(urls) == 0 {
		return nil, ErrNoBackends
	}
	if strategy != RoundRobin && strategy != LeastConnections {
		return nil, ErrUnsupportedStrategy
	}

	pool := &Pool{
		backends: make([]*Backend, 0, len(urls)),
		strategy: strategy,
	}
	for _, url := range urls {
		if strings.TrimSpace(url) == "" {
			return nil, ErrEmptyBackendURL
		}
		backend := &Backend{url: url}
		backend.healthy.Store(true)
		pool.backends = append(pool.backends, backend)
	}

	return pool, nil
}

// Strategy reports the pool's configured selection strategy.
func (p *Pool) Strategy() Strategy {
	return p.strategy
}

// Backends returns a copy of the pool membership. The returned Backend values
// are shared and expose only concurrency-safe state operations.
func (p *Pool) Backends() []*Backend {
	backends := make([]*Backend, len(p.backends))
	copy(backends, p.backends)
	return backends
}

// Acquire selects a healthy backend and records one active request against it.
// The caller must call Release on the returned backend when that request
// completes. It returns false when every backend is unhealthy.
func (p *Pool) Acquire() (*Backend, bool) {
	p.selectionMu.Lock()
	defer p.selectionMu.Unlock()

	var backend *Backend
	switch p.strategy {
	case RoundRobin:
		backend = p.nextRoundRobin()
	case LeastConnections:
		backend = p.nextLeastConnections()
	}
	if backend == nil {
		return nil, false
	}

	backend.Acquire()
	return backend, true
}

func (p *Pool) nextRoundRobin() *Backend {
	start := p.next.Add(1) - 1
	for offset := range len(p.backends) {
		backend := p.backends[(start+uint64(offset))%uint64(len(p.backends))]
		if backend.Healthy() {
			return backend
		}
	}
	return nil
}

func (p *Pool) nextLeastConnections() *Backend {
	var selected *Backend
	for _, backend := range p.backends {
		if !backend.Healthy() {
			continue
		}
		if selected == nil || backend.ActiveConnections() < selected.ActiveConnections() {
			selected = backend
		}
	}
	return selected
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
