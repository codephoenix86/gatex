package balancer

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	defaultHealthCheckPath     = "/healthz"
	defaultHealthCheckInterval = 10 * time.Second
	defaultHealthCheckTimeout  = 2 * time.Second
)

var (
	errNegativeHealthCheckInterval = errors.New("health check interval cannot be negative")
	errNegativeHealthCheckTimeout  = errors.New("health check timeout cannot be negative")
	errInvalidHealthCheckPath      = errors.New("health check path must start with /")
)

// HealthChecker probes a pool's backends and updates their health state. Its
// methods are safe to use concurrently with request selection.
type HealthChecker struct {
	client   *http.Client
	interval time.Duration
	timeout  time.Duration
	path     string
}

// NewHealthChecker creates a health checker. Zero interval, timeout, and path
// values use safe defaults. The supplied client must remain usable for the
// checker's lifetime.
func NewHealthChecker(interval, timeout time.Duration, path string, client *http.Client) (*HealthChecker, error) {
	if interval < 0 {
		return nil, errNegativeHealthCheckInterval
	}
	if timeout < 0 {
		return nil, errNegativeHealthCheckTimeout
	}
	if path != "" && !strings.HasPrefix(path, "/") {
		return nil, errInvalidHealthCheckPath
	}
	if interval == 0 {
		interval = defaultHealthCheckInterval
	}
	if timeout == 0 {
		timeout = defaultHealthCheckTimeout
	}
	if path == "" {
		path = defaultHealthCheckPath
	}
	if client == nil {
		client = &http.Client{}
	}

	return &HealthChecker{
		client:   client,
		interval: interval,
		timeout:  timeout,
		path:     path,
	}, nil
}

// Start launches one ticker-driven health-check loop for pool. The returned
// channel closes after ctx is cancelled and the loop has stopped.
func (h *HealthChecker) Start(ctx context.Context, pool *Pool) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Run(ctx, pool)
	}()
	return done
}

// Run checks pool immediately and then after every configured interval until
// ctx is cancelled. It is useful when a caller needs to own the goroutine.
func (h *HealthChecker) Run(ctx context.Context, pool *Pool) {
	h.Check(ctx, pool)

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.Check(ctx, pool)
		}
	}
}

// Check probes every backend concurrently and records the result. A successful
// health check is any HTTP response with a 2xx or 3xx status code.
func (h *HealthChecker) Check(ctx context.Context, pool *Pool) {
	backends := pool.Backends()
	var group sync.WaitGroup
	group.Add(len(backends))
	for _, backend := range backends {
		go func() {
			defer group.Done()
			h.checkBackend(ctx, backend)
		}()
	}
	group.Wait()
}

func (h *HealthChecker) checkBackend(ctx context.Context, backend *Backend) {
	checkURL, err := healthCheckURL(backend.URL(), h.path)
	if err != nil {
		backend.SetHealthy(false)
		return
	}

	requestContext, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, checkURL, nil)
	if err != nil {
		backend.SetHealthy(false)
		return
	}

	response, err := h.client.Do(request)
	if err != nil {
		backend.SetHealthy(false)
		return
	}
	response.Body.Close()
	backend.SetHealthy(response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusBadRequest)
}

func healthCheckURL(backendURL, path string) (string, error) {
	backend, err := url.Parse(backendURL)
	if err != nil {
		return "", err
	}
	return backend.ResolveReference(&url.URL{Path: path}).String(), nil
}
