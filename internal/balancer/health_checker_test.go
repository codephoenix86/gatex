package balancer

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewHealthChecker(t *testing.T) {
	t.Parallel()

	checker, err := NewHealthChecker(0, 0, "", nil)
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}
	if checker.interval != defaultHealthCheckInterval {
		t.Errorf("interval = %s, want %s", checker.interval, defaultHealthCheckInterval)
	}
	if checker.timeout != defaultHealthCheckTimeout {
		t.Errorf("timeout = %s, want %s", checker.timeout, defaultHealthCheckTimeout)
	}
	if checker.path != defaultHealthCheckPath {
		t.Errorf("path = %q, want %q", checker.path, defaultHealthCheckPath)
	}

	for _, test := range []struct {
		name     string
		interval time.Duration
		timeout  time.Duration
		path     string
		wantErr  error
	}{
		{
			name:     "negative interval",
			interval: -time.Second,
			timeout:  time.Second,
			path:     "/healthz",
			wantErr:  errNegativeHealthCheckInterval,
		},
		{
			name:     "negative timeout",
			interval: time.Second,
			timeout:  -time.Second,
			path:     "/healthz",
			wantErr:  errNegativeHealthCheckTimeout,
		},
		{
			name:     "relative path",
			interval: time.Second,
			timeout:  time.Second,
			path:     "healthz",
			wantErr:  errInvalidHealthCheckPath,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewHealthChecker(test.interval, test.timeout, test.path, nil)
			if !errors.Is(err, test.wantErr) {
				t.Errorf("NewHealthChecker() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestHealthCheckerCheckUpdatesBackendHealth(t *testing.T) {
	t.Parallel()

	healthyServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/ready" {
			http.NotFound(writer, request)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer healthyServer.Close()

	unhealthyServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer unhealthyServer.Close()

	pool, err := NewPool([]string{healthyServer.URL, unhealthyServer.URL})
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	checker, err := NewHealthChecker(time.Second, time.Second, "/ready", healthyServer.Client())
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}

	checker.Check(context.Background(), pool)
	backends := pool.Backends()
	if !backends[0].Healthy() {
		t.Error("healthy backend was marked unhealthy")
	}
	if backends[1].Healthy() {
		t.Error("unhealthy backend was marked healthy")
	}
}

func TestHealthCheckerStartChecksUntilContextCancellation(t *testing.T) {
	t.Parallel()

	var healthy atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if healthy.Load() {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	pool, err := NewPool([]string{server.URL})
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	checker, err := NewHealthChecker(10*time.Millisecond, time.Second, "/healthz", server.Client())
	if err != nil {
		t.Fatalf("NewHealthChecker() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := checker.Start(ctx, pool)

	eventually(t, time.Second, func() bool { return !pool.Backends()[0].Healthy() })
	healthy.Store(true)
	eventually(t, time.Second, func() bool { return pool.Backends()[0].Healthy() })

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("health checker did not stop after context cancellation")
	}
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
