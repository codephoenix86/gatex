package balancer

import (
	"errors"
	"sync"
	"testing"
)

func TestNewPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		urls    []string
		wantErr error
	}{
		{
			name:    "no backends",
			wantErr: ErrNoBackends,
		},
		{
			name:    "empty backend URL",
			urls:    []string{""},
			wantErr: errors.New("balancer backend URL cannot be empty"),
		},
		{
			name: "backends start healthy",
			urls: []string{"http://users-1.internal", "http://users-2.internal"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pool, err := NewPool(test.urls)
			if test.wantErr != nil {
				if err == nil || err.Error() != test.wantErr.Error() {
					t.Fatalf("NewPool() error = %v, want %v", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewPool() error = %v", err)
			}

			backends := pool.Backends()
			if len(backends) != len(test.urls) {
				t.Fatalf("len(Backends()) = %d, want %d", len(backends), len(test.urls))
			}
			for index, backend := range backends {
				if backend.URL() != test.urls[index] {
					t.Errorf("backend %d URL = %q, want %q", index, backend.URL(), test.urls[index])
				}
				if !backend.Healthy() {
					t.Errorf("backend %d starts unhealthy", index)
				}
			}
		})
	}
}

func TestPoolBackendsReturnsMembershipCopy(t *testing.T) {
	t.Parallel()

	pool, err := NewPool([]string{"http://users.internal"})
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}

	backends := pool.Backends()
	backends[0] = nil
	if pool.Backends()[0] == nil {
		t.Error("Backends() exposed the pool membership slice")
	}
}

func TestBackendState(t *testing.T) {
	t.Parallel()

	pool, err := NewPool([]string{"http://users.internal"})
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	backend := pool.Backends()[0]

	backend.SetHealthy(false)
	if backend.Healthy() {
		t.Error("Healthy() = true after SetHealthy(false)")
	}

	backend.Acquire()
	backend.Acquire()
	if got := backend.ActiveConnections(); got != 2 {
		t.Errorf("ActiveConnections() = %d, want 2", got)
	}
	backend.Release()
	backend.Release()
	backend.Release()
	if got := backend.ActiveConnections(); got != 0 {
		t.Errorf("ActiveConnections() = %d, want 0", got)
	}
}

func TestBackendStateIsSafeUnderConcurrentAccess(t *testing.T) {
	pool, err := NewPool([]string{"http://users.internal"})
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	backend := pool.Backends()[0]

	const workers = 32
	const iterations = 500

	var group sync.WaitGroup
	group.Add(workers)
	for worker := range workers {
		go func() {
			defer group.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				backend.SetHealthy((worker+iteration)%2 == 0)
				_ = backend.Healthy()
				backend.Acquire()
				_ = backend.ActiveConnections()
				backend.Release()
			}
		}()
	}
	group.Wait()

	if got := backend.ActiveConnections(); got != 0 {
		t.Errorf("ActiveConnections() = %d, want 0", got)
	}
}
