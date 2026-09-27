package balancer

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
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
			wantErr: ErrEmptyBackendURL,
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
				if !errors.Is(err, test.wantErr) {
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

func TestNewPoolWithStrategy(t *testing.T) {
	t.Parallel()

	pool, err := NewPoolWithStrategy([]string{"http://users.internal"}, LeastConnections)
	if err != nil {
		t.Fatalf("NewPoolWithStrategy() error = %v", err)
	}
	if got := pool.Strategy(); got != LeastConnections {
		t.Errorf("Strategy() = %q, want %q", got, LeastConnections)
	}

	_, err = NewPoolWithStrategy([]string{"http://users.internal"}, "random")
	if !errors.Is(err, ErrUnsupportedStrategy) {
		t.Errorf("NewPoolWithStrategy() error = %v, want %v", err, ErrUnsupportedStrategy)
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

func TestPoolAcquire(t *testing.T) {
	t.Parallel()

	urls := []string{
		"http://users-1.internal",
		"http://users-2.internal",
		"http://users-3.internal",
	}
	tests := []struct {
		name              string
		strategy          Strategy
		healthy           []bool
		activeConnections []int
		wantURLs          []string
		wantAvailable     bool
	}{
		{
			name:          "round robin rotates through every backend",
			strategy:      RoundRobin,
			wantURLs:      []string{urls[0], urls[1], urls[2], urls[0]},
			wantAvailable: true,
		},
		{
			name:          "round robin advances past an unhealthy backend",
			strategy:      RoundRobin,
			healthy:       []bool{false, true, true},
			wantURLs:      []string{urls[1], urls[2], urls[1], urls[2]},
			wantAvailable: true,
		},
		{
			name:              "least connections selects the least busy backend",
			strategy:          LeastConnections,
			activeConnections: []int{2, 1, 0},
			wantURLs:          []string{urls[2]},
			wantAvailable:     true,
		},
		{
			name:              "least connections skips an unhealthy idle backend",
			strategy:          LeastConnections,
			healthy:           []bool{false, true, true},
			activeConnections: []int{0, 1, 2},
			wantURLs:          []string{urls[1]},
			wantAvailable:     true,
		},
		{
			name:          "no strategy selects an unhealthy backend",
			strategy:      RoundRobin,
			healthy:       []bool{false, false, false},
			wantAvailable: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			pool, err := NewPoolWithStrategy(urls, test.strategy)
			if err != nil {
				t.Fatalf("NewPoolWithStrategy() error = %v", err)
			}
			backends := pool.Backends()
			for index, healthy := range test.healthy {
				backends[index].SetHealthy(healthy)
			}
			for index, active := range test.activeConnections {
				for range active {
					backends[index].Acquire()
				}
			}

			for request, wantURL := range test.wantURLs {
				backend, ok := pool.Acquire()
				if !ok {
					t.Fatalf("Acquire() selected no backend at request %d", request)
				}
				if got := backend.URL(); got != wantURL {
					t.Errorf("Acquire() backend at request %d = %q, want %q", request, got, wantURL)
				}
				backend.Release()
			}

			if test.wantAvailable {
				return
			}
			if backend, ok := pool.Acquire(); ok || backend != nil {
				t.Errorf("Acquire() = (%v, %t), want (nil, false)", backend, ok)
			}
		})
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

func TestPoolAcquireIsSafeUnderConcurrentLoad(t *testing.T) {
	t.Parallel()

	for _, strategy := range []Strategy{RoundRobin, LeastConnections} {
		t.Run(string(strategy), func(t *testing.T) {
			t.Parallel()

			pool, err := NewPoolWithStrategy([]string{
				"http://backend-1.internal",
				"http://backend-2.internal",
				"http://backend-3.internal",
			}, strategy)
			if err != nil {
				t.Fatalf("NewPoolWithStrategy() error = %v", err)
			}
			backends := pool.Backends()

			const workers = 32
			const iterations = 250
			start := make(chan struct{})
			var group sync.WaitGroup
			var failedAcquisitions atomic.Int64
			selectionCounts := make([]atomic.Int64, len(backends))

			group.Add(workers)
			for range workers {
				go func() {
					defer group.Done()
					<-start
					for range iterations {
						backend, ok := pool.Acquire()
						if !ok {
							failedAcquisitions.Add(1)
							continue
						}
						for index, candidate := range backends {
							if backend == candidate {
								selectionCounts[index].Add(1)
								break
							}
						}
						runtime.Gosched()
						backend.Release()
					}
				}()
			}
			close(start)
			group.Wait()

			if got := failedAcquisitions.Load(); got != 0 {
				t.Errorf("failed acquisitions = %d, want 0", got)
			}
			var selections int64
			for index, backend := range backends {
				selections += selectionCounts[index].Load()
				if got := backend.ActiveConnections(); got != 0 {
					t.Errorf("backend %d active connections = %d, want 0", index, got)
				}
			}
			if want := int64(workers * iterations); selections != want {
				t.Errorf("selections = %d, want %d", selections, want)
			}
		})
	}
}
