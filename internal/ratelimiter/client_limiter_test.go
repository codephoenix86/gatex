package ratelimiter

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientLimiterKeepsAllowancesIndependent(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	limiter := mustClientLimiter(t, 1, 2, clock)

	assertClientAllows(t, limiter, "client-a", true, true, false)
	assertClientAllows(t, limiter, "client-b", true, true, false)
}

func TestClientLimiterSharesAllowanceForMatchingKeys(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	limiter := mustClientLimiter(t, 1, 1, clock)

	if !limiter.Allow("shared-client") {
		t.Fatal("first request was denied")
	}
	if limiter.Allow("shared-client") {
		t.Fatal("second request with the same key was allowed")
	}
}

func TestClientLimiterAllowsAtMostBurstForConcurrentClientRequests(t *testing.T) {
	t.Parallel()

	const (
		burst   = 100
		workers = 1_000
	)
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	limiter := mustClientLimiter(t, 1, burst, clock)

	start := make(chan struct{})
	var allowed atomic.Int64
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			<-start
			if limiter.Allow("shared-client") {
				allowed.Add(1)
			}
		}()
	}
	close(start)
	group.Wait()

	if got := allowed.Load(); got != burst {
		t.Errorf("allowed requests = %d, want %d", got, burst)
	}
}

func TestClientLimiterRefillsIndependentClientsUnderConcurrentLoad(t *testing.T) {
	t.Parallel()

	const (
		clients           = 32
		burst             = 4
		requestsPerClient = 16
	)
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	limiter := mustClientLimiter(t, 4, burst, clock)

	initialAllowed := runConcurrentClientRequests(limiter, clients, requestsPerClient)
	for client, allowed := range initialAllowed {
		if got := allowed.Load(); got != burst {
			t.Errorf("client %d initial allowed requests = %d, want %d", client, got, burst)
		}
	}

	clock.Advance(500 * time.Millisecond)
	refillAllowed := runConcurrentClientRequests(limiter, clients, requestsPerClient)
	for client, allowed := range refillAllowed {
		if got := allowed.Load(); got != 2 {
			t.Errorf("client %d allowed requests after refill = %d, want 2", client, got)
		}
	}
}

func TestClientLimiterRemovesIdleClientsOpportunistically(t *testing.T) {
	t.Parallel()

	const shardCount = 4
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	limiter, err := newClientLimiter(1, 1, clientLimiterOptions{
		shardCount:      shardCount,
		clientIdleTTL:   time.Minute,
		cleanupInterval: 10 * time.Second,
		now:             clock.Now,
	})
	if err != nil {
		t.Fatalf("newClientLimiter() error = %v", err)
	}

	firstKey := "idle-client"
	secondKey := keyForSameShard(firstKey, shardCount)
	limiter.Allow(firstKey)
	if got := clientCount(limiter); got != 1 {
		t.Fatalf("client count = %d, want 1", got)
	}

	clock.Advance(time.Minute)
	limiter.Allow(secondKey)
	if got := clientCount(limiter); got != 1 {
		t.Errorf("client count after cleanup = %d, want 1", got)
	}
	shard := limiter.shardFor(firstKey)
	shard.mu.Lock()
	_, exists := shard.clients[firstKey]
	shard.mu.Unlock()
	if exists {
		t.Error("idle client was not removed")
	}
}

func TestClientLimiterKeepsIdleBucketUntilItWouldBeFull(t *testing.T) {
	t.Parallel()

	const shardCount = 4
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	limiter, err := newClientLimiter(1.0/3600, 1, clientLimiterOptions{
		shardCount:      shardCount,
		clientIdleTTL:   time.Minute,
		cleanupInterval: 10 * time.Second,
		now:             clock.Now,
	})
	if err != nil {
		t.Fatalf("newClientLimiter() error = %v", err)
	}

	idleKey := "slow-client"
	replacementKey := keyForSameShard(idleKey, shardCount)
	limiter.Allow(idleKey)
	clock.Advance(time.Minute)
	limiter.Allow(replacementKey)

	shard := limiter.shardFor(idleKey)
	shard.mu.Lock()
	_, exists := shard.clients[idleKey]
	shard.mu.Unlock()
	if !exists {
		t.Error("idle bucket was removed before its burst could naturally refill")
	}
}

func TestNewClientLimiterRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	valid := clientLimiterOptions{
		shardCount:      1,
		clientIdleTTL:   time.Minute,
		cleanupInterval: time.Second,
		now:             clock.Now,
	}
	tests := []struct {
		name   string
		mutate func(*clientLimiterOptions)
		rate   float64
		burst  int
	}{
		{
			name:  "invalid rate",
			rate:  0,
			burst: 1,
		},
		{
			name:  "invalid burst",
			rate:  1,
			burst: 0,
		},
		{
			name: "invalid shard count",
			mutate: func(options *clientLimiterOptions) {
				options.shardCount = 0
			},
			rate:  1,
			burst: 1,
		},
		{
			name: "invalid idle TTL",
			mutate: func(options *clientLimiterOptions) {
				options.clientIdleTTL = 0
			},
			rate:  1,
			burst: 1,
		},
		{
			name: "invalid cleanup interval",
			mutate: func(options *clientLimiterOptions) {
				options.cleanupInterval = 0
			},
			rate:  1,
			burst: 1,
		},
		{
			name: "nil clock",
			mutate: func(options *clientLimiterOptions) {
				options.now = nil
			},
			rate:  1,
			burst: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			options := valid
			if test.mutate != nil {
				test.mutate(&options)
			}
			if _, err := newClientLimiter(test.rate, test.burst, options); err == nil {
				t.Fatal("newClientLimiter() error = nil, want validation error")
			}
		})
	}
}

func mustClientLimiter(t *testing.T, tokensPerSecond float64, burst int, clock *fakeClock) *ClientLimiter {
	t.Helper()
	limiter, err := newClientLimiter(tokensPerSecond, burst, clientLimiterOptions{
		shardCount:      4,
		clientIdleTTL:   10 * time.Minute,
		cleanupInterval: time.Minute,
		now:             clock.Now,
	})
	if err != nil {
		t.Fatalf("newClientLimiter() error = %v", err)
	}
	return limiter
}

func assertClientAllows(t *testing.T, limiter *ClientLimiter, clientKey string, want ...bool) {
	t.Helper()
	for index, expected := range want {
		if got := limiter.Allow(clientKey); got != expected {
			t.Fatalf("Allow(%q) call %d = %t, want %t", clientKey, index+1, got, expected)
		}
	}
}

func keyForSameShard(clientKey string, shardCount int) string {
	wanted := hashClientKey(clientKey) % uint64(shardCount)
	for candidate := 0; ; candidate++ {
		key := "replacement-" + strconv.Itoa(candidate)
		if key != clientKey && hashClientKey(key)%uint64(shardCount) == wanted {
			return key
		}
	}
}

func clientCount(limiter *ClientLimiter) int {
	count := 0
	for index := range limiter.shards {
		shard := &limiter.shards[index]
		shard.mu.Lock()
		count += len(shard.clients)
		shard.mu.Unlock()
	}
	return count
}

func runConcurrentClientRequests(limiter *ClientLimiter, clients, requestsPerClient int) []atomic.Int64 {
	allowed := make([]atomic.Int64, clients)
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(clients * requestsPerClient)
	for client := range clients {
		clientKey := "client-" + strconv.Itoa(client)
		for range requestsPerClient {
			go func() {
				defer group.Done()
				<-start
				if limiter.Allow(clientKey) {
					allowed[client].Add(1)
				}
			}()
		}
	}
	close(start)
	group.Wait()
	return allowed
}
