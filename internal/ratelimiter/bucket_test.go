package ratelimiter

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewBucketRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		tokensPerSecond float64
		burst           int
	}{
		{name: "zero rate", tokensPerSecond: 0, burst: 1},
		{name: "negative rate", tokensPerSecond: -1, burst: 1},
		{name: "NaN rate", tokensPerSecond: math.NaN(), burst: 1},
		{name: "infinite rate", tokensPerSecond: math.Inf(1), burst: 1},
		{name: "zero burst", tokensPerSecond: 1, burst: 0},
		{name: "negative burst", tokensPerSecond: 1, burst: -1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewBucket(test.tokensPerSecond, test.burst); err == nil {
				t.Fatal("NewBucket() error = nil, want validation error")
			}
		})
	}
}

func TestBucketStartsFullAndRefillsLazily(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	bucket := mustBucket(t, 4, 3, clock.Now)

	assertAllows(t, bucket, true, true, true, false)

	clock.Advance(250 * time.Millisecond)
	assertAllows(t, bucket, true, false)

	clock.Advance(10 * time.Second)
	assertAllows(t, bucket, true, true, true, false)
}

func TestBucketPreservesFractionalTokens(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	bucket := mustBucket(t, 2, 1, clock.Now)

	assertAllows(t, bucket, true, false)
	clock.Advance(250 * time.Millisecond)
	assertAllows(t, bucket, false)
	clock.Advance(250 * time.Millisecond)
	assertAllows(t, bucket, true, false)
}

func TestBucketIgnoresClockMovingBackward(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	clock := newFakeClock(start)
	bucket := mustBucket(t, 1, 1, clock.Now)

	assertAllows(t, bucket, true)
	clock.Set(start.Add(-time.Second))
	assertAllows(t, bucket, false)
	clock.Set(start.Add(time.Second))
	assertAllows(t, bucket, true)
}

func TestBucketAllowsAtMostBurstConcurrentRequests(t *testing.T) {
	t.Parallel()

	const (
		burst   = 100
		workers = 1_000
	)
	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	bucket := mustBucket(t, 1, burst, clock.Now)

	start := make(chan struct{})
	var allowed atomic.Int64
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			<-start
			if bucket.Allow() {
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

func mustBucket(t *testing.T, tokensPerSecond float64, burst int, now func() time.Time) *Bucket {
	t.Helper()
	bucket, err := newBucket(tokensPerSecond, burst, now)
	if err != nil {
		t.Fatalf("newBucket() error = %v", err)
	}
	return bucket
}

func assertAllows(t *testing.T, bucket *Bucket, want ...bool) {
	t.Helper()
	for index, expected := range want {
		if got := bucket.Allow(); got != expected {
			t.Fatalf("Allow() call %d = %t, want %t", index+1, got, expected)
		}
	}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}

func (c *fakeClock) Set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}
