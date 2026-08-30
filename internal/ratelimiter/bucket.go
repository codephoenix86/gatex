package ratelimiter

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// Bucket is a concurrency-safe token bucket. Tokens are replenished lazily
// when Allow is called, so a bucket does not require a background goroutine.
type Bucket struct {
	mu sync.Mutex

	tokensPerSecond float64
	capacity        float64
	tokens          float64
	lastRefill      time.Time
	now             func() time.Time
}

// NewBucket creates a full token bucket that replenishes tokensPerSecond up to
// burst. Both values must be greater than zero.
func NewBucket(tokensPerSecond float64, burst int) (*Bucket, error) {
	return newBucket(tokensPerSecond, burst, time.Now)
}

func newBucket(tokensPerSecond float64, burst int, now func() time.Time) (*Bucket, error) {
	if tokensPerSecond <= 0 || math.IsNaN(tokensPerSecond) || math.IsInf(tokensPerSecond, 0) {
		return nil, fmt.Errorf("tokens per second must be finite and greater than zero")
	}
	if burst <= 0 {
		return nil, fmt.Errorf("burst must be greater than zero")
	}
	if now == nil {
		return nil, fmt.Errorf("clock cannot be nil")
	}

	capacity := float64(burst)
	return &Bucket{
		tokensPerSecond: tokensPerSecond,
		capacity:        capacity,
		tokens:          capacity,
		lastRefill:      now(),
		now:             now,
	}, nil
}

// Allow reports whether one token is available and consumes it when allowed.
func (b *Bucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.refill(b.now())
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func (b *Bucket) refill(now time.Time) {
	if !now.After(b.lastRefill) {
		return
	}

	elapsed := now.Sub(b.lastRefill).Seconds()
	b.tokens = min(b.capacity, b.tokens+elapsed*b.tokensPerSecond)
	b.lastRefill = now
}
