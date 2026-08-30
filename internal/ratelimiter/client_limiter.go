package ratelimiter

import (
	"fmt"
	"math"
	"sync"
	"time"
)

const (
	defaultShardCount      = 64
	defaultClientIdleTTL   = 10 * time.Minute
	defaultCleanupInterval = time.Minute
)

// ClientLimiter maintains an independent token bucket for every client key.
// Its sharded map reduces lock contention between unrelated clients. Idle
// entries are removed opportunistically during requests; no cleanup goroutine
// is required.
type ClientLimiter struct {
	shards []clientShard

	tokensPerSecond float64
	burst           int
	clientIdleTTL   time.Duration
	cleanupInterval time.Duration
	now             func() time.Time
}

type clientShard struct {
	mu          sync.Mutex
	clients     map[string]*clientEntry
	lastCleanup time.Time
}

type clientEntry struct {
	bucket   *Bucket
	lastSeen time.Time
}

type clientLimiterOptions struct {
	shardCount      int
	clientIdleTTL   time.Duration
	cleanupInterval time.Duration
	now             func() time.Time
}

// NewClientLimiter creates a limiter with one lazily-created bucket per client
// key. Reusing a key, including an empty key, shares that client's allowance.
func NewClientLimiter(tokensPerSecond float64, burst int) (*ClientLimiter, error) {
	return newClientLimiter(tokensPerSecond, burst, clientLimiterOptions{
		shardCount:      defaultShardCount,
		clientIdleTTL:   defaultClientIdleTTL,
		cleanupInterval: defaultCleanupInterval,
		now:             time.Now,
	})
}

func newClientLimiter(tokensPerSecond float64, burst int, options clientLimiterOptions) (*ClientLimiter, error) {
	if err := validateBucketConfig(tokensPerSecond, burst); err != nil {
		return nil, err
	}
	if options.shardCount <= 0 {
		return nil, fmt.Errorf("shard count must be greater than zero")
	}
	if options.clientIdleTTL <= 0 {
		return nil, fmt.Errorf("client idle TTL must be greater than zero")
	}
	if options.cleanupInterval <= 0 {
		return nil, fmt.Errorf("cleanup interval must be greater than zero")
	}
	if options.now == nil {
		return nil, fmt.Errorf("clock cannot be nil")
	}

	startedAt := options.now()
	clientIdleTTL := max(options.clientIdleTTL, bucketRefillDuration(tokensPerSecond, burst))
	limiter := &ClientLimiter{
		shards:          make([]clientShard, options.shardCount),
		tokensPerSecond: tokensPerSecond,
		burst:           burst,
		clientIdleTTL:   clientIdleTTL,
		cleanupInterval: options.cleanupInterval,
		now:             options.now,
	}
	for index := range limiter.shards {
		limiter.shards[index] = clientShard{
			clients:     make(map[string]*clientEntry),
			lastCleanup: startedAt,
		}
	}
	return limiter, nil
}

// Allow reports whether the client has a token available and consumes it when
// allowed. A bucket is allocated only when a client key is first observed.
func (l *ClientLimiter) Allow(clientKey string) bool {
	now := l.now()
	shard := l.shardFor(clientKey)

	shard.mu.Lock()
	shard.cleanup(now, l.clientIdleTTL, l.cleanupInterval)
	entry := shard.clients[clientKey]
	if entry == nil {
		entry = &clientEntry{
			bucket:   newBucketAt(l.tokensPerSecond, l.burst, l.now, now),
			lastSeen: now,
		}
		shard.clients[clientKey] = entry
	} else if now.After(entry.lastSeen) {
		entry.lastSeen = now
	}
	allowed := entry.bucket.allowAt(now)
	shard.mu.Unlock()
	return allowed
}

func (l *ClientLimiter) shardFor(clientKey string) *clientShard {
	return &l.shards[hashClientKey(clientKey)%uint64(len(l.shards))]
}

func (s *clientShard) cleanup(now time.Time, idleTTL, interval time.Duration) {
	if now.Before(s.lastCleanup.Add(interval)) {
		return
	}

	for key, entry := range s.clients {
		if !now.Before(entry.lastSeen.Add(idleTTL)) {
			delete(s.clients, key)
		}
	}
	s.lastCleanup = now
}

func hashClientKey(clientKey string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)

	hash := uint64(offset64)
	for index := range len(clientKey) {
		hash ^= uint64(clientKey[index])
		hash *= prime64
	}
	return hash
}

func bucketRefillDuration(tokensPerSecond float64, burst int) time.Duration {
	nanoseconds := math.Ceil(float64(burst) / tokensPerSecond * float64(time.Second))
	if nanoseconds >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(nanoseconds)
}
