package cache

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestCacheStoresIndependentResponseCopies(t *testing.T) {
	t.Parallel()

	cache := mustCache(t, time.Minute, 2, time.Now)
	original := Response{
		StatusCode: http.StatusCreated,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       []byte(`{"version":1}`),
	}
	cache.Set("resource", original)

	original.Header.Set("Content-Type", "text/plain")
	original.Body[11] = '2'

	first, ok := cache.Get("resource")
	if !ok {
		t.Fatal("Get() miss, want hit")
	}
	assertResponse(t, first, http.StatusCreated, "application/json", `{"version":1}`)

	first.Header.Set("Content-Type", "text/html")
	first.Body[11] = '3'
	second, ok := cache.Get("resource")
	if !ok {
		t.Fatal("second Get() miss, want hit")
	}
	assertResponse(t, second, http.StatusCreated, "application/json", `{"version":1}`)
}

func TestCacheExpiresEntriesAtTTL(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	cache := mustCache(t, time.Minute, 2, clock.Now)
	cache.Set("resource", Response{StatusCode: http.StatusOK})

	clock.Advance(time.Minute - time.Nanosecond)
	if _, ok := cache.Get("resource"); !ok {
		t.Fatal("Get() before TTL was a miss")
	}

	clock.Advance(time.Nanosecond)
	if _, ok := cache.Get("resource"); ok {
		t.Fatal("Get() at TTL was a hit")
	}
	if got := cacheEntryCount(cache); got != 0 {
		t.Errorf("entry count after expiry = %d, want 0", got)
	}
}

func TestCacheEvictsLeastRecentlyUsedEntry(t *testing.T) {
	t.Parallel()

	cache := mustCache(t, time.Minute, 2, time.Now)
	cache.Set("first", Response{StatusCode: http.StatusOK})
	cache.Set("second", Response{StatusCode: http.StatusCreated})
	if _, ok := cache.Get("first"); !ok {
		t.Fatal("Get(first) miss, want hit")
	}

	cache.Set("third", Response{StatusCode: http.StatusAccepted})

	if _, ok := cache.Get("second"); ok {
		t.Error("least recently used entry was not evicted")
	}
	if _, ok := cache.Get("first"); !ok {
		t.Error("recently used entry was evicted")
	}
	if _, ok := cache.Get("third"); !ok {
		t.Error("new entry was not retained")
	}
}

func TestCacheDiscardsExpiredEntriesBeforeLRUEviction(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	cache := mustCache(t, time.Minute, 2, clock.Now)
	cache.Set("stale", Response{StatusCode: http.StatusOK})
	clock.Advance(30 * time.Second)
	cache.Set("live", Response{StatusCode: http.StatusCreated})
	clock.Advance(29 * time.Second)
	if _, ok := cache.Get("stale"); !ok {
		t.Fatal("Get(stale) before TTL was a miss")
	}

	clock.Advance(2 * time.Second)
	cache.Set("new", Response{StatusCode: http.StatusAccepted})

	if _, ok := cache.Get("stale"); ok {
		t.Error("expired entry was retained")
	}
	if _, ok := cache.Get("live"); !ok {
		t.Error("live least recently used entry was evicted instead of expired entry")
	}
	if _, ok := cache.Get("new"); !ok {
		t.Error("new entry was not retained")
	}
}

func TestCacheReplacingEntryRefreshesValueAndTTL(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(time.Unix(1_700_000_000, 0))
	cache := mustCache(t, time.Minute, 1, clock.Now)
	cache.Set("resource", Response{StatusCode: http.StatusOK, Body: []byte("old")})

	clock.Advance(45 * time.Second)
	cache.Set("resource", Response{StatusCode: http.StatusAccepted, Body: []byte("new")})
	clock.Advance(30 * time.Second)

	response, ok := cache.Get("resource")
	if !ok {
		t.Fatal("Get() after refreshed TTL was a miss")
	}
	if response.StatusCode != http.StatusAccepted || string(response.Body) != "new" {
		t.Errorf("Get() = status %d body %q, want status %d body %q", response.StatusCode, response.Body, http.StatusAccepted, "new")
	}
	if got := cacheEntryCount(cache); got != 1 {
		t.Errorf("entry count after replacement = %d, want 1", got)
	}
}

func TestCacheSupportsConcurrentAccessWithinCapacity(t *testing.T) {
	t.Parallel()

	const (
		maxEntries = 16
		workers    = 100
	)
	cache := mustCache(t, time.Minute, maxEntries, time.Now)
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(workers)

	for worker := range workers {
		go func() {
			defer group.Done()
			<-start

			key := fmt.Sprintf("key-%d", worker%32)
			marker := fmt.Sprintf("worker-%d", worker)
			cache.Set(key, Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"X-Marker": {marker}},
				Body:       []byte(marker),
			})
			if response, ok := cache.Get(key); ok && response.Header.Get("X-Marker") != string(response.Body) {
				t.Errorf("Get(%q) returned inconsistent response: header %q body %q", key, response.Header.Get("X-Marker"), response.Body)
			}
		}()
	}

	close(start)
	group.Wait()

	if got := cacheEntryCount(cache); got > maxEntries {
		t.Errorf("entry count = %d, want at most %d", got, maxEntries)
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ttl        time.Duration
		maxEntries int
		now        func() time.Time
		wantErr    error
	}{
		{name: "zero TTL", maxEntries: 1, now: time.Now, wantErr: ErrInvalidTTL},
		{name: "negative TTL", ttl: -time.Second, maxEntries: 1, now: time.Now, wantErr: ErrInvalidTTL},
		{name: "zero maximum entries", ttl: time.Second, now: time.Now, wantErr: ErrInvalidMaxEntries},
		{name: "negative maximum entries", ttl: time.Second, maxEntries: -1, now: time.Now, wantErr: ErrInvalidMaxEntries},
		{name: "nil clock", ttl: time.Second, maxEntries: 1, wantErr: errNilClock},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := newWithClock(test.ttl, test.maxEntries, test.now)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("newWithClock() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func mustCache(t *testing.T, ttl time.Duration, maxEntries int, now func() time.Time) *Cache {
	t.Helper()
	cache, err := newWithClock(ttl, maxEntries, now)
	if err != nil {
		t.Fatalf("newWithClock() error = %v", err)
	}
	return cache
}

func assertResponse(t *testing.T, response Response, status int, contentType, body string) {
	t.Helper()
	if response.StatusCode != status {
		t.Errorf("status = %d, want %d", response.StatusCode, status)
	}
	if got := response.Header.Get("Content-Type"); got != contentType {
		t.Errorf("Content-Type = %q, want %q", got, contentType)
	}
	if got := string(response.Body); got != body {
		t.Errorf("body = %q, want %q", got, body)
	}
}

func cacheEntryCount(cache *Cache) int {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return len(cache.entries)
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
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}
