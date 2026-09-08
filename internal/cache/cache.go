package cache

import (
	"container/list"
	"errors"
	"net/http"
	"sync"
	"time"
)

var (
	// ErrInvalidTTL indicates that cached entries would not have a positive
	// lifetime.
	ErrInvalidTTL = errors.New("cache TTL must be greater than zero")

	// ErrInvalidMaxEntries indicates that the cache would not have room for an
	// entry.
	ErrInvalidMaxEntries = errors.New("cache max entries must be greater than zero")

	errNilClock = errors.New("cache clock cannot be nil")
)

// Response is the replayable portion of an upstream HTTP response. Cache
// copies response headers and body bytes when values enter and leave the cache,
// so callers cannot mutate shared cache state.
type Response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// Cache is a fixed-capacity, least-recently-used response cache. Entries share
// one configured TTL and expire lazily during lookups. Cache is safe for use by
// concurrent request goroutines.
type Cache struct {
	mu         sync.Mutex
	ttl        time.Duration
	maxEntries int
	entries    map[string]*entry
	recency    *list.List
	now        func() time.Time
}

type entry struct {
	key       string
	response  Response
	expiresAt time.Time
	element   *list.Element
}

// New creates an empty response cache with a fixed TTL and entry limit.
func New(ttl time.Duration, maxEntries int) (*Cache, error) {
	return newWithClock(ttl, maxEntries, time.Now)
}

func newWithClock(ttl time.Duration, maxEntries int, now func() time.Time) (*Cache, error) {
	if ttl <= 0 {
		return nil, ErrInvalidTTL
	}
	if maxEntries <= 0 {
		return nil, ErrInvalidMaxEntries
	}
	if now == nil {
		return nil, errNilClock
	}

	return &Cache{
		ttl:        ttl,
		maxEntries: maxEntries,
		entries:    make(map[string]*entry, maxEntries),
		recency:    list.New(),
		now:        now,
	}, nil
}

// Get returns an independent copy of the response stored for key. A hit makes
// the entry most recently used. Expired entries are removed and reported as
// misses.
func (c *Cache) Get(key string) (Response, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cached := c.entries[key]
	if cached == nil {
		return Response{}, false
	}
	if !c.now().Before(cached.expiresAt) {
		c.remove(cached)
		return Response{}, false
	}

	c.recency.MoveToFront(cached.element)
	return cloneResponse(cached.response), true
}

// Set stores an independent copy of response under key. Replacing an existing
// key refreshes its TTL and makes it most recently used. At capacity, Set
// evicts the least recently used entry.
func (c *Cache) Set(key string, response Response) {
	stored := cloneResponse(response)

	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	expiresAt := now.Add(c.ttl)

	if cached := c.entries[key]; cached != nil {
		cached.response = stored
		cached.expiresAt = expiresAt
		c.recency.MoveToFront(cached.element)
		return
	}
	if len(c.entries) >= c.maxEntries {
		c.removeExpired(now)
	}

	cached := &entry{
		key:       key,
		response:  stored,
		expiresAt: expiresAt,
	}
	cached.element = c.recency.PushFront(cached)
	c.entries[key] = cached

	if len(c.entries) > c.maxEntries {
		oldest := c.recency.Back()
		c.remove(oldest.Value.(*entry))
	}
}

func (c *Cache) remove(cached *entry) {
	delete(c.entries, cached.key)
	c.recency.Remove(cached.element)
}

func (c *Cache) removeExpired(now time.Time) {
	for element := c.recency.Back(); element != nil; {
		previous := element.Prev()
		cached := element.Value.(*entry)
		if !now.Before(cached.expiresAt) {
			c.remove(cached)
		}
		element = previous
	}
}

func cloneResponse(response Response) Response {
	return Response{
		StatusCode: response.StatusCode,
		Header:     response.Header.Clone(),
		Body:       append([]byte(nil), response.Body...),
	}
}
