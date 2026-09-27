package proxy

import (
	"net/http"
	"net/url"
	"sync"
	"testing"
)

func TestReverseProxyUsesSharedBufferPool(t *testing.T) {
	t.Parallel()

	proxy := newReverseProxy(&url.URL{Scheme: "http", Host: "backend.internal"}, http.DefaultTransport)
	if proxy.BufferPool != sharedReverseProxyBufferPool {
		t.Error("reverse proxy does not use the shared response buffer pool")
	}
}

func TestReverseProxyBufferPoolReturnsFixedSizeBuffers(t *testing.T) {
	t.Parallel()

	pool := newReverseProxyBufferPool()
	buffer := pool.Get()
	if len(buffer) != reverseProxyBufferSize {
		t.Errorf("buffer length = %d, want %d", len(buffer), reverseProxyBufferSize)
	}
	if cap(buffer) != reverseProxyBufferSize {
		t.Errorf("buffer capacity = %d, want %d", cap(buffer), reverseProxyBufferSize)
	}
	pool.Put(buffer)
}

func TestReverseProxyBufferPoolIgnoresUnexpectedBuffers(t *testing.T) {
	t.Parallel()

	pool := newReverseProxyBufferPool()
	pool.Put(nil)
	pool.Put(make([]byte, reverseProxyBufferSize/2))
	pool.Put(make([]byte, reverseProxyBufferSize, reverseProxyBufferSize*2))

	buffer := pool.Get()
	if len(buffer) != reverseProxyBufferSize || cap(buffer) != reverseProxyBufferSize {
		t.Fatalf("buffer size = %d/%d, want %d/%d", len(buffer), cap(buffer), reverseProxyBufferSize, reverseProxyBufferSize)
	}
}

func TestReverseProxyBufferPoolSupportsConcurrentUse(t *testing.T) {
	t.Parallel()

	pool := newReverseProxyBufferPool()
	var workers sync.WaitGroup
	for worker := byte(0); worker < 64; worker++ {
		workers.Add(1)
		go func(value byte) {
			defer workers.Done()
			for range 100 {
				buffer := pool.Get()
				buffer[0] = value
				buffer[len(buffer)-1] = value
				pool.Put(buffer)
			}
		}(worker)
	}
	workers.Wait()
}
