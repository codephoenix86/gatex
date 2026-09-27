package proxy

import "sync"

const reverseProxyBufferSize = 32 * 1024

type reverseProxyBuffer [reverseProxyBufferSize]byte

// reverseProxyBufferPool reuses the buffers that ReverseProxy needs while
// streaming upstream response bodies to clients.
type reverseProxyBufferPool struct {
	buffers sync.Pool
}

func newReverseProxyBufferPool() *reverseProxyBufferPool {
	return &reverseProxyBufferPool{
		buffers: sync.Pool{
			New: func() any {
				return new(reverseProxyBuffer)
			},
		},
	}
}

func (p *reverseProxyBufferPool) Get() []byte {
	buffer := p.buffers.Get().(*reverseProxyBuffer)
	return buffer[:]
}

func (p *reverseProxyBufferPool) Put(buffer []byte) {
	// Retain only the fixed-size buffers created by this pool. This prevents an
	// unexpectedly large external buffer from increasing retained memory.
	if cap(buffer) != reverseProxyBufferSize {
		return
	}
	buffer = buffer[:reverseProxyBufferSize]
	p.buffers.Put((*reverseProxyBuffer)(buffer))
}

var sharedReverseProxyBufferPool = newReverseProxyBufferPool()
