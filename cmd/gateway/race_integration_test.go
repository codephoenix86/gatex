package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/codephoenix86/gatex/internal/config"
	"github.com/codephoenix86/gatex/internal/proxy"
)

func TestGatewayEndToEndHandlesConcurrentTrafficAndProbes(t *testing.T) {
	t.Parallel()

	var firstBackendCalls atomic.Int64
	firstBackend := concurrentMockBackend(t, "first", &firstBackendCalls)
	var secondBackendCalls atomic.Int64
	secondBackend := concurrentMockBackend(t, "second", &secondBackendCalls)

	gatewayServer := newEndToEndGateway(t, config.Config{
		ListenAddress: ":8080",
		BackendPools: map[string]config.Pool{
			"workers": {
				Strategy: config.RoundRobin,
				Backends: []config.Backend{
					{URL: firstBackend.URL},
					{URL: secondBackend.URL},
				},
			},
		},
		Routes: []config.Route{{PathPrefix: "/work", BackendPool: "workers"}},
	})

	const (
		requestCount = 200
		probeCount   = 40
	)
	start := make(chan struct{})
	var group sync.WaitGroup
	var failures atomic.Int64
	group.Add(requestCount + probeCount)

	for requestNumber := range requestCount {
		go func() {
			defer group.Done()
			<-start

			request, err := http.NewRequest(http.MethodGet, gatewayServer.URL+"/work/items", nil)
			if err != nil {
				failures.Add(1)
				return
			}
			request.Header.Set(proxy.RequestIDHeader, fmt.Sprintf("concurrent-request-%d", requestNumber))
			response, err := gatewayServer.Client().Do(request)
			if err != nil {
				failures.Add(1)
				return
			}
			body, readErr := io.ReadAll(response.Body)
			closeErr := response.Body.Close()
			if readErr != nil || closeErr != nil || response.StatusCode != http.StatusOK ||
				(string(body) != "first" && string(body) != "second") ||
				response.Header.Get(proxy.RequestIDHeader) != request.Header.Get(proxy.RequestIDHeader) {
				failures.Add(1)
			}
		}()
	}

	for probeNumber := range probeCount {
		go func() {
			defer group.Done()
			<-start

			path := "/readyz"
			wantBody := `{"status":"ready"}`
			if probeNumber%2 == 0 {
				path = "/metrics"
				wantBody = "go_gc_duration_seconds"
			}
			response, err := gatewayServer.Client().Get(gatewayServer.URL + path)
			if err != nil {
				failures.Add(1)
				return
			}
			body, readErr := io.ReadAll(response.Body)
			closeErr := response.Body.Close()
			if readErr != nil || closeErr != nil || response.StatusCode != http.StatusOK || !strings.Contains(string(body), wantBody) {
				failures.Add(1)
			}
		}()
	}

	close(start)
	group.Wait()

	if got := failures.Load(); got != 0 {
		t.Errorf("failed concurrent responses = %d, want 0", got)
	}
	if got := firstBackendCalls.Load(); got != requestCount/2 {
		t.Errorf("first backend calls = %d, want %d", got, requestCount/2)
	}
	if got := secondBackendCalls.Load(); got != requestCount/2 {
		t.Errorf("second backend calls = %d, want %d", got, requestCount/2)
	}
}

func concurrentMockBackend(t *testing.T, name string, calls *atomic.Int64) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if _, err := writer.Write([]byte(name)); err != nil {
			t.Errorf("write concurrent mock backend response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	return server
}
