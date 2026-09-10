package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/codephoenix86/gatex/internal/requestmeta"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const unmatchedRoute = "unmatched"

// Recorder owns the process-local Prometheus registry and HTTP measurements.
// Each Recorder is independent, which avoids global collector collisions in
// tests and when Gatex is embedded in another process.
type Recorder struct {
	registry        *prometheus.Registry
	requests        *prometheus.CounterVec
	requestErrors   *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
}

// New creates a recorder with Gatex HTTP, Go runtime, and process collectors.
func New() *Recorder {
	registry := prometheus.NewRegistry()
	requests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gatex",
		Subsystem: "http",
		Name:      "requests_total",
		Help:      "Total number of gateway HTTP requests.",
	}, []string{"method", "route", "status"})
	requestErrors := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "gatex",
		Subsystem: "http",
		Name:      "request_errors_total",
		Help:      "Total number of gateway HTTP requests completed with a 5xx status.",
	}, []string{"method", "route", "status"})
	requestDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "gatex",
		Subsystem: "http",
		Name:      "request_duration_seconds",
		Help:      "Gateway HTTP request latency in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "route", "status"})

	registry.MustRegister(
		requests,
		requestErrors,
		requestDuration,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return &Recorder{
		registry:        registry,
		requests:        requests,
		requestErrors:   requestErrors,
		requestDuration: requestDuration,
	}
}

// RegisterCircuitBreakers adds live, per-pool circuit-breaker states to this
// recorder. A recorder should register at most one source.
func (m *Recorder) RegisterCircuitBreakers(source CircuitBreakerStateSource) {
	m.registry.MustRegister(newCircuitBreakerCollector(source))
}

// Instrument records request count, 5xx count, and latency after next
// completes. Route labels use configured path prefixes rather than raw paths
// to keep Prometheus label cardinality bounded.
func (m *Recorder) Instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		startedAt := time.Now()
		response := &responseWriter{ResponseWriter: writer}

		defer func() {
			recovered := recover()
			status := response.status()
			if recovered != nil && !response.wroteHeader {
				status = http.StatusInternalServerError
			}
			m.observe(request, status, time.Since(startedAt))
			if recovered != nil {
				panic(recovered)
			}
		}()

		next.ServeHTTP(response, request)
	})
}

// Handler returns the scrape endpoint backed by this recorder's registry.
func (m *Recorder) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{EnableOpenMetrics: true})
}

func (m *Recorder) observe(request *http.Request, status int, duration time.Duration) {
	method := metricMethod(request.Method)
	route := requestmeta.Route(request.Context())
	if route == "" {
		route = unmatchedRoute
	}
	statusLabel := strconv.Itoa(status)
	labels := []string{method, route, statusLabel}
	m.requests.WithLabelValues(labels...).Inc()
	m.requestDuration.WithLabelValues(labels...).Observe(duration.Seconds())
	if status >= http.StatusInternalServerError {
		m.requestErrors.WithLabelValues(labels...).Inc()
	}
}

func metricMethod(method string) string {
	switch method {
	case http.MethodConnect,
		http.MethodDelete,
		http.MethodGet,
		http.MethodHead,
		http.MethodOptions,
		http.MethodPatch,
		http.MethodPost,
		http.MethodPut,
		http.MethodTrace:
		return method
	default:
		return "OTHER"
	}
}

type responseWriter struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
}

func (w *responseWriter) WriteHeader(statusCode int) {
	if statusCode >= 100 && statusCode < 200 && statusCode != http.StatusSwitchingProtocols {
		w.ResponseWriter.WriteHeader(statusCode)
		return
	}
	if w.wroteHeader {
		return
	}
	w.statusCode = statusCode
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *responseWriter) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

// Unwrap preserves streaming and connection-control capabilities from the
// underlying response writer.
func (w *responseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *responseWriter) status() int {
	if !w.wroteHeader {
		return http.StatusOK
	}
	return w.statusCode
}
