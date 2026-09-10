package metrics

import (
	"github.com/codephoenix86/gatex/internal/breaker"
	"github.com/prometheus/client_golang/prometheus"
)

var circuitBreakerStates = []breaker.State{
	breaker.StateClosed,
	breaker.StateOpen,
	breaker.StateHalfOpen,
}

// CircuitBreakerStateSource provides a point-in-time state snapshot keyed by
// backend-pool name.
type CircuitBreakerStateSource interface {
	CircuitBreakerStates() map[string]breaker.State
}

type circuitBreakerCollector struct {
	source CircuitBreakerStateSource
	state  *prometheus.Desc
}

func newCircuitBreakerCollector(source CircuitBreakerStateSource) prometheus.Collector {
	return &circuitBreakerCollector{
		source: source,
		state: prometheus.NewDesc(
			prometheus.BuildFQName("gatex", "circuit_breaker", "state"),
			"Current circuit-breaker state as a one-hot gauge.",
			[]string{"pool", "state"},
			nil,
		),
	}
}

func (c *circuitBreakerCollector) Describe(descriptions chan<- *prometheus.Desc) {
	descriptions <- c.state
}

func (c *circuitBreakerCollector) Collect(measurements chan<- prometheus.Metric) {
	for pool, current := range c.source.CircuitBreakerStates() {
		for _, state := range circuitBreakerStates {
			value := 0.0
			if current == state {
				value = 1
			}
			measurements <- prometheus.MustNewConstMetric(
				c.state,
				prometheus.GaugeValue,
				value,
				pool,
				state.String(),
			)
		}
	}
}
