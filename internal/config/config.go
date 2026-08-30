// Package config defines Gatex's file-based configuration contract.
package config

import (
	"fmt"
	"math"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	RoundRobin       = "round_robin"
	LeastConnections = "least_connections"
)

// Config is the top-level YAML configuration for one gateway instance.
type Config struct {
	ListenAddress string          `yaml:"listen_address"`
	Timeouts      Timeouts        `yaml:"timeouts"`
	RateLimit     RateLimit       `yaml:"rate_limit"`
	BackendPools  map[string]Pool `yaml:"backend_pools"`
	Routes        []Route         `yaml:"routes"`
}

// Timeouts controls incoming request deadlines and the future proxy transport.
type Timeouts struct {
	Request        time.Duration `yaml:"request"`
	Dial           time.Duration `yaml:"dial"`
	TLSHandshake   time.Duration `yaml:"tls_handshake"`
	ResponseHeader time.Duration `yaml:"response_header"`
	IdleConnection time.Duration `yaml:"idle_connection"`
}

// RateLimit is a token-bucket configuration. Zero values disable a limit.
type RateLimit struct {
	RequestsPerSecond float64 `yaml:"requests_per_second"`
	Burst             int     `yaml:"burst"`
}

// Pool groups interchangeable upstreams and selects a balancing strategy.
type Pool struct {
	Strategy       string         `yaml:"strategy"`
	Backends       []Backend      `yaml:"backends"`
	HealthCheck    HealthCheck    `yaml:"health_check"`
	CircuitBreaker CircuitBreaker `yaml:"circuit_breaker"`
}

// Backend identifies an upstream base URL.
type Backend struct {
	URL string `yaml:"url"`
}

// HealthCheck configures how the gateway determines whether an upstream is
// eligible to receive traffic. Zero values use the balancer's defaults.
type HealthCheck struct {
	Path     string        `yaml:"path"`
	Interval time.Duration `yaml:"interval"`
	Timeout  time.Duration `yaml:"timeout"`
}

// CircuitBreaker controls when upstream failures stop normal traffic to a
// backend pool. A zero failure threshold uses the breaker's default.
type CircuitBreaker struct {
	FailureThreshold int `yaml:"failure_threshold"`
}

// Route maps a path prefix to one named backend pool.
type Route struct {
	PathPrefix  string     `yaml:"path_prefix"`
	BackendPool string     `yaml:"backend_pool"`
	RateLimit   *RateLimit `yaml:"rate_limit,omitempty"`
}

// Load reads, decodes, and validates a YAML configuration file.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate rejects ambiguous routes and invalid upstream or limit settings.
func (c Config) Validate() error {
	if strings.TrimSpace(c.ListenAddress) == "" {
		return fmt.Errorf("listen_address is required")
	}
	if len(c.BackendPools) == 0 {
		return fmt.Errorf("at least one backend_pool is required")
	}
	if err := validateRateLimit("rate_limit", c.RateLimit); err != nil {
		return err
	}

	for name, pool := range c.BackendPools {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("backend pool name cannot be empty")
		}
		if pool.Strategy != RoundRobin && pool.Strategy != LeastConnections {
			return fmt.Errorf("backend pool %q has unsupported strategy %q", name, pool.Strategy)
		}
		if len(pool.Backends) == 0 {
			return fmt.Errorf("backend pool %q must contain at least one backend", name)
		}
		for _, backend := range pool.Backends {
			parsed, err := url.Parse(backend.URL)
			if err != nil || parsed.Scheme == "" || parsed.Host == "" {
				return fmt.Errorf("backend pool %q has invalid backend URL %q", name, backend.URL)
			}
		}
		if err := validateHealthCheck(name, pool.HealthCheck); err != nil {
			return err
		}
		if pool.CircuitBreaker.FailureThreshold < 0 {
			return fmt.Errorf("backend pool %q circuit_breaker failure_threshold cannot be negative", name)
		}
	}

	if len(c.Routes) == 0 {
		return fmt.Errorf("at least one route is required")
	}
	for _, route := range c.Routes {
		if !strings.HasPrefix(route.PathPrefix, "/") {
			return fmt.Errorf("route path_prefix %q must start with /", route.PathPrefix)
		}
		if _, ok := c.BackendPools[route.BackendPool]; !ok {
			return fmt.Errorf("route %q references unknown backend pool %q", route.PathPrefix, route.BackendPool)
		}
		if route.RateLimit != nil {
			if err := validateRateLimit("route rate_limit", *route.RateLimit); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateHealthCheck(poolName string, check HealthCheck) error {
	if check.Interval < 0 {
		return fmt.Errorf("backend pool %q health_check interval cannot be negative", poolName)
	}
	if check.Timeout < 0 {
		return fmt.Errorf("backend pool %q health_check timeout cannot be negative", poolName)
	}
	if check.Path != "" && !strings.HasPrefix(check.Path, "/") {
		return fmt.Errorf("backend pool %q health_check path %q must start with /", poolName, check.Path)
	}
	return nil
}

func validateRateLimit(name string, limit RateLimit) error {
	if math.IsNaN(limit.RequestsPerSecond) || math.IsInf(limit.RequestsPerSecond, 0) {
		return fmt.Errorf("%s requests_per_second must be finite", name)
	}
	if limit.RequestsPerSecond < 0 {
		return fmt.Errorf("%s requests_per_second cannot be negative", name)
	}
	if limit.Burst < 0 {
		return fmt.Errorf("%s burst cannot be negative", name)
	}
	if (limit.RequestsPerSecond == 0) != (limit.Burst == 0) {
		return fmt.Errorf("%s requests_per_second and burst must both be set or both be zero", name)
	}
	return nil
}
