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
	Auth          Auth            `yaml:"auth"`
	CORS          CORS            `yaml:"cors"`
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

// Auth configures credentials accepted by protected routes. Multiple API keys
// allow operators to rotate credentials without downtime.
type Auth struct {
	APIKeys []string `yaml:"api_keys"`
}

// CORS controls which browser origins may call the gateway. An empty
// AllowedOrigins list disables CORS handling.
type CORS struct {
	AllowedOrigins   []string      `yaml:"allowed_origins"`
	AllowedMethods   []string      `yaml:"allowed_methods"`
	AllowedHeaders   []string      `yaml:"allowed_headers"`
	ExposedHeaders   []string      `yaml:"exposed_headers"`
	AllowCredentials bool          `yaml:"allow_credentials"`
	MaxAge           time.Duration `yaml:"max_age"`
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
// backend pool and how recovery probes are admitted. Zero values use the
// breaker's defaults.
type CircuitBreaker struct {
	FailureThreshold    int           `yaml:"failure_threshold"`
	OpenTimeout         time.Duration `yaml:"open_timeout"`
	HalfOpenMaxRequests int           `yaml:"half_open_max_requests"`
}

// Route maps a path prefix to one named backend pool and optionally requires
// gateway authentication.
type Route struct {
	PathPrefix  string     `yaml:"path_prefix"`
	BackendPool string     `yaml:"backend_pool"`
	RateLimit   *RateLimit `yaml:"rate_limit,omitempty"`
	Protected   bool       `yaml:"protected,omitempty"`
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

// Validate rejects invalid routing, upstream, rate-limit, and authentication
// settings.
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
	for index, apiKey := range c.Auth.APIKeys {
		if !isValidAPIKey(apiKey) {
			return fmt.Errorf("auth api_keys[%d] must contain only visible ASCII characters", index)
		}
	}
	if err := validateCORS(c.CORS); err != nil {
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
		if pool.CircuitBreaker.OpenTimeout < 0 {
			return fmt.Errorf("backend pool %q circuit_breaker open_timeout cannot be negative", name)
		}
		if pool.CircuitBreaker.HalfOpenMaxRequests < 0 {
			return fmt.Errorf("backend pool %q circuit_breaker half_open_max_requests cannot be negative", name)
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
		if route.Protected && len(c.Auth.APIKeys) == 0 {
			return fmt.Errorf("protected route %q requires at least one auth.api_keys entry", route.PathPrefix)
		}
		if route.RateLimit != nil {
			if err := validateRateLimit("route rate_limit", *route.RateLimit); err != nil {
				return err
			}
		}
	}
	return nil
}

func isValidAPIKey(apiKey string) bool {
	if apiKey == "" {
		return false
	}
	for _, char := range apiKey {
		if char < 0x21 || char > 0x7e {
			return false
		}
	}
	return true
}

func validateCORS(cors CORS) error {
	if cors.MaxAge < 0 {
		return fmt.Errorf("cors max_age cannot be negative")
	}
	if len(cors.AllowedOrigins) == 0 {
		if len(cors.AllowedMethods) > 0 || len(cors.AllowedHeaders) > 0 || len(cors.ExposedHeaders) > 0 || cors.AllowCredentials || cors.MaxAge > 0 {
			return fmt.Errorf("cors allowed_origins is required when CORS options are configured")
		}
		return nil
	}
	if len(cors.AllowedMethods) == 0 {
		return fmt.Errorf("cors allowed_methods must contain at least one method")
	}

	allowsEveryOrigin := false
	for index, origin := range cors.AllowedOrigins {
		if origin == "*" {
			allowsEveryOrigin = true
			continue
		}
		parsed, err := url.Parse(origin)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return fmt.Errorf("cors allowed_origins[%d] has invalid origin %q", index, origin)
		}
	}
	if allowsEveryOrigin && cors.AllowCredentials {
		return fmt.Errorf("cors allow_credentials cannot be used with wildcard origin")
	}
	for index, method := range cors.AllowedMethods {
		if method == "*" || !isHTTPToken(method) || method != strings.ToUpper(method) {
			return fmt.Errorf("cors allowed_methods[%d] has invalid method %q", index, method)
		}
	}
	for index, header := range cors.AllowedHeaders {
		if !isHTTPToken(header) {
			return fmt.Errorf("cors allowed_headers[%d] has invalid header %q", index, header)
		}
	}
	for index, header := range cors.ExposedHeaders {
		if !isHTTPToken(header) {
			return fmt.Errorf("cors exposed_headers[%d] has invalid header %q", index, header)
		}
	}
	return nil
}

func isHTTPToken(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') {
			continue
		}
		switch char {
		case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
			continue
		default:
			return false
		}
	}
	return true
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
