package config

import (
	"math"
	"strings"
	"testing"
	"time"
)

func validConfig() Config {
	return Config{
		ListenAddress: ":8080",
		RateLimit:     RateLimit{RequestsPerSecond: 10, Burst: 20},
		BackendPools: map[string]Pool{
			"users": {
				Strategy: RoundRobin,
				Backends: []Backend{{URL: "http://users.internal:8080"}},
			},
		},
		Routes: []Route{{PathPrefix: "/users", BackendPool: "users"}},
	}
}

func TestExampleConfigurationLoads(t *testing.T) {
	t.Parallel()

	if _, err := Load("../../configs/gateway.example.yaml"); err != nil {
		t.Fatalf("Load(example configuration) error = %v", err)
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{
			name: "valid configuration",
		},
		{
			name: "valid protected route",
			mutate: func(cfg *Config) {
				cfg.Auth.APIKeys = []string{"current-key", "next-key"}
				cfg.Routes[0].Protected = true
			},
		},
		{
			name: "valid CORS configuration",
			mutate: func(cfg *Config) {
				cfg.CORS = CORS{
					AllowedOrigins: []string{"https://app.example.com"},
					AllowedMethods: []string{"GET", "POST"},
					AllowedHeaders: []string{"Content-Type", "X-API-Key"},
					ExposedHeaders: []string{"X-Request-ID"},
					MaxAge:         10 * time.Minute,
				}
			},
		},
		{
			name: "unknown route pool",
			mutate: func(cfg *Config) {
				cfg.Routes[0].BackendPool = "missing"
			},
			wantErr: "unknown backend pool",
		},
		{
			name: "invalid backend URL",
			mutate: func(cfg *Config) {
				cfg.BackendPools["users"] = Pool{
					Strategy: RoundRobin,
					Backends: []Backend{{URL: "users.internal:8080"}},
				}
			},
			wantErr: "invalid backend URL",
		},
		{
			name: "partial rate limit",
			mutate: func(cfg *Config) {
				cfg.RateLimit = RateLimit{RequestsPerSecond: 10}
			},
			wantErr: "must both be set",
		},
		{
			name: "NaN rate limit",
			mutate: func(cfg *Config) {
				cfg.RateLimit = RateLimit{RequestsPerSecond: math.NaN(), Burst: 1}
			},
			wantErr: "must be finite",
		},
		{
			name: "infinite route rate limit",
			mutate: func(cfg *Config) {
				cfg.Routes[0].RateLimit = &RateLimit{RequestsPerSecond: math.Inf(1), Burst: 1}
			},
			wantErr: "must be finite",
		},
		{
			name: "protected route without API keys",
			mutate: func(cfg *Config) {
				cfg.Routes[0].Protected = true
			},
			wantErr: "requires at least one auth.api_keys entry",
		},
		{
			name: "empty API key",
			mutate: func(cfg *Config) {
				cfg.Auth.APIKeys = []string{""}
			},
			wantErr: "visible ASCII",
		},
		{
			name: "API key containing whitespace",
			mutate: func(cfg *Config) {
				cfg.Auth.APIKeys = []string{"invalid key"}
			},
			wantErr: "visible ASCII",
		},
		{
			name: "CORS options without origins",
			mutate: func(cfg *Config) {
				cfg.CORS.AllowedMethods = []string{"GET"}
			},
			wantErr: "allowed_origins is required",
		},
		{
			name: "CORS configuration without methods",
			mutate: func(cfg *Config) {
				cfg.CORS.AllowedOrigins = []string{"https://app.example.com"}
			},
			wantErr: "allowed_methods must contain",
		},
		{
			name: "invalid CORS origin",
			mutate: func(cfg *Config) {
				cfg.CORS.AllowedOrigins = []string{"app.example.com"}
				cfg.CORS.AllowedMethods = []string{"GET"}
			},
			wantErr: "invalid origin",
		},
		{
			name: "credentialed wildcard CORS origin",
			mutate: func(cfg *Config) {
				cfg.CORS.AllowedOrigins = []string{"*"}
				cfg.CORS.AllowedMethods = []string{"GET"}
				cfg.CORS.AllowCredentials = true
			},
			wantErr: "allow_credentials cannot be used with wildcard",
		},
		{
			name: "lowercase CORS method",
			mutate: func(cfg *Config) {
				cfg.CORS.AllowedOrigins = []string{"*"}
				cfg.CORS.AllowedMethods = []string{"get"}
			},
			wantErr: "invalid method",
		},
		{
			name: "wildcard CORS method",
			mutate: func(cfg *Config) {
				cfg.CORS.AllowedOrigins = []string{"*"}
				cfg.CORS.AllowedMethods = []string{"*"}
			},
			wantErr: "invalid method",
		},
		{
			name: "invalid CORS header",
			mutate: func(cfg *Config) {
				cfg.CORS.AllowedOrigins = []string{"*"}
				cfg.CORS.AllowedMethods = []string{"GET"}
				cfg.CORS.AllowedHeaders = []string{"invalid header"}
			},
			wantErr: "invalid header",
		},
		{
			name: "negative CORS max age",
			mutate: func(cfg *Config) {
				cfg.CORS.MaxAge = -time.Second
			},
			wantErr: "max_age cannot be negative",
		},
		{
			name: "relative health check path",
			mutate: func(cfg *Config) {
				pool := cfg.BackendPools["users"]
				pool.HealthCheck.Path = "healthz"
				cfg.BackendPools["users"] = pool
			},
			wantErr: "health_check path",
		},
		{
			name: "negative health check interval",
			mutate: func(cfg *Config) {
				pool := cfg.BackendPools["users"]
				pool.HealthCheck.Interval = -1
				cfg.BackendPools["users"] = pool
			},
			wantErr: "health_check interval cannot be negative",
		},
		{
			name: "negative circuit breaker failure threshold",
			mutate: func(cfg *Config) {
				pool := cfg.BackendPools["users"]
				pool.CircuitBreaker.FailureThreshold = -1
				cfg.BackendPools["users"] = pool
			},
			wantErr: "circuit_breaker failure_threshold cannot be negative",
		},
		{
			name: "negative circuit breaker open timeout",
			mutate: func(cfg *Config) {
				pool := cfg.BackendPools["users"]
				pool.CircuitBreaker.OpenTimeout = -1
				cfg.BackendPools["users"] = pool
			},
			wantErr: "circuit_breaker open_timeout cannot be negative",
		},
		{
			name: "negative circuit breaker half-open max requests",
			mutate: func(cfg *Config) {
				pool := cfg.BackendPools["users"]
				pool.CircuitBreaker.HalfOpenMaxRequests = -1
				cfg.BackendPools["users"] = pool
			},
			wantErr: "circuit_breaker half_open_max_requests cannot be negative",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig()
			if test.mutate != nil {
				test.mutate(&cfg)
			}

			err := cfg.Validate()
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Validate() error = %v, want error containing %q", err, test.wantErr)
			}
		})
	}
}
