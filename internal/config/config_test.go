package config

import (
	"math"
	"strings"
	"testing"
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
