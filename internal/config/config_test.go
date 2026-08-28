package config

import (
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
