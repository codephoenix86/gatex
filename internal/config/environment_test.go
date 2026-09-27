package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadExpandsEnvironmentPlaceholders(t *testing.T) {
	t.Setenv("GATEX_TEST_LISTEN_ADDRESS", ":9090")
	t.Setenv("GATEX_TEST_REQUEST_TIMEOUT", "7s")
	t.Setenv("GATEX_TEST_API_KEY", `key:#"value`)
	t.Setenv("GATEX_TEST_BACKEND_HOST", "users.internal")

	path := writeConfigFile(t, `
listen_address: "${GATEX_TEST_LISTEN_ADDRESS}"
timeouts:
  request: "${GATEX_TEST_REQUEST_TIMEOUT}"
auth:
  api_keys:
    - "${GATEX_TEST_API_KEY}"
backend_pools:
  users:
    strategy: round_robin
    backends:
      - url: "http://${GATEX_TEST_BACKEND_HOST}:8080/base?price=$5"
routes:
  - path_prefix: /users
    backend_pool: users
    protected: true
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ListenAddress != ":9090" {
		t.Errorf("listen address = %q, want %q", cfg.ListenAddress, ":9090")
	}
	if cfg.Timeouts.Request != 7*time.Second {
		t.Errorf("request timeout = %s, want %s", cfg.Timeouts.Request, 7*time.Second)
	}
	if got := cfg.Auth.APIKeys; len(got) != 1 || got[0] != `key:#"value` {
		t.Errorf("API keys = %q, want the environment value unchanged", got)
	}
	if got := cfg.BackendPools["users"].Backends[0].URL; got != "http://users.internal:8080/base?price=$5" {
		t.Errorf("backend URL = %q", got)
	}
}

func TestLoadRejectsMissingEnvironmentPlaceholder(t *testing.T) {
	path := writeConfigFile(t, `
listen_address: "${GATEX_CONFIG_TEST_MISSING_7F2A9C}"
backend_pools:
  users:
    strategy: round_robin
    backends:
      - url: http://users.internal:8080
routes:
  - path_prefix: /users
    backend_pool: users
`)

	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), `environment variable "GATEX_CONFIG_TEST_MISSING_7F2A9C" is not set`) {
		t.Fatalf("Load() error = %v, want a missing environment variable error", err)
	}
}

func writeConfigFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
