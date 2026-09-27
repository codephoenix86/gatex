package main

import "testing"

func TestConfigPathFromEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		set   bool
		want  string
	}{
		{name: "unset", want: defaultConfigPath},
		{name: "empty", value: "", set: true, want: defaultConfigPath},
		{name: "whitespace", value: "  ", set: true, want: defaultConfigPath},
		{name: "configured", value: "/etc/gatex/production.yaml", set: true, want: "/etc/gatex/production.yaml"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			lookup := func(name string) (string, bool) {
				if name != configPathEnvironmentVariable {
					t.Fatalf("lookup name = %q, want %q", name, configPathEnvironmentVariable)
				}
				return test.value, test.set
			}

			if got := configPathFromEnvironment(lookup); got != test.want {
				t.Errorf("config path = %q, want %q", got, test.want)
			}
		})
	}
}
