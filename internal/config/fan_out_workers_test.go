package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestLoadFanOutWorkers(t *testing.T) {
	for _, tc := range []struct {
		name, declaration string
		want              int
		wantErr           bool
	}{
		{name: "absent"},
		{name: "one", declaration: "1", want: 1},
		{name: "four", declaration: "4", want: 4},
		{name: "larger", declaration: "64", want: 64},
		{name: "zero", declaration: "0", wantErr: true},
		{name: "negative", declaration: "-1", wantErr: true},
		{name: "null", declaration: "null", wantErr: true},
		{name: "empty", declaration: " ", wantErr: true},
		{name: "string", declaration: "'4'", wantErr: true},
		{name: "float", declaration: "1.5", wantErr: true},
		{name: "integral float", declaration: "4.0", wantErr: true},
		{name: "boolean", declaration: "true", wantErr: true},
		{name: "sequence", declaration: "[4]", wantErr: true},
		{name: "mapping", declaration: "{count: 4}", wantErr: true},
		{name: "overflow", declaration: "18446744073709551616", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := "llm:\n  backend: anthropic\n  session:\n    lock_ttl: 10s\n    rotate_after_turns: 40\n    rotate_on_parse_failures: 3\n"
			if tc.declaration != "" {
				source += "runtime:\n  fan_out_workers: " + tc.declaration + "\n"
			}
			path := filepath.Join(t.TempDir(), "swarm.yaml")
			if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(path)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "runtime.fan_out_workers") {
					t.Fatalf("Load = %v, want worker declaration refusal", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.declaration == "" {
				if cfg.Runtime.FanOutWorkers != nil {
					t.Fatal("absent declaration acquired a backend default during parsing")
				}
			} else if cfg.Runtime.FanOutWorkers == nil || *cfg.Runtime.FanOutWorkers != tc.want {
				t.Fatalf("fan_out_workers = %v, want %d", cfg.Runtime.FanOutWorkers, tc.want)
			}
		})
	}
}

func TestValidateFanOutWorkersDefersBackendCapacity(t *testing.T) {
	for _, backend := range []string{"", "sqlite", "postgres"} {
		for _, workers := range []int{-1, 0, 1, 4, 64} {
			t.Run(fmt.Sprintf("%s/%d", backend, workers), func(t *testing.T) {
				cfg := Config{Runtime: RuntimeConfig{FanOutWorkers: &workers}, Store: StoreConfig{Backend: backend}, Database: DatabaseConfig{PoolSize: 1}}
				err := cfg.ValidateOperationalControls()
				if (err != nil) != (workers <= 0) {
					t.Fatalf("ValidateOperationalControls = %v", err)
				}
				if cfg.Runtime.FanOutWorkers == nil || *cfg.Runtime.FanOutWorkers != workers || cfg.Database.PoolSize != 1 {
					t.Fatal("declaration validation changed worker count or pool size")
				}
			})
		}
	}
}

func TestFanOutWorkersRoundTripPreservesPresence(t *testing.T) {
	zero, four := 0, 4
	for _, workers := range []*int{nil, &zero, &four} {
		raw, err := yaml.Marshal(RuntimeConfig{FanOutWorkers: workers})
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "fan_out_workers:") != (workers != nil) {
			t.Fatalf("presence changed during marshaling: %s", raw)
		}
		var cfg RuntimeConfig
		err = yaml.Unmarshal(raw, &cfg)
		if workers != nil && *workers == 0 {
			if err == nil {
				t.Fatal("explicit zero became an absent default")
			}
			if cfg.FanOutWorkers == nil || *cfg.FanOutWorkers != 0 {
				t.Fatal("validation erased the explicit-zero declaration")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if (cfg.FanOutWorkers == nil) != (workers == nil) || (workers != nil && *cfg.FanOutWorkers != *workers) {
			t.Fatal("round trip changed worker declaration")
		}
	}
}

func TestFanOutWorkersYAMLAliasesAndMerges(t *testing.T) {
	for _, value := range []string{"4", "0", "null", "4.0", "'4'"} {
		for _, source := range []string{
			"workers: &workers " + value + "\nruntime:\n  fan_out_workers: *workers\n",
			"defaults: &defaults\n  fan_out_workers: " + value + "\nruntime:\n  <<: *defaults\n",
		} {
			var cfg Config
			err := yaml.Unmarshal([]byte(source), &cfg)
			if value != "4" {
				if err == nil || !strings.Contains(err.Error(), "runtime.fan_out_workers") {
					t.Fatalf("source %q: error = %v, want worker declaration refusal", source, err)
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Runtime.FanOutWorkers == nil || *cfg.Runtime.FanOutWorkers != 4 {
				t.Fatalf("source %q lost worker declaration", source)
			}
		}
	}
}
