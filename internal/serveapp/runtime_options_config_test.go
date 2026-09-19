package serveapp

import (
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
)

func TestConfiguredFanOutWorkersSnapshotsDeclarationWithoutResolvingCapacity(t *testing.T) {
	for _, backend := range []string{"", "sqlite", "postgres"} {
		for _, count := range []int{1, 4, 64} {
			t.Run(fmt.Sprintf("%s/%d", backend, count), func(t *testing.T) {
				declared := count
				cfg := &config.Config{Runtime: config.RuntimeConfig{FanOutWorkers: &declared}, Store: config.StoreConfig{Backend: backend}, Database: config.DatabaseConfig{PoolSize: 1}}
				workers, err := configuredFanOutWorkers(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if workers == nil || *workers != count || workers == cfg.Runtime.FanOutWorkers {
					t.Fatalf("workers = %v, want independent declaration %d", workers, count)
				}
				if cfg.Database.PoolSize != 1 {
					t.Fatal("mapping resized the declared pool")
				}
				declared++
				if *workers != count {
					t.Fatal("later config mutation changed boot options")
				}
			})
		}
	}
}

func TestConfiguredFanOutWorkersPreservesAbsenceAndRejectsInvalid(t *testing.T) {
	workers, err := configuredFanOutWorkers(&config.Config{})
	if err != nil || workers != nil {
		t.Fatalf("absent declaration = %v, %v; want nil, nil", workers, err)
	}
	if _, err := configuredFanOutWorkers(nil); err == nil {
		t.Fatal("missing config was accepted")
	}
	for _, count := range []int{0, -1} {
		cfg := &config.Config{Runtime: config.RuntimeConfig{FanOutWorkers: &count}}
		workers, err := configuredFanOutWorkers(cfg)
		if err == nil || !strings.Contains(err.Error(), "runtime.fan_out_workers") || workers != nil {
			t.Fatalf("invalid %d declaration = %v, %v; want refusal", count, workers, err)
		}
		if cfg.Runtime.FanOutWorkers == nil || *cfg.Runtime.FanOutWorkers != count {
			t.Fatal("mapping erased the explicit invalid declaration")
		}
	}
}
