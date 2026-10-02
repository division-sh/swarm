//go:build unix

package testpostgres

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCapacityProbeFailedStartupCleansUpBeforeRemoval(t *testing.T) {
	const childEnv = "SWARM_CAPACITY_STARTUP_CHILD"
	if root := os.Getenv(childEnv); root != "" {
		probe := &CapacityProbe{bin: root, data: filepath.Join(root, "data"), socket: root, log: filepath.Join(root, "postgres.log")}
		t.Cleanup(func() {
			probe.stop(t)
			if err := os.RemoveAll(probe.data); err != nil {
				t.Error(err)
			}
		})
		probe.start(t, 300)
		t.Fatal("startup fixture must reject readiness")
	}
	for _, shutdown := range []bool{true, false} {
		name := "shutdown_succeeds"
		if !shutdown {
			name = "shutdown_uncertain_retains_data"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			data := filepath.Join(root, "data")
			if err := os.Mkdir(data, 0o700); err != nil {
				t.Fatal(err)
			}
			stopStatus := "0"
			if !shutdown {
				stopStatus = "1"
			}
			// Model pg_ctl launching a server but returning a failed readiness
			// wait. Its independent log proves stop happens before removal.
			script := "#!/bin/sh\nfor arg do operation=$arg; done\necho \"$operation\" >> \"$0.log\"\nif [ \"$operation\" = start ]; then exit 1; fi\nexit " + stopStatus + "\n"
			if err := os.WriteFile(filepath.Join(root, "pg_ctl"), []byte(script), 0o700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCapacityProbeFailedStartupCleansUpBeforeRemoval$")
			command.Env = append(os.Environ(), childEnv+"="+root)
			out, err := command.CombinedOutput()
			if err == nil || ctx.Err() != nil || !strings.Contains(string(out), "private PostgreSQL pg_ctl: exit status 1") {
				t.Fatalf("startup failure was not exercised: %v\n%s", err, out)
			}
			operations, err := os.ReadFile(filepath.Join(root, "pg_ctl.log"))
			if err != nil || string(operations) != "start\nstop\n" {
				t.Fatalf("failed startup cleanup skipped shutdown: %q, %v", operations, err)
			}
			_, err = os.Stat(data)
			if shutdown && !errors.Is(err, os.ErrNotExist) || !shutdown && err != nil {
				t.Fatalf("data retention does not follow confirmed shutdown: %v", err)
			}
		})
	}
}

func TestCapacityProbeQuotedTemporaryPath(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "capacity 'quoted'-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Remove(root); err != nil {
			t.Error(err)
		}
	})
	t.Setenv("TMPDIR", root)
	probe := StartCapacityProbe(t, RequiredMaxConnections)
	connection, err := ParseConnection(probe.DSN)
	if err != nil {
		t.Fatal(err)
	}
	db, err := connection.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := ValidateServerCapacity(context.Background(), db); err != nil {
		t.Fatalf("quoted temporary path did not yield a usable server: %v", err)
	}
}
