package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testpostgres"
)

func completionHostPreflight(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, tool := range []string{"go", "git"} {
		if _, err := exec.LookPath(tool); err != nil {
			return fmt.Errorf("test preflight requires %s: %w", tool, err)
		}
	}
	capacity, err := testpostgres.RunCapacityFromEnvironment()
	if err != nil {
		return err
	}
	admission, err := testpostgres.DefaultRunAdmission(os.Stderr)
	if err != nil {
		return err
	}
	if err := admission.CheckCapacity(capacity); err != nil {
		return err
	}
	root, err := os.MkdirTemp("", "swarm-preflight-")
	if err != nil {
		return fmt.Errorf("test scratch preflight: %w", err)
	}
	defer os.RemoveAll(root)
	if err := checkCompletionScratchSpace(root, capacity); err != nil {
		return err
	}
	file := filepath.Join(root, "write-probe")
	if err := os.WriteFile(file, []byte("test scratch"), 0600); err != nil {
		return fmt.Errorf("test scratch is not writable: %w", err)
	}
	return nil
}

func completionPlanPreflight(ctx context.Context, plan testplanning.RunPlan) (int, error) {
	capacity, err := testpostgres.RunCapacityFromEnvironment()
	if err != nil {
		return 0, err
	}
	connection, shared, err := testpostgres.ConnectionFromEnvironmentIfSet()
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if shared {
		db, err := connection.Open()
		if err != nil {
			return 0, err
		}
		defer db.Close()
		if err := testpostgres.ValidateServerCapacity(ctx, db); err != nil {
			return 0, err
		}
	} else if output, err := exec.CommandContext(ctx, "docker", "info", "--format", "{{.ServerVersion}}").CombinedOutput(); err != nil {
		return 0, fmt.Errorf("owned test PostgreSQL requires a working Docker daemon: %v (%s)", err, strings.TrimSpace(string(output)))
	}
	if planNeedsNativePostgres(plan) {
		bin := os.Getenv("TEST_POSTGRES_BIN")
		if bin == "" {
			output, err := exec.CommandContext(ctx, "pg_config", "--bindir").Output()
			if err != nil {
				return 0, fmt.Errorf("selected native PostgreSQL proofs require pg_config or TEST_POSTGRES_BIN: %w", err)
			}
			bin = strings.TrimSpace(string(output))
		}
		for _, name := range []string{"initdb", "pg_ctl"} {
			info, err := os.Stat(filepath.Join(bin, name))
			if err != nil || info.Mode()&0111 == 0 {
				return 0, fmt.Errorf("selected native PostgreSQL proof requires executable %s: %v", filepath.Join(bin, name), err)
			}
		}
		// Serve's retained native probes use the same explicitly resolved tools.
		if err := os.Setenv("TEST_POSTGRES_BIN", bin); err != nil {
			return 0, err
		}
		root, err := testpostgres.NewSocketDirectory("swarm-pg-")
		if err != nil {
			return 0, err
		}
		defer os.RemoveAll(root)
		listener, err := net.Listen("unix", filepath.Join(root, ".s.PGSQL.5432"))
		if err != nil {
			return 0, fmt.Errorf("native PostgreSQL socket geometry: %w", err)
		}
		_ = listener.Close()
	}
	fmt.Fprintf(os.Stderr, "test preflight: capacity=%d shared-postgres=%t native-postgres=%t\n", capacity, shared, planNeedsNativePostgres(plan))
	return capacity, nil
}

func planNeedsNativePostgres(plan testplanning.RunPlan) bool {
	for _, unit := range plan.Units {
		for _, root := range unit.SelectedRoots {
			switch root.Name {
			case "TestServePostgresLossAndRestartFromDurableState", "TestServePostgresRemotePossessionOutlivesLocalLoss", "TestServePostgresLostCommitResponseRecoversDurablePublication", "TestServePostgresSilentMonitorWithdrawsReadinessAndJoins", "TestManagerCapacityAdmissionNative", "TestManagerCapacityRefusesExistingResourcesNative", "TestPostgresHelperCapacityAdmissionNative", "TestPostgresManagerCapacityCacheNative", "TestGoldenPostgresStoreCapacityAdmission", "TestCapacityProbeQuotedTemporaryPath":
				return true
			}
		}
	}
	return false
}
