package releasee2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

const numericScatterSource = "tests/tier11-flow-composition/test-numeric-data-scatter-park"

// Establish the approved observable interruption boundary before giving the
// larger corpus any recovery credit. No private runtime hooks stop the pump.
func TestGoldenNumericDataScatterParkRestartBothStores(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s is required for both-store numeric recovery proof", goldenPostgresEnv)
	}
	base := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, base)
	lifecycle := buildOwnedMockLifecycleBinary(t, base)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := filepath.Join(base, backend)
			store := goldenSQLiteStore(root)
			if backend == "postgres" {
				store = goldenPostgresStore(t, dsn)
			}
			writeReleaseFile(t, filepath.Join(root, "go.mod"), "module numeric-hostile-ancestor\n\ngo 1.23.0\n")
			project := filepath.Join(root, "yaml-project")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), numericScatterSource), filepath.Join(project, "contracts"))
			writeReleaseFile(t, filepath.Join(project, ".swarm/swarm.yaml"), goldenRuntimeConfig(store))
			writeReleaseFile(t, filepath.Join(project, "api-token"), goldenAPIToken+"\n")
			env := goldenProcessEnv(t, root, store.passwordEnv, 0)
			assertGoldenProcessHasNoExternalExecutables(t, env)
			writeReleaseFile(t, filepath.Join(root, "home/.config/swarm/swarm.yaml"), "connection:\n  api_token_file: api-token\n")
			cliEnv := make([]string, 0, len(env))
			for _, entry := range env {
				if !strings.HasPrefix(entry, goldenPostgresPass+"=") {
					cliEnv = append(cliEnv, entry)
				}
			}
			verified := runReleaseCommand(t, goldenStartupTimeout, project, env, "", binary,
				"verify", "contracts", "--config", ".swarm/swarm.yaml", "--json")
			if verified.err != nil {
				t.Fatalf("numeric verify: %v\n%s", verified.err, verified.output)
			}
			start := func() *releaseServeProcess {
				p := startReleaseServe(t, releaseProcessSpec{
					BinaryPath: binary, InternalMockLifecycleBinary: lifecycle,
					WorkingDir: project, Source: "contracts", ConfigPath: ".swarm/swarm.yaml",
					Store: backend, TokenFile: "api-token", Token: goldenAPIToken, Env: env,
				})
				ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
				defer cancel()
				if err := p.waitReady(ctx); err != nil {
					t.Fatal(err)
				}
				return p
			}
			p := start()
			runID := uuid.NewString()
			issued := time.Now()
			result := runReleaseCommand(t, goldenStartupTimeout, project, cliEnv, "", binary,
				"run", "start", "--connect", p.apiBase,
				"--bundle-hash", goldenServedBundleHash(t, p.rpc, "mock_only"),
				"--run-id", runID, "--idempotency-key", "numeric-"+runID,
				"--data", "item.registered=contracts/data/items.jsonl", "--no-follow")
			if result.err != nil || !strings.Contains(result.output, "run_id="+runID) {
				t.Fatalf("numeric compiled run start: %v\n%s", result.err, result.output)
			}
			pauseFullLifecycleRun(t, p.rpc, runID)
			ctx, cancel := context.WithTimeout(context.Background(), goldenRunDeadline)
			defer cancel()
			checkpoint, err := readNumericFeed(ctx, p.rpc, runID)
			if err != nil {
				t.Fatalf("public numeric checkpoint: %v\nprocess evidence:\n%s", err, p.output.String())
			}
			predecessor, err := listGoldenEvents(ctx, p.rpc, runID)
			if err != nil {
				t.Fatal(err)
			}
			unsettled := 0
			for _, event := range predecessor {
				for _, delivery := range event.Deliveries {
					if !delivery.Terminal {
						unsettled++
					}
				}
			}
			t.Logf("public pre-kill checkpoint after %s: status=%s feed=%s cursor=%d/%d owed=%d events=%d unsettled=%d", time.Since(issued), "paused", checkpoint.Status, checkpoint.Cursor, checkpoint.Cardinality, checkpoint.Owed, len(predecessor), unsettled)
			if checkpoint.Cardinality != 100 || checkpoint.Status != fanoutobligation.StatusOpen || checkpoint.Cursor >= 100 || checkpoint.Owed == 0 {
				t.Fatalf("approved interrupted checkpoint is unreachable: %#v; no settled-restart credit", checkpoint)
			}
			if err := p.killAndWait(5 * time.Second); err != nil {
				t.Fatal(err)
			}
			p = start()
			var continued any
			if err := p.rpc.call(ctx, "run.continue", map[string]any{"run_id": runID, "idempotency_key": "numeric-continue-" + runID}, &continued); err != nil {
				t.Fatal(err)
			}
			if err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
				feed, err := readNumericFeed(ctx, p.rpc, runID)
				if err != nil {
					return false, err
				}
				return feed.Status == fanoutobligation.StatusClosed && feed.Cursor == 100 && feed.Owed == 0, nil
			}); err != nil {
				t.Fatal(err)
			}
			final, err := listGoldenEvents(ctx, p.rpc, runID)
			if err != nil {
				t.Fatal(err)
			}
			if countGoldenEvents(final, "item.registered") != 100 {
				t.Fatalf("recovered row count = %d, want 100", countGoldenEvents(final, "item.registered"))
			}
			if err := p.stopAndWait(10 * time.Second); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func readNumericFeed(ctx context.Context, rpc *releaseRPCClient, runID string) (fanoutobligation.IntentReadback, error) {
	var page fanoutobligation.ListPage
	if err := rpc.call(ctx, "run.fan_out.list", map[string]any{"run_id": runID, "limit": 1}, &page); err != nil {
		return fanoutobligation.IntentReadback{}, err
	}
	if err := page.Validate(fanoutobligation.ListQuery{RunID: runID, Limit: 1}); err != nil {
		return fanoutobligation.IntentReadback{}, err
	}
	if len(page.Intents) != 1 || page.NextCursor != "" {
		return fanoutobligation.IntentReadback{}, fmt.Errorf("numeric feed must have exactly one intent: %#v", page)
	}
	return page.Intents[0], nil
}
