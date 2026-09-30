package releasee2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

const numericScatterSource = "tests/tier11-flow-composition/test-numeric-data-scatter-park"

// Establish the approved observable interruption boundary before giving the
// larger corpus any recovery credit. No private runtime hooks stop the pump.
func TestGoldenNumericDataScatterParkRestartBothStores(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.ArtifactID("tests/tier11-flow-composition/test-numeric-data-scatter-park"))
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
					ShutdownGrace: runtimepkg.DefaultShutdownGrace,
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
			ctx, cancel := context.WithTimeout(context.Background(), goldenRunDeadline)
			defer cancel()
			if err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
				feed, err := readNumericFeed(ctx, p.rpc, runID)
				if err != nil {
					return false, err
				}
				if feed.Cursor == feed.Cardinality {
					return false, fmt.Errorf("feed finished before a partial checkpoint was observable: %+v", feed)
				}
				return feed.Cursor > 0, nil
			}); err != nil {
				t.Fatalf("public partial-work checkpoint: %v\nprocess evidence:\n%s", err, p.output.String())
			}
			pauseFullLifecycleRun(t, p.rpc, runID)
			checkpoint, err := readNumericFeed(ctx, p.rpc, runID)
			if err != nil {
				t.Fatalf("public numeric checkpoint: %v\nprocess evidence:\n%s", err, p.output.String())
			}
			if checkpoint.Runtime.Availability != "available" || checkpoint.Runtime.Eligible == nil || *checkpoint.Runtime.Eligible || checkpoint.Runtime.Reason != "run_paused" {
				t.Fatalf("runtime-enriched paused readback lost exact admission: %+v", checkpoint.Runtime)
			}
			listed := runReleaseCommand(t, goldenStartupTimeout, project, cliEnv, "", binary,
				"run", "fan-out", "list", runID, "--api-server", p.apiBase, "--api-token-file", "api-token", "--limit", "1", "--json")
			var cliPage fanoutobligation.ListPage
			if listed.err != nil {
				t.Fatalf("compiled fan-out reader: %v\n%s", listed.err, listed.output)
			}
			if err := json.Unmarshal([]byte(strings.TrimSpace(listed.output)), &cliPage); err != nil {
				t.Fatalf("compiled fan-out reader JSON: %v\n%s", err, listed.output)
			}
			if err := cliPage.Validate(fanoutobligation.ListQuery{RunID: runID, Limit: 1}); err != nil || len(cliPage.Intents) != 1 || cliPage.Intents[0].Key != checkpoint.Key || cliPage.Intents[0].BundleHash != checkpoint.BundleHash || cliPage.RunStatus != "paused" {
				t.Fatalf("compiled CLI changed deployment readback: %+v, %v", cliPage, err)
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
			if checkpoint.Cardinality != 100 || checkpoint.Status != fanoutobligation.StatusOpen || checkpoint.Cursor == 0 || checkpoint.Cursor >= 100 || checkpoint.Owed == 0 || unsettled == 0 {
				t.Fatalf("approved interrupted checkpoint is unreachable: %#v; no settled-restart credit", checkpoint)
			}
			beforeRestart := captureFullLifecycleEvidence(t, p.rpc, runID)
			if err := p.killAndWait(5 * time.Second); err != nil {
				t.Fatal(err)
			}
			p = start()
			retained, err := readNumericFeed(ctx, p.rpc, runID)
			if err != nil || retained.Cursor != checkpoint.Cursor || retained.Owed != checkpoint.Owed || retained.Status != checkpoint.Status {
				t.Fatalf("paused restart changed feed: before=%+v after=%+v error=%v\n%s", checkpoint, retained, err, p.output.String())
			}
			waitForFullLifecycleRunStatus(t, p.rpc, runID, "paused")
			afterRestart := captureFullLifecycleEvidence(t, p.rpc, runID)
			for eventID, before := range beforeRestart.EventFacts {
				if after := afterRestart.EventFacts[eventID]; after != before {
					t.Fatalf("paused restart changed event/delivery %s: before=%s after=%s", eventID, before, after)
				}
			}
			for eventID, encoded := range afterRestart.EventFacts {
				if _, existed := beforeRestart.EventFacts[eventID]; existed {
					continue
				}
				var row fullLifecycleEvent
				if err := json.Unmarshal([]byte(encoded), &row); err != nil {
					t.Fatal(err)
				}
				// Active topology can project lifecycle observations while dispatch
				// is parked. Only its canonical non-delivery subtype is exempt.
				if row.EventName != string(events.EventTypePlatformRuntimeLog) || len(row.Deliveries) != 0 || len(row.DeadLetters) != 0 || row.NoDelivery == nil || row.NoDelivery.Reason != events.NoDeliveryNoSubscriberByDesign.Code() {
					t.Fatalf("paused restart produced executable event %s: %s", eventID, encoded)
				}
			}
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
			if err := pollReleaseCondition(ctx, 10*time.Millisecond, func() (bool, error) {
				rows, err := listGoldenEvents(ctx, p.rpc, runID)
				if err != nil {
					return false, err
				}
				completed := 0
				for _, row := range rows {
					if row.EventName != "item.registered" {
						continue
					}
					if len(row.Deliveries) != 1 || row.Deliveries[0].SubscriberType != "node" || len(row.DeadLetters) != 0 {
						return false, fmt.Errorf("wrong intake recipient for %s: %+v", row.EventID, row)
					}
					delivery := row.Deliveries[0]
					if delivery.Status == "dead_letter" {
						return false, fmt.Errorf("intake dead letter: %+v", delivery)
					}
					if delivery.Status == "delivered" && delivery.Terminal {
						completed++
					}
				}
				return completed == 100, nil
			}); err != nil {
				t.Fatalf("numeric intake convergence: %v\n%s", err, p.output.String())
			}
			shutdownStarted := time.Now()
			if err := p.stopAndWait(runtimepkg.DefaultShutdownGrace + 5*time.Second); err != nil {
				t.Fatalf("numeric shutdown: %v\n%s", err, p.output.String())
			}
			t.Logf("100-item shutdown took %s with documented default grace %s", time.Since(shutdownStarted), runtimepkg.DefaultShutdownGrace)
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
