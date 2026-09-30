package releasee2e

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// H retains the selected database across real child generations. The controls
// themselves are compiled CLI/authenticated RPC, not private store calls.
func TestStandingPauseAuthorityPublicBothStores(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s is required for both-store standing authority proof", goldenPostgresEnv)
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
			spec := prepareFullLifecycleProject(t, binary, root, store, false)
			spec.InternalMockLifecycleBinary = lifecycle
			// Keep deployment configuration out of the CLI's structural project
			// configuration, just as in the numeric public journey.
			writeReleaseFile(t, filepath.Join(spec.WorkingDir, ".swarm/swarm.yaml"), fullLifecycleRuntimeConfig(store, false))
			if err := os.Remove(filepath.Join(spec.WorkingDir, spec.ConfigPath)); err != nil {
				t.Fatal(err)
			}
			spec.ConfigPath = ".swarm/swarm.yaml"
			cliEnv := make([]string, 0, len(spec.Env))
			for _, entry := range spec.Env {
				if !strings.HasPrefix(entry, goldenPostgresPass+"=") {
					cliEnv = append(cliEnv, entry)
				}
			}
			start := func() *releaseServeProcess {
				p := startReleaseServe(t, spec)
				ctx, cancel := context.WithTimeout(context.Background(), fullLifecycleStartupLimit)
				defer cancel()
				if err := p.waitReady(ctx); err != nil {
					t.Fatal(err)
				}
				return p
			}
			p := start()
			bundle := requireFullLifecycleHealth(t, p.rpc)
			standing := waitForFullLifecycleStandingRun(t, p.rpc, bundle, "", 0, "")
			card := waitForFullLifecycleCard(t, p.rpc, standing.RunID, "lifecycle_ready")
			decideFullLifecycleCard(t, p.rpc, card, "standing-initial-"+standing.RunID)
			assertFullLifecycleCardDecided(t, p.rpc, card.CardID)
			cli := func(args ...string) releaseCommandResult {
				writeReleaseFile(t, filepath.Join(spec.WorkingDir, "operator.yaml"), "connection:\n  api_server: "+strconv.Quote(p.apiBase)+"\n  api_token_file: api-token\n")
				return runReleaseCommand(t, fullLifecycleStartupLimit, spec.WorkingDir, cliEnv, "", binary, append([]string{"--config", "operator.yaml"}, args...)...)
			}
			assertRollback := func(command, runID, status, key string) {
				t.Helper()
				beforeRun := waitForFullLifecycleRunStatus(t, p.rpc, runID, status)
				before := captureFullLifecycleEvidence(t, p.rpc, runID)
				withStandingOperatorWriteFault(t, root, store, func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					var result any
					if err := p.rpc.call(ctx, "standing."+command, map[string]any{"service_id": standing.Origin.ServiceID, "idempotency_key": key}, &result); err == nil {
						t.Fatalf("injected %s failure was acknowledged", command)
					}
				})
				if !reflect.DeepEqual(beforeRun, waitForFullLifecycleRunStatus(t, p.rpc, runID, status)) || !reflect.DeepEqual(before, captureFullLifecycleEvidence(t, p.rpc, runID)) {
					t.Fatalf("failed %s changed predecessor public facts", command)
				}
			}
			assertRollback("suspend", standing.RunID, "running", "failed-active-suspend")
			result := cli("standing", "suspend", standing.Origin.ServiceID, "--reason", "operator-custom-suspension", "--idempotency-key", "standing-suspend")
			if result.err != nil || !strings.Contains(result.output, "state=suspended run="+standing.RunID) {
				t.Fatalf("compiled standing suspend: %v\n%s", result.err, result.output)
			}
			assertRefusal := func(runID, key string) {
				t.Helper()
				beforeRun := waitForFullLifecycleRunStatus(t, p.rpc, runID, "paused")
				before := captureFullLifecycleEvidence(t, p.rpc, runID)
				result := cli("control", "continue", runID, "--idempotency-key", key+"-cli")
				var exit *exec.ExitError
				if !errors.As(result.err, &exit) || exit.ExitCode() != 3 || !strings.Contains(result.output, "RUN_NOT_PAUSED") {
					t.Fatalf("compiled continue did not refuse standing authority: %v\n%s", result.err, result.output)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				var ignored any
				if err := p.rpc.call(ctx, "run.continue", map[string]any{"run_id": runID, "idempotency_key": key + "-rpc"}, &ignored); err == nil || !strings.Contains(err.Error(), "RUN_NOT_PAUSED") {
					t.Fatalf("authenticated continue did not refuse standing authority: %v", err)
				}
				afterRun := waitForFullLifecycleRunStatus(t, p.rpc, runID, "paused")
				after := captureFullLifecycleEvidence(t, p.rpc, runID)
				if !reflect.DeepEqual(beforeRun, afterRun) || !reflect.DeepEqual(before, after) {
					t.Fatalf("refused continue mutated public facts: run=%+v -> %+v events=%+v -> %+v", beforeRun, afterRun, before, after)
				}
			}
			assertRefusal(standing.RunID, "suspended")
			assertFreshNoop := func(command, runID, key string) {
				t.Helper()
				before := captureFullLifecycleEvidence(t, p.rpc, runID)
				beforeRun := waitForFullLifecycleRunStatus(t, p.rpc, runID, map[string]string{"suspend": "paused", "resume": "running"}[command])
				result := cli("standing", command, standing.Origin.ServiceID, "--idempotency-key", key+"-cli")
				if result.err != nil || !strings.Contains(result.output, "run="+runID) {
					t.Fatalf("fresh compiled %s: %v\n%s", command, result.err, result.output)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				var response struct {
					RunID string `json:"run_id"`
				}
				if err := p.rpc.call(ctx, "standing."+command, map[string]any{"service_id": standing.Origin.ServiceID, "idempotency_key": key + "-rpc"}, &response); err != nil || response.RunID != runID {
					t.Fatalf("fresh authenticated %s: response=%+v err=%v", command, response, err)
				}
				afterRun := waitForFullLifecycleRunStatus(t, p.rpc, runID, beforeRun.Status)
				if !reflect.DeepEqual(beforeRun, afterRun) || !reflect.DeepEqual(before, captureFullLifecycleEvidence(t, p.rpc, runID)) {
					t.Fatalf("fresh %s changed public authority or domain facts", command)
				}
			}
			assertFreshNoop("suspend", standing.RunID, "repeat-suspend")
			assertRollback("reset", standing.RunID, "paused", "failed-no-child-reset")
			ctx, cancel := context.WithTimeout(context.Background(), fullLifecycleRunLimit)
			defer cancel()
			var reset struct {
				ServiceID      string `json:"service_id"`
				RunID          string `json:"run_id"`
				Generation     int64  `json:"generation"`
				EffectiveState string `json:"effective_state"`
			}
			if err := p.rpc.call(ctx, "standing.reset", map[string]any{"service_id": standing.Origin.ServiceID, "idempotency_key": "standing-reset"}, &reset); err != nil {
				t.Fatalf("standing reset: %v\n%s", err, p.output.String())
			}
			if reset.ServiceID != standing.Origin.ServiceID || reset.Generation != standing.Origin.Generation+1 || reset.RunID == standing.RunID || reset.EffectiveState != "suspended" {
				t.Fatalf("reset did not preserve the suspension in one exact successor: %+v", reset)
			}
			assertRefusal(reset.RunID, "reset-suspended")
			firstReset := reset
			if err := p.rpc.call(ctx, "standing.reset", map[string]any{"service_id": reset.ServiceID, "idempotency_key": "standing-reset-second"}, &reset); err != nil {
				t.Fatalf("second suspended reset: %v", err)
			}
			if reset.Generation != firstReset.Generation+1 || reset.RunID == firstReset.RunID || reset.EffectiveState != "suspended" {
				t.Fatalf("second reset did not create one suspended successor: %+v -> %+v", firstReset, reset)
			}
			assertRefusal(reset.RunID, "second-reset-suspended")
			assertFreshNoop("suspend", reset.RunID, "repeat-after-reset")
			beforeRestart := waitForFullLifecycleRunStatus(t, p.rpc, reset.RunID, "paused")
			beforeFacts := captureFullLifecycleEvidence(t, p.rpc, reset.RunID)
			if err := p.stopAndWait(15 * time.Second); err != nil {
				t.Fatalf("suspended shutdown: %v\n%s", err, p.output.String())
			}
			p = start()
			restored := waitForFullLifecycleStandingRun(t, p.rpc, bundle, reset.ServiceID, reset.Generation, reset.RunID)
			// Reconciliation owns the run-control diagnostic, not semantic N.
			beforeRestart.ControlReason = "standing_suspended"
			if !reflect.DeepEqual(beforeRestart, restored) || !reflect.DeepEqual(beforeFacts, captureFullLifecycleEvidence(t, p.rpc, reset.RunID)) {
				t.Fatalf("restart changed suspended generation: before=%+v after=%+v", beforeRestart, restored)
			}
			assertRefusal(reset.RunID, "retained-suspended")
			retainedReset := reset
			if err := p.rpc.call(ctx, "standing.reset", map[string]any{"service_id": reset.ServiceID, "idempotency_key": "standing-reset-retained"}, &reset); err != nil {
				t.Fatalf("retained suspended reset: %v", err)
			}
			if reset.Generation != retainedReset.Generation+1 || reset.RunID == retainedReset.RunID || reset.EffectiveState != "suspended" {
				t.Fatalf("retained reset did not create one suspended successor: %+v -> %+v", retainedReset, reset)
			}
			restored = waitForFullLifecycleRunStatus(t, p.rpc, reset.RunID, "paused")
			assertRefusal(reset.RunID, "retained-reset-suspended")
			result = cli("standing", "resume", reset.ServiceID, "--reason", "operator-resume", "--idempotency-key", "standing-resume")
			if result.err != nil || !strings.Contains(result.output, "state=active run="+reset.RunID) {
				t.Fatalf("compiled standing resume: %v\n%s", result.err, result.output)
			}
			resumed := waitForFullLifecycleRunStatus(t, p.rpc, reset.RunID, "running")
			if resumed.Origin != restored.Origin || !resumed.StartedAt.Equal(restored.StartedAt) {
				t.Fatalf("standing resume replaced generation identity: before=%+v after=%+v", restored, resumed)
			}
			card = waitForFullLifecycleCard(t, p.rpc, reset.RunID, "lifecycle_ready")
			decideFullLifecycleCard(t, p.rpc, card, "standing-resumed-"+reset.RunID)
			assertFullLifecycleCardDecided(t, p.rpc, card.CardID)
			assertFreshNoop("resume", reset.RunID, "repeat-resume")
			var replay standingRuntimePublicResult
			if err := p.rpc.call(ctx, "standing.reset", map[string]any{"service_id": reset.ServiceID, "idempotency_key": "standing-reset"}, &replay); err != nil {
				t.Fatalf("original reset replay: %v", err)
			}
			if replay.ServiceID != firstReset.ServiceID || replay.RunID != firstReset.RunID || replay.Generation != firstReset.Generation || replay.EffectiveState != firstReset.EffectiveState {
				t.Fatalf("historical replay lost its original response: replay=%+v first=%+v", replay, firstReset)
			}
			if current := waitForFullLifecycleStandingRun(t, p.rpc, bundle, reset.ServiceID, reset.Generation, reset.RunID); current.RunID != reset.RunID {
				t.Fatal("keyed replay replaced the current generation")
			}
			receipt := sendFullLifecycleTelegramUpdate(t, p, 7001, 42)
			approveFullLifecycleEffect(t, p.rpc, reset.RunID, "standing-resumed-turn")
			waitForFullLifecycleConvergence(t, p, reset.RunID, 1)
			requireFullLifecycleReceiptEvents(t, p.rpc, reset.RunID, 7001, "42", receipt)
			if err := p.stopAndWait(15 * time.Second); err != nil {
				t.Fatalf("resumed shutdown: %v\n%s", err, p.output.String())
			}
		})
	}
}

func withStandingOperatorWriteFault(t *testing.T, root string, store goldenStoreSelection, run func()) {
	t.Helper()
	db := store.diagnosticDB
	if store.name == "sqlite" {
		var err error
		db, err = sql.Open("sqlite", filepath.Join(root, "runtime.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if store.name == "sqlite" {
		if _, err := db.ExecContext(ctx, `CREATE TRIGGER fail_standing_operator BEFORE UPDATE ON standing_services BEGIN SELECT RAISE(ABORT, 'injected standing operator failure'); END`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := db.ExecContext(context.Background(), `DROP TRIGGER fail_standing_operator`); err != nil {
				t.Error(err)
			}
		}()
	} else {
		if _, err := db.ExecContext(ctx, `CREATE FUNCTION fail_standing_operator() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected standing operator failure'; END $$`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := db.ExecContext(context.Background(), `DROP FUNCTION fail_standing_operator()`); err != nil {
				t.Error(err)
			}
		}()
		if _, err := db.ExecContext(ctx, `CREATE TRIGGER fail_standing_operator BEFORE UPDATE ON standing_services FOR EACH ROW EXECUTE FUNCTION fail_standing_operator()`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := db.ExecContext(context.Background(), `DROP TRIGGER fail_standing_operator ON standing_services`); err != nil {
				t.Error(err)
			}
		}()
	}
	run()
}
