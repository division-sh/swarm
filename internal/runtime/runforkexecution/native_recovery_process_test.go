package runforkexecution

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

type nativeSelectedCrashLoader struct {
	admittedFixtureSelectedContractSourceLoader
}

func (l nativeSelectedCrashLoader) LoadRunForkSelectedContractSourceForRequest(ctx context.Context, req SelectedContractSourceLoadRequest) (LoadedSelectedContractSource, error) {
	loaded, err := l.admittedFixtureSelectedContractSourceLoader.LoadRunForkSelectedContractSourceForRequest(ctx, req)
	if err != nil {
		return loaded, err
	}
	artifact, err := sourceartifact.AdmitDirectory(l.SourceRoot)
	if err != nil {
		return LoadedSelectedContractSource{}, err
	}
	projection, err := sourceartifact.MaterializeRuntimeProjection(artifact)
	if err != nil {
		return LoadedSelectedContractSource{}, err
	}
	loaded.RuntimeProjection, loaded.Cleanup = projection, projection.Release
	return loaded, nil
}

// This is internal retained selected execution, not public serve or paid Claude.
// The child really emits through its native worker and HTTP gateway before
// dying with an unactivated, still-owned selected execution.
func TestSelectedForkNativeEmitProcessDeathBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			var selected startupownership.Store
			var construct func() SelectedContractExecutionOwner
			var dsn string
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				db, selected = storetest.Database(s), s
				var sequence int
				var name string
				if err := db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &name, &dsn); err != nil {
					t.Fatal(err)
				}
				construct = func() SelectedContractExecutionOwner { return newSelectedContractSQLiteExecutionOwnerForTest(t, s) }
			} else {
				dsn, db, _ = testutil.StartPostgres(t)
				s := storetest.AdmitPostgresRuntimeStore(t, db)
				selected = s
				construct = func() SelectedContractExecutionOwner { return newSelectedContractExecutionOwnerForTest(t, s) }
			}
			checkpoint := killSelectedForkAtCheckpoint(t, backend, dsn, "native_before_activation")
			requireNativeSelectedForkEmit(t, db, checkpoint.ForkRun)
			var state, status string
			if err := db.QueryRow(`SELECT e.state,r.status FROM run_fork_selected_contract_runtime_executions e JOIN runs r ON r.run_id=e.fork_run_id WHERE r.run_id=$1`, checkpoint.ForkRun).Scan(&state, &status); err != nil || state != "quiesced" || status != "paused" {
				t.Fatalf("crash did not retain the unactivated selected execution: state=%s status=%s err=%v", state, status, err)
			}
			before := selectedPreparationDatabaseSnapshot(t, db, backend)
			for i := 0; i < 2; i++ {
				t.Run("restart-"+strconv.Itoa(i+1), func(t *testing.T) {
					ctx := runForkTestContext(t)
					capability := selectedContractTestProcessCapability(t, ctx, selected)
					owner := construct()
					process, _ := worklifetime.ProcessFromContext(ctx)
					if err := owner.BindSelectedProcess(ctx, process, capability); err != nil {
						t.Fatal(err)
					}
					recovered, err := owner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly), SelectedForkRecoveryEnvironment{})
					if err != nil || len(recovered) != 1 || recovered[0].RunID != checkpoint.ForkRun || recovered[0].Disposition != runfork.SelectedForkRecoveryControlOnly {
						t.Fatalf("native selected recovery: %+v err=%v", recovered, err)
					}
					requireNativeSelectedForkEmit(t, db, checkpoint.ForkRun)
					after := selectedPreparationDatabaseSnapshot(t, db, backend)
					for _, table := range []string{"events", "entity_state", "entity_mutations", "run_fork_selected_contract_executions"} {
						if !reflect.DeepEqual(before[table], after[table]) {
							t.Errorf("native selected recovery repeated business work in %s", table)
						}
					}
					if err := owner.RetireSelectedContexts(ctx); err != nil {
						t.Fatal(err)
					}
				})
			}
			var generations int
			if err := db.QueryRow(`SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, checkpoint.ForkRun).Scan(&generations); err != nil || generations != 1 {
				t.Fatalf("native selected recovery reissued execution: count=%d err=%v", generations, err)
			}
			if err := db.QueryRow(`SELECT e.state,r.status FROM run_fork_selected_contract_runtime_executions e JOIN runs r ON r.run_id=e.fork_run_id WHERE r.run_id=$1`, checkpoint.ForkRun).Scan(&state, &status); err != nil || state != "quiesced" || status != "paused" {
				t.Fatalf("native selected recovery did not withdraw executable authority: state=%s status=%s err=%v", state, status, err)
			}
		})
	}
}

func requireNativeSelectedForkEmit(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	var emitted, turns, settled, projected, delivered int
	for _, query := range []struct {
		text string
		into *int
	}{
		{`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='worker/task.completed'`, &emitted},
		{`SELECT COUNT(*) FROM agent_turns WHERE run_id=$1 AND agent_id='test-agent' AND execution_mode='mock'`, &turns},
		{`SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN agent_turns t ON t.completion_attempt_id=a.attempt_id WHERE t.run_id=$1 AND t.agent_id='test-agent' AND a.state='settled'`, &settled},
		{`SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN agent_turns t ON t.completion_attempt_id=a.attempt_id WHERE t.run_id=$1 AND a.completion_projection_phase IS NOT NULL`, &projected},
		{`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND subscriber_type='agent' AND subscriber_id='test-agent' AND status='delivered'`, &delivered},
	} {
		if err := db.QueryRow(query.text, runID).Scan(query.into); err != nil {
			t.Fatal(err)
		}
	}
	// A terminal emit ends this turn. Selected-fork completion does not acquire
	// the normal-delivery response-continuation projection owner.
	if emitted != 1 || turns != 1 || settled != 1 || projected != 0 || delivered != 1 {
		t.Fatalf("native selected transport cardinality emit=%d turns=%d settled=%d projected=%d delivered=%d", emitted, turns, settled, projected, delivered)
	}
	var payloadRaw, callsRaw []byte
	var sessionID string
	if err := db.QueryRow(`SELECT e.payload FROM events e JOIN events p ON p.event_id=e.source_event_id WHERE e.run_id=$1 AND p.run_id=$1 AND e.event_name='worker/task.completed' AND p.event_name='task.assigned'`, runID).Scan(&payloadRaw); err != nil {
		t.Fatalf("native selected emit lost its exact fork-local cause: %v", err)
	}
	var payload struct {
		Result string `json:"fork_result"`
	}
	if err := json.Unmarshal(payloadRaw, &payload); err != nil || payload.Result != "selected-native-once" {
		t.Fatalf("native selected emission is not the exact tool output: %s, %v", payloadRaw, err)
	}
	if err := db.QueryRow(`SELECT tool_calls,session_id FROM agent_turns WHERE run_id=$1 AND agent_id='test-agent' AND execution_mode='mock'`, runID).Scan(&callsRaw, &sessionID); err != nil {
		t.Fatal(err)
	}
	var calls []llm.ToolCall
	if err := json.Unmarshal(callsRaw, &calls); err != nil || len(calls) != 1 || calls[0].Name != "emit_task_completed" || sessionID == "" {
		t.Fatalf("native selected completion lost its exact call/session: %s, %q, %v", callsRaw, sessionID, err)
	}
	arguments, ok := calls[0].Arguments.(map[string]any)
	if !ok || arguments["fork_result"] != payload.Result {
		t.Fatalf("native selected event disagrees with its captured call: %+v", calls[0])
	}
}

func nativeSelectedForkCrashFixture(t *testing.T, repo string) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "contracts")
	if err := os.CopyFS(root, os.DirFS(filepath.Join(repo, "internal/runtime/runforkexecution/testdata/selected_fork_flow_scoped_mcp"))); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "worker", "mocks"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, contents := range map[string]string{
		"agents.yaml": "test-agent:\n  role: test-agent\n  model: regular\n  intent: prompts/test-agent.md\n  subscriptions: [task.assigned]\n  emit_events: [task.completed]\n  mock:\n    kind: python\n    module: mocks/worker.py\n",
		"mocks/worker.py": `def handle(input):
    results = input["tool_results"] or []
    if not results:
        return {"calls": [{"name": "emit_task_completed", "arguments": {"fork_result": "selected-native-once"}}], "usage": {"input_tokens": 1, "output_tokens": 1}}
    return {"text": "selected-native-once committed", "usage": {"input_tokens": 1, "output_tokens": 1}}
`,
	} {
		if err := os.WriteFile(filepath.Join(root, "worker", name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}
