package runforkexecution

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
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
			var selected startupownership.Store
			var construct func() SelectedContractExecutionOwner
			var dsn string
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStore(t)
				selected, dsn = s, s.Path()
				construct = func() SelectedContractExecutionOwner { return newSelectedContractSQLiteExecutionOwnerForTest(t, s) }
			} else {
				dsn = testutil.StartPostgresDSN(t)
				s, _ := storetest.StartPostgresRuntimeStoreWithReopen(t, dsn)
				selected = s
				construct = func() SelectedContractExecutionOwner { return newSelectedContractExecutionOwnerForTest(t, s) }
			}
			checkpoint := killSelectedForkAtCheckpoint(t, backend, dsn, "native_before_activation")
			requireNativeSelectedForkEmit(t, selected, checkpoint.ForkRun)
			state, err := storetest.ReadSelectedExecutionStorage(context.Background(), selected, checkpoint.ForkRun)
			if err != nil || state.State != "quiesced" || state.RunStatus != "paused" {
				t.Fatalf("crash did not retain the unactivated selected execution: state=%+v err=%v", state, err)
			}
			before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(context.Background(), selected)
			if err != nil {
				t.Fatal(err)
			}
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
					requireNativeSelectedForkEmit(t, selected, checkpoint.ForkRun)
					after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
					if err != nil {
						t.Fatal(err)
					}
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
			state, err = storetest.ReadSelectedExecutionStorage(context.Background(), selected, checkpoint.ForkRun)
			if err != nil || state.Occurrences != 1 || state.State != "quiesced" || state.RunStatus != "paused" {
				t.Fatalf("native selected recovery reissued execution or retained executable authority: state=%+v err=%v", state, err)
			}
		})
	}
}

func requireNativeSelectedForkEmit(t *testing.T, selected startupownership.Store, runID string) {
	t.Helper()
	ctx := context.Background()
	emitted, err := storetest.ReadLifecycleEventCardinality(ctx, selected, runID, "worker/task.completed")
	if err != nil {
		t.Fatal(err)
	}
	turnRows, err := storetest.ReadManagedAgentTurnStorage(ctx, selected, runID, "test-agent")
	if err != nil {
		t.Fatal(err)
	}
	var turns []storetest.ManagedAgentTurnStorageRow
	for _, row := range turnRows {
		if row.ExecutionMode == "mock" {
			turns = append(turns, row)
		}
	}
	completion, err := storetest.ReadManagedTurnEffectStorage(ctx, selected, runID, "test-agent")
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := storetest.ReadManagedDeliveryStorage(ctx, selected, runID, "test-agent")
	if err != nil {
		t.Fatal(err)
	}
	// A terminal emit ends this turn. Selected-fork completion does not acquire
	// the normal-delivery response-continuation projection owner.
	if emitted != 1 || len(turns) != 1 || completion.Settled != 1 || completion.Projected != 0 || delivery.Delivered != 1 {
		t.Fatalf("native selected transport cardinality emit=%d turns=%d settled=%d projected=%d delivered=%d", emitted, len(turns), completion.Settled, completion.Projected, delivery.Delivered)
	}
	reader := selected.(interface {
		ListOperatorEvents(context.Context, operatorread.OperatorEventListOptions) (operatorread.OperatorEventListResult, error)
	})
	page, err := reader.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: runID, EventName: "worker/task.completed"}, Limit: 2})
	if err != nil || len(page.Events) != 1 || page.NextCursor != "" {
		t.Fatalf("native selected emit lost its exact cardinality: %+v %v", page, err)
	}
	event := storetest.LoadCanonicalEventRecord(t, ctx, selected, page.Events[0].EventID)
	parent := storetest.LoadCanonicalEventRecord(t, ctx, selected, event.ParentEventID())
	if event.RunID() != runID || parent.RunID() != runID || parent.Type() != "task.assigned" || parent.ID() != turns[0].TriggerEventID {
		t.Fatalf("native selected emit lost its exact fork-local cause: event=%+v parent=%+v err=%v", event, parent, err)
	}
	payloadRaw := event.Payload()
	var payload struct {
		Result string `json:"fork_result"`
	}
	if err := json.Unmarshal(payloadRaw, &payload); err != nil || payload.Result != "selected-native-once" {
		t.Fatalf("native selected emission is not the exact tool output: %s, %v", payloadRaw, err)
	}
	callsRaw, sessionID := turns[0].ToolCalls, turns[0].SessionID
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
