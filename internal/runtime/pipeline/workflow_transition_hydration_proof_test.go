package pipeline

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
)

func TestCompiledTransitionPersistedCoordinatesAndTimerCauseOnBothStores(t *testing.T) {
	bundle := loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml": `name: transition-hydration
stages:
  ready: {initial: true}
  waiting:
    timers:
      - {id: deadline, after: 1h, advances_to: done}
  done: {terminal: true}
`,
		"entities.yaml": "test_entity:\n  marker: text\n",
		"events.yaml":   "work: {}\n",
		"nodes.yaml": `router:
  execution_type: system_node
  event_handlers:
    work:
      guard: {id: admitted-check, check: "true", on_fail: reject}
      rules:
        - {id: unselected, condition: "false", advances_to: waiting}
        - {id: selected, condition: else, advances_to: waiting}
`,
	})
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newCompiledAdapterFixture(t, backend, bundle, ".", "ready", true)
			event := f.event("work")
			result, err := f.execute("work", event)
			if err != nil {
				t.Fatal(err)
			}
			before, found := f.load()
			if !found || before.CurrentState != "waiting" || len(before.TransitionHistory) != 1 {
				t.Fatalf("selected transition missing: %#v", before)
			}
			record := before.TransitionHistory[0]
			if record.TriggerEventID != event.ID() || record.From != "ready" || record.To != "waiting" ||
				!reflect.DeepEqual(record.GuardsEvaluated, []string{"admitted-check"}) ||
				!record.Evidence.RuleSelection().Ref().Equal(result.RuleSelection.Ref()) || result.RuleSelection.DisplayLabel() != "selected" {
				t.Fatalf("lost executed rule/guard/event coordinates: %#v", record)
			}
			timers := listWorkflowTimerOwnerActivations(t, f.store, f.ctx, f.entityID, true)
			if len(timers) != 1 || timers[0].Ref.Cause != timeridentity.WorkflowTimerActivationCauseTransition {
				t.Fatalf("transition entry timer = %#v", timers)
			}
			timer := timers[0]
			wantTimerID := timeridentity.WorkflowTimerActivationID(correlation.RunIDFromContext(f.ctx), f.entityID, f.path,
				timer.Ref.DeclarationKey, timer.Ref.DeclarationRevision, string(timeridentity.WorkflowTimerActivationCauseTransition), timer.Ref.Generation.KeySuffix(),
				event.ID(), string(event.Type()), record.Evidence.ID(), "ready", "waiting")
			if timer.Ref.ActivationID != wantTimerID {
				t.Fatalf("timer does not carry the actual transition cause: got %s want %s", timer.Ref.ActivationID, wantTimerID)
			}
			restarted := newPostgresWorkflowInstanceStoreForTest(f.db)
			if backend == "sqlite" {
				restarted = newSQLiteWorkflowInstanceStoreForTest(t, f.db)
			}
			route := testWorkflowInstanceRoute(f.path)
			reloaded, found, err := restarted.Load(f.ctx, testRunScopedWorkflowInstanceFromContext(f.ctx, route.InstancePath))
			if err != nil || !found || !reflect.DeepEqual(before.TransitionHistory, reloaded.TransitionHistory) {
				t.Fatalf("restart changed selected evidence: %v, %v, %#v", found, err, reloaded)
			}
			selectSQL := "SELECT config FROM flow_instances WHERE instance_path = ? AND run_id = ?"
			updateSQL := "UPDATE flow_instances SET config = ? WHERE instance_path = ? AND run_id = ?"
			if backend == "postgres" {
				selectSQL = "SELECT config FROM flow_instances WHERE instance_path = $1 AND run_id = $2"
				updateSQL = "UPDATE flow_instances SET config = $1::jsonb WHERE instance_path = $2 AND run_id = $3"
			}
			var original []byte
			if err := f.db.QueryRowContext(f.ctx, selectSQL, f.path, correlation.RunIDFromContext(f.ctx)).Scan(&original); err != nil {
				t.Fatal(err)
			}
			// Change actual stored wire coordinates, not an already-validated Go carrier.
			for _, tc := range []struct {
				name  string
				path  []string
				value any
			}{
				{"flow", []string{"evidence", "evidence", "compiled", "Flow"}, "foreign"},
				{"node", []string{"evidence", "evidence", "compiled", "Node"}, "foreign"},
				{"handler", []string{"evidence", "evidence", "compiled", "HandlerEvent"}, "other"},
				{"carrier", []string{"evidence", "evidence", "compiled", "AdvanceCarrier"}, "handler.advances_to"},
				{"rule_reference", []string{"evidence", "evidence", "compiled", "RuleRef"}, ""},
				{"source", []string{"evidence", "evidence", "compiled", "From"}, "foreign"},
				{"target", []string{"evidence", "evidence", "compiled", "To"}, "foreign"},
				{"loop", []string{"evidence", "evidence", "compiled", "LoopID"}, "foreign"},
				{"timer", []string{"evidence", "evidence", "compiled", "TimerID"}, "foreign"},
				{"gate", []string{"evidence", "evidence", "compiled", "DecisionID"}, "foreign"},
				{"selected_context", []string{"evidence", "evidence", "selection_context"}, "on_complete"},
				{"selected_rule", []string{"evidence", "evidence", "selected_rule"}, ""},
				{"evaluated_guards", []string{"evidence", "evidence", "guards"}, []string{"never-evaluated"}},
				{"format", []string{"evidence", "evidence", "format"}, 9000},
				{"missing_evidence", []string{"evidence"}, nil},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var config map[string]any
					if err := json.Unmarshal(original, &config); err != nil {
						t.Fatal(err)
					}
					row := config["transition_history"].([]any)[0].(map[string]any)
					for _, key := range tc.path[:len(tc.path)-1] {
						row = row[key].(map[string]any)
					}
					key := tc.path[len(tc.path)-1]
					if reflect.DeepEqual(row[key], tc.value) {
						t.Fatal("hostile mutation would not change persisted coordinate")
					}
					row[key] = tc.value
					hostile, err := json.Marshal(config)
					if err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(hostile, &config); err != nil {
						t.Fatal(err)
					}
					if _, err := f.db.ExecContext(f.ctx, updateSQL, string(hostile), f.path, correlation.RunIDFromContext(f.ctx)); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if _, err := f.db.ExecContext(f.ctx, updateSQL, string(original), f.path, correlation.RunIDFromContext(f.ctx)); err != nil {
							t.Error(err)
						}
					})
					if _, _, err := restarted.Load(f.ctx, testRunScopedWorkflowInstanceFromContext(f.ctx, route.InstancePath)); err == nil {
						t.Fatal("hydration accepted corrupted transition evidence")
					}
					var after []byte
					if err := f.db.QueryRowContext(f.ctx, selectSQL, f.path, correlation.RunIDFromContext(f.ctx)).Scan(&after); err != nil {
						t.Fatal(err)
					}
					var afterConfig map[string]any
					if err := json.Unmarshal(after, &afterConfig); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(config, afterConfig) {
						t.Fatal("failed hydration repaired or changed persisted data")
					}
					if got := listWorkflowTimerOwnerActivations(t, f.store, f.ctx, f.entityID, true); !reflect.DeepEqual(got, timers) {
						t.Fatal("failed hydration changed timer ownership")
					}
				})
			}
			final, _ := f.load()
			if !reflect.DeepEqual(before, final) {
				t.Fatal("restored source state changed across hostile hydration")
			}
		})
	}
}
