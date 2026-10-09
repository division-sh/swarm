package pipeline_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestWorkflowCurrentTransitionNativeBytesAndHistoricalCutsBothStores(t *testing.T) {
	bundle := loadPipelineLifecycleFixtureBundle(t, map[string]string{
		"schema.yaml":   "name: current-transition\nstages:\n  left: {}\n  right: {}\n",
		"entities.yaml": "test_entity:\n  marker: text\n",
		"events.yaml":   "step.left:\nstep.right:\nobserve:\nfield.write:\n",
		"nodes.yaml": `router:
  execution_type: system_node
  event_handlers:
    step.left: {advances_to: left}
    step.right: {advances_to: right}
    observe: {}
    field.write:
      data_accumulation:
        writes: [{target_field: marker, value: "changed"}]
`,
	})
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, _, reopen := openTimerReplayNativeStore(t, backend)
			ctx := testAuthorActivityContext(t, context.Background())
			runID := uuid.NewString()
			at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
			storetest.RequireRunningRun(t, ctx, selected, runID, at)
			ctx = withLiveGateExecution(correlation.WithRunID(ctx, runID))
			source := semanticview.Wrap(bundle)
			bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatal(err)
			}
			nodes, err := pipeline.LoadWorkflowNodes(source)
			if err != nil {
				t.Fatal(err)
			}
			coordinator := newTimerReplayCoordinator(t, bus, selected, pipeline.PipelineCoordinatorOptions{Module: proposedEffectProofModule{source: source, nodes: nodes}})
			bus.SetInterceptors(coordinator)
			owner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: flowidentity.StoredRoute(".", runID, runID)}
			entityID := flowidentity.EntityID(runID)
			plan := commitA2FixtureConstruction(t, coordinator, selected, ctx, owner, pipeline.WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(),
				EntityType: "test_entity", CurrentState: "left", Fields: map[string]any{"marker": "initial"},
			}, at)
			admitAttachment, _ := newTimerReplayAttachmentOwner(t, ctx, selected)
			admitAttachment(plan.Readiness)
			publish := func(name string, index int) events.Event {
				t.Helper()
				event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(name), "operator", "", []byte(`{}`), 0,
					runID, events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
					eventtest.RootRoutingSource(entityID), at.Add(-time.Duration(index)*time.Second))
				if err := bus.PublishAndWait(ctx, event); err != nil {
					t.Fatalf("publish %s at %d: %v", name, index, err)
				}
				return event
			}
			load := func() pipeline.WorkflowInstance {
				t.Helper()
				instance, found, err := selected.LoadWorkflowInstance(ctx, owner)
				if err != nil || !found {
					t.Fatalf("native current header: found=%v error=%v", found, err)
				}
				return instance
			}
			planner := selected.(interface {
				PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
			})
			cuts := map[int]runfork.RunForkPlan{}
			var twoRecordHeaderBytes, twoRecordMetadataBytes int
			for index := 0; index <= 200; index++ {
				if index > 0 {
					name, from, to := "step.right", "left", "right"
					if index%2 == 0 {
						name, from, to = "step.left", "right", "left"
					}
					before := load()
					event := publish(name, index)
					after := load()
					if len(after.TransitionHistory) == 0 {
						t.Fatalf("native append %d lost transition evidence", index)
					}
					last := after.TransitionHistory[len(after.TransitionHistory)-1]
					if after.Revision != before.Revision+1 || last.TriggerEventID != event.ID() || last.From != from || last.To != to || !last.FiredAt.Equal(event.CreatedAt()) {
						t.Fatalf("native append %d lost exact backdated evidence: %+v", index, after)
					}
				}
				if index != 0 && index != 1 && index != 2 && index != 20 && index != 200 {
					continue
				}
				before := load()
				if index > 0 && len(before.TransitionHistory) != 1 {
					t.Errorf("native current evidence grew to %d records at %d transitions", len(before.TransitionHistory), index)
				}
				probe := publish("observe", index)
				if after := load(); !reflect.DeepEqual(before.TransitionHistory, after.TransitionHistory) {
					t.Fatal("no-op update changed current evidence")
				}
				cut, err := planner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: probe.ID()})
				if err != nil || len(cut.Entities) != 1 || cut.Entities[0].MaterializationMetadata == nil {
					t.Fatalf("fixed-cut native metadata: %+v %v", cut, err)
				}
				metadata := cut.Entities[0].MaterializationMetadata
				var header struct {
					History []pipeline.WorkflowTransitionRecord `json:"transition_history"`
				}
				if err := json.Unmarshal(metadata.FlowConfig, &header); err != nil || !reflect.DeepEqual(header.History, before.TransitionHistory) {
					t.Fatalf("metadata did not copy current evidence at %d: %+v %v", index, header, err)
				}
				record, err := selected.LoadWorkflowTargetPersistence(ctx, owner, identity.NormalizeEntityID(entityID))
				if err != nil {
					t.Fatal(err)
				}
				if index == 2 {
					twoRecordHeaderBytes = len(record.Lifecycle.Config)
				} else if index > 2 && len(record.Lifecycle.Config) != twoRecordHeaderBytes {
					t.Errorf("equal-sized native evidence grew live header: %d bytes, want %d", len(record.Lifecycle.Config), twoRecordHeaderBytes)
				}
				physical, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
				if err != nil {
					t.Fatal(err)
				}
				factBytes := currentTransitionMetadataFactBytes(t, physical["run_fork_fact_revisions"], runID, entityID, before.TransitionHistory)
				if index == 2 {
					twoRecordMetadataBytes = factBytes
				} else if index > 2 && factBytes > twoRecordMetadataBytes+8 {
					// Updated-at fractional precision may vary; the retained
					// transition has identical size at all these even cuts.
					t.Errorf("native metadata grew with trajectory: %d bytes, two-transition baseline %d", factBytes, twoRecordMetadataBytes)
				}
				t.Logf("backend=%s transitions=%d live_header_bytes=%d recorded_flow_config_bytes=%d latest_metadata_fact_bytes=%d", backend, index, len(record.Lifecycle.Config), len(metadata.FlowConfig), factBytes)
				cuts[index] = cut
			}
			latest := load()
			publish("field.write", 201)
			if after := load(); after.Fields["marker"] != "changed" || !reflect.DeepEqual(after.TransitionHistory, latest.TransitionHistory) {
				t.Fatalf("field-only write lost current transition: %+v", after)
			}
			cold := reopen()
			restarted, found, err := cold.LoadWorkflowInstance(ctx, owner)
			if err != nil || !found || !reflect.DeepEqual(restarted.TransitionHistory, latest.TransitionHistory) {
				t.Fatalf("cold hydration changed latest evidence: %+v %v", restarted, err)
			}
			for index, cut := range cuts {
				again, err := planner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: cut.ForkPoint.EventID})
				if err != nil || !reflect.DeepEqual(again.Entities, cut.Entities) || again.ForkPoint.Revision != cut.ForkPoint.Revision {
					t.Fatalf("later source progression changed cut %d: %+v %v", index, again, err)
				}
			}
		})
	}
}

// Byte observation only: semantic cut selection/reconstruction above is owned
// by PlanRunFork. Observe its actual immutable metadata copies, never fold them.
func currentTransitionMetadataFactBytes(t *testing.T, table storetest.SelectedForkStorageTableSnapshot, runID, entityID string, history []pipeline.WorkflowTransitionRecord) int {
	t.Helper()
	columns := map[string]int{}
	for index, name := range table.Columns {
		columns[name] = index
	}
	for _, name := range []string{"run_id", "family", "fact_key", "fact"} {
		if _, found := columns[name]; !found {
			t.Fatalf("metadata byte witness omitted %s", name)
		}
	}
	var bytes int
	for _, row := range table.Rows {
		var values []any
		if err := json.Unmarshal([]byte(row), &values); err != nil {
			t.Fatal(err)
		}
		if values[columns["run_id"]] != runID || values[columns["family"]] != "entity_metadata" || values[columns["fact_key"]] != entityID {
			continue
		}
		raw, ok := values[columns["fact"]].(string)
		if !ok {
			t.Fatalf("physical metadata bytes unavailable: %s", row)
		}
		var fact struct {
			Config struct {
				History []pipeline.WorkflowTransitionRecord `json:"transition_history"`
			} `json:"flow_config"`
		}
		if err := json.Unmarshal([]byte(raw), &fact); err != nil {
			t.Fatal(err)
		}
		if reflect.DeepEqual(fact.Config.History, history) {
			bytes = len(raw)
		}
	}
	if bytes == 0 {
		t.Fatalf("no exact native metadata byte witness for %d current records", len(history))
	}
	return bytes
}
