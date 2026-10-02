package pipeline_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/testutil/flowroutefixture"
	"github.com/google/uuid"
)

// M18 exercises persisted sources through actual delivery execution, lifecycle
// planning and selected-store settlement, not fabricated activation requests.
func TestA2MembershipSnapshotValidationOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, tc := range []struct {
			name      string
			members   []any
			mapSource map[string]any
			refuse    bool
		}{
			{name: "duplicate_adjacent", members: []any{"a", "a"}, refuse: true},
			{name: "duplicate_separated", members: []any{"a", "b", "a"}, refuse: true},
			{name: "empty_list", members: []any{}},
			{name: "distinct_exact_text", members: []any{" a ", "a", "a "}},
			{name: "empty_map", members: []any{}, mapSource: map[string]any{}},
			{name: "canonical_exact_map_keys", members: []any{"\t", " ", " a ", "a", "a "},
				mapSource: map[string]any{"a ": "last", "a": "middle", " a ": "third", " ": "second", "\t": "first"}},
		} {
			t.Run(backend.name+"/"+tc.name, func(t *testing.T) {
				selected := backend.open(t)
				files := canonicalrouting.ArrivalJoinRoutingFiles(t, canonicalrouting.ArrivalJoinPayloadDirectedBeforeArm)
				var membership any = tc.members
				if tc.mapSource != nil {
					// Change only the catalog field type before source admission; the
					// canonical ordinary-connect declarations remain unchanged.
					files["orders/entities.yaml"] = strings.Replace(files["orders/entities.yaml"], `expected: "[text]"`, "expected: map[text]text", 1)
					membership = tc.mapSource
				}
				source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, files))
				runID, instanceID := uuid.NewString(), uuid.NewString()
				path := "orders/" + instanceID
				f := newA2MembershipAdmission(t, selected, source, runID, pipeline.WorkflowInstance{
					InstanceID: instanceID, StorageRef: path, EntityID: flowidentity.EntityID(path),
					WorkflowName: "orders", WorkflowVersion: source.WorkflowVersion(), CurrentState: "dispatching",
					EntityType: "order_state", Fields: map[string]any{"order_id": instanceID, "expected": membership},
				})
				before := f.load(t)
				entry, found, err := workflowlifecycle.LoadStageEntry(before.Bookkeeping)
				if err != nil || !found || entry.Stage != "dispatching" || entry.Cause != "construction" ||
					len(a2KnownTargetArms(t, before)) != 0 || !reflect.DeepEqual(before.Fields["expected"], membership) {
					t.Fatalf("source was not actually persisted before arming: instance=%#v entry=%#v err=%v", before, entry, err)
				}
				footprint, commits := f.footprint(t), f.observer.witnesses()
				event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(path+"/dispatch.completed"),
					"operator", "", []byte(`{}`), 0, runID,
					events.EnvelopeForTargetRoute(events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, before.EntityID), path),
						events.RouteIdentity{FlowID: "orders", FlowInstance: path, EntityID: before.EntityID}),
					eventtest.ConcreteTemplateRoutingSource("orders", path, before.EntityID), time.Now().UTC())
				node := externalPipelineSourceNode(t, source, "orders", "dispatcher")
				handlerStatus, deliveryStatus := "completed", "delivered"
				if tc.refuse {
					handlerStatus, deliveryStatus = "failed", "dead_letter"
				}
				if err := f.bus.PublishAcknowledged(f.ctx, event); err != nil {
					t.Fatal(err)
				}
				a2KnownTargetWaitForSettlement(t, f.ctx, f.bus, f.probe, event, node.Key(), handlerStatus, deliveryStatus, f.logger)
				if tc.refuse {
					f.assertUnchanged(t, before, footprint, commits)
					failure := f.failure(t, event.ID(), node.Key())
					if failure.Class != failures.ClassSchemaInvalid || failure.Detail.Code != "join_members_invalid" || !failure.Deterministic ||
						failure.Detail.Attributes["source"] != "state.expected" || failure.Detail.Attributes["row_id"] != "awaiting" {
						t.Fatalf("refusal did not come from membership snapshot validation: %#v", failure)
					}
					return
				}
				after := f.load(t)
				newEntry, found, err := workflowlifecycle.LoadStageEntry(after.Bookkeeping)
				arms := a2KnownTargetArms(t, after)
				members := make([]string, len(tc.members))
				for index, member := range tc.members {
					members[index] = member.(string)
				}
				if err != nil || !found || newEntry == entry || newEntry.Stage != "awaiting" || newEntry.EventID != event.ID() ||
					len(arms) != 1 || !reflect.DeepEqual(arms[0].Members, members) ||
					arms[0].JoinRef().StageEntry() != newEntry || arms[0].Completed() != 0 || len(after.TransitionHistory) != 1 ||
					!reflect.DeepEqual(after.Fields["expected"], membership) {
					t.Fatalf("lawful membership was not atomically armed with the new entry: entry=%#v arms=%#v err=%v", newEntry, arms, err)
				}
				wantStatus, wantEvent := joinruntime.StatusOpen, "platform.join_timeout"
				if len(tc.members) == 0 {
					wantStatus, wantEvent = joinruntime.StatusClosed, "platform.join_complete"
					if arms[0].CloseReason != joinruntime.CloseReasonComplete || !arms[0].OutcomePending || arms[0].OutcomeFired || after.CurrentState != "awaiting" {
						t.Fatalf("zero members did not retain deferred completion: %#v", arms[0])
					}
				}
				var schedules int
				if err := selected.db.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM timers WHERE run_id=$1 AND fire_event=$2", runID, wantEvent).Scan(&schedules); err != nil ||
					schedules != 1 || arms[0].Status != wantStatus || len(f.observer.witnesses()) != len(commits)+1 {
					t.Fatalf("membership entry/arm/schedule was not one selected business commit: schedules=%d status=%s err=%v", schedules, arms[0].Status, err)
				}
			})
		}
		t.Run(backend.name+"/oversize_fan_out_source", func(t *testing.T) {
			selected := backend.open(t)
			repo := pipeline.WorkflowRepoRoot()
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo,
				canonicalrouting.CopyForkFanOutCarrier(t, false, true), contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			source, runID := semanticview.Wrap(bundle), uuid.NewString()
			f := newA2MembershipAdmission(t, selected, source, runID, pipeline.WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: runID, WorkflowName: source.WorkflowName(),
				WorkflowVersion: source.WorkflowVersion(), CurrentState: "pending", EntityType: "root", Fields: map[string]any{},
			})
			before, footprint, commits := f.load(t), f.footprint(t), f.observer.witnesses()
			entry, found, err := workflowlifecycle.LoadStageEntry(before.Bookkeeping)
			if err != nil || !found || entry.Stage != "pending" || entry.Cause != "construction" || len(a2KnownTargetArms(t, before)) != 0 {
				t.Fatalf("oversize source baseline lacks a real unarmed entry: entry=%#v found=%v err=%v", entry, found, err)
			}
			items := make([]string, contracts.DefaultFanOutMaxItems+1)
			for index := range items {
				items[index] = fmt.Sprintf("member-%04d", index)
			}
			payload, err := json.Marshal(map[string]any{"items": items})
			if err != nil {
				t.Fatal(err)
			}
			event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "items.ready", "operator", "", payload, 0, runID,
				events.EnvelopeForEntityID(events.EventEnvelope{}, runID), eventtest.RootRoutingSource(runID), time.Now().UTC())
			if err := f.bus.PublishAcknowledged(f.ctx, event); err != nil {
				t.Fatal(err)
			}
			node := externalPipelineSourceNode(t, source, ".", "fan-out-source")
			a2KnownTargetWaitForSettlement(t, f.ctx, f.bus, f.probe, event, node.Key(), "failed", "dead_letter", f.logger)
			publication, found, err := selected.events.LoadPreparedPublishEvent(f.ctx, event.ID())
			var persisted struct {
				Items []string `json:"items"`
			}
			if err != nil || !found || json.Unmarshal(publication.Event.Event().Payload(), &persisted) != nil || !reflect.DeepEqual(persisted.Items, items) {
				t.Fatalf("oversize source did not reach persisted publication intact: found=%v err=%v", found, err)
			}
			f.assertUnchanged(t, before, footprint, commits)
			failure := f.failure(t, event.ID(), node.Key())
			if failure.Class != failures.ClassFanOutBoundExceeded || failure.Detail.Code != "fan_out_bound" || !failure.Deterministic ||
				fmt.Sprint(failure.Detail.Attributes["actual"]) != "1001" || fmt.Sprint(failure.Detail.Attributes["authored_limit"]) != "0" ||
				fmt.Sprint(failure.Detail.Attributes["effective_limit"]) != "1000" || failure.Detail.Attributes["items_from"] != "payload.items" {
				t.Fatalf("lost exact typed source-bound diagnostic: %#v", failure)
			}
		})
	}
}

type a2MembershipAdmission struct {
	selected gateRecoveryStoreCase
	ctx      context.Context
	owner    flowidentity.RunScopedFlowInstance
	bus      *runtimebus.EventBus
	pc       *pipeline.PipelineCoordinator
	probe    *lifecycleprobe.Probe
	logger   *exactJoinRuntimeLogger
	observer *a2SameCommitObservedPersistence
}

func newA2MembershipAdmission(t *testing.T, selected gateRecoveryStoreCase, source semanticview.Source, runID string, initial pipeline.WorkflowInstance) *a2MembershipAdmission {
	t.Helper()
	insertGateRecoveryRun(t, selected, runID)
	f := &a2MembershipAdmission{selected: selected, owner: testRunScopedWorkflowInstanceForRun(runID, initial.StorageRef),
		ctx:   withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID)),
		probe: lifecycleprobe.New(), logger: &exactJoinRuntimeLogger{},
		observer: &a2SameCommitObservedPersistence{WorkflowPersistenceOwner: selected.events.(pipeline.WorkflowPersistenceOwner)}}
	nodes, err := pipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatal(err)
	}
	f.bus, err = newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source, TestLifecycleProbe: f.probe, Logger: f.logger},
		"platform.join_complete", "platform.join_timeout")
	if err != nil {
		t.Fatal(err)
	}
	schedules, _ := newExactJoinScheduleLifecycleForTest(t, f.ctx, selected, f.bus)
	selected.persistence = pipeline.NewWorkflowPersistence(f.observer)
	f.pc = newGateRecoveryCoordinator(f.bus, selected, pipeline.PipelineCoordinatorOptions{
		Module: proposedEffectProofModule{source: source, nodes: nodes}, GenericSchedules: schedules, TestLifecycleProbe: f.probe,
	})
	f.bus.SetInterceptors(f.pc)
	commitA2FixtureConstruction(t, f.pc, selected.events, f.ctx, f.owner, initial, time.Now().UTC())
	if initial.StorageRef != runID {
		if err := flowroutefixture.Publish(f.bus, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: f.owner}); err != nil {
			t.Fatalf("publish admitted existing receiver: %v", err)
		}
	}
	return f
}

func (f *a2MembershipAdmission) load(t *testing.T) pipeline.WorkflowInstance {
	t.Helper()
	instance, found, err := f.pc.Load(f.ctx, f.owner)
	if err != nil || !found {
		t.Fatalf("read actual membership owner: found=%v err=%v", found, err)
	}
	return instance
}

func (f *a2MembershipAdmission) footprint(t *testing.T) [6]int {
	t.Helper()
	var counts [6]int
	for index, table := range []string{"entity_mutations", "timers", "fan_out_intents", "fan_out_obligation_barriers", "fan_out_outcomes", "events"} {
		query := "SELECT COUNT(*) FROM " + table + " WHERE run_id=$1"
		if table == "events" {
			query += " AND event_name IN ('platform.join_complete','platform.join_timeout','items.child','batch.completed')"
		}
		if err := f.selected.db.QueryRowContext(f.ctx, query, f.owner.RunID).Scan(&counts[index]); err != nil {
			t.Fatalf("read %s refusal footprint: %v", table, err)
		}
	}
	return counts
}

func (f *a2MembershipAdmission) assertUnchanged(t *testing.T, before pipeline.WorkflowInstance, footprint [6]int, commits []a2SameCommitMutationWitness) {
	t.Helper()
	if after := f.load(t); !reflect.DeepEqual(before, after) || f.footprint(t) != footprint || !reflect.DeepEqual(commits, f.observer.witnesses()) {
		t.Fatalf("invalid source leaked state/entry/arm/schedule/history/intent/outcome/emission: before=%#v after=%#v footprint=%v -> %v", before, after, footprint, f.footprint(t))
	}
}

func (f *a2MembershipAdmission) failure(t *testing.T, eventID, nodeKey string) failures.Envelope {
	t.Helper()
	var raw string
	if err := f.selected.db.QueryRowContext(f.ctx, "SELECT CAST(failure AS TEXT) FROM event_deliveries WHERE event_id=$1 AND subscriber_id=$2", eventID, nodeKey).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var failure failures.Envelope
	if err := json.Unmarshal([]byte(raw), &failure); err != nil || failure.SchemaVersion != failures.EnvelopeSchemaVersion || failure.Retryable {
		t.Fatalf("lost durable terminal failure envelope: %s err=%v", raw, err)
	}
	assertExactJoinDeliveryStatus(t, f.selected, f.ctx, eventID, nodeKey, "dead_letter")
	assertExactJoinDeliveryCount(t, f.selected, f.ctx, eventID, nodeKey, 1)
	return failure
}
