package runforkpersistence

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

func a2ForkEntry(t *testing.T, runID, entityID, instancePath, stage, occurrence string) timeridentity.StageEntryRef {
	t.Helper()
	route := flowidentity.RouteForInstancePath(instancePath)
	if entityID == runID && instancePath == runID {
		route = flowidentity.StoredRoute(".", runID, runID)
	}
	entry := timeridentity.StageEntryRef{RunID: runID, FlowScope: route.ScopeKey, InstanceID: route.InstanceID, InstancePath: route.InstancePath,
		EntityID: entityID, Stage: stage, Cause: "delivery", EventID: "event-" + occurrence, OccurrenceID: "delivery-" + occurrence, TransitionID: "transition-" + occurrence}
	if err := entry.Validate(); err != nil {
		t.Fatal(err)
	}
	return entry
}

func a2ForkEntity(t *testing.T, entry timeridentity.StageEntryRef, accumulator map[string]any) runfork.RunForkEntityState {
	t.Helper()
	bookkeeping := map[string]any{"business": map[string]any{"nested": []any{"unchanged"}}}
	if err := workflowlifecycle.StoreStageEntry(bookkeeping, entry); err != nil {
		t.Fatal(err)
	}
	return runfork.RunForkEntityState{EntityID: entry.EntityID, CurrentState: entry.Stage, Bookkeeping: bookkeeping, Accumulator: accumulator}
}

func a2ForkJoinRef(t *testing.T, entry timeridentity.StageEntryRef, generation attemptgeneration.Generation) timeridentity.JoinRef {
	t.Helper()
	declarationScope := entry.FlowScope
	if entry.EntityID == entry.RunID && entry.InstancePath == entry.RunID {
		declarationScope = "."
	}
	node, err := identity.AdmitExecutableNodeDeclaration(declarationScope, "collector")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := timeridentity.NewJoinRef(node, "result", entry.Stage, "join")
	if err != nil {
		t.Fatal(err)
	}
	ref, err = ref.BindStageEntry(entry, generation)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestA2ForkStageEntryProjectsExactOwnersAndNestedOrigin(t *testing.T) {
	for _, owner := range []struct{ name, entityID, path string }{{"root", "source", "source"}, {"nonroot", "entity", "review/one"}} {
		t.Run(owner.name, func(t *testing.T) {
			source := a2ForkEntry(t, "source", owner.entityID, owner.path, "draft", "original")
			before := source
			projection, err := runfork.ProjectEntityOwnership("source", "child", owner.entityID, owner.path)
			if err != nil {
				t.Fatal(err)
			}
			child, err := projectRunForkStageEntry(source, "source", "child", projection)
			want := source
			want.RunID, want.OriginRunID, want.EntityID = "child", "source", projection.Fork.EntityID
			if owner.name == "root" {
				want.FlowScope, want.InstanceID, want.InstancePath = ".", "child", "child"
			}
			if err != nil || child != want || source != before {
				t.Fatalf("exact owner/history projection = %#v, want %#v: %v", child, want, err)
			}
			projection, err = runfork.ProjectEntityOwnership("child", "grandchild", child.EntityID, child.InstancePath)
			if err != nil {
				t.Fatal(err)
			}
			grandchild, err := projectRunForkStageEntry(child, "child", "grandchild", projection)
			want.RunID, want.EntityID = "grandchild", projection.Fork.EntityID
			if owner.name == "root" {
				want.FlowScope, want.InstanceID, want.InstancePath = ".", "grandchild", "grandchild"
			}
			if err != nil || grandchild != want || child.OriginRunID != "source" {
				t.Fatalf("nested fork invented a fresh occurrence/origin: %#v %v", grandchild, err)
			}
		})
	}
}

func TestA2ForkStageEntryRejectsHostileOwnership(t *testing.T) {
	for _, hostile := range []string{"source_run", "source_entity", "source_path", "root_scope", "root_instance", "origin_self", "origin_space", "origin_destination", "fork_entity", "fork_path"} {
		t.Run(hostile, func(t *testing.T) {
			source := a2ForkEntry(t, "source", "source", "source", "draft", "original")
			projection, err := runfork.ProjectEntityOwnership("source", "child", "source", "source")
			if err != nil {
				t.Fatal(err)
			}
			switch hostile {
			case "source_run":
				source.RunID = "foreign"
			case "source_entity":
				source.EntityID = "foreign"
			case "source_path":
				source.InstancePath = "foreign/one"
			case "root_scope":
				source.FlowScope = "source"
			case "root_instance":
				source.InstanceID = "foreign"
			case "origin_self":
				source.OriginRunID = "source"
			case "origin_space":
				source.OriginRunID = " ancestor "
			case "origin_destination":
				source.OriginRunID = "child"
			case "fork_entity":
				projection.Fork.EntityID = "foreign"
			case "fork_path":
				projection.Fork.FlowInstance = "foreign/one"
			}
			before := source
			if _, err := projectRunForkStageEntry(source, "source", "child", projection); err == nil {
				t.Fatal("accepted hostile ownership/history")
			}
			if source != before {
				t.Fatal("refused projection changed source")
			}
		})
	}
}

func TestA2ForkEntryProjectionPreservesExactOldClosedArm(t *testing.T) {
	for _, hostile := range []string{"exact", "missing_old_arm", "foreign_arm_owner", "foreign_generation", "root_declaration", "missing_bookkeeping", "current_state"} {
		t.Run(hostile, func(t *testing.T) {
			now := time.Unix(100, 0).UTC()
			loop, err := loopruntime.New("source", "source", "", "review", "revision", "start", "draft", 4, now)
			if err != nil {
				t.Fatal(err)
			}
			oldGeneration := loop.Generation()
			oldEntry := a2ForkEntry(t, "source", "source", "source", "draft", "old")
			old, err := joinruntime.NewActivation(a2ForkJoinRef(t, oldEntry, oldGeneration), []string{"a", "b"}, nil, now, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := old.Add("a", map[string]any{"payload": map[string]any{"event_id": "business"}}); err != nil {
				t.Fatal(err)
			}
			old.Status, old.CloseReason, old.TimerCancelled, old.OutcomePending = joinruntime.StatusClosed, joinruntime.CloseReasonUntil, true, true
			if _, err := loop.Repeat("draft", "repeat", now.Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			newEntry := a2ForkEntry(t, "source", "source", "source", "draft", "new")
			newArm, err := joinruntime.NewActivation(a2ForkJoinRef(t, newEntry, loop.Generation()), []string{"a", "b"}, nil, now.Add(time.Second), time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if err := loop.Close("done", "close", now.Add(2*time.Second)); err != nil {
				t.Fatal(err)
			}
			buckets := map[string]map[string]any{}
			for _, arm := range []joinruntime.Activation{old, newArm} {
				if err := joinruntime.Store(buckets, arm); err != nil {
					t.Fatal(err)
				}
			}
			if err := loopruntime.Store(buckets, loop); err != nil {
				t.Fatal(err)
			}
			switch hostile {
			case "missing_old_arm":
				delete(buckets["handler_joins:"+old.JoinRef().Node().Key()]["handler_joins"].(map[string]any), old.Key())
			case "foreign_arm_owner":
				foreign := oldEntry
				foreign.EntityID = "never-owned"
				delete(buckets["handler_joins:"+old.JoinRef().Node().Key()]["handler_joins"].(map[string]any), old.Key())
				ref, err := old.JoinRef().Declaration().BindStageEntry(foreign, oldGeneration)
				if err != nil {
					t.Fatal(err)
				}
				old, err = old.WithForkReference(ref)
				if err != nil {
					t.Fatal(err)
				}
				if err := joinruntime.Store(buckets, old); err != nil {
					t.Fatal(err)
				}
			case "foreign_generation":
				foreign := oldGeneration
				foreign.ActivationID = "never-admitted"
				delete(buckets["handler_joins:"+old.JoinRef().Node().Key()]["handler_joins"].(map[string]any), old.Key())
				old, err = old.WithForkReference(a2ForkJoinRef(t, oldEntry, foreign))
				if err != nil {
					t.Fatal(err)
				}
				if err := joinruntime.Store(buckets, old); err != nil {
					t.Fatal(err)
				}
			case "root_declaration":
				node, err := identity.AdmitExecutableNodeDeclaration("foreign", "collector")
				if err != nil {
					t.Fatal(err)
				}
				ref, err := timeridentity.NewJoinRef(node, "result", "draft", "join")
				if err != nil {
					t.Fatal(err)
				}
				ref, err = ref.BindStageEntry(oldEntry, oldGeneration)
				if err != nil {
					t.Fatal(err)
				}
				foreignArm, err := joinruntime.NewActivation(ref, []string{"a", "b"}, nil, now, time.Time{})
				if err != nil {
					t.Fatal(err)
				}
				if err := joinruntime.Store(buckets, foreignArm); err != nil {
					t.Fatal(err)
				}
			}
			entity := a2ForkEntity(t, newEntry, runtimeengine.NewStateCarrier(nil, nil, buckets).PersistedStateBuckets())
			if hostile == "missing_bookkeeping" {
				delete(entity.Bookkeeping, "stage_entry")
			}
			if hostile == "current_state" {
				entity.CurrentState = "different"
			}
			projection, err := runfork.ProjectEntityOwnership("source", "child", "source", "source")
			if err != nil {
				t.Fatal(err)
			}
			before := projectionJSON(t, entity)
			bookkeeping, accumulator, correspondence, err := projectRunForkEntityExecutionState(entity, "source", "child", projection)
			if projectionJSON(t, entity) != before {
				t.Fatal("entry/arm projection mutated source evidence")
			}
			if hostile != "exact" && hostile != "missing_old_arm" {
				if err == nil {
					t.Fatal("hostile retained state borrowed another owner/generation")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			entry, found, err := workflowlifecycle.LoadStageEntry(bookkeeping)
			if err != nil || !found || entry.OccurrenceID != newEntry.OccurrenceID || entry.OriginRunID != "source" || entry.FlowScope != "." {
				t.Fatalf("current bookkeeping projection = %#v %v", entry, err)
			}
			admitted, err := correspondence.AdmitSource(oldGeneration)
			if err != nil {
				t.Fatal(err)
			}
			pair, err := correspondence.Bind(admitted)
			if err != nil {
				t.Fatal(err)
			}
			carrier, err := runtimeengine.StateCarrierFromPersisted(nil, nil, nil, accumulator)
			if err != nil {
				t.Fatal(err)
			}
			arms, err := joinruntime.List(carrier.StateBuckets)
			wantArms, wantOld := 2, 1
			if hostile == "missing_old_arm" {
				wantArms, wantOld = 1, 0
			}
			if err != nil || len(arms) != wantArms {
				t.Fatalf("retained arms = %#v %v", arms, err)
			}
			foundOld, foundNew := 0, 0
			for _, arm := range arms {
				ref := arm.JoinRef()
				if ref.StageEntry().OccurrenceID == newEntry.OccurrenceID {
					foundNew++
					if ref.Generation().Attempt != 2 || ref.StageEntry() != entry {
						t.Fatal("new arm lost its exact current entry")
					}
					continue
				}
				foundOld++
				if ref.Generation() != pair.Generation() || ref.Generation().Attempt != 1 || ref.StageEntry().OccurrenceID != oldEntry.OccurrenceID || ref.StageEntry().OriginRunID != "source" || ref.StageEntry().FlowScope != "." || ref.FlowPath() != "." {
					t.Fatal("history selected newest arm or changed declaration/inherited occurrence")
				}
				if !reflect.DeepEqual(arm.Outputs, old.Outputs) || arm.Status != old.Status || arm.CloseReason != old.CloseReason || arm.OutcomePending != old.OutcomePending || arm.TimerCancelled != old.TimerCancelled || arm.ArmedAt != old.ArmedAt || arm.FireAt != old.FireAt {
					t.Fatal("old arm lost frozen business/lifecycle evidence")
				}
				arm.Outputs["a"].Value.(map[string]any)["payload"].(map[string]any)["event_id"] = "child-only"
			}
			if foundOld != wantOld || foundNew != 1 {
				t.Fatal("projection invented a missing arm or lost the current arm")
			}
			bookkeeping["business"].(map[string]any)["nested"].([]any)[0] = "child-only"
			if projectionJSON(t, entity) != before {
				t.Fatal("child aliases source bookkeeping/output storage")
			}
		})
	}
}

func TestA2ForkEntryProjectionPreservesUnrelatedAccumulatorEvidence(t *testing.T) {
	for _, withJoin := range []bool{false, true} {
		t.Run(fmt.Sprintf("join_%t", withJoin), func(t *testing.T) {
			entry := a2ForkEntry(t, "source", "source", "source", "draft", "original")
			raw := map[string]any{
				"total": float64(7), "sequence": []any{float64(2), map[string]any{"nested": "business"}},
				"opaque_bucket":           map[string]any{"handler_joins": "authored-value", "nested": []any{"unchanged"}},
				"handler_joins_not_owned": "business", "nullable": nil,
			}
			if withJoin {
				arm, err := joinruntime.NewActivation(a2ForkJoinRef(t, entry, attemptgeneration.Generation{}), []string{"a"}, nil, time.Unix(100, 0), time.Time{})
				if err != nil {
					t.Fatal(err)
				}
				buckets := map[string]map[string]any{}
				if err := joinruntime.Store(buckets, arm); err != nil {
					t.Fatal(err)
				}
				for key, value := range buckets {
					raw[key] = value
				}
			}
			entity := a2ForkEntity(t, entry, raw)
			before := projectionJSON(t, entity)
			projection, err := runfork.ProjectEntityOwnership("source", "child", "source", "source")
			if err != nil {
				t.Fatal(err)
			}
			_, child, _, err := projectRunForkEntityExecutionState(entity, "source", "child", projection)
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"total", "sequence", "opaque_bucket", "handler_joins_not_owned", "nullable"} {
				if !reflect.DeepEqual(child[key], raw[key]) {
					t.Fatalf("unrelated evidence %q changed: %#v", key, child[key])
				}
			}
			child["sequence"].([]any)[1].(map[string]any)["nested"] = "child-only"
			child["opaque_bucket"].(map[string]any)["nested"].([]any)[0] = "child-only"
			if projectionJSON(t, entity) != before {
				t.Fatal("fork projection aliases source evidence")
			}
		})
	}
}

func TestA2ForkNodeReceiptSerializationDoesNotAuthorizeReplay(t *testing.T) {
	for _, root := range []bool{true, false} {
		name := "nonroot"
		if root {
			name = "root"
		}
		t.Run(name, func(t *testing.T) {
			snapshot, event, delivery := replayReceiverProjectionFixture(t, "flow")
			source := delivery.Route
			source.Initialization = events.ReceiverInitialization{}
			if root {
				source.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: snapshot.RunID, EntityID: snapshot.RunID})
			} else {
				source.Target = events.MustExistingEntityTarget(source.Target.Route())
			}
			target := source.Target.Route()
			entry := a2ForkEntry(t, snapshot.RunID, target.EntityID, target.FlowInstance, "draft", "retained")
			entry.OriginRunID = eventtest.UUID("a2-retained-origin")
			arm, err := joinruntime.NewActivation(a2ForkJoinRef(t, entry, attemptgeneration.Generation{}), []string{"a"}, nil, time.Unix(100, 0), time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			source.Recipient = events.MustNodeDeliveryRecipient(arm.JoinRef().Node())
			source.AgentIdentity = agentidentity.Identity{}
			source.ConnectClaim = events.ConnectExecutionClaim{}
			source.Context.Joins = []events.JoinAdmissionReceipt{{Ref: arm.JoinRef(), Disposition: events.JoinAdmissionBound}}
			if _, err := source.Identity(); err != nil {
				t.Fatal(err)
			}
			before := projectionJSON(t, source)
			childRun := eventtest.UUID("a2-outgoing-child")
			wire, err := json.Marshal(source)
			if err != nil {
				t.Fatal(err)
			}
			var restored events.DeliveryRoute
			if err := json.Unmarshal(wire, &restored); err != nil {
				t.Fatal(err)
			}
			if restored.Target.Route() != target || !reflect.DeepEqual(restored.Context.Joins, source.Context.Joins) || restored.Context.Joins[0].Ref.StageEntry() != entry {
				t.Fatal("legal node route serialization changed its retained receipt/owner/origin")
			}
			if _, err := restored.Identity(); err != nil {
				t.Fatal(err)
			}
			// A valid historical node carrier is not permission for agent-only replay.
			delivery.Route = source
			delivery.FinalSelection = deliverylifecycle.AbsentSelection()
			for index := range snapshot.Deliveries {
				snapshot.Deliveries[index].Snapshot.FinalSelection = deliverylifecycle.AbsentSelection()
			}
			beforeSnapshot, beforeDelivery := projectionJSON(t, snapshot), projectionJSON(t, delivery)
			projected, err := projectRunForkReplayInitializedReceiver(snapshot, event, delivery, childRun)
			if err == nil || err.Error() != "ordinary replay receiver requires an admitted agent delivery" || !reflect.DeepEqual(projected, events.DeliveryRoute{}) {
				t.Fatalf("node replay policy refusal = %#v %v", projected, err)
			}
			if projectionJSON(t, source) != before || projectionJSON(t, snapshot) != beforeSnapshot || projectionJSON(t, delivery) != beforeDelivery {
				t.Fatal("refused node replay mutated source route/snapshot/delivery")
			}
		})
	}
}
