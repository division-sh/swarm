package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestSelectedContractSourceRejectsRehomedChildStateBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, owner := range []string{"root", "static"} {
				for _, corrupt := range []string{"flow", "type"} {
					t.Run(owner+"/"+corrupt, func(t *testing.T) {
						ctx := testAuthorActivityContext()
						sourceRun, child, event := seedSelectedActivityProjectionFixture(t, fixture, backend.name == "postgres", owner == "root", true, false, json.RawMessage(`{"value":"unchanged"}`))
						store := fixture.store.(selectedActivityProjectionStore)
						loaded, err := store.LoadRunForkSelectedContractSourceEvents(ctx, sourceRun, child.ForkRunID, []string{event.ID()}, originalCarriageForRun(t, store, sourceRun))
						if err != nil || len(loaded) != 1 {
							t.Fatalf("valid materialized child: count=%d err=%v", len(loaded), err)
						}
						entityID, flow := event.RoutingSource().Route().EntityID, "flow-a"
						if owner == "root" {
							entityID, flow = child.ForkRunID, child.ForkRunID
						}
						entityType := "default"
						if corrupt == "flow" {
							flow = "foreign-owner"
						} else {
							entityType = "foreign-type"
						}
						result, err := fixture.db.ExecContext(ctx, `UPDATE entity_state SET flow_instance=$3, entity_type=$4 WHERE run_id=$1 AND entity_id=$2`, child.ForkRunID, entityID, flow, entityType)
						if err != nil {
							t.Fatal(err)
						}
						if rows, err := result.RowsAffected(); err != nil || rows != 1 {
							t.Fatalf("corrupt exact child row: rows=%d err=%v", rows, err)
						}
						before := selectedActivityPersistedSourceEvidence(t, fixture.db, event.ID())
						loaded, err = store.LoadRunForkSelectedContractSourceEvents(ctx, sourceRun, child.ForkRunID, []string{event.ID()}, originalCarriageForRun(t, store, sourceRun))
						if err == nil || !strings.Contains(err.Error(), "exact child route/type") {
							t.Fatalf("rehomed child supplied source generations: events=%#v err=%v", loaded, err)
						}
						if after := selectedActivityPersistedSourceEvidence(t, fixture.db, event.ID()); !reflect.DeepEqual(before, after) {
							t.Fatal("rejected child ownership changed source evidence")
						}
						var gotFlow, gotType string
						if err := fixture.db.QueryRowContext(ctx, `SELECT flow_instance, entity_type FROM entity_state WHERE run_id=$1 AND entity_id=$2`, child.ForkRunID, entityID).Scan(&gotFlow, &gotType); err != nil || gotFlow != flow || gotType != entityType {
							t.Fatalf("loader rewrote corrupt child ownership: flow=%q type=%q err=%v", gotFlow, gotType, err)
						}
					})
				}
			}
		})
	}
}

func TestSelectedContractAbsentSourcePersistenceRejectsContradictoryEnvelopeBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			sourceRun, child, event := seedSelectedActivityProjectionFixture(t, fixture, backend.name == "postgres", true, true, false, json.RawMessage(`{"value":"unchanged"}`))
			owner := fixture.store.(selectedActivityProjectionStore)
			original := originalCarriageForRun(t, owner, sourceRun)
			if _, err := owner.LoadRunForkSelectedContractSourceEvents(ctx, sourceRun, child.ForkRunID, []string{event.ID()}, original); err != nil {
				t.Fatalf("typed source control: %v", err)
			}
			// Corrupt only the discriminant. The nonempty persisted envelope Source
			// must not be interpreted as lawful source absence or silently repaired.
			before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
			if _, err := fixture.db.ExecContext(ctx, `UPDATE events SET routing_source_kind='absent', routing_source_authority=NULL WHERE event_id=$1`, event.ID()); err == nil {
				t.Fatal("durable event owner accepted contradictory absence")
			}
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
				t.Fatal("invalid source rejection mutated durable state")
			}
			if _, err := owner.LoadRunForkSelectedContractSourceEvents(ctx, sourceRun, child.ForkRunID, []string{event.ID()}, original); err != nil {
				t.Fatalf("rejected corruption damaged the valid source: %v", err)
			}
		})
	}
}

func TestSelectedContractOrdinarySourceStatePresenceBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, state := range []string{"absent", "zero", "fieldless", "missing", "corrupt", "wrong-header-flow", "wrong-header-type", "loop"} {
				t.Run(state, func(t *testing.T) {
					sourceRun, child, event := seedSelectedOrdinaryRootProjectionFixture(t, fixture, backend.name == "postgres", state)
					ctx := testAuthorActivityContext()
					switch state {
					case "missing":
						if _, err := fixture.db.ExecContext(ctx, `DELETE FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, child.ForkRunID, child.ForkRunID); err != nil {
							t.Fatal(err)
						}
					case "corrupt":
						if _, err := fixture.db.ExecContext(ctx, `UPDATE flow_instances SET accumulator='{"handler_loops":123}' WHERE run_id=$1 AND entity_id=$2`, child.ForkRunID, child.ForkRunID); err != nil {
							t.Fatal(err)
						}
					case "wrong-header-flow":
						if _, err := fixture.db.ExecContext(ctx, `UPDATE flow_instances SET flow_template='foreign' WHERE run_id=$1 AND entity_id=$2`, child.ForkRunID, child.ForkRunID); err != nil {
							t.Fatal(err)
						}
					case "wrong-header-type":
						if _, err := fixture.db.ExecContext(ctx, `UPDATE flow_instances SET entity_type='foreign' WHERE run_id=$1 AND entity_id=$2`, child.ForkRunID, child.ForkRunID); err != nil {
							t.Fatal(err)
						}
					}
					if state == "fieldless" {
						var headers, fields int
						if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND entity_id=$1`, child.ForkRunID).Scan(&headers); err != nil {
							t.Fatal(err)
						}
						if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND entity_id=$1`, child.ForkRunID).Scan(&fields); err != nil || headers != 1 || fields != 0 {
							t.Fatalf("fieldless fixture must contain only its constructed header: headers=%d fields=%d err=%v", headers, fields, err)
						}
					}
					before := selectedActivityPersistedSourceEvidence(t, fixture.db, event.ID())
					beforeAll := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
					loaded, err := fixture.store.(selectedActivityProjectionStore).LoadRunForkSelectedContractSourceEvents(ctx, sourceRun, child.ForkRunID, []string{event.ID()}, originalCarriageForRun(t, fixture.store, sourceRun))
					if after := selectedActivityPersistedSourceEvidence(t, fixture.db, event.ID()); !reflect.DeepEqual(before, after) {
						t.Fatal("ordinary preparation changed source evidence")
					}
					if afterAll := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres"); !reflect.DeepEqual(beforeAll, afterAll) {
						t.Fatal("ordinary preparation changed complete source/child/application state")
					}
					if state == "missing" {
						if !errors.Is(err, sql.ErrNoRows) || !strings.Contains(err.Error(), "load fork-local loop state") {
							t.Fatalf("present source state must require its child row: %v", err)
						}
						return
					}
					if state == "corrupt" {
						if err == nil || !strings.Contains(err.Error(), "handler_loops") {
							t.Fatalf("corrupt child state cannot mean zero generations: %v", err)
						}
						return
					}
					if strings.HasPrefix(state, "wrong-header-") {
						if err == nil || !strings.Contains(err.Error(), "exact child route/type") {
							t.Fatalf("wrong header supplied source generations: count=%d err=%v", len(loaded), err)
						}
						return
					}
					if err != nil || len(loaded) != 1 {
						t.Fatalf("ordinary source projection count=%d err=%v", len(loaded), err)
					}
					got := loaded[0]
					wantSource := events.RouteIdentity{FlowID: ".", FlowInstance: child.ForkRunID, EntityID: child.ForkRunID}
					if got.RoutingSource.Route() != wantSource || got.RoutingSource.Kind() != event.RoutingSource().Kind() || got.RoutingSource.Authority() != event.RoutingSource().Authority() {
						t.Fatalf("root source was not projected: %#v", got.RoutingSource)
					}
					if state == "loop" {
						var sourceHash, childHash string
						if err := fixture.db.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=$1`, sourceRun).Scan(&sourceHash); err != nil {
							t.Fatal(err)
						}
						if err := fixture.db.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=$1`, child.ForkRunID).Scan(&childHash); err != nil || sourceHash == childHash {
							t.Fatalf("original/selected declaration distinction not exercised: source=%q child=%q %v", sourceHash, childHash, err)
						}
						selected, err := semanticview.CompileOriginalLoopCarriage(selectedActivityProducerSource(t))
						if err != nil {
							t.Fatal(err)
						}
						if err := selected.RequireSource(childHash); err != nil {
							t.Fatalf("replacement fixture is not actual selected S: %v", err)
						}
						beforeWrongSource := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
						if _, err := fixture.store.(selectedActivityProjectionStore).LoadRunForkSelectedContractSourceEvents(ctx, sourceRun, child.ForkRunID, []string{event.ID()}, selected); err == nil {
							t.Fatal("selected S reinterpreted original event lineage")
						}
						if !reflect.DeepEqual(beforeWrongSource, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
							t.Fatal("rejected selected-S interpretation changed persistence")
						}
						var accumulator []byte
						if err := fixture.db.QueryRowContext(ctx, `SELECT accumulator FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, child.ForkRunID, child.ForkRunID).Scan(&accumulator); err != nil {
							t.Fatal(err)
						}
						var buckets map[string]map[string]any
						if err := json.Unmarshal(accumulator, &buckets); err != nil {
							t.Fatal(err)
						}
						activations, err := loopruntime.List(buckets)
						if err != nil || len(activations) != 1 {
							t.Fatalf("fork generation count=%d err=%v", len(activations), err)
						}
						var payload map[string]json.RawMessage
						if err := json.Unmarshal(got.Payload, &payload); err != nil {
							t.Fatal(err)
						}
						var want map[string]json.RawMessage
						if err := json.Unmarshal(event.Payload(), &want); err != nil {
							t.Fatal(err)
						}
						want["opaque_revision"], _ = json.Marshal(activations[0].Generation().RevisionID)
						if !reflect.DeepEqual(payload, want) {
							t.Fatalf("ordinary declared-generation remint changed unrelated fields: got=%s want=%v", got.Payload, want)
						}
					} else if !bytes.Equal(got.Payload, event.Payload()) {
						t.Fatalf("ordinary business entity_id/payload changed: got=%s want=%s", got.Payload, event.Payload())
					}
					again, err := runfork.ProjectSelectedContractSourceEvent(sourceRun, child.ForkRunID, got)
					if err != nil || !reflect.DeepEqual(again, got) {
						t.Fatalf("ordinary projection after persistence is not idempotent: %v", err)
					}
					if state == "absent" {
						var rows int
						if err := fixture.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id IN ($1,$2)`, sourceRun, child.ForkRunID).Scan(&rows); err != nil || rows != 0 {
							t.Fatalf("absent root acquired synthetic state: rows=%d err=%v", rows, err)
						}
					}
				})
			}
		})
	}
}

func seedSelectedOrdinaryRootProjectionFixture(t *testing.T, fixture authorActivityReceiptFixture, postgres bool, state string) (string, runfork.RunForkMaterialization, events.Event) {
	t.Helper()
	runID, parentID, eventID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	at := time.Date(2026, 7, 14, 12, 1, 0, 0, time.UTC)
	declarations := selectedActivityProducerSourceWithRootFields(t, state == "loop", false, state != "fieldless")
	ctx := seedSelectedActivitySourceRun(t, fixture, runID, declarations)
	ctx = correlation.WithRunID(ctx, runID)
	parent := eventtest.ExistingRunRootIngress(parentID, "activity.seeded", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, at)
	if err := commitSemanticPipelineProcessedEventFixture(ctx, fixture.store, parent); err != nil {
		t.Fatal(err)
	}
	payload := map[string]json.RawMessage{"number": json.RawMessage(`9007199254740993`)}
	for field, value := range map[string]string{"entity_id": runID, "source_run_id": runID, "source_event_id": parentID, "parent_event_id": parentID, "flow_instance": "business/flow"} {
		payload[field], _ = json.Marshal(value)
	}
	if state != "absent" {
		bundle, ok := semanticview.Bundle(declarations)
		if !ok {
			t.Fatal("source-state fixture requires its admitted bundle")
		}
		req := sqliteFlowActivationRequest(bundle, ".", runID, "", runID)
		req.Instance = flowidentity.Stored(req.ContractBundle, ".", runID, runID, runID, "")
		req.OccurredAt = at
		plan := constructHistoricalSourceFixture(t, correlation.WithRunID(ctx, runID), fixture.store.(agentFixtureFlowStore), req)
		persisted, err := plan.PersistenceRecord()
		if err != nil {
			t.Fatal(err)
		}
		record := persisted.State
		record.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
		record.ExpectedState, record.ExpectedRevision = "pending", 1
		record.UpdatedAt = at
		buckets := map[string]map[string]any{}
		if state == "loop" {
			activation, err := loopruntime.New(runID, runID, ".", "revision", "opaque_revision", parentID, "pending", 3, at)
			if err != nil {
				t.Fatal(err)
			}
			if err := loopruntime.Store(buckets, activation); err != nil {
				t.Fatal(err)
			}
			payload["opaque_revision"], _ = json.Marshal(activation.Generation().RevisionID)
		}
		record.Accumulator = json.RawMessage(forkTestJSON(t, buckets))
		if _, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: record}); err != nil {
			t.Fatal(err)
		}
		captureFanOutBarrierForkRevision(t, ctx, fixture.db, runID, postgres)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	source, err := pinrouting.AdmitNodeExecutionRoutingSource(declarations, mustPersistenceRootNode("reader"), ".", events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: runID})
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.ChildForProducerWithRoutingSource(eventID, "ordinary.ready", eventtest.Producer(events.EventProducerPlatform, "workflow"), "", raw, 1,
		events.EventLineage{RunID: runID, ParentEventID: parentID, ExecutionMode: executionmode.Live},
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, source.Route()), source, at.Add(time.Second))
	if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, nil); err != nil {
		t.Fatal(err)
	}
	fixture.advance()
	selected := fixture.store.(selectedActivityProjectionStore)
	if state == "fieldless" {
		request := selectedSourceMaterializationRequest(t, ctx, selected, runID, eventID, declarations)
		child, err := selected.MaterializeRunForkForSelectedContractExecution(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		return runID, child, event
	}
	child := materializeSelectedActivityFixture(t, ctx, selected, runID, eventID)
	return runID, child, event
}

func TestSelectedContractActivitySourceProjectionBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, cell := range []struct {
				name                               string
				root, independentTarget, wrongFlow bool
			}{
				{name: "root_source", root: true},
				{name: "root_source_independent_target", root: true, independentTarget: true},
				{name: "static_source"},
				{name: "static_source_independent_target", independentTarget: true},
				{name: "root_payload_disagrees", root: true, wrongFlow: true},
				{name: "static_payload_disagrees", wrongFlow: true},
			} {
				t.Run(cell.name, func(t *testing.T) {
					sourceRun, child, original := seedSelectedActivityProjectionFixture(t, fixture, backend.name == "postgres", cell.root, cell.independentTarget, cell.wrongFlow, json.RawMessage(`{"value":"unchanged"}`))
					before := selectedActivityPersistedSourceEvidence(t, fixture.db, original.ID())
					store := fixture.store.(selectedActivityProjectionStore)
					loaded, err := store.LoadRunForkSelectedContractSourceEvents(testAuthorActivityContext(), sourceRun, child.ForkRunID, []string{original.ID()}, originalCarriageForRun(t, store, sourceRun))
					if after := selectedActivityPersistedSourceEvidence(t, fixture.db, original.ID()); !reflect.DeepEqual(before, after) {
						t.Fatal("activity preparation mutated persisted source routing/payload evidence")
					}
					if cell.wrongFlow {
						if err == nil || !strings.Contains(err.Error(), "contradicts its producer flow") {
							t.Fatalf("payload/source disagreement must fail at shared projection: events=%#v err=%v", loaded, err)
						}
						return
					}
					if err != nil || len(loaded) != 1 {
						t.Fatalf("persistent activity preparation: events=%#v err=%v", loaded, err)
					}
					got := loaded[0]
					wantRoute := original.RoutingSource().Route()
					wantFlow := wantRoute.FlowInstance
					if cell.root {
						wantRoute.EntityID, wantRoute.FlowInstance, wantFlow = child.ForkRunID, child.ForkRunID, child.ForkRunID
					}
					if got.RoutingSource.Route() != wantRoute || got.RoutingSource.Kind() != original.RoutingSource().Kind() || got.RoutingSource.Authority() != original.RoutingSource().Authority() || got.SourceEventID != original.ID() || got.EventName != string(original.Type()) || got.ExecutionMode != original.ExecutionMode() || got.Scope != string(original.Scope()) {
						t.Fatalf("loaded activity lost source identity: got=%#v original=%#v wantRoute=%#v", got, original, wantRoute)
					}
					var payload map[string]json.RawMessage
					if err := json.Unmarshal(got.Payload, &payload); err != nil {
						t.Fatal(err)
					}
					for field, want := range map[string]string{
						"flow_instance": wantFlow, "entity_id": wantRoute.EntityID, "source_run_id": child.ForkRunID,
						"source_event_id": activityidentity.ForkLineageEventID(child.ForkRunID, original.ID()),
					} {
						var actual string
						if err := json.Unmarshal(payload[field], &actual); err != nil || actual != want {
							t.Errorf("payload %s=%s want %q: %v", field, payload[field], want, err)
						}
					}
					if string(payload["input"]) != `{"value":"unchanged"}` {
						t.Errorf("activity input changed: %s", payload["input"])
					}
					again, err := runfork.ProjectSelectedContractSourceEvent(sourceRun, child.ForkRunID, got)
					if err != nil || !reflect.DeepEqual(again, got) {
						t.Fatalf("runtime projection after persistent preparation is not idempotent: got=%#v again=%#v err=%v", got, again, err)
					}
				})
			}
		})
	}
}

func TestSelectedContractActivitySourceProjectionPreservesNumericPayloadBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, owner := range []string{"root", "static"} {
				t.Run(owner, func(t *testing.T) {
					input := json.RawMessage(`{"large":9007199254740993,"decimal":1.2300e+09,"negative_zero":-0,"nested":[18446744073709551615]}`)
					sourceRun, child, original := seedSelectedActivityProjectionFixture(t, fixture, backend.name == "postgres", owner == "root", true, false, input)
					before := selectedActivityPersistedSourceEvidence(t, fixture.db, original.ID())
					loaded, err := fixture.store.(selectedActivityProjectionStore).LoadRunForkSelectedContractSourceEvents(testAuthorActivityContext(), sourceRun, child.ForkRunID, []string{original.ID()}, originalCarriageForRun(t, fixture.store, sourceRun))
					if err != nil || len(loaded) != 1 {
						t.Fatalf("load numeric activity: events=%#v err=%v", loaded, err)
					}
					if after := selectedActivityPersistedSourceEvidence(t, fixture.db, original.ID()); !reflect.DeepEqual(before, after) {
						t.Fatal("numeric projection changed persisted source evidence")
					}
					projected, err := runfork.ProjectSelectedContractSourceEvent(sourceRun, child.ForkRunID, loaded[0])
					if err != nil {
						t.Fatal(err)
					}
					var originalInput, projectedInput map[string]json.RawMessage
					if err := json.Unmarshal(input, &originalInput); err != nil {
						t.Fatal(err)
					}
					var payload map[string]json.RawMessage
					if err := json.Unmarshal(projected.Payload, &payload); err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(payload["input"], &projectedInput); err != nil {
						t.Fatal(err)
					}
					for _, field := range []string{"large", "decimal", "negative_zero", "nested"} {
						if !bytes.Equal(originalInput[field], projectedInput[field]) {
							t.Errorf("persistent Load -> shared projection changed input.%s: got %s want exact %s", field, projectedInput[field], originalInput[field])
						}
					}
				})
			}
		})
	}
}

func seedSelectedActivityProjectionFixture(t *testing.T, fixture authorActivityReceiptFixture, postgres, root, independentTarget, wrongFlow bool, input json.RawMessage) (string, runfork.RunForkMaterialization, events.Event) {
	return seedSelectedActivityProjectionFixtureWithPostEvent(t, fixture, postgres, root, independentTarget, wrongFlow, input, nil)
}

func seedSelectedActivityProjectionFixtureWithPostEvent(t *testing.T, fixture authorActivityReceiptFixture, postgres, root, independentTarget, wrongFlow bool, input json.RawMessage, postEvent func(context.Context, string)) (string, runfork.RunForkMaterialization, events.Event) {
	t.Helper()
	runID, parentID := uuid.NewString(), uuid.NewString()
	declarations := selectedActivityProducerSource(t)
	ctx := seedSelectedActivitySourceRun(t, fixture, runID, declarations)
	ctx = correlation.WithRunID(ctx, runID)
	flowID := "flow-a"
	if root {
		flowID = "."
	}
	node := mustPersistenceNode(flowID, "reader")
	at := time.Date(2026, 7, 14, 12, 1, 0, 0, time.UTC)
	parent := eventtest.ExistingRunRootIngress(parentID, "activity.seeded", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, at)
	if err := commitSemanticPipelineProcessedEventFixture(ctx, fixture.store, parent); err != nil {
		t.Fatal(err)
	}
	instance := constructSelectedActivityProducerFixture(t, ctx, fixture.store.(agentFixtureFlowStore), declarations, parent, flowID, at)
	flowInstance, entityID := instance.InstancePath, instance.EntityID
	source, err := pinrouting.AdmitNodeExecutionRoutingSource(declarations, node, flowID, events.RouteIdentity{FlowID: flowID, FlowInstance: flowInstance, EntityID: entityID})
	if err != nil {
		t.Fatal(err)
	}
	captureFanOutBarrierForkRevision(t, ctx, fixture.db, runID, postgres)
	activityFlow := flowInstance
	if wrongFlow {
		activityFlow = "unrelated/receiver"
	}
	fact := activityidentity.Fact{RunID: runID, SourceEventID: parentID, EntityID: entityID,
		Owner: activityidentity.MustNodeOwner(node), ExecutionFlowID: flowID, HandlerEventKey: "review.inspect",
		ActivityID: "inspect", Tool: "provider.read", Attempt: 1}
	eventID := activityidentity.RequestEventID(fact)
	results := runtimecontracts.ActivityResultEventsForSite(runtimecontracts.ActivitySite{Node: node, HandlerEventKey: "review.inspect", Spec: runtimecontracts.ActivitySpec{ID: "inspect", Tool: "provider.read"}})
	payload, err := json.Marshal(map[string]any{
		"activity_id": "inspect", "tool": "provider.read", "input": input,
		"effect_class": string(runtimecontracts.ActivityEffectClassReadOnly), "fork_policy": string(runtimecontracts.ActivityForkReexecuteRead),
		"success_event": results.SuccessEvent, "failure_event": results.FailureEvent, "attempt": 1,
		"entity_id": entityID, "node_id": fact.Owner.Key(), "flow_id": flowID, "flow_instance": activityFlow,
		"handler_event_key": "review.inspect", "source_run_id": runID, "source_event_id": parentID,
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, source.Route())
	if independentTarget {
		envelope = events.EnvelopeForTargetRoute(envelope, events.RouteIdentity{FlowID: "receiver", FlowInstance: "receiver/other", EntityID: uuid.NewString()})
	}
	event := eventtest.ChildForProducerWithRoutingSource(eventID, "platform.activity_requested", eventtest.Producer(events.EventProducerPlatform, "workflow"), "", payload, 1,
		events.EventLineage{RunID: runID, ParentEventID: parentID, ExecutionMode: executionmode.Live}, envelope, source, at.Add(time.Second))
	if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, nil); err != nil {
		t.Fatal(err)
	}
	if postEvent != nil {
		postEvent(ctx, runID)
	}
	fixture.advance()
	child := materializeSelectedActivityFixture(t, ctx, fixture.store.(selectedActivityProjectionStore), runID, eventID)
	// The producer is chosen from the complete root/keyless-child construction.
	wantEntities := 2
	if child.MaterializedEntityCount != wantEntities {
		t.Fatalf("activity fixture did not materialize its complete constructed topology: count=%d want=%d", child.MaterializedEntityCount, wantEntities)
	}
	return runID, child, event
}

func constructSelectedActivityProducerFixture(t *testing.T, ctx context.Context, selected agentFixtureFlowStore, source semanticview.Source, parent events.Event, flowID string, at time.Time) flowidentity.Instance {
	t.Helper()
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("activity producer requires its admitted constructor source")
	}
	runID := parent.RunID()
	req := sqliteFlowActivationRequest(bundle, ".", runID, "", runID)
	req.Instance = flowidentity.Stored(source, ".", runID, runID, runID, "")
	req.OccurredAt = at
	plan := constructHistoricalSourceFixture(t, correlation.WithInboundEvent(correlation.WithRunID(ctx, runID), parent), selected, req)
	for _, constructed := range plan.ConstructionPlans() {
		if constructed.Identity.TemplateID == flowID {
			if err := constructed.Identity.ValidateConstruction(source, runID); err != nil {
				t.Fatal(err)
			}
			return constructed.Identity
		}
	}
	t.Fatalf("root constructor omitted activity producer %s", flowID)
	return flowidentity.Instance{}
}

func TestSelectedContractForkDoesNotCopyRotationReceiptBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			receiptKey := uuid.NewString()
			sourceRunID, child, _ := seedSelectedActivityProjectionFixtureWithPostEvent(t, fixture, backend.name == "postgres", false, true, false, json.RawMessage(`{"value":"unchanged"}`), func(ctx context.Context, runID string) {
				receiptCtx := runtimeeffects.WithDifferentOwner(ctx, runtimeeffects.OwnerBuildTestInfrastructure)
				identity := agentmemory.Identity(mustTestAgentIdentityForRun(runID, "fork-receipt-agent", ""))
				seedTestAgentRow(t, receiptCtx, fixture.db, backend.name == "postgres", identity, "active")
				owner := fixture.store.(llmSessionAttemptJourneyOwner)
				predecessor, _, err := owner.AcquireLiveSession(receiptCtx, identity, "fork-worker")
				if err != nil {
					t.Fatalf("acquire source receipt session: %v", err)
				}
				if _, err := owner.Rotate(receiptCtx, predecessor, sessions.RotationMetadata{OperationID: receiptKey}); err != nil {
					t.Fatalf("seed source receipt: %v", err)
				}
			})
			var sourceReceipts, forkReceipts int
			if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM agent_sessions WHERE run_id=$1 AND rotation_operation_id=$2`, sourceRunID, receiptKey).Scan(&sourceReceipts); err != nil {
				t.Fatal(err)
			}
			if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM agent_sessions WHERE run_id=$1 AND rotation_operation_id=$2`, child.ForkRunID, receiptKey).Scan(&forkReceipts); err != nil {
				t.Fatal(err)
			}
			if sourceReceipts != 1 || forkReceipts != 0 {
				t.Fatalf("source/fork receipt rows=%d/%d, want 1/0", sourceReceipts, forkReceipts)
			}
		})
	}
}

func selectedActivityPersistedSourceEvidence(t *testing.T, db *sql.DB, eventID string) []string {
	t.Helper()
	var kind, authority, route, target string
	var payload []byte
	if err := db.QueryRow(`SELECT routing_source_kind, COALESCE(routing_source_authority,''), CAST(source_route AS TEXT), CAST(target_route AS TEXT), payload_bytes FROM events WHERE event_id=$1`, eventID).Scan(&kind, &authority, &route, &target, &payload); err != nil {
		t.Fatal(err)
	}
	return []string{kind, authority, route, target, string(payload)}
}

type selectedActivityProjectionStore interface {
	PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	MaterializeRunForkForSelectedContractExecution(context.Context, runforkreadiness.MaterializeRequest) (runfork.RunForkMaterialization, error)
	LoadRunForkSelectedContractSourceEvents(context.Context, string, string, []string, semanticview.OriginalLoopCarriage) ([]runfork.RunForkSelectedContractSourceEvent, error)
	LoadRunForkSelectedContractSourceEventModes(context.Context, string, []string) ([]executionmode.Mode, error)
}

func selectedActivityProducerSource(t *testing.T) semanticview.Source {
	return selectedActivityProducerSourceWithLoops(t, false, false)
}

func selectedActivityProducerSourceWithLoops(t *testing.T, ordinaryRootLoop, activityLoop bool) semanticview.Source {
	return selectedActivityProducerSourceWithRootFields(t, ordinaryRootLoop, activityLoop, true)
}

func selectedActivityProducerSourceWithRootFields(t *testing.T, ordinaryRootLoop, activityLoop, rootFields bool) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"schema.yaml":   "name: activity-projection\nstages:\n  pending: {initial: true}\n",
		"entities.yaml": "default:\n  name: text\n",
		"events.yaml":   "ordinary.ready:\nreview.inspect:\nsupport.drafted:\n",
		"nodes.yaml": `reader:
  execution_type: system_node
  subscribes_to: [review.inspect]
  event_handlers:
    review.inspect:
      activity: {id: inspect, tool: provider.read}
support:
  execution_type: system_node
  subscribes_to: [support.drafted]
  event_handlers:
    support.drafted:
      activity: {id: send_support_reply, tool: telegram.send_message, approval: {decision: support_reply}}
`,
		"flow-a/schema.yaml":   "name: flow-a\nstages:\n  pending: {initial: true}\n",
		"flow-a/entities.yaml": "default:\n  name: text\n",
		"flow-a/events.yaml":   "review.accepted:\nreview.inspect:\n",
		"flow-a/nodes.yaml": `writer:
  execution_type: system_node
  subscribes_to: [review.accepted]
  event_handlers:
    review.accepted:
      activity: {id: commit, tool: provider.write}
reader:
  execution_type: system_node
  subscribes_to: [review.inspect]
  event_handlers:
    review.inspect:
      activity: {id: inspect, tool: provider.read}
`,
		"tools.yaml": `provider.read:
  handler_type: http
  effect_class: read_only
  http: {method: GET, url: "http://127.0.0.1:1/read"}
provider.write:
  handler_type: http
  effect_class: non_idempotent_write
  http: {method: POST, url: "http://127.0.0.1:1/write"}
telegram.send_message:
  handler_type: http
  effect_class: non_idempotent_write
  http: {method: POST, url: "http://127.0.0.1:1/send"}
`,
	}
	if !rootFields {
		delete(files, "entities.yaml")
	}
	if ordinaryRootLoop {
		files["schema.yaml"] += "  closed: {terminal: true}\n  exhausted: {terminal: true}\nloops:\n  revision:\n    revision_field: opaque_revision\n    max_attempts: 3\n    escape: {advances_to: exhausted}\n"
		files["events.yaml"] = "ordinary.ready:\n  opaque_revision: text\nordinary.start:\nordinary.retry:\n  opaque_revision: text\nordinary.close:\n  opaque_revision: text\n"
		files["nodes.yaml"] = `reader:
  execution_type: system_node
  subscribes_to: [ordinary.start, ordinary.ready, ordinary.retry, ordinary.close]
  event_handlers:
    ordinary.start:
      loop: {start: revision, from: pending}
      advances_to: pending
    ordinary.ready:
      loop: {admit: revision, from: pending}
      advances_to: pending
    ordinary.retry:
      loop: {repeat: revision, from: pending}
      advances_to: pending
    ordinary.close:
      loop: {close: revision, from: pending}
      advances_to: closed
`
	}
	if activityLoop {
		files["flow-a/schema.yaml"] += "  review: {}\n  closed: {terminal: true}\n  exhausted: {terminal: true}\nloops:\n  revision:\n    revision_field: revision_id\n    max_attempts: 3\n    escape: {advances_to: exhausted}\n"
		files["flow-a/events.yaml"] = "review.accepted:\n  revision_id: text\nreview.inspect:\n  revision_id: text\nreview.start:\nreview.retry:\n  revision_id: text\nreview.close:\n  revision_id: text\n"
		files["flow-a/nodes.yaml"] = `writer:
  execution_type: system_node
  subscribes_to: [review.start, review.accepted, review.retry, review.close]
  event_handlers:
    review.start:
      loop: {start: revision, from: pending}
      advances_to: review
    review.accepted:
      loop: {admit: revision, from: review}
      advances_to: review
      activity: {id: commit, tool: provider.write}
    review.retry:
      loop: {repeat: revision, from: review}
      advances_to: review
    review.close:
      loop: {close: revision, from: review}
      advances_to: closed
reader:
  execution_type: system_node
  subscribes_to: [review.inspect]
  event_handlers:
    review.inspect:
      loop: {admit: revision, from: review}
      advances_to: review
      activity: {id: inspect, tool: provider.read}
`
	}
	for path, content := range files {
		writeStateOnlyAcquisitionFixtureFile(t, filepath.Join(root, path), content)
	}
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

func seedSelectedActivitySourceRun(t *testing.T, fixture authorActivityReceiptFixture, runID string, source semanticview.Source) context.Context {
	t.Helper()
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle.SourceArtifact == nil {
		t.Fatal("activity source run requires its actual admitted declarations")
	}
	ctx := testAuthorActivityContextForBundle(bundle.SourceArtifact.BundleHash())
	requireRunFixtureForTest(t, ctx, fixture.store, semanticRunFixture{
		Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, Artifact: bundle.SourceArtifact,
		StartedAt: time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC),
	})
	return ctx
}

func originalCarriageForRun(t testing.TB, selected any, runID string) semanticview.OriginalLoopCarriage {
	t.Helper()
	store, ok := selected.(runforkexecution.SourceArtifactSelectedContractSourceStore)
	if !ok {
		t.Fatal("fixture requires the actual source artifact reader")
	}
	repo := canonicalrouting.RepoRoot(t)
	loaded, err := (runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo,
		PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repo), Store: store}).LoadRunForkSelectedContractSourceForRequest(context.Background(), runforkexecution.SelectedContractSourceLoadRequest{
		SourceRunID: runID, Selection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
	})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Cleanup != nil {
		t.Cleanup(func() {
			if err := loaded.Cleanup(); err != nil {
				t.Error(err)
			}
		})
	}
	owner, err := semanticview.CompileOriginalLoopCarriage(loaded.Source)
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

func materializeSelectedActivityFixture(t *testing.T, ctx context.Context, store selectedActivityProjectionStore, sourceRunID, eventID string) runfork.RunForkMaterialization {
	t.Helper()
	source := selectedActivityProducerSource(t)
	request := selectedSourceMaterializationRequest(t, ctx, store, sourceRunID, eventID, source)
	materialized, err := store.MaterializeRunForkForSelectedContractExecution(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	return materialized
}

func selectedSourceMaterializationRequest(t *testing.T, ctx context.Context, store selectedActivityProjectionStore, sourceRunID, eventID string, source semanticview.Source) runforkreadiness.MaterializeRequest {
	t.Helper()
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("activity fixture has no admitted source artifact")
	}
	if _, err := store.(selectedSourceArtifactStore).EnsureSourceArtifact(ctx, bundle.SourceArtifact); err != nil {
		t.Fatal(err)
	}
	return prepareSelectedStoreMaterializationForTest(t, ctx, store, sourceRunID, eventID,
		runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: bundle.SourceArtifact.BundleHash()}, bundle.SourceArtifact.BundleHash())
}
