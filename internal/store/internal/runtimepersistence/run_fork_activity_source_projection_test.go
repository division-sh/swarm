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
	runtimecore "github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
						loaded, err := store.LoadRunForkSelectedContractSourceEvents(ctx, sourceRun, child.ForkRunID, []string{event.ID()})
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
						loaded, err = store.LoadRunForkSelectedContractSourceEvents(ctx, sourceRun, child.ForkRunID, []string{event.ID()})
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

func TestSelectedContractOrdinarySourceStatePresenceBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, state := range []string{"absent", "zero", "missing", "corrupt", "loop"} {
				t.Run(state, func(t *testing.T) {
					sourceRun, child, event := seedSelectedOrdinaryRootProjectionFixture(t, fixture, backend.name == "postgres", state)
					ctx := testAuthorActivityContext()
					switch state {
					case "missing":
						if _, err := fixture.db.ExecContext(ctx, `DELETE FROM entity_state WHERE run_id=$1 AND entity_id=$2`, child.ForkRunID, child.ForkRunID); err != nil {
							t.Fatal(err)
						}
					case "corrupt":
						if _, err := fixture.db.ExecContext(ctx, `UPDATE entity_state SET accumulator='{"handler_loops":123}' WHERE run_id=$1 AND entity_id=$2`, child.ForkRunID, child.ForkRunID); err != nil {
							t.Fatal(err)
						}
					}
					before := selectedActivityPersistedSourceEvidence(t, fixture.db, event.ID())
					loaded, err := fixture.store.(selectedActivityProjectionStore).LoadRunForkSelectedContractSourceEvents(ctx, sourceRun, child.ForkRunID, []string{event.ID()})
					if after := selectedActivityPersistedSourceEvidence(t, fixture.db, event.ID()); !reflect.DeepEqual(before, after) {
						t.Fatal("ordinary preparation changed source evidence")
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
					if err != nil || len(loaded) != 1 {
						t.Fatalf("ordinary source projection count=%d err=%v", len(loaded), err)
					}
					got := loaded[0]
					if got.RoutingSource != eventtest.RootRoutingSource(child.ForkRunID) {
						t.Fatalf("root source was not projected: %#v", got.RoutingSource)
					}
					if state == "loop" {
						var accumulator []byte
						if err := fixture.db.QueryRowContext(ctx, `SELECT accumulator FROM entity_state WHERE run_id=$1 AND entity_id=$2`, child.ForkRunID, child.ForkRunID).Scan(&accumulator); err != nil {
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
	ctx := testAuthorActivityContext()
	runID, parentID, eventID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	at := time.Date(2026, 7, 14, 12, 1, 0, 0, time.UTC)
	seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	parent := eventtest.ExistingRunRootIngress(parentID, "activity.seeded", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, at)
	if err := commitSemanticPipelineProcessedEventFixture(ctx, fixture.store, parent); err != nil {
		t.Fatal(err)
	}
	payload := map[string]json.RawMessage{"number": json.RawMessage(`9007199254740993`)}
	for field, value := range map[string]string{"entity_id": runID, "source_run_id": runID, "source_event_id": parentID, "parent_event_id": parentID, "flow_instance": "business/flow"} {
		payload[field], _ = json.Marshal(value)
	}
	if state != "absent" {
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
			if _, err := fixture.db.ExecContext(ctx, `INSERT INTO entity_mutations (run_id,entity_id,domain,path,old_value,new_value,caused_by_event,writer_type,writer_id,handler_step,created_at) VALUES ($1,$2,'accumulator','handler_loops','null',$3,$4,'platform','source-state-fixture','seed',$5)`, runID, runID, forkTestJSON(t, buckets[loopruntime.BucketKey]), parentID, at); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := fixture.db.ExecContext(ctx, `INSERT INTO entity_state (run_id,entity_id,flow_instance,entity_type,current_state,gates,fields,bookkeeping,accumulator,revision,entered_state_at,created_at,updated_at) VALUES ($1,$2,$3,'default','pending','{}','{}','{}',$4,1,$5,$6,$7)`, runID, runID, runID, forkTestJSON(t, buckets), at, at, at); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.ExecContext(ctx, `INSERT INTO entity_mutations (run_id,entity_id,domain,path,old_value,new_value,caused_by_event,writer_type,writer_id,handler_step,created_at) VALUES ($1,$2,'lifecycle_state','','null','"pending"',$3,'platform','source-state-fixture','seed',$4)`, runID, runID, parentID, at); err != nil {
			t.Fatal(err)
		}
		captureFanOutBarrierForkRevision(t, ctx, fixture.db, runID, postgres)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	source, err := pinrouting.AdmitNodeExecutionRoutingSource(selectedActivityProducerSource(t), mustPersistenceRootNode("reader"), ".", events.RouteIdentity{FlowID: ".", EntityID: runID})
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.NewChildEvent(events.ChildEventInput{
		Facts:   events.EventFacts{ID: eventID, Type: "ordinary.ready", Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "workflow"}, Payload: raw, ChainDepth: 1, Envelope: events.EnvelopeForSourceRoute(events.EventEnvelope{}, source.Route()), RoutingSource: source, CreatedAt: at.Add(time.Second)},
		Lineage: events.EventLineage{RunID: runID, ParentEventID: parentID, ExecutionMode: executionmode.Live},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, nil); err != nil {
		t.Fatal(err)
	}
	fixture.advance()
	child := materializeSelectedActivityFixture(t, ctx, fixture.store.(selectedActivityProjectionStore), runID, eventID)
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
					loaded, err := store.LoadRunForkSelectedContractSourceEvents(testAuthorActivityContext(), sourceRun, child.ForkRunID, []string{original.ID()})
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
						wantRoute.EntityID, wantFlow = child.ForkRunID, child.ForkRunID
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
					loaded, err := fixture.store.(selectedActivityProjectionStore).LoadRunForkSelectedContractSourceEvents(testAuthorActivityContext(), sourceRun, child.ForkRunID, []string{original.ID()})
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
	t.Helper()
	ctx := testAuthorActivityContext()
	runID, parentID, eventID, entityID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	flowID, flowInstance := "flow-a", "flow-a"
	if root {
		flowID, flowInstance, entityID = ".", runID, runID
	}
	node := mustPersistenceNode(flowID, "reader")
	source, err := pinrouting.AdmitNodeExecutionRoutingSource(selectedActivityProducerSource(t), node, flowID, events.RouteIdentity{FlowID: flowID, FlowInstance: flowInstance, EntityID: entityID})
	if err != nil {
		t.Fatal(err)
	}
	seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	at := time.Date(2026, 7, 14, 12, 1, 0, 0, time.UTC)
	parent := eventtest.ExistingRunRootIngress(parentID, "activity.seeded", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, at)
	if err := commitSemanticPipelineProcessedEventFixture(ctx, fixture.store, parent); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ExecContext(ctx, `INSERT INTO entity_mutations (run_id,entity_id,domain,path,old_value,new_value,caused_by_event,writer_type,writer_id,handler_step,created_at) VALUES ($1,$2,'lifecycle_state','','null','"pending"',$3,'platform','activity-fixture','seed',$4)`, runID, entityID, parentID, at); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ExecContext(ctx, `INSERT INTO entity_state (run_id,entity_id,flow_instance,entity_type,current_state,gates,fields,bookkeeping,accumulator,revision,entered_state_at,created_at,updated_at) VALUES ($1,$2,$3,'default','pending','{}','{}','{}','{}',1,$4,$5,$6)`, runID, entityID, flowInstance, at, at, at); err != nil {
		t.Fatal(err)
	}
	captureFanOutBarrierForkRevision(t, ctx, fixture.db, runID, postgres)
	activityFlow := flowInstance
	if wrongFlow {
		activityFlow = "unrelated/receiver"
	}
	payload, err := json.Marshal(map[string]any{
		"activity_id": "inspect", "tool": "provider.read", "input": input,
		"effect_class": string(runtimecontracts.ActivityEffectClassReadOnly), "fork_policy": string(runtimecontracts.ActivityForkReexecuteRead),
		"success_event": "read.succeeded", "failure_event": "read.failed", "attempt": 1,
		"entity_id": entityID, "node_id": node.Key(), "flow_id": flowID, "flow_instance": activityFlow,
		"handler_event_key": "review.inspect", "source_run_id": runID, "source_event_id": parentID,
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope := events.EnvelopeForSourceRoute(events.EventEnvelope{}, source.Route())
	if independentTarget {
		envelope = events.EnvelopeForTargetRoute(envelope, events.RouteIdentity{FlowID: "receiver", FlowInstance: "receiver/other", EntityID: uuid.NewString()})
	}
	event, err := events.NewChildEvent(events.ChildEventInput{
		Facts:   events.EventFacts{ID: eventID, Type: "platform.activity_requested", Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "workflow"}, Payload: payload, ChainDepth: 1, Envelope: envelope, RoutingSource: source, CreatedAt: at.Add(time.Second)},
		Lineage: events.EventLineage{RunID: runID, ParentEventID: parentID, ExecutionMode: executionmode.Live},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, nil); err != nil {
		t.Fatal(err)
	}
	fixture.advance()
	child := materializeSelectedActivityFixture(t, ctx, fixture.store.(selectedActivityProjectionStore), runID, eventID)
	if child.MaterializedEntityCount != 1 {
		t.Fatalf("activity fixture did not materialize its producer state: count=%d", child.MaterializedEntityCount)
	}
	return runID, child, event
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
	LoadRunForkSelectedContractSourceEvents(context.Context, string, string, []string) ([]runfork.RunForkSelectedContractSourceEvent, error)
	LoadRunForkSelectedContractSourceEventModes(context.Context, string, []string) ([]executionmode.Mode, error)
}

func selectedActivityProducerSource(t *testing.T) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	for path, content := range map[string]string{
		"schema.yaml":          "name: activity-projection\nstages:\n  pending: {initial: true}\n",
		"entities.yaml":        "default:\n  name: text\n",
		"events.yaml":          "ordinary.ready: {}\n",
		"nodes.yaml":           "reader:\n  id: reader\n  execution_type: system_node\n",
		"flow-a/schema.yaml":   "name: flow-a\nmode: static\nstages:\n  pending: {initial: true}\n",
		"flow-a/entities.yaml": "default:\n  name: text\n",
		"flow-a/events.yaml":   "review.accepted: {}\nreview.inspect: {}\n",
		"flow-a/nodes.yaml": `writer:
  id: writer
  execution_type: system_node
  subscribes_to: [review.accepted]
  event_handlers:
    review.accepted: {}
reader:
  id: reader
  execution_type: system_node
  subscribes_to: [review.inspect]
  event_handlers:
    review.inspect: {}
`,
	} {
		writeStateOnlyAcquisitionFixtureFile(t, filepath.Join(root, path), content)
	}
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

// Only these activity fixtures replace the legacy platform-control seed. The
// producer route comes from its declared node, never from an activity payload.
func stampSelectedActivityProducerFixture(t *testing.T, db *sql.DB, runID, entityID, eventID, parentEventID, node string) {
	t.Helper()
	source, err := pinrouting.AdmitNodeExecutionRoutingSource(selectedActivityProducerSource(t), mustPersistenceNode("flow-a", node), "flow-a", events.RouteIdentity{
		FlowID: "flow-a", FlowInstance: "flow-a", EntityID: entityID,
	})
	if err != nil {
		t.Fatal(err)
	}
	route, err := json.Marshal(source.Route())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE events SET event_class='child', source_event_id=$6::uuid, routing_source_kind=$3, routing_source_authority=$4, source_route=$5::jsonb WHERE run_id=$1::uuid AND event_id=$2::uuid`, runID, eventID, source.Kind().StorageCode(), source.Authority().StorageCode(), string(route), parentEventID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE entity_state SET flow_instance='flow-a' WHERE run_id=$1::uuid AND entity_id=$2::uuid`, runID, entityID); err != nil {
		t.Fatal(err)
	}
}

func materializeSelectedActivityFixture(t *testing.T, ctx context.Context, store selectedActivityProjectionStore, sourceRunID, eventID string) runfork.RunForkMaterialization {
	t.Helper()
	source := selectedActivityProducerSource(t)
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("activity fixture has no admitted source artifact")
	}
	if _, err := store.(selectedSourceArtifactStore).EnsureSourceArtifact(ctx, bundle.SourceArtifact); err != nil {
		t.Fatal(err)
	}
	sourceFact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	effective, err := runtimecore.AdmitEffectiveSourceProjection(runtimecore.EffectiveSourceProjectionRequest{Source: source, SourceArtifactFact: sourceFact})
	if err != nil {
		t.Fatal(err)
	}
	source = effective.Source()
	plan, err := store.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatal(err)
	}
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: source})
	if err != nil {
		t.Fatal(err)
	}
	history, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{Plan: plan, Source: source, FrontierAdmission: frontier})
	if err != nil {
		t.Fatal(err)
	}
	topology, err := runforkexecution.BuildSelectedContractRouteTopology(runforkexecution.SelectedContractRouteTopologyRequest{Admission: frontier, RouteAdmission: history})
	if err != nil {
		t.Fatal(err)
	}
	planning, err := runforkexecution.BuildSelectedContractRecipientPlanning(runforkexecution.SelectedContractRecipientPlanningRequest{Admission: frontier, RouteAdmission: history, RouteTopology: topology})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(frontier.FrontierEvents))
	for i, event := range frontier.FrontierEvents {
		ids[i] = event.SourceEventID
	}
	modes, err := store.LoadRunForkSelectedContractSourceEventModes(ctx, sourceRunID, ids)
	if err != nil || len(modes) != len(ids) {
		t.Fatalf("load exact activity fixture source modes: count=%d err=%v", len(modes), err)
	}
	sourceModes := map[string]executionmode.Mode{}
	for i, id := range ids {
		sourceModes[id] = modes[i]
	}
	readiness, err := runforkreadiness.Admit(runforkreadiness.AdmissionRequest{Binding: runforkreadiness.Binding{
		Plan: plan, ContractSelection: frontier.ContractSelection, SourceArtifactFact: sourceFact, EffectiveSourceIdentity: effective.Identity(), FrontierAdmission: frontier, RecipientPlanning: planning, SourceModes: sourceModes,
	}, Source: source})
	if err != nil {
		t.Fatal(err)
	}
	materialized, err := store.MaterializeRunForkForSelectedContractExecution(ctx, runforkreadiness.MaterializeRequest{
		SourceRunID: sourceRunID, At: eventID, ContractSelection: frontier.ContractSelection,
		SourceArtifactFact: sourceFact, EffectiveSourceIdentity: effective.Identity(), Readiness: readiness,
		FrontierAdmission: frontier, RouteTopology: topology, RecipientPlanning: planning,
	})
	if err != nil {
		t.Fatal(err)
	}
	return materialized
}
