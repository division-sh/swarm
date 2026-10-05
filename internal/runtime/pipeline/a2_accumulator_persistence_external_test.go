package pipeline_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/accumulator"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type a2AccumulatorPersistenceProof struct {
	selected    gateRecoveryStoreCase
	ctx         context.Context
	runID       string
	bus         *runtimebus.EventBus
	module      proposedEffectProofModule
	pc          *pipeline.PipelineCoordinator
	persistence *a2AccumulatorPersistenceObserver
	logger      *exactJoinRuntimeLogger
}

type a2AccumulatorPersistenceObserver struct {
	pipeline.WorkflowPersistenceOwner
	err error
}

func (o *a2AccumulatorPersistenceObserver) CommitWorkflowEngineMutation(ctx context.Context, command pipeline.WorkflowEngineMutationCommand) (pipeline.CommittedWorkflowEngineMutation, error) {
	result, err := o.WorkflowPersistenceOwner.CommitWorkflowEngineMutation(ctx, command)
	o.err = err
	return result, err
}

func newA2AccumulatorPersistenceProof(t *testing.T, selected gateRecoveryStoreCase) *a2AccumulatorPersistenceProof {
	t.Helper()
	source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, map[string]string{
		"schema.yaml": `name: accumulator-persistence-proof
stages:
  active: {initial: true}
pins:
  inputs:
    - seed
    - item.keyed
    - item.nested
    - item.unkeyed
  outputs:
    - item.recorded
`,
		"entities.yaml": "work:\n  marker: text?\n",
		"types.yaml": `types:
  AuthoredNested:
    id: text
    marker: text
`,
		"events.yaml": `seed:
item.keyed: &item
  id: text?
  marker: text
  event_id: text?
  event_type: text?
  source: text?
  received_at: text?
  payload: AuthoredNested?
item.nested: *item
item.unkeyed: *item
item.recorded:
  marker: text
`,
		"nodes.yaml": `seed:
  execution_type: system_node
  subscribes_to: [seed]
  event_handlers:
    seed:
      {}
collector:
  execution_type: system_node
  subscribes_to: [item.keyed, item.nested, item.unkeyed]
  event_handlers:
    item.keyed:
      accumulate: {into: items, from: payload, key: payload.id}
      data_accumulation:
        writes: [{target_field: marker, value: payload.marker}]
      emit: {event: item.recorded, fields: {marker: payload.marker}}
    item.nested:
      accumulate: {into: items, from: payload, key: payload.payload.id}
      data_accumulation:
        writes: [{target_field: marker, value: payload.marker}]
      emit: {event: item.recorded, fields: {marker: payload.marker}}
    item.unkeyed:
      accumulate: {into: items, from: payload}
      data_accumulation:
        writes: [{target_field: marker, value: payload.marker}]
      emit: {event: item.recorded, fields: {marker: payload.marker}}
`,
	}))
	runID := uuid.NewString()
	insertGateRecoveryRun(t, selected, runID)
	ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
	bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatal(err)
	}
	module := proposedEffectProofModule{source: source, nodes: []pipeline.WorkflowNode{
		{Node: externalPipelineSourceNode(t, source, ".", "seed"), Subscriptions: []events.EventType{"seed"}, ExecutionType: contracts.SystemNodeExecutionType},
		{Node: externalPipelineSourceNode(t, source, ".", "collector"), Subscriptions: []events.EventType{"item.keyed", "item.nested", "item.unkeyed"}, ExecutionType: contracts.SystemNodeExecutionType},
	}}
	observer := &a2AccumulatorPersistenceObserver{WorkflowPersistenceOwner: selected.events.(pipeline.WorkflowPersistenceOwner)}
	selected.persistence = pipeline.NewWorkflowPersistence(observer)
	proof := &a2AccumulatorPersistenceProof{selected: selected, ctx: ctx, runID: runID, bus: bus, module: module, persistence: observer}
	proof.restart()
	commitKeylessConstructorComponent(t, ctx, selected, proof.pc, source)
	proof.execute(t, proof.publish(t, "seed", `{}`), "")
	return proof
}

func (p *a2AccumulatorPersistenceProof) restart() {
	p.pc = newGateRecoveryCoordinator(p.bus, p.selected, pipeline.PipelineCoordinatorOptions{Module: p.module})
}

type a2AccumulatorPublication struct {
	event events.Event
	route events.DeliveryRoute
}

func (p *a2AccumulatorPersistenceProof) publish(t *testing.T, name, payload string) a2AccumulatorPublication {
	t.Helper()
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), events.EventType(name), "transport-operator", uuid.NewString(), []byte(payload), 0, p.runID, events.EventEnvelope{}, time.Now().UTC())
	if err := p.bus.Publish(p.ctx, event); err != nil {
		t.Fatalf("publish %s: %v", name, err)
	}
	prepared, found, err := p.selected.events.LoadPreparedPublishEvent(p.ctx, event.ID())
	if err != nil || !found || len(prepared.DeliveryRoutes) != 1 {
		t.Fatalf("retained %s publication: found=%v routes=%#v err=%v", name, found, prepared.DeliveryRoutes, err)
	}
	return a2AccumulatorPublication{event: prepared.Event.Event(), route: prepared.DeliveryRoutes[0]}
}

func (p *a2AccumulatorPersistenceProof) execute(t *testing.T, publication a2AccumulatorPublication, want failures.Class) {
	t.Helper()
	delivery, err := events.NewDeliveryEvent(publication.event, publication.route)
	if err != nil {
		t.Fatal(err)
	}
	forward, _, outcome, executionErr := p.pc.InterceptDeliveryRoute(p.ctx, delivery, publication.route)
	if forward {
		t.Fatal("authored accumulator delivery was forwarded instead of consumed")
	}
	failure, typed := failures.EnvelopeFromError(executionErr)
	if disposition, disposed := outcome.Disposition(); disposed && disposition.Failure() != nil {
		failure, typed = *disposition.Failure(), true
	}
	if want != "" {
		if !typed || failure.Class != want {
			t.Fatalf("execution failure=%#v err=%v, want %s", failure, executionErr, want)
		}
	} else if executionErr != nil || typed {
		t.Fatalf("execute %s: failure=%#v err=%v persistence=%v diagnostics=%v", publication.event.Type(), failure, executionErr, p.persistence.err, p.logger)
	}
}

type a2AccumulatorPersistedEvidence struct {
	Revision     int64
	Fields       string
	Accumulator  string
	Mutations    int
	Publications int
}

func (p *a2AccumulatorPersistenceProof) evidence(t *testing.T) a2AccumulatorPersistedEvidence {
	t.Helper()
	var evidence a2AccumulatorPersistedEvidence
	var fields, buckets []byte
	if err := p.selected.db.QueryRowContext(p.ctx, `SELECT revision, fields, accumulator FROM entity_state WHERE run_id=$1 AND flow_instance=$2`, p.runID, p.runID).Scan(&evidence.Revision, &fields, &buckets); err != nil {
		t.Fatal(err)
	}
	evidence.Fields = a2AccumulatorJSONHash(t, fields)
	evidence.Accumulator = a2AccumulatorJSONHash(t, buckets)
	if err := p.selected.db.QueryRowContext(p.ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1`, p.runID).Scan(&evidence.Mutations); err != nil {
		t.Fatal(err)
	}
	if err := p.selected.db.QueryRowContext(p.ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='item.recorded'`, p.runID).Scan(&evidence.Publications); err != nil {
		t.Fatal(err)
	}
	return evidence
}

func a2AccumulatorJSONHash(t *testing.T, raw []byte) string {
	t.Helper()
	var value any
	if err := canonicaljson.DecodePreservingNumberLexemes(raw, &value); err != nil {
		t.Fatal(err)
	}
	hash, err := canonicaljson.Hash(value)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func (p *a2AccumulatorPersistenceProof) accumulator(t *testing.T, handler string) *accumulator.State {
	t.Helper()
	instance, found, err := p.pc.Load(p.ctx, testRunScopedWorkflowInstanceForRun(p.runID, p.runID))
	if err != nil || !found {
		t.Fatalf("reconstructed entity read: found=%v err=%v", found, err)
	}
	node := p.module.nodes[1].Node
	nodeBucket, ok := instance.StateBuckets[node.Key()].(map[string]any)
	if !ok {
		t.Fatalf("missing accumulator node bucket: %#v", instance.StateBuckets)
	}
	buckets, ok := nodeBucket["handler_accumulators"].(map[string]any)
	if !ok {
		t.Fatalf("missing handler accumulators: %#v", nodeBucket)
	}
	raw, ok := buckets[timeridentity.NewAccumulatorBucketRef(node, handler).Key()].(map[string]any)
	if !ok {
		t.Fatalf("missing exact %s accumulator: %#v", handler, buckets)
	}
	state := accumulator.Load(raw)
	if err := state.Err(); err != nil {
		t.Fatalf("persisted accumulator evidence is corrupt: %v", err)
	}
	return state
}

func TestA2AccumulatorKeyedExecutionPersistenceOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			proof := newA2AccumulatorPersistenceProof(t, backend.open(t))
			const original = `{"id":" exact ","marker":"first","event_id":"business-event","event_type":"business-type","source":"business-source","received_at":"business-time","payload":{"id":"nested","marker":"nested-marker"}}`
			proof.execute(t, proof.publish(t, "item.keyed", original), "")
			before := proof.evidence(t)
			if before.Publications != 1 {
				t.Fatalf("first publication count=%d, want one", before.Publications)
			}
			proof.restart()
			state := proof.accumulator(t, "item.keyed")
			var expected map[string]any
			if err := json.Unmarshal([]byte(original), &expected); err != nil {
				t.Fatal(err)
			}
			if len(state.Items) != 1 || !reflect.DeepEqual(state.Items[0], expected) || len(state.Received) != 1 || state.Received[" exact "] == "" {
				t.Fatalf("business payload/key changed at persistence: %#v", state)
			}
			proof.execute(t, proof.publish(t, "item.keyed", `{"payload":{"marker":"nested-marker","id":"nested"},"received_at":"business-time","source":"business-source","event_type":"business-type","event_id":"business-event","marker":"first","id":" exact "}`), "")
			// A new delivery still settles through the selected mutation owner.
			// The business projection, authored history and output stay unchanged.
			want := before
			want.Revision++
			if got := proof.evidence(t); got != want {
				t.Fatalf("reordered duplicate changed business state/history/effects: got=%#v want=%#v", got, want)
			}
			before = proof.evidence(t)
			for _, field := range []string{"marker", "event_id", "event_type", "source", "received_at", "payload"} {
				t.Run("changed_"+field, func(t *testing.T) {
					var changed map[string]any
					if err := json.Unmarshal([]byte(original), &changed); err != nil {
						t.Fatal(err)
					}
					if field == "payload" {
						changed[field].(map[string]any)["marker"] = "changed-authored-nested-field"
					} else {
						changed[field] = "changed-business-value"
					}
					payload, err := json.Marshal(changed)
					if err != nil {
						t.Fatal(err)
					}
					proof.restart()
					proof.execute(t, proof.publish(t, "item.keyed", string(payload)), failures.ClassConflictingDuplicate)
					if got := proof.evidence(t); got != before {
						t.Fatalf("conflict changed persisted state/history/effects: got=%#v before=%#v", got, before)
					}
				})
			}
			// The existing fixture payload admitter intentionally permits malformed
			// carriers here; these controls exercise the defensive runtime key gate.
			for _, key := range []string{"", `""`, "null", "1", "true", `{"id":"record"}`, `["list"]`} {
				t.Run("invalid_key_"+key, func(t *testing.T) {
					payload := `{"marker":"refused"}`
					if key != "" {
						payload = `{"id":` + key + `,"marker":"refused"}`
					}
					proof.restart()
					if key == "null" {
						// Top-level null is already forbidden by payload admission;
						// do not forge a durable carrier to bypass that owner.
						event := eventtest.ExistingRunRootIngress(uuid.NewString(), "item.keyed", "transport-operator", "", []byte(payload), 0, proof.runID, events.EventEnvelope{}, time.Now().UTC())
						if err := proof.bus.Publish(proof.ctx, event); err == nil || !strings.Contains(err.Error(), "top-level null") {
							t.Fatalf("null key escaped payload admission: %v", err)
						}
						var committed int
						if err := proof.selected.db.QueryRowContext(proof.ctx, `SELECT COUNT(*) FROM events WHERE event_id=$1`, event.ID()).Scan(&committed); err != nil || committed != 0 {
							t.Fatalf("null publication was committed: %d %v", committed, err)
						}
					} else {
						proof.execute(t, proof.publish(t, "item.keyed", payload), failures.ClassSchemaInvalid)
					}
					if got := proof.evidence(t); got != before {
						t.Fatalf("invalid key changed persisted state/history/effects: got=%#v before=%#v", got, before)
					}
				})
			}
			proof.execute(t, proof.publish(t, "item.nested", original), "")
			proof.restart()
			nested := proof.accumulator(t, "item.nested")
			if len(nested.Items) != 1 || len(nested.Received) != 1 || nested.Received["nested"] == "" || nested.Received[" exact "] != "" {
				t.Fatalf("nested business key borrowed another root: %#v", nested)
			}
			if got := proof.accumulator(t, "item.keyed"); len(got.Items) != 1 {
				t.Fatalf("nested handler crossed accumulator ownership: %#v", got)
			}
			nestedBefore := proof.evidence(t)
			for _, payload := range []string{
				`{"id":"not-a-fallback","marker":"refused","payload":{"marker":"nested"}}`,
				`{"id":"not-a-fallback","marker":"refused","payload":{"id":null,"marker":"nested"}}`,
				`{"id":"not-a-fallback","marker":"refused","payload":{"id":1,"marker":"nested"}}`,
			} {
				proof.restart()
				proof.execute(t, proof.publish(t, "item.nested", payload), failures.ClassSchemaInvalid)
				if got := proof.evidence(t); got != nestedBefore {
					t.Fatalf("invalid nested key changed state/history/effects: got=%#v before=%#v", got, nestedBefore)
				}
			}
		})
	}
}

func TestA2AccumulatorUnkeyedDurableMultiplicityAndOrderOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			proof := newA2AccumulatorPersistenceProof(t, backend.open(t))
			publications := []a2AccumulatorPublication{
				proof.publish(t, "item.unkeyed", `{"marker":"same","event_id":"business-not-delivery","payload":{"id":"authored","marker":"nested"}}`),
				proof.publish(t, "item.unkeyed", `{"marker":"same","event_id":"business-not-delivery","payload":{"id":"authored","marker":"nested"}}`),
				proof.publish(t, "item.unkeyed", `{"marker":"published-earlier"}`),
				proof.publish(t, "item.unkeyed", `{"marker":"published-later"}`),
			}
			for _, index := range []int{1, 0, 3, 2} {
				proof.restart()
				proof.execute(t, publications[index], "")
			}
			proof.restart()
			state := proof.accumulator(t, "item.unkeyed")
			markers := make([]string, len(state.Items))
			for index, item := range state.Items {
				markers[index], _ = item["marker"].(string)
			}
			if !reflect.DeepEqual(markers, []string{"same", "same", "published-later", "published-earlier"}) || len(state.Received) != 0 || len(state.Deliveries) != 4 || !reflect.DeepEqual(state.Items[0], state.Items[1]) {
				t.Fatalf("unkeyed multiplicity/order changed: %#v", state)
			}
			for _, publication := range publications {
				var deliveryID string
				if err := proof.selected.db.QueryRowContext(proof.ctx, `SELECT delivery_id FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node'`, publication.event.ID()).Scan(&deliveryID); err != nil {
					t.Fatal(err)
				}
				if state.Deliveries[deliveryID] == "" {
					t.Fatalf("append lacks exact durable delivery evidence: %s", deliveryID)
				}
			}
			before := proof.evidence(t)
			if before.Publications != 4 {
				t.Fatalf("new durable deliveries produced %d effects, want four", before.Publications)
			}
			for _, publication := range publications {
				proof.restart()
				handoff, err := proof.selected.events.ProveHandoff(proof.ctx, publication.event.ID(), publication.route)
				if err != nil {
					t.Fatal(err)
				}
				if err := proof.bus.AcceptCommittedDeliveryHandoffs([]deliverylifecycle.DurableHandoffProof{handoff}); err != nil {
					t.Fatal(err)
				}
				proof.execute(t, publication, "")
				if got := proof.evidence(t); got != before {
					t.Fatalf("durable retry changed append/effects: got=%#v before=%#v", got, before)
				}
			}
		})
	}
}

func TestA2AccumulatorPublicationFailureRollsBackOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			proof := newA2AccumulatorPersistenceProof(t, backend.open(t))
			proof.execute(t, proof.publish(t, "item.keyed", `{"id":"accepted","marker":"before"}`), "")
			before := proof.evidence(t)
			fault := `CREATE TRIGGER a2_accumulator_publication_failure BEFORE INSERT ON events WHEN NEW.event_name='item.recorded' BEGIN SELECT RAISE(ABORT,'a2_accumulator_publication_fault'); END`
			if proof.selected.postgres {
				if _, err := proof.selected.db.Exec(`CREATE FUNCTION a2_accumulator_publication_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_name='item.recorded' THEN RAISE EXCEPTION 'a2_accumulator_publication_fault'; END IF; RETURN NEW; END $$`); err != nil {
					t.Fatal(err)
				}
				fault = `CREATE TRIGGER a2_accumulator_publication_failure BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION a2_accumulator_publication_fail()`
			}
			if _, err := proof.selected.db.Exec(fault); err != nil {
				t.Fatal(err)
			}
			publication := proof.publish(t, "item.keyed", `{"id":"refused-at-commit","marker":"must-not-persist"}`)
			delivery, err := events.NewDeliveryEvent(publication.event, publication.route)
			if err != nil {
				t.Fatal(err)
			}
			forward, _, outcome, executionErr := proof.pc.InterceptDeliveryRoute(proof.ctx, delivery, publication.route)
			disposition, disposed := outcome.Disposition()
			if forward || executionErr == nil && (!disposed || disposition.Failure() == nil) {
				t.Fatalf("publication fault was not refused: forward=%v outcome=%#v err=%v", forward, outcome, executionErr)
			}
			if proof.persistence.err == nil || !strings.Contains(proof.persistence.err.Error(), "a2_accumulator_publication_fault") {
				t.Fatalf("refusal did not reach the real publication fault: %v", proof.persistence.err)
			}
			if got := proof.evidence(t); got != before {
				t.Fatalf("publication rollback leaked state/history/effects: got=%#v before=%#v", got, before)
			}
			proof.restart()
			if state := proof.accumulator(t, "item.keyed"); len(state.Items) != 1 || state.Received["refused-at-commit"] != "" {
				t.Fatalf("failed transaction retained admission: %#v", state)
			}
			var delivered int
			if err := proof.selected.db.QueryRowContext(proof.ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND status='delivered'`, publication.event.ID()).Scan(&delivered); err != nil || delivered != 0 {
				t.Fatalf("failed transaction acknowledged delivery: %d %v", delivered, err)
			}
			drop := "DROP TRIGGER a2_accumulator_publication_failure"
			if proof.selected.postgres {
				drop += " ON events"
			}
			if _, err := proof.selected.db.Exec(drop); err != nil {
				t.Fatal(err)
			}
		})
	}
}
