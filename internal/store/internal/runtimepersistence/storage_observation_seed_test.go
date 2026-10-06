package runtimepersistence

import (
	"context"
	"database/sql"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
	"strings"
	"testing"
	"time"
)

func seedEntityToolStorageWitness(t *testing.T, fixture authorActivityReceiptFixture, runID, entityID string) {
	t.Helper()
	ctx := testAuthorActivityContext()
	requireRunFixtureForTest(t, ctx, fixture.store, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	if _, err := fixture.db.ExecContext(ctx, `INSERT INTO entity_state
		(run_id, entity_id, flow_instance, entity_type, current_state, fields, bookkeeping, revision)
		VALUES ($1, $2, 'storage-witness/one', 'storage_witness', 'queued', '{}', '{}', 7)`, runID, entityID); err != nil {
		t.Fatal(err)
	}
	for index, value := range []any{nil, `""`, `7`, `{"name":"second"}`} {
		if _, err := fixture.db.ExecContext(ctx, `INSERT INTO entity_mutations
			(mutation_id, run_id, entity_id, domain, path, new_value, writer_type, writer_id, created_at)
			VALUES ($1, $2, $3, 'authored_field', 'storage_value', $4, 'platform', 'storage-witness', $5)`,
			uuid.NewString(), runID, entityID, value, time.Unix(1700000000+int64(index), 0).UTC()); err != nil {
			t.Fatal(err)
		}
	}
}

func seedDeliveryRecoveryClaim(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context) runtimedelivery.ClaimedObligation {
	t.Helper()
	runID := uuid.NewString()
	seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "recovery.requested", "gateway", "", nil, 0, runID, events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("agent-a"), AgentIdentity: mustTestAgentIdentityForRun(runID, "agent-a", "recovery/instance-a")}
	selected := fixture.store.(deliveryFixtureStore)
	if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimDeliveryFixture(ctx, selected, event, route)
	if err != nil {
		t.Fatal(err)
	}
	return claimed
}

func seedWorkflowSideEffectStorageRows(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, claimed runtimedelivery.ClaimedObligation, count int) {
	t.Helper()
	runID, deliveryID, eventID := claimed.Claim.RunID(), claimed.Snapshot.DeliveryID, claimed.Snapshot.EventID
	now := time.Now().UTC().Truncate(time.Microsecond)
	outputIDs := make([]string, count)
	for i := range outputIDs {
		outputIDs[i] = uuid.NewString()
		if err := commitSemanticParentFixture(ctx, fixture.store, runID, outputIDs[i], now); err != nil {
			t.Fatal(err)
		}
	}
	if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_intents
			(run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,bundle_hash,semantic_digest,source_kind,source_event_id,source_field,
			cardinality,cursor,status,next_chunk_size,capsule,created_at,updated_at)
			VALUES ($1,$2,'physical','fan_out','physical.source',$3,$4,'event_payload_field',$5,'items',$6,$6,'closed',1,'{}',$7,$7)`,
			runID, deliveryID, "bundle-v2:sha256:"+strings.Repeat("1", 64), "sha256:"+strings.Repeat("2", 64), eventID, count, now); err != nil {
			return err
		}
		for i, outputID := range outputIDs {
			if _, err := tx.ExecContext(ctx, `INSERT INTO entity_mutations
				(mutation_id,run_id,entity_id,domain,path,new_value,caused_by_event,writer_type,writer_id,created_at)
				VALUES ($1,$2,$3,'authored_field','items','[]',$4,'platform','physical-footprint',$5)`,
				uuid.NewString(), runID, uuid.NewString(), eventID, now); err != nil {
				return err
			}
			timerID := uuid.NewString()
			status, fired, accepted := "active", any(nil), any(nil)
			if i != 0 {
				status, fired, accepted = "fired", now, now
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO timers
				(timer_id,timer_name,schedule_scope,schedule_key,immutable_hash,run_id,fire_event,fire_payload,routing_source,execution_mode,
				fire_at,initial_fire_at,recurring,owner_node,owner_kind,due_basis_kind,due_basis_absolute,task_type,status,fired_at,accepted_at,created_at)
				VALUES ($1,'physical-footprint','run',$7,'physical-footprint-hash',$2,'physical.fire','{}','{"kind":"root"}','live',
				$3,$3,FALSE,'physical-node','system','absolute',$3,'timer',$4,$5,$6,$3)`, timerID, runID, now, status, fired, accepted, timerID); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_outcomes
				(outcome_id,run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,event_id,created_at)
				VALUES ($1,$2,$3,'physical','fan_out','physical.source',$4,'committed',$5,$6)`,
				uuid.NewString(), runID, deliveryID, i, outputID, now); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func seedWorkflowProjectionFaultCut(t *testing.T, fixture authorActivityReceiptFixture, ctx context.Context) (string, string) {
	t.Helper()
	run, entity := uuid.NewString(), uuid.NewString()
	seedAuthorActivityReceiptRun(t, fixture, ctx, run)
	// Exact physical cuts qualify fault admission/restoration, not lawful
	// workflow creation. The pipeline roots exercise the real engine owner.
	if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO flow_instances(run_id,entity_id,instance_path,current_state,flow_template,mode,status,config,stage_defined,gates,bookkeeping,accumulator,revision,entered_state_at,created_at,updated_at) VALUES($1,$2,'storage-ref','queued','projection-flow','static','active','{"config":{},"workflow_version":"1.0.0","instance_id":"storage-ref","flow_path":"storage-ref"}',TRUE,'{}','{}','{}',1,$3,$3,$3)`, run, entity, time.Now().UTC()); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO entity_state(run_id,entity_id,flow_instance,entity_type,current_state,revision,fields,gates,accumulator) VALUES($1,$2,'storage-ref','test_entity','queued',1,'{}','{}','{}')`, run, entity)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return run, entity
}
