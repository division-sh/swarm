package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

// These are projection/finalizer differential controls, not a replacement for
// the private-writer contributor guard or actual lifecycle-owner execution.
func TestRunForkExactFactsTransitiveBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, s exactFactStore) {
		f := newExactFactFixture(t, s)
		exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
			seedRunForkRevisionMatrixFacts(t, ctx, tx, f, true, s.postgres)
			compareExactWithWhole(t, ctx, tx, s, f.runID, exactEffects(t, f.runID, exactMatrixRefs(t, f)...), 1, true)
		})
		t.Run("selection_and_dead_letter_outcome_join", func(t *testing.T) {
			failure := failures.Normalize(failures.New(failures.ClassComputeFailure, "joined_failure", "exact-fact-proof", "settle", nil), "exact-fact-proof", "settle")
			encoded, err := json.Marshal(failure)
			if err != nil {
				t.Fatal(err)
			}
			exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
				mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO event_delivery_attempts (delivery_id,claim_version,claim_token,started_at,lease_expires_at,open_marker,closure_kind,outcome,completed_at,duration_ms,reason_code,failure) VALUES ($1,1,$2,$3,$4,FALSE,'settled','dead_letter',$3,0,'joined_failure',$5)`, f.deliveryID, uuid.NewString(), f.at, f.at.Add(time.Minute), string(encoded))
				mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE dead_letters SET delivery_id=$1,claim_version=1,settlement_ref_kind='settled',failure=$3 WHERE dead_letter_id=$2`, f.deliveryID, f.deadLetterID, string(encoded))
				mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO event_delivery_handler_rule_selections (delivery_id,selection_context,disposition) VALUES ($1,'none','not_applicable')`, f.deliveryID)
				mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE event_deliveries SET status='dead_letter',claim_version=1,next_eligible_at=NULL,started_at=$1,settled_at=$1,reason_code='joined_failure',failure=$3 WHERE delivery_id=$2`, f.at, f.deliveryID, string(encoded))
				// Omitting the joined dead-letter contributor is not detected by an
				// unrelated exact capture; full validation must expose that omission.
				mustExecRunForkRevisionMatrix(t, ctx, tx, `SAVEPOINT omitted_contributor`)
				if _, err := finalizeRunForkRevisionMatrix(ctx, tx, s.postgres, exactEffects(t, f.runID, exactFactRef(t, runforkrevision.FamilyEventDeliveries, f.deliveryID))); err != nil {
					t.Fatal(err)
				}
				if err := validateRunForkRevisionMatrix(ctx, tx, s.postgres, f.runID); err == nil || !strings.Contains(err.Error(), "dead_letters") {
					t.Fatalf("omitted joined contributor full-validation error=%v", err)
				}
				mustExecRunForkRevisionMatrix(t, ctx, tx, `ROLLBACK TO SAVEPOINT omitted_contributor`)
				mustExecRunForkRevisionMatrix(t, ctx, tx, `RELEASE SAVEPOINT omitted_contributor`)
				compareExactWithWhole(t, ctx, tx, s, f.runID, exactEffects(t, f.runID,
					exactFactRef(t, runforkrevision.FamilyEventDeliveries, f.deliveryID),
					exactFactRef(t, runforkrevision.FamilyDeadLetters, f.deadLetterID)), 2, true)
			})
		})
		t.Run("structured_intent_outcome_barrier_with_pipe_path", func(t *testing.T) {
			// A delimiter in a valid declaration must remain a structured SQL
			// coordinate, never be recovered by splitting the ledger key.
			key := exactMatrixIntentKey(f)
			key.ElementRef.SemanticPath = `handlers["items|ready"].rules[0]`
			intent, err := runforkrevision.FanOutIntentFact(key)
			if err != nil {
				t.Fatal(err)
			}
			outcome, err := runforkrevision.FanOutOutcomeFact(key, 0)
			if err != nil {
				t.Fatal(err)
			}
			barrier, err := runforkrevision.FanOutBarrierFact(key)
			if err != nil {
				t.Fatal(err)
			}
			exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
				mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO fan_out_intents (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,bundle_hash,semantic_digest,source_kind,source_event_id,source_field,cardinality,cursor,status,next_chunk_size,capsule,created_at,updated_at) SELECT run_id,triggering_delivery_id,flow_path,declaration_family,$1,bundle_hash,semantic_digest,source_kind,source_event_id,source_field,cardinality,1,'closed',next_chunk_size,capsule,created_at,updated_at FROM fan_out_intents WHERE run_id=$2 AND triggering_delivery_id=$3`, key.ElementRef.SemanticPath, f.runID, f.deliveryID)
				mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO fan_out_outcomes (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,event_id,created_at) VALUES ($1,$2,'root','handler_rule',$3,0,'committed',$4,$5)`, f.runID, f.deliveryID, key.ElementRef.SemanticPath, f.eventID, f.at)
				mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO fan_out_obligation_barriers (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,bundle_hash,semantic_digest,target_flow_path,target_node_id,handler_event,join_id,route_scope_key,route_instance_id,route_instance_path,entity_id,routing_source,execution_mode,timer_handle,status,created_at,updated_at) VALUES ($1,$2,'root','handler_rule',$3,$4,$5,'root','matrix-node','matrix.complete','matrix-join','root','','root',$6,$7,'live',$8,'armed',$9,$9)`, f.runID, f.deliveryID, key.ElementRef.SemanticPath, "bundle-v2:sha256:"+strings.Repeat("1", 64), "sha256:"+strings.Repeat("1", 64), f.entityID, `{"kind":"root"}`, `{}`, f.at)
				compareExactWithWhole(t, ctx, tx, s, f.runID, exactEffects(t, f.runID, intent, outcome, barrier), 3, true)
			})
			exactTransaction(t, s, func(ctx context.Context, tx *sql.Tx) {
				for _, table := range []string{"fan_out_obligation_barriers", "fan_out_outcomes", "fan_out_intents"} {
					mustExecRunForkRevisionMatrix(t, ctx, tx, `DELETE FROM `+table+` WHERE run_id=$1 AND triggering_delivery_id=$2 AND semantic_path=$3`, f.runID, f.deliveryID, key.ElementRef.SemanticPath)
				}
				compareExactWithWhole(t, ctx, tx, s, f.runID, exactEffects(t, f.runID, intent, outcome, barrier), 4, true)
			})
		})
	})
}
