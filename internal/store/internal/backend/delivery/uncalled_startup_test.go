package delivery

import (
	"database/sql"
	"maps"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/tests/support/testutil"
)

func TestUncalledStartupClaimEligibilityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			if backend == "postgres" {
				_, db, _ = testutil.StartEmptyPostgres(t)
			} else {
				var err error
				db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "eligibility.sqlite"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
			}
			rows, err := db.QueryContext(t.Context(), uncalledStartupEligibilityFixture+uncalledStartupClaimsSelection, "current")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			got := make(map[string]bool)
			for rows.Next() {
				var id string
				var exhausted bool
				if err := rows.Scan(&id, &exhausted); err != nil {
					t.Fatal(err)
				}
				if _, duplicate := got[id]; duplicate {
					t.Fatalf("duplicate startup candidate %q", id)
				}
				got[id] = exhausted
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if want := map[string]bool{"bare": false, "exhausted": true}; !maps.Equal(got, want) {
				t.Fatalf("startup admission = %v, want %v", got, want)
			}
		})
	}
}

// Exercise the shared query on each dialect. Every excluded row varies one
// predicate from the exhausted case; the served proof supplies real DDL/state.
const uncalledStartupEligibilityFixture = `WITH cases(id,process,maximum,saved,canceled,settled,extra) AS (VALUES
	('bare','old',1,FALSE,FALSE,FALSE,FALSE),
	('exhausted','old',1,FALSE,FALSE,FALSE,FALSE),
	('still_current','current',1,FALSE,FALSE,FALSE,FALSE),
	('budget_left','old',2,FALSE,FALSE,FALSE,FALSE),
	('saved_completion','old',1,TRUE,FALSE,FALSE,FALSE),
	('canceled','old',1,FALSE,TRUE,FALSE,FALSE),
	('settled','old',1,FALSE,FALSE,TRUE,FALSE),
	('extra_attempt','old',1,FALSE,FALSE,FALSE,TRUE)
), runs AS (
	SELECT id AS run_id, 'bundle' AS bundle_hash, 'running' AS status FROM cases
), event_deliveries AS (
	SELECT id AS delivery_id,id AS run_id,'in_progress' AS status,'agent' AS subscriber_type,'agent-id' AS subscriber_id,
		'normal_runtime' AS execution_authority_kind,'bundle' AS authority_bundle_hash,id AS execution_authority_id,
		1 AS execution_authority_generation,'route' AS route_identity,1 AS retry_count,maximum AS max_retries,
		2 AS claim_version,2 AS current_attempt_version,TRUE AS current_attempt_open FROM cases
), runtime_generation_grants AS (
	SELECT id AS grant_id,1 AS runtime_generation,'bundle' AS bundle_hash,process AS process_authority_id,
		'admitted' AS state,CAST(NULL AS TEXT) AS selected_execution_id FROM cases
), event_delivery_attempts AS (
	SELECT id AS delivery_id,2 AS claim_version,TRUE AS open_marker,'open' AS closure_kind,
		CAST(NULL AS INTEGER) AS completed_at,'' AS outcome,'' AS reason_code,'new' AS claim_token FROM cases
	UNION ALL SELECT id,1,FALSE,'settled',1,'retry_scheduled','effect_recovery_outcome_unconfirmed','prior' FROM cases WHERE id<>'bare'
), runtime_external_effect_attempts AS (
	SELECT id AS attempt_id,id AS operation_id,id AS origin_delivery_id,id AS origin_run_id,'delivery' AS origin_kind,
		1 AS origin_claim_version,'prior' AS origin_claim_token,'route' AS origin_route_identity,
		'agent' AS origin_subscriber_type,'agent-id' AS origin_subscriber_id,1 AS launched_at,
		CASE WHEN saved THEN 'settled' ELSE 'outcome_uncertain' END AS state,
		CASE WHEN saved THEN 'response_settled' END AS completion_projection_phase,saved AS completion_continuation_active
	FROM cases WHERE id<>'bare'
	UNION ALL SELECT 'extra',id,id,id,'delivery',2,'new','route','agent','agent-id',NULL,'authorized',NULL,FALSE FROM cases WHERE extra
), runtime_external_effect_operations AS (
	SELECT id AS operation_id,id AS agent_run_id,'agent-id' AS agent_id,'provider_turn' AS effect_kind,
		'normal_agent' AS authority_kind,'bundle' AS bundle_hash,'outcome_uncertain' AS state FROM cases
), runtime_agent_turn_lifetimes AS (
	SELECT 'delivery' AS origin_kind,id AS origin_id,id AS run_id,'agent-id' AS agent_id,
		id AS first_attempt_id,id AS current_attempt_id,1 AS first_launched_at,
		CASE WHEN canceled THEN 'terminate' END AS cancel_reason,CASE WHEN settled THEN 1 END AS settled_at,
		CAST(NULL AS TEXT) AS origin_evidence FROM cases WHERE id<>'bare'
), `
