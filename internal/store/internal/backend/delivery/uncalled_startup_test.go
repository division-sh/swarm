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
			got := make(map[string]UncalledStartupKind)
			for rows.Next() {
				var id string
				var kind UncalledStartupKind
				if err := rows.Scan(&id, &kind); err != nil {
					t.Fatal(err)
				}
				if _, duplicate := got[id]; duplicate {
					t.Fatalf("duplicate startup candidate %q", id)
				}
				got[id] = kind
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			want := map[string]UncalledStartupKind{
				"bare": UncalledStartupFirstCall, "workspace": UncalledStartupFirstCall,
				"exhausted":    UncalledStartupInterruptedRetry,
				"http_failure": UncalledStartupFailedRetry, "launch_rejected": UncalledStartupFailedRetry,
				"multi_terminal": UncalledStartupFailedRetry,
			}
			if !maps.Equal(got, want) {
				t.Fatalf("startup admission = %v, want %v", got, want)
			}
		})
	}
}

// Exercise the shared query on each dialect. The served proof supplies real
// DDL and claim history; these projections vary the evidence at the selector.
const uncalledStartupEligibilityFixture = `WITH existing(id,process,maximum,saved,canceled,settled,extra) AS (VALUES
	('bare','old',1,FALSE,FALSE,FALSE,FALSE),
	('exhausted','old',1,FALSE,FALSE,FALSE,FALSE),
	('still_current','current',1,FALSE,FALSE,FALSE,FALSE),
	('budget_left','old',2,FALSE,FALSE,FALSE,FALSE),
	('saved_completion','old',1,TRUE,FALSE,FALSE,FALSE),
	('canceled','old',1,FALSE,TRUE,FALSE,FALSE),
	('settled','old',1,FALSE,FALSE,TRUE,FALSE),
	('extra_attempt','old',1,FALSE,FALSE,FALSE,TRUE)
), ordinary(id) AS (VALUES
	('http_failure'),('launch_rejected'),('multi_terminal'),('workspace'),
	('ordinary_budget_left'),('ordinary_current_process'),('ordinary_saved'),('ordinary_canceled'),
	('ordinary_v2_attempt'),('wrong_claim_token'),('wrong_claim_version'),('wrong_route'),
	('foreign_run'),('foreign_actor'),('foreign_source'),('wrong_lifetime'),('active_drain'),
	('ordinary_fork'),('nonterminal_provider'),('wrong_history'),('uncertain_provider')
), cases AS (
	SELECT *, FALSE AS ordinary FROM existing
	UNION ALL
	SELECT id,CASE WHEN id='ordinary_current_process' THEN 'current' ELSE 'old' END,
		CASE WHEN id='ordinary_budget_left' THEN 2 ELSE 1 END,
		id='ordinary_saved',id='ordinary_canceled',FALSE,id='ordinary_v2_attempt',TRUE FROM ordinary
), runs AS (
	SELECT id AS run_id, 'bundle' AS bundle_hash, 'running' AS status,
		CASE WHEN id='ordinary_fork' THEN 'fork_materialization' ELSE 'event' END AS origin_kind FROM cases
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
		CAST(NULL AS INTEGER) AS completed_at,'' AS outcome,'' AS reason_code,'new' AS claim_token,
		CAST(NULL AS TEXT) AS failure FROM cases
	UNION ALL SELECT id,1,FALSE,'settled',1,'retry_scheduled',
		CASE WHEN id='wrong_history' THEN 'other' WHEN ordinary THEN 'handler_failure' ELSE 'effect_recovery_outcome_unconfirmed' END,
		'prior','failure' FROM cases WHERE id<>'bare'
), runtime_external_effect_attempts AS (
	SELECT id AS attempt_id,id AS operation_id,id AS origin_delivery_id,
		CASE WHEN id='foreign_run' THEN 'another-run' ELSE id END AS origin_run_id,'delivery' AS origin_kind,
		CASE WHEN id='wrong_claim_version' THEN 3 ELSE 1 END AS origin_claim_version,
		CASE WHEN id='wrong_claim_token' THEN 'other' ELSE 'prior' END AS origin_claim_token,
		CASE WHEN id='wrong_route' THEN 'other' ELSE 'route' END AS origin_route_identity,
		'agent' AS origin_subscriber_type,CASE WHEN id='foreign_actor' THEN 'another-agent' ELSE 'agent-id' END AS origin_subscriber_id,
		1 AS launched_at, CASE WHEN id NOT IN ('launch_rejected','nonterminal_provider') THEN 1 END AS response_observed_at,
		CASE WHEN id<>'nonterminal_provider' THEN 1 END AS completed_at,
		CASE WHEN saved THEN 'settled' WHEN id='nonterminal_provider' THEN 'launched'
			WHEN ordinary AND id<>'uncertain_provider' THEN 'terminal_failure' ELSE 'outcome_uncertain' END AS state,
		CASE WHEN saved THEN 'response_settled' END AS completion_projection_phase,saved AS completion_continuation_active
	FROM cases WHERE id NOT IN ('bare','workspace')
	UNION ALL SELECT id||'-v2',id,id,id,'delivery',2,'new','route','agent','agent-id',NULL,NULL,NULL,'authorized',NULL,FALSE FROM cases WHERE extra
	UNION ALL SELECT id||'-attempt-2',id,id,id,'delivery',1,'prior','route','agent','agent-id',1,1,1,'terminal_failure',NULL,FALSE FROM cases WHERE id='multi_terminal'
), runtime_external_effect_operations AS (
	SELECT id AS operation_id,id AS agent_run_id,'agent-id' AS agent_id,'provider_turn' AS effect_kind,
		'normal_agent' AS authority_kind,CASE WHEN id='foreign_source' THEN 'other' ELSE 'bundle' END AS bundle_hash,
		CASE WHEN ordinary AND id<>'uncertain_provider' THEN 'terminal_failure' ELSE 'outcome_uncertain' END AS state FROM cases
), runtime_agent_turn_lifetimes AS (
	SELECT 'delivery' AS origin_kind,id AS origin_id,id AS run_id,'agent-id' AS agent_id,
		id AS first_attempt_id,id AS admitted_attempt_id,
		CASE WHEN id='wrong_lifetime' THEN 'http_failure' WHEN id='multi_terminal' THEN id||'-attempt-2' ELSE id END AS current_attempt_id,
		1 AS first_launched_at,
		CASE WHEN canceled THEN 'terminate' END AS cancel_reason,CASE WHEN settled THEN 1 END AS settled_at,
		CAST(NULL AS TEXT) AS origin_evidence FROM cases WHERE id NOT IN ('bare','workspace')
), runtime_provider_attempt_drains AS (
	SELECT 'delivery' AS origin_kind,id AS origin_delivery_id FROM cases WHERE id='active_drain'
), `
