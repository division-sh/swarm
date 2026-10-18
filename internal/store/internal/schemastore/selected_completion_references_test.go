package schemastore

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	"github.com/google/uuid"
)

// These are schema-owner reference tests, not admitted reset/cohort receipts.
// The runtime writer must still prove the complete canonical manifest atomically.
func verifySelectedCompletionReferencePairs(t *testing.T, db *sql.DB, run, otherRun string) {
	t.Helper()
	for _, cut := range []struct{ kind, child string }{{"deployment_revision", run}, {"event", otherRun}} {
		faults := []string{"none", "not_closed", "missing_execution", "missing_binding", "missing_child", "missing_source", "missing_kind", "missing_revision", "missing_generation", "missing_state", "missing_reset", "missing_reset_revision", "other_child", "changed_cut", "invalid_fingerprint"}
		if cut.kind == "event" {
			faults = append(faults, "false_null_event")
		}
		for _, fault := range faults {
			t.Run(cut.kind+"/"+fault, func(t *testing.T) {
				child := cut.child
				tx, err := db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				var execution, binding, source, kind string
				var revision, generation int64
				var event sql.NullString
				if err := tx.QueryRow(`SELECT execution_id,binding_id,source_run_id,fork_point_kind,fork_revision,generation,fork_event_id
					FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, child).Scan(&execution, &binding, &source, &kind, &revision, &generation, &event); err != nil {
					t.Fatal(err)
				}
				if fault != "not_closed" {
					if _, err := tx.Exec(`UPDATE run_fork_selected_contract_runtime_executions SET state='closed' WHERE execution_id=$1`, execution); err != nil {
						t.Fatal(err)
					}
				}
				reset := uuid.NewString()
				if _, err := tx.Exec(`INSERT INTO runtime_reset_operations (operation_id,actor_token_id,revision,phase,active_slot,record)
					VALUES ($1,'schema-reference-test',1,'completed',NULL,'{}')`, reset); err != nil {
					t.Fatal(err)
				}
				fingerprint := "sha256:" + strings.Repeat("a", 64)
				args := []any{execution, binding, child, source, kind, revision, generation, event, "closed", reset, int64(1), fingerprint, fingerprint, fingerprint, `{"schema_reference_only":true}`}
				switch fault {
				case "missing_execution":
					args[0] = nil
				case "missing_binding":
					args[1] = nil
				case "missing_child":
					args[2] = nil
				case "missing_source":
					args[3] = nil
				case "missing_kind":
					args[4] = nil
				case "missing_revision":
					args[5] = nil
				case "missing_generation":
					args[6] = nil
				case "false_null_event":
					args[7] = nil
				case "missing_state":
					args[8] = nil
				case "missing_reset":
					args[9] = nil
				case "missing_reset_revision":
					args[10] = nil
				case "other_child":
					if child == run {
						args[2] = otherRun
					} else {
						args[2] = run
					}
				case "changed_cut":
					args[5] = revision + 1
				case "invalid_fingerprint":
					args[12] = "not-a-fingerprint"
				}
				_, err = tx.Exec(`INSERT INTO run_fork_selected_contract_execution_detachments
					(execution_id,binding_id,fork_run_id,source_run_id,fork_point_kind,fork_revision,generation,fork_event_id,execution_state,
					reset_operation_id,reset_revision,planned_runs_fingerprint,execution_fingerprint,manifest_fingerprint,manifest)
					VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, args...)
				if fault != "none" {
					if err == nil {
						t.Fatalf("completion reference accepted contradictory owner/cut/state: %s", fault)
					}
					return
				}
				if err != nil {
					t.Fatalf("exact closed execution reference refused: %v", err)
				}
				var gotExecution, gotChild, gotReset, gotManifest string
				if err := tx.QueryRow(`SELECT execution_id,fork_run_id,reset_operation_id,CAST(manifest AS TEXT)
					FROM run_fork_selected_contract_execution_detachments WHERE execution_id=$1`, execution).Scan(&gotExecution, &gotChild, &gotReset, &gotManifest); err != nil {
					t.Fatal(err)
				}
				if gotExecution != execution || gotChild != child || gotReset != reset || !strings.Contains(gotManifest, `"schema_reference_only"`) {
					t.Fatal("completion reference readback changed its exact owner/reset/manifest")
				}
				if cut.kind == "deployment_revision" {
					verifySelectedCompletionMembers(t, tx, execution, child, otherRun, fingerprint)
				}
				if _, err := tx.Exec(`UPDATE run_fork_selected_contract_runtime_executions SET state='failed' WHERE execution_id=$1`, execution); err == nil {
					t.Fatal("positive completion reference allowed the canonical closed execution to reopen")
				}
			})
		}
	}
}

func verifySelectedCompletionMembers(t *testing.T, tx *sql.Tx, execution, run, otherRun, fingerprint string) {
	t.Helper()
	unreceipted := uuid.NewString()
	if _, err := tx.Exec(`INSERT INTO run_fork_selected_contract_runtime_executions
		(execution_id,binding_id,fork_run_id,source_run_id,fork_point_kind,fork_revision,generation,fork_event_id,
		executable_coordinate_fingerprint,admission_fingerprint,container_plan_fingerprint,actor_census_fingerprint,
		effective_config_fingerprint,declaration_plan_fingerprint,declaration_plan,preparation_fingerprint,preparation_binding,state,terminal_at)
		SELECT $2,binding_id,fork_run_id,source_run_id,fork_point_kind,fork_revision,generation+1,fork_event_id,
		executable_coordinate_fingerprint,admission_fingerprint,container_plan_fingerprint,actor_census_fingerprint,
		effective_config_fingerprint,declaration_plan_fingerprint,declaration_plan,preparation_fingerprint,preparation_binding,'closed',terminal_at
		FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1`, execution, unreceipted); err != nil {
		t.Fatal(err)
	}
	var turn, attempt, surface string
	if err := tx.QueryRow(`SELECT turn_id,completion_attempt_id,capability_surface_id FROM agent_turns WHERE run_id=$1 ORDER BY turn_id LIMIT 1`, run).Scan(&turn, &attempt, &surface); err != nil {
		t.Fatal(err)
	}
	turnInsert := `INSERT INTO run_fork_selected_contract_turn_detachments
		(execution_id,turn_id,fork_run_id,completion_attempt_id,capability_surface_id,row_fingerprint) VALUES ($1,$2,$3,$4,$5,$6)`
	for _, fault := range []string{"none", "null_execution", "null_turn", "null_run", "null_attempt", "null_surface", "wrong_run", "wrong_attempt", "wrong_surface", "missing_receipt"} {
		args := []any{execution, turn, run, attempt, surface, fingerprint}
		switch fault {
		case "null_execution":
			args[0] = nil
		case "null_turn":
			args[1] = nil
		case "null_run":
			args[2] = nil
		case "null_attempt":
			args[3] = nil
		case "null_surface":
			args[4] = nil
		case "wrong_run":
			args[2] = otherRun
		case "wrong_attempt":
			if err := tx.QueryRow(`SELECT completion_attempt_id FROM agent_turns WHERE run_id=$1 AND turn_id<>$2`, run, turn).Scan(&args[3]); err != nil {
				t.Fatal(err)
			}
		case "wrong_surface":
			if err := tx.QueryRow(`SELECT capability_surface_id FROM agent_turns WHERE run_id=$1 AND turn_id<>$2`, run, turn).Scan(&args[4]); err != nil {
				t.Fatal(err)
			}
		case "missing_receipt":
			args[0] = unreceipted
		}
		verifyCompletionReferenceStatement(t, tx, "turn/"+fault, turnInsert, args, fault == "none")
	}
	for _, binding := range []any{nil, `{"version":"activity-execution.v1","grant_id":"00000000-0000-4000-8000-000000000001","state_version":1}`} {
		request := uuid.NewString()
		if _, err := tx.Exec(`INSERT INTO activity_attempts
			(request_event_id,run_id,execution_mode,execution_binding,node_id,handler_event_key,activity_id,tool,status,success_event,failure_event,input_hash,result_event_id,result_event_type,result_payload,completed_at)
			VALUES ($1,$2,'live',$3,'schema','start','schema:1','schema.tool','succeeded','schema.done','schema.failed','schema-input',$4,'schema.done','{}',CURRENT_TIMESTAMP)`, request, run, binding, uuid.NewString()); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`INSERT INTO activity_attempt_live_parents (request_event_id,run_id,reply_context_present) VALUES ($1,$2,FALSE)`, request, run); err != nil {
			t.Fatal(err)
		}
		if err := activityjournal.RequireLiveAttemptParentsTx(context.Background(), tx, request); err != nil {
			t.Fatalf("original attached journal refused: %v", err)
		}
		origin := "installed_grant"
		if binding == nil {
			origin = "copied_terminal"
		}
		insert := `INSERT INTO run_fork_selected_contract_activity_detachments
			(execution_id,request_event_id,fork_run_id,origin_kind,lineage,row_fingerprint) VALUES ($1,$2,$3,$4,'{}',$5)`
		for _, fault := range []string{"none", "null_execution", "null_request", "null_run", "null_origin", "wrong_run", "missing_request", "missing_receipt", "unknown_origin"} {
			args := []any{execution, request, run, origin, fingerprint}
			switch fault {
			case "null_execution":
				args[0] = nil
			case "null_request":
				args[1] = nil
			case "null_run":
				args[2] = nil
			case "null_origin":
				args[3] = nil
			case "wrong_run":
				args[2] = otherRun
			case "missing_request":
				args[1] = uuid.NewString()
			case "missing_receipt":
				args[0] = unreceipted
			case "unknown_origin":
				args[3] = "inferred_from_run"
			}
			verifyCompletionReferenceStatement(t, tx, origin+"/"+fault, insert, args, fault == "none")
		}
		if _, err := tx.Exec(insert, execution, request, run, origin, fingerprint); err != nil {
			t.Fatal(err)
		}
		if err := activityjournal.RequireLiveAttemptParentsTx(context.Background(), tx, request); err == nil {
			t.Error("native journal accepted simultaneous live and retained membership: " + origin)
		}
		verifyCompletionReferenceStatement(t, tx, origin+"/canonical_delete", `DELETE FROM activity_attempts WHERE request_event_id=$1`, []any{request}, false)
		var original sql.NullString
		if err := tx.QueryRow(`SELECT execution_binding FROM activity_attempts WHERE request_event_id=$1`, request).Scan(&original); err != nil || original.Valid != (binding != nil) {
			t.Fatalf("membership changed original binding presence: %+v %v", original, err)
		}
	}
	if _, err := tx.Exec(turnInsert, execution, turn, run, attempt, surface, fingerprint); err != nil {
		t.Fatal(err)
	}
	verifyCompletionReferenceStatement(t, tx, "turn/canonical_delete", `DELETE FROM agent_turns WHERE turn_id=$1`, []any{turn}, false)
	verifyCompletionReferenceStatement(t, tx, "execution/duplicate_receipt", `INSERT INTO run_fork_selected_contract_execution_detachments SELECT * FROM run_fork_selected_contract_execution_detachments WHERE execution_id=$1`, []any{execution}, false)
}

func verifyCompletionReferenceStatement(t *testing.T, tx *sql.Tx, name, query string, args []any, accepted bool) {
	t.Helper()
	if _, err := tx.Exec(`SAVEPOINT selected_completion_reference`); err != nil {
		t.Fatal(err)
	}
	_, mutationErr := tx.Exec(query, args...)
	if _, err := tx.Exec(`ROLLBACK TO SAVEPOINT selected_completion_reference`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`RELEASE SAVEPOINT selected_completion_reference`); err != nil {
		t.Fatal(err)
	}
	if (mutationErr == nil) != accepted {
		t.Fatalf("completion reference %s: accepted=%t err=%v", name, accepted, mutationErr)
	}
}
