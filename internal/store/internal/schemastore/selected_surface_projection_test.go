package schemastore

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
)

func verifySelectedSurfaceProjection(t *testing.T, db *sql.DB, run, otherRun string) {
	t.Helper()
	var execution, otherExecution string
	if err := db.QueryRow(`SELECT execution_id FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, run).Scan(&execution); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT execution_id FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`, otherRun).Scan(&otherExecution); err != nil {
		t.Fatal(err)
	}
	insert := `INSERT INTO managed_agent_capability_surfaces
		(surface_id,integrity_hash,authority_kind,authority_id,execution_kind,execution_authority_id,run_id,actor_id,provider,transport,surface,selected_execution_id,selected_fork_run_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'schema-actor','schema-provider','api','{}',$8,$9)`
	for _, fault := range []string{"none", "missing_execution", "missing_child", "different_execution", "different_child", "generic_live_run"} {
		args := []any{uuid.NewString(), uuid.NewString(), "provider_turn", uuid.NewString(), "selected_contract_fork", execution, nil, execution, run}
		switch fault {
		case "missing_execution":
			args[7] = nil
		case "missing_child":
			args[8] = nil
		case "different_execution":
			args[7] = otherExecution
		case "different_child":
			args[8] = otherRun
		case "generic_live_run":
			args[6] = run
		}
		_, err := db.Exec(insert, args...)
		if fault == "none" && err != nil {
			t.Fatalf("exact selected surface projection refused: %v", err)
		}
		if fault != "none" && err == nil {
			t.Fatalf("selected surface accepted contradictory projection: %s", fault)
		}
	}
	for _, kind := range []string{"normal_agent", "selected_fork_preparation"} {
		if _, err := db.Exec(insert, uuid.NewString(), uuid.NewString(), "startup_probe", uuid.NewString(), kind, uuid.NewString(), nil, nil, nil); err != nil {
			t.Fatalf("runless %s surface refused: %v", kind, err)
		}
		if _, err := db.Exec(insert, uuid.NewString(), uuid.NewString(), "startup_probe", uuid.NewString(), kind, uuid.NewString(), nil, execution, run); err == nil {
			t.Fatalf("runless %s accepted selected projections", kind)
		}
	}
}
