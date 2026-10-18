package schemastore

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
)

func verifySelectedGrantExactExecution(t *testing.T, db *sql.DB, bundle, run, binding, otherBinding string) {
	t.Helper()
	var execution string
	if err := db.QueryRow(`SELECT execution_id FROM run_fork_selected_contract_runtime_executions
		WHERE binding_id=$1 AND fork_run_id=$2 AND generation=1`, binding, run).Scan(&execution); err != nil {
		t.Fatal(err)
	}
	insert := `INSERT INTO runtime_generation_grants
		(fact_id,grant_id,process_authority_id,process_owner_id,state_version,state,bundle_hash,
		 runtime_instance_id,runtime_generation,source_set_revision,snapshot,selected_binding_id,selected_fork_run_id,selected_execution_id)
		VALUES ($1,$2,$3,'schema-owner',$4,$5,$6,$7,1,NULL,'{}',$8,$9,$10)`
	grant, process, instance := uuid.NewString(), uuid.NewString(), uuid.NewString()
	for version, state := range []string{"prepared", "probe_settled", "admitted", "retired"} {
		if _, err := db.Exec(insert, uuid.NewString(), grant, process, version+1, state, bundle, instance, binding, run, execution); err != nil {
			t.Fatalf("exact selected grant version %s refused: %v", state, err)
		}
	}
	for _, wrongBinding := range []any{otherBinding, nil} {
		if _, err := db.Exec(insert, uuid.NewString(), uuid.NewString(), process, 5, "retired", bundle, instance, wrongBinding, run, execution); err == nil {
			t.Fatalf("selected grant accepted a different or absent canonical binding: %v", wrongBinding)
		}
	}
	var versions int
	if err := db.QueryRow(`SELECT COUNT(*) FROM runtime_generation_grants WHERE grant_id=$1`, grant).Scan(&versions); err != nil || versions != 4 {
		t.Fatalf("exact selected grant history changed: versions=%d error=%v", versions, err)
	}
	// The selected tuple is absent for ordinary source-set grants; the new
	// constraint must not turn their genuine NULL projections into a refusal.
	if _, err := db.Exec(`INSERT INTO runtime_generation_grants
		(fact_id,grant_id,process_authority_id,process_owner_id,state_version,state,bundle_hash,
		 runtime_instance_id,runtime_generation,source_set_revision,snapshot)
		VALUES ($1,$2,$3,'schema-owner',1,'prepared',$4,$5,1,'exact-source-revision','{}')`,
		uuid.NewString(), uuid.NewString(), process, bundle, instance); err != nil {
		t.Fatalf("ordinary source-set grant refused: %v", err)
	}
}
