package schemastore

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
)

func verifyTurnLiveParents(t *testing.T, db *sql.DB, bundle, run, otherRun string) {
	t.Helper()
	var turns, attempts, surfaces [2]string
	for i := range turns {
		turns[i], attempts[i], surfaces[i] = uuid.NewString(), uuid.NewString(), uuid.NewString()
		if _, err := db.Exec(`INSERT INTO managed_agent_capability_surfaces
			(surface_id,integrity_hash,authority_kind,authority_id,execution_kind,execution_authority_id,run_id,actor_id,provider,transport,surface)
			VALUES ($1,$4,'provider_turn',$2,'normal_agent','schema-owner',$3,'schema-actor','schema-provider','api','{}')`, surfaces[i], turns[i], run, surfaces[i]); err != nil {
			t.Fatal(err)
		}
		operation := uuid.NewString()
		if _, err := db.Exec(`INSERT INTO runtime_external_effect_operations
			(operation_id,effect_kind,effect_class,execution_mode,bundle_hash,authority_kind,authority_id,generation,startup_authority_id,request_fingerprint,state)
			VALUES ($1,'schema-probe','write_or_unknown','live',$2,'serve_registration','schema-owner',1,$3,'schema-request','settled')`, operation, bundle, uuid.NewString()); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO runtime_external_effect_attempts
			(attempt_id,operation_id,attempt_ordinal,adapter,transport,execution_mode,generation,execution_owner,lease_expires_at,fence_generation,
			 usage_target_kind,usage_target_id,capability_surface_id,state,authorized_at)
			VALUES ($1,$2,1,'schema-adapter','api','live',1,'schema-owner',CURRENT_TIMESTAMP,1,'agent_turn',$3,$4,'settled',CURRENT_TIMESTAMP)`, attempts[i], operation, turns[i], surfaces[i]); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO agent_turns
			(turn_id,run_id,agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,
			 session_id,flow_instance,memory_enabled,capability_surface_id,agent_frame_bytes,completion_attempt_id,completion_evidence,execution_mode)
			VALUES ($1,$2,'schema-actor','schema-actor','declared','root','','',$3,'',FALSE,$4,$5,$6,'{}','live')`,
			turns[i], run, uuid.NewString(), surfaces[i], []byte("{}"), attempts[i]); err != nil {
			t.Fatal(err)
		}
	}
	pair := `INSERT INTO agent_turn_live_parents (turn_id,run_id,completion_attempt_id,capability_surface_id) VALUES ($1,$2,$3,$4)`
	for _, fault := range []string{"run", "attempt", "surface", "null_run", "null_attempt", "null_surface"} {
		args := []any{turns[0], run, attempts[0], surfaces[0]}
		switch fault {
		case "run":
			args[1] = otherRun
		case "attempt":
			args[2] = attempts[1]
		case "surface":
			args[3] = surfaces[1]
		case "null_run":
			args[1] = nil
		case "null_attempt":
			args[2] = nil
		case "null_surface":
			args[3] = nil
		}
		if _, err := db.Exec(pair, args...); err == nil {
			t.Fatalf("turn accepted mismatched live parents: %s", fault)
		}
	}
	if _, err := db.Exec(pair, turns[0], run, attempts[0], surfaces[0]); err != nil {
		t.Fatalf("exact turn live parents refused: %v", err)
	}
}
