package schemastore

import (
	"context"
	"database/sql"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/google/uuid"
)

func verifyActivityExecutionBindingColumn(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	ctx := context.Background()
	rows, err := db.QueryContext(ctx, `SELECT * FROM activity_attempts WHERE 1=0`)
	if err != nil {
		t.Fatal(err)
	}
	columns, err := rows.Columns()
	closeErr := rows.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("read generated activity columns: %v, close: %v", err, closeErr)
	}
	present := map[string]bool{}
	for _, column := range columns {
		present[column] = true
	}
	if present["effect_class"] || !present["attempt"] || !present["execution_binding"] || !present["reply_context_present"] {
		t.Fatalf("generated schema retained authored class or lost native identity: %v", columns)
	}
	binding := processbinding.ActivityExecutionBinding{Version: processbinding.ActivityExecutionBindingVersion, GrantID: uuid.NewString(), StateVersion: 3}
	raw, err := canonicaljson.Bytes(binding)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{nil, string(raw)} {
		requestID := uuid.NewString()
		// Schema evidence only: this direct insert grants no runtime dispatch or recovery authority.
		if _, err := db.ExecContext(ctx, `INSERT INTO activity_attempts
					(request_event_id,run_id,execution_mode,node_id,handler_event_key,activity_id,tool,status,success_event,failure_event,input_hash,execution_binding)
					VALUES ($1,$2,'live','check','start','check:1','provider.check','started','check.done','check.failed','exact-input',$3)`, requestID, runID, value); err != nil {
			t.Fatal(err)
		}
		var stored []byte
		if err := db.QueryRowContext(ctx, `SELECT execution_binding FROM activity_attempts WHERE request_event_id=$1`, requestID).Scan(&stored); err != nil {
			t.Fatal(err)
		}
		if value == nil {
			if stored != nil {
				t.Fatalf("SQL NULL was replaced with attribution: %s", stored)
			}
			continue
		}
		var got processbinding.ActivityExecutionBinding
		if err := canonicaljson.DecodeInto(stored, &got); err != nil || got != binding {
			t.Fatalf("exact reference changed across native DDL: got=%+v err=%v", got, err)
		}
	}
}

func verifyActivityReplyLiveParents(t *testing.T, db *sql.DB, runID, sourceEventID string) {
	t.Helper()
	replies := []string{uuid.NewString(), uuid.NewString()}
	for _, id := range replies {
		if _, err := db.Exec(`INSERT INTO reply_contexts
			(reply_context_id,run_id,request_event_id,source_run_id,source_request_event_id,source_run_owner_key,
			 requester_flow_id,request_output_pin,reply_input_pin,provider_flow_id,provider_input_pin,provider_output_pin,origin_route,request_correlation_id)
			VALUES ($1,$2,$3,$2,$3,$4,'.','request','reply','provider','input','output','{}',$1)`, id, runID, sourceEventID, "run:"+runID); err != nil {
			t.Fatal(err)
		}
	}
	binding := processbinding.ActivityExecutionBinding{Version: processbinding.ActivityExecutionBindingVersion, GrantID: uuid.NewString(), StateVersion: 3}
	raw, err := canonicaljson.Bytes(binding)
	if err != nil {
		t.Fatal(err)
	}
	insert := `INSERT INTO activity_attempts
		(request_event_id,run_id,execution_mode,node_id,handler_event_key,activity_id,tool,status,success_event,failure_event,input_hash,execution_binding,reply_context_id,reply_context_present)
		VALUES ($1,$2,'live','check','start','check:1','provider.check','started','check.done','check.failed','exact-input',$3,$4,$5)`
	pair := `INSERT INTO activity_attempt_live_parents (request_event_id,run_id,reply_context_id,reply_context_present) VALUES ($1,$2,$3,$4)`
	for _, attributed := range []bool{false, true} {
		for _, present := range []bool{false, true} {
			id := uuid.NewString()
			var attribution, reply any
			if attributed {
				attribution = string(raw)
			}
			if present {
				reply = replies[0]
			}
			if _, err := db.Exec(insert, id, runID, attribution, reply, present); err != nil {
				t.Fatalf("exact journal presence refused: %v", err)
			}
			var before []byte
			if err := db.QueryRow(`SELECT execution_binding FROM activity_attempts WHERE request_event_id=$1`, id).Scan(&before); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(pair, id, runID, reply, present); err != nil {
				t.Fatalf("exact journal live parents refused: %v", err)
			}
			if _, err := db.Exec(`DELETE FROM activity_attempt_live_parents WHERE request_event_id=$1`, id); err != nil {
				t.Fatal(err)
			}
			for _, fault := range []string{"false_presence", "null_presence", "false_null_reply", "different_reply", "different_run"} {
				args := []any{id, runID, reply, present}
				switch fault {
				case "false_presence":
					args[2], args[3] = nil, !present
					if !present {
						args[2] = replies[0]
					}
				case "null_presence":
					args[3] = nil
				case "false_null_reply":
					args[2] = nil
					if !present {
						args[2] = replies[0]
					}
				case "different_reply":
					args[2] = replies[1]
				case "different_run":
					args[1] = uuid.NewString()
				}
				if _, err := db.Exec(pair, args...); err == nil {
					t.Fatalf("journal accepted contradictory live parents: attributed=%t reply=%t fault=%s", attributed, present, fault)
				}
			}
			if _, err := db.Exec(insert, uuid.NewString(), runID, attribution, reply, !present); err == nil {
				t.Fatal("canonical journal accepted a false reply-presence discriminator")
			}
			if _, err := db.Exec(pair, id, runID, reply, present); err != nil {
				t.Fatal(err)
			}
			var got []byte
			if err := db.QueryRow(`SELECT execution_binding FROM activity_attempts WHERE request_event_id=$1`, id).Scan(&got); err != nil || (got == nil) != (attribution == nil) || string(got) != string(before) {
				t.Fatalf("reply-parent validation changed original execution attribution: %s %v", got, err)
			}
		}
	}
}
