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
	binding := processbinding.ActivityExecutionBinding{Version: processbinding.ActivityExecutionBindingVersion, GrantID: uuid.NewString(), StateVersion: 3}
	raw, err := canonicaljson.Bytes(binding)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{nil, string(raw)} {
		requestID := uuid.NewString()
		// Schema evidence only: this direct insert grants no runtime dispatch or recovery authority.
		if _, err := db.ExecContext(ctx, `INSERT INTO activity_attempts
					(request_event_id,run_id,execution_mode,node_id,handler_event_key,activity_id,tool,effect_class,status,success_event,failure_event,input_hash,execution_binding)
					VALUES ($1,$2,'live','check','start','check:1','provider.check','non_idempotent_write','started','check.done','check.failed','exact-input',$3)`, requestID, runID, value); err != nil {
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
