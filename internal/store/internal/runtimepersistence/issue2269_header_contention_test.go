package runtimepersistence

import (
	"bytes"
	"reflect"
	"testing"
	"time"
)

func TestIssue2269HeaderContentionPreservesCompanionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, record := constructWorkflowMutationFixture(t, backend, "review", time.Now().UTC())
			var fields []byte
			var dataCreatedAt, dataUpdatedAt any
			if err := f.db.QueryRowContext(f.ctx, `SELECT fields,created_at,updated_at FROM entity_state WHERE run_id=$1 AND entity_id=$2`, record.Identity.RunID, record.EntityID).Scan(&fields, &dataCreatedAt, &dataUpdatedAt); err != nil {
				t.Fatal(err)
			}
			if err := AdvanceGateHeaderRevisionForTest(f.ctx, f.store, record); err != nil {
				t.Fatal(err)
			}
			var headerRevision int64
			if err := f.db.QueryRowContext(f.ctx, `SELECT revision FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, record.Identity.RunID, record.EntityID).Scan(&headerRevision); err != nil || headerRevision != record.ExpectedRevision+1 {
				t.Fatalf("exact header increment=%d err=%v", headerRevision, err)
			}
			var after []byte
			var afterCreatedAt, afterUpdatedAt any
			if err := f.db.QueryRowContext(f.ctx, `SELECT fields,created_at,updated_at FROM entity_state WHERE run_id=$1 AND entity_id=$2`, record.Identity.RunID, record.EntityID).Scan(&after, &afterCreatedAt, &afterUpdatedAt); err != nil || !bytes.Equal(fields, after) || !reflect.DeepEqual(dataCreatedAt, afterCreatedAt) || !reflect.DeepEqual(dataUpdatedAt, afterUpdatedAt) {
				t.Fatalf("header-only contender changed companion: fields=%s created=%v updated=%v err=%v", after, afterCreatedAt, afterUpdatedAt, err)
			}
			if err := AdvanceGateHeaderRevisionForTest(f.ctx, f.store, record); err == nil {
				t.Fatal("stale header contender was accepted")
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT revision FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, record.Identity.RunID, record.EntityID).Scan(&headerRevision); err != nil || headerRevision != record.ExpectedRevision+1 {
				t.Fatalf("stale contender changed header=%d err=%v", headerRevision, err)
			}
		})
	}
}
