package runtimepersistence

import (
	"bytes"
	"testing"
	"time"
)

func TestIssue2269HeaderContentionPreservesCompanionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, record := constructWorkflowMutationFixture(t, backend, "review", time.Now().UTC())
			var fields []byte
			var fieldRevision int64
			if err := f.db.QueryRowContext(f.ctx, `SELECT fields,revision FROM entity_state WHERE run_id=$1 AND entity_id=$2`, record.Identity.RunID, record.EntityID).Scan(&fields, &fieldRevision); err != nil {
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
			var afterRevision int64
			if err := f.db.QueryRowContext(f.ctx, `SELECT fields,revision FROM entity_state WHERE run_id=$1 AND entity_id=$2`, record.Identity.RunID, record.EntityID).Scan(&after, &afterRevision); err != nil || !bytes.Equal(fields, after) || afterRevision != fieldRevision {
				t.Fatalf("header-only contender changed companion: fields=%s revision=%d err=%v", after, afterRevision, err)
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
