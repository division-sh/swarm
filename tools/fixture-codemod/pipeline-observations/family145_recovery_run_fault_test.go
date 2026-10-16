package main

import (
	"strings"
	"testing"
)

func TestFamily145RecoveryRunFaultKeepsExactPhysicalWriteAndOriginalWriterCoordinator(t *testing.T) {
	owner := family145ConversationForkOwnerSource(t, "internal/backend/eventrecord/fixture_chain_depth.go", "ClearEventRunForRecoveryTx")
	for _, cut := range []string{
		"UPDATE events SET run_id = NULL WHERE event_id = $1::uuid", "UPDATE events SET run_id = NULL WHERE event_id = ?",
		"tx.ExecContext(ctx, query, eventID)", "result.RowsAffected()", "if changed != 1", "return err",
	} {
		if !strings.Contains(owner, cut) {
			t.Fatalf("named recovery fault changed exact write/binding/absence/error contract: %s", cut)
		}
	}
	bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_event_support.go", "ClearEventRunForRecoveryForTest")
	for _, cut := range []string{"validateStandaloneStorageEvent(selected, eventID)", "runUnrevisionedEventFixtureTransactionForTest(ctx, selected,", "eventrecord.ClearEventRunForRecoveryTx(ctx, tx, postgres, eventID)"} {
		if !strings.Contains(bridge, cut) {
			t.Fatalf("recovery fault lost exact unrevisioned selected writer: %s", cut)
		}
	}
	queue := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/recovery_run_fault_queue_test.go", "RecoveryRunFaultQueuedWritersForTest")
	if !strings.Contains(queue, "return selected.backend.QueuedWritersForTest()") {
		t.Fatal("admission proof stopped observing the original selected SQLite coordinator")
	}
}
