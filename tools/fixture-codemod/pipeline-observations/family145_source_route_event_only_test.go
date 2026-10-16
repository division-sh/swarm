package main

import (
	"strings"
	"testing"
)

func TestFamily145NativeEventOnlyRecordUsesOriginalMutationAndStrictRecordOwner(t *testing.T) {
	owner := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_event_support.go", "InsertNativeCanonicalEventRecordForTest")
	for _, cut := range []string{"eventFixtureDialectForTest(selected)", "admitted.Class() == events.EventAdmissionSelectedForkReplay", "eventrecord.FromAdmitted(admitted, settlement)", "runEventFixtureMutationForTest(ctx, selected,", "eventrecordpostgres.Insert(ctx, attempt, record)", "eventrecordsqlite.Insert(ctx, attempt, record)", "attempt.WithSQL(ctx,", "eventrecordpostgres.Load(ctx, tx, record.EventID)", "eventrecordsqlite.Load(ctx, tx, record.EventID)", "!found || !record.Equal(existing)"} {
		if strings.Count(owner, cut) != 1 {
			t.Fatalf("native event-only record lost closed original owner or exact replay cut: %s", cut)
		}
	}
	for _, escape := range []string{"CommitSemanticEvent", "CommitFlowInstanceActivation", "NewTransaction", "EnsureRun", "time.Now("} {
		if strings.Contains(owner, escape) {
			t.Fatalf("event-only record gained construction/publication/reconstructed owner/clock: %s", escape)
		}
	}
}
