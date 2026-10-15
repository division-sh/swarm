package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145PublicationHistoryCuts = []string{
	"beforeReplay := []pipeline.WorkflowInstance{load(0), load(1)}",
	"beforeStorage, err := storetest.ReadRunPublicationHistoryStorage(ctx, selected.events, runID)",
	"eventsBefore, revisionsBefore := beforeStorage.Events, beforeStorage.Revisions",
	"for _, event := range replay",
	"bus.PublishAcknowledged(ctx, event)",
	"waitForGateRecoveryQuiescence(t, bus, ctx)",
	"afterStorage, err := storetest.ReadRunPublicationHistoryStorage(ctx, selected.events, runID)",
	"eventsAfter, revisionsAfter := afterStorage.Events, afterStorage.Revisions",
	"if eventsAfter != eventsBefore || revisionsAfter != revisionsBefore",
}

func family145PublicationHistoryProofError(before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("multi-until snapshot lost original replay/later-arm/history/receipt assertions: %v", err)
	}
	// The earlier journey also joins work; locate this exact replay window first.
	window := strings.Index(after, family145PublicationHistoryCuts[0])
	if window < 0 {
		return fmt.Errorf("original replay window disappeared")
	}
	after = after[window:]
	cursor := -1
	for _, cut := range family145PublicationHistoryCuts {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("multi-until snapshot changed original cut, selected owner, run, context or destination: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "QueryRow") || strings.Contains(after, "run_fork_revisions") {
		return fmt.Errorf("replay footprint retains its raw history interpreter")
	}
	return nil
}

func TestFamily145PublicationHistoryFootprintRetainsAssertionsAndSnapshotCuts(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Function != "TestA2MultiUntilIndependentRecipientEntriesAndRestartOnBothStores" {
			continue
		}
		matched++
		if err := family145PublicationHistoryProofError(row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range family145PublicationHistoryCuts {
			at := strings.Index(row.After, family145PublicationHistoryCuts[0])
			hostile := row.After[:at] + strings.Replace(row.After[at:], cut, "foreignReplayCut()", 1)
			if family145PublicationHistoryProofError(row.Before, hostile) == nil {
				t.Fatalf("lost replay snapshot binding/order passed: %s", cut)
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.After)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if family145PublicationHistoryProofError(row.Before, strings.Replace(row.After, condition, "false", 1)) == nil {
				t.Fatalf("lost original until/replay assertion passed: %s", condition)
			}
		}
		queries := family145A2ClaimLiteralQueries(t, row.Before)
		want := "SELECT (SELECT COUNT(*) FROM events WHERE run_id=$1), (SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1)"
		if len(queries) != 2 || queries[0] != want || queries[1] != want {
			t.Fatalf("original replay snapshot membership changed: %v", queries)
		}
	}
	if matched != 1 {
		t.Fatalf("multi-until replay snapshot inventory=%d/1", matched)
	}
	owner := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_event_support.go", "ReadRunPublicationHistoryStorageForTest")
	for _, cut := range []string{"readFanOutSemanticStorage(ctx, selected, []string{runID},", "eventrecord.CountRunEventStorageTx(ctx, tx, runID)", "runforkrevision.CountActivityJournalRevisionsForTest(ctx, tx, runID)", "out.Revisions = int(revisions)", "if int64(out.Revisions) != revisions"} {
		if strings.Count(owner, cut) != 1 {
			t.Fatalf("snapshot split the original native transaction, scope or int destination: %s", cut)
		}
	}
	for _, tc := range []struct{ file, function, query string }{
		{"internal/backend/eventrecord/run_name_cardinality.go", "CountRunEventStorageTx", "SELECT COUNT(*) FROM events WHERE run_id=$1"},
		{"internal/backend/runforkrevision/postgres.go", "CountActivityJournalRevisionsForTest", "SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1"},
	} {
		component := family145ConversationForkOwnerSource(t, tc.file, tc.function)
		if queries := family145A2ClaimLiteralQueries(t, component); len(queries) != 1 || queries[0] != tc.query {
			t.Fatalf("%s changed original component membership: %v", tc.function, queries)
		}
	}
}
