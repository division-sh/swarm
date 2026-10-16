package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145CompoundFootprintCuts = map[string][]string{
	"TestA2PersistedJoinHydrationRefusesCorruptionOnBothStores": {
		"footprint := func() [5]int",
		"counts, err := storetest.ReadA2JoinHydrationFootprintStorage(p.ctx, p.selected.events, p.runID)",
		"return counts",
	},
	"TestA2JoinCancellationSettlesAlreadyBoundWorkOnBothStores": {
		"pending, err := storetest.CountRunNonterminalDeliveryStorage(ctx, selected.events, runID)",
		"if err != nil || pending != 0",
	},
	"a2CancellationFootprint": {
		"result, err := storetest.ReadA2CancellationFootprintStorage(ctx, selected.events, runID)",
		"return result",
	},
	"footprint": {
		"storage, err := storetest.ReadA2RetainedSourceFootprintStorage(f.p.ctx, f.p.selected.events, f.p.runID)",
		"result.Mutations, result.Facts, result.Revisions, result.LatestRevision = storage.Mutations, storage.Facts, storage.Revisions, storage.LatestRevision",
		"result.Publications, result.Deliveries, result.Outcomes = storage.Publications, storage.Deliveries, storage.Outcomes",
		"result.Cursor, result.Cardinality, result.Status = storage.Cursor, storage.Cardinality, storage.Status",
		"diagnostic := storage.Diagnostic",
		"state, err := json.Marshal(f.p.instance(t))",
		"result.State = a2AccumulatorJSONHash(t, state)",
		"result.Source = sha256.Sum256(f.readRaw(t))",
		"result.Diagnostic = sha256.Sum256(diagnostic)",
		"return result",
	},
}

func family145CompoundFootprintRow(file, function string) bool {
	return family145CompoundFootprintCuts[function] != nil && (function != "footprint" || file == "internal/runtime/pipeline/a2_map_fan_out_source_corruption_external_test.go")
}

func family145CompoundFootprintProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("compound footprint lost original rejection, replay, claim, receipt or no-mutation assertions: %v", err)
	}
	cursor := -1
	for _, cut := range family145CompoundFootprintCuts[function] {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("compound footprint changed original context/owner/run, slot or independent cut: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "QueryRow") {
		return fmt.Errorf("compound footprint retains a raw interpreter")
	}
	return nil
}

func TestFamily145CompoundFootprintsPreserveEverySlotScopeAndIndependentCut(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if !family145CompoundFootprintRow(row.File, row.Function) {
			continue
		}
		matched++
		if err := family145CompoundFootprintProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range family145CompoundFootprintCuts[row.Function] {
			for _, replacement := range []string{"foreignFootprint()", cut + "\n" + cut} {
				if family145CompoundFootprintProofError(row.Function, row.Before, strings.Replace(row.After, cut, replacement, 1)) == nil {
					t.Fatalf("missing/foreign/duplicate footprint binding passed: %s/%s", row.Function, cut)
				}
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.After)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if family145CompoundFootprintProofError(row.Function, row.Before, strings.Replace(row.After, condition, "false", 1)) == nil {
				t.Fatalf("lost original footprint assertion passed: %s/%s", row.Function, condition)
			}
		}
	}
	if matched != 4 {
		t.Fatalf("compound footprint caller inventory=%d/4", matched)
	}
	for _, tc := range []struct{ function, ordered string }{
		{"ReadA2JoinHydrationFootprintStorageForTest", "mutationlog.CountRunMutationStorageTx,\n\t\t\tcountFootprintRevisionsForTest,\n\t\t\tgenericschedule.CountTimerRowsForRun,\n\t\t\teventrecord.CountRunEventStorageTx,\n\t\t\tpipelinepersistence.CountNestedRunFanOutOutcomesTx,"},
		{"ReadA2CancellationFootprintStorageForTest", "eventrecord.CountRunEventStorageTx,\n\t\t\tdeliveryadapter.CountRunDeliveryStorageTx,\n\t\t\tdeliveryadapter.CountRunDeliveryAttemptStorageTx,\n\t\t\tmutationlog.CountRunMutationStorageTx,\n\t\t\tcountFootprintRevisionsForTest,"},
		{"ReadA2MapHistoryFootprintStorageForTest", "out.Facts, err = checkedFootprintCountForTest(facts)"},
		{"ReadA2RetainedSourceFootprintStorageForTest", "out.Deliveries, err = deliveryadapter.CountRunEventLinkedDeliveryStorageTx(ctx, tx, runID)"},
	} {
		owner := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_event_support.go", tc.function)
		for _, cut := range []string{"readFanOutSemanticStorage(ctx, selected, []string{runID},", tc.ordered} {
			if strings.Count(family145GateCountTokens(owner), family145GateCountTokens(strings.TrimSuffix(cut, ","))) != 1 {
				t.Fatalf("%s split native snapshot or changed array/destination scope: %s", tc.function, cut)
			}
		}
		if strings.Contains(tc.function, "Hydration") || strings.Contains(tc.function, "Cancellation") {
			if strings.Count(owner, "out[i], err = count(ctx, tx, runID)") != 1 {
				t.Fatalf("%s moved array components out of their original snapshot", tc.function)
			}
		}
	}
	sourceOwner := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_event_support.go", "ReadA2RetainedSourceFootprintStorageForTest")
	cursor := -1
	for _, cut := range []string{
		"out.Mutations, err = mutationlog.CountRunMutationStorageTx(ctx, tx, runID)",
		"revisions, facts, err := runforkrevision.CountNotifyFanOutRevisionStorageForTest(ctx, tx, runID)",
		"out.Facts, err = checkedFootprintCountForTest(facts)",
		"out.Revisions, err = checkedFootprintCountForTest(revisions)",
		"out.LatestRevision, err = runforkrevision.ReadLatestRunRevisionStorageForTestTx(ctx, tx, runID)",
		"out.Publications, err = eventrecord.CountRunEventStorageTx(ctx, tx, runID)",
		"out.Deliveries, err = deliveryadapter.CountRunEventLinkedDeliveryStorageTx(ctx, tx, runID)",
		"out.Outcomes, err = pipelinepersistence.CountNestedRunFanOutOutcomesTx(ctx, tx, runID)",
		"metadata, err := pipelinepersistence.ReadFanOutSourceFootprintMetadataStorageTx(ctx, tx, runID)",
		"out.Cursor, out.Cardinality, out.Status, out.Diagnostic = metadata.Cursor, metadata.Cardinality, metadata.Status, metadata.Diagnostic",
	} {
		at := strings.Index(sourceOwner, cut)
		if at <= cursor || strings.Count(sourceOwner, cut) != 1 {
			t.Fatalf("source footprint changed original snapshot components/destinations: %s", cut)
		}
		cursor = at
	}
	for _, tc := range []struct{ path, function, query string }{
		{"internal/backend/mutationlog/selected_cleanup_observation.go", "CountRunMutationStorageTx", "SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1"},
		{"internal/backend/runforkrevision/postgres.go", "CountActivityJournalRevisionsForTest", "SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1"},
		{"internal/backend/runforkrevision/postgres.go", "CountNotifyFanOutRevisionStorageForTest", "SELECT (SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1), (SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1)"},
		{"internal/backend/runforkrevision/postgres.go", "ReadLatestRunRevisionStorageForTestTx", "SELECT COALESCE(MAX(revision),0) FROM run_fork_revisions WHERE run_id=$1"},
		{"internal/backend/genericschedule/test_workflow_scheduler_observation.go", "CountTimerRowsForRun", "SELECT COUNT(*) FROM timers WHERE run_id=$1"},
		{"internal/backend/eventrecord/run_name_cardinality.go", "CountRunEventStorageTx", "SELECT COUNT(*) FROM events WHERE run_id=$1"},
		{"internal/backend/pipelinepersistence/nested_fan_out_observation.go", "CountNestedRunFanOutOutcomesTx", "SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1"},
		{"internal/backend/delivery/read_projections.go", "CountRunDeliveryStorageTx", "SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1"},
		{"internal/backend/delivery/read_projections.go", "CountRunDeliveryAttemptStorageTx", "SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id=$1)"},
		{"internal/backend/delivery/read_projections.go", "CountRunEventLinkedDeliveryStorageTx", "SELECT COUNT(*) FROM event_deliveries WHERE event_id IN (SELECT event_id FROM events WHERE run_id=$1)"},
		{"internal/backend/delivery/read_projections.go", "CountRunNonterminalDeliveryStorageTx", "SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status NOT IN ('delivered','dead_letter')"},
		{"internal/backend/pipelinepersistence/fan_out_semantic_storage.go", "ReadFanOutSourceFootprintMetadataStorageTx", "SELECT cursor,cardinality,status,blocked_reason FROM fan_out_intents WHERE run_id=$1"},
	} {
		owner := family145ConversationForkOwnerSource(t, tc.path, tc.function)
		if queries := family145A2ClaimLiteralQueries(t, owner); len(queries) != 1 || queries[0] != tc.query {
			t.Fatalf("%s changed original physical membership/NULL/max/status scope: %v", tc.function, queries)
		}
	}
}
