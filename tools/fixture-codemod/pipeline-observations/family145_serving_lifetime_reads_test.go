package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145ServingLifetimeHelpers = map[string][]string{
	"readServingLifetimeState": {
		"selected notifyAllChildrenStore",
		"metadata, err := storetest.ReadServingLifetimeMetadataStorage(context.Background(), selected, runID)",
		"state.cursor, state.status, state.generation, state.retry = metadata.Cursor, metadata.Status, metadata.Generation, metadata.Retry",
		"storetest.CountNestedRunFanOutOutcomes(context.Background(), selected, runID)",
		"&state.outcomes",
		"storetest.CountPhysicalRunEvents(context.Background(), selected, runID)",
		"&state.events",
		"storetest.CountServingLifetimeHistoryStorage(context.Background(), selected, runID)",
		"&state.history",
		"count, err := query.read()",
		"*query.out = count",
		"return state",
	},
	"assertServingLifetimeClaimOwner": {
		"selected notifyAllChildrenStore",
		"owner, err := storetest.ReadServingLifetimeClaimOwnerStorage(context.Background(), selected, runID)",
		"if err != nil || owner != want",
		"t.Fatalf(\"exact claim cleanup authority: owner=%q want=%q err=%v\", owner, want, err)",
	},
	"waitServingMatrixTriggerReceipt": {
		"selected notifyAllChildrenStore",
		"deadline := time.Now().Add(5 * time.Second)",
		"outcome, err := storetest.ReadNestedPipelineReceiptOutcome(context.Background(), selected, eventID)",
		"if outcome != \"success\"",
		"if !errors.Is(err, sql.ErrNoRows) || time.Now().After(deadline)",
		"time.Sleep(time.Millisecond)",
	},
}

func family145ServingLifetimeReadProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("serving lifetime read lost exact claim/receipt/retirement/error/deadline assertions: %s %v", function, err)
	}
	cursor := -1
	for _, cut := range family145ServingLifetimeHelpers[function] {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("serving read changed original independent cut/owner/run/event/destination: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "QueryRow") || strings.Contains(after, "*sql.DB") {
		return fmt.Errorf("shared serving observation still transports raw authority")
	}
	return nil
}

func TestFamily145ServingLifetimeReadChainPreservesAll21CallerCutsAndAssertions(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	helpers, callers, calls := 0, 0, 0
	for _, row := range rows {
		if family145ServingLifetimeHelpers[row.Function] != nil {
			helpers++
			if err := family145ServingLifetimeReadProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			for _, cut := range family145ServingLifetimeHelpers[row.Function] {
				if family145ServingLifetimeReadProofError(row.Function, row.Before, strings.Replace(row.After, cut, "foreignServingRead()", 1)) == nil {
					t.Fatalf("lost context/scope/slot/receipt cut passed: %s", cut)
				}
			}
			continue
		}
		if !strings.HasPrefix(row.File, "internal/runtime/conformance/") {
			continue
		}
		normalized, count := row.After, 0
		for helper := range family145ServingLifetimeHelpers {
			from, to := helper+"(t, f.selected,", helper+"(t, f.db,"
			count += strings.Count(normalized, from)
			normalized = strings.ReplaceAll(normalized, from, to)
		}
		if count == 0 {
			continue
		}
		callers++
		calls += count
		if row.Function == "TestFanOutServingM06SameBundleGenerationReplacementOnBothBackends" {
			// Compose only the previously reviewed native-runtime constructor cut.
			current := "newNotifyAllChildrenRuntime(t, f.selected, f.sources[0], time.Now,"
			original := "newNotifyAllChildrenRuntime(t, f.selected, f.db, f.sources[0], time.Now,"
			if strings.Count(normalized, current) != 1 || strings.Count(row.Before, original) != 1 {
				t.Fatal("reviewed same-bundle successor constructor binding disappeared")
			}
			normalized = strings.Replace(normalized, current, original, 1)
		}
		if normalized != row.Before {
			t.Fatalf("mechanical caller propagation changed a lifecycle/fault assertion, argument, cut or other operation: %s", row.Function)
		}
	}
	if helpers != 3 || callers != 8 || calls != 21 {
		t.Fatalf("serving read chain inventory: helpers=%d/3 callers=%d/8 calls=%d/21", helpers, callers, calls)
	}
	for _, tc := range []struct{ path, function, query string }{
		{"pipelinepersistence/fan_out_semantic_storage.go", "ReadServingLifetimeMetadataStorageTx", "SELECT cursor,status,claim_generation,CASE WHEN retry_ready_at IS NULL THEN 0 ELSE 1 END FROM fan_out_intents WHERE run_id=$1"},
		{"pipelinepersistence/fan_out_semantic_storage.go", "ReadServingLifetimeClaimOwnerStorageTx", "SELECT COALESCE(claim_owner,'') FROM fan_out_intents WHERE run_id=$1"},
		{"runforkrevision/postgres.go", "CountServingLifetimeHistoryStorageTx", "SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND family='fan_out_obligations'"},
		{"pipelinepersistence/nested_fan_out_observation.go", "ReadNestedPipelineReceiptOutcomeTx", "SELECT outcome FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'"},
	} {
		owner := family145ConversationForkOwnerSource(t, "internal/backend/"+tc.path, tc.function)
		if queries := family145A2ClaimLiteralQueries(t, owner); len(queries) != 1 || queries[0] != tc.query {
			t.Fatalf("serving read changed exact physical predicate/NULL/status/family: %s %v", tc.function, queries)
		}
	}
}
