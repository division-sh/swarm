package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145GateFailureReadCuts = map[string][]string{
	"a2_count_join_execution_external_test.go|TestA2CountJoinRealExecutionAndRestartOnBothStores": {
		"raw, err := storetest.ReadGateEventFailure(ctx, selected.events, event.ID())",
	},
	"a2_join_until_race_external_test.go|TestA2UntilAndFinalArrivalHaveOneDurableCloseWinnerOnBothStores": {
		"raw, err := storetest.ReadGateEventFailure(ctx, selected.events, input.ID())",
	},
	"a2_join_close_rollback_external_test.go|TestA2JoinCloseSQLFailureRollsBackAndRecoversOnBothStores": {
		"failureJSON, err := storetest.ReadGateEventFailure(ctx, selected.events, failed.ID())",
	},
	"a2_bound_reply_binding_external_test.go|testA2BoundReplyJourney": {
		"raw, err := storetest.ReadGateSubscriberFailure(ctx, selected.events, replyID, collector.Key())",
	},
	"a2_publication_binding_external_test.go|testA2StageEntryPublicationBinding": {
		"raw, err := storetest.ReadGateDeadLetterFailure(ctx, selected.events, outputID)",
		"failure, readErr := storetest.ReadGateSubscriberFailureOrEmptyObject(ctx, selected.events, triggerID, worker.Key())",
		"raw, err := storetest.ReadGateSubscriberFailure(ctx, selected.events, outputID, collectors[0].Key())",
	},
	"a2_join_result_types_external_test.go|TestA2JoinResultTypesRealExecutionAndRestartOnBothStores": {
		"rawFailure, err := storetest.ReadGateNodeFailure(ctx, selected.events, event.ID(), node.Key())",
	},
	"a2_membership_snapshot_admission_external_test.go|failure": {
		"raw, err := storetest.ReadGateSubscriberFailure(f.ctx, f.selected.events, eventID, nodeKey)",
	},
	"internal/serveapp/receiver_mixed_agent_test.go|TestReceiverCompositionMixedAgentBothStores": {
		"diagnosticStore = p.deps.EventStore",
		"db, _, _ = selectedRuntimeStoreForTest(t, p)",
		"if failure, err := storetest.ReadGateDeadLetterFailure(context.Background(), diagnosticStore, published.EventID); err == nil {",
	},
}

func family145GateFailureReadProofError(key, before, after string) error {
	if parts := strings.SplitN(key, "|", 2); len(parts) == 2 {
		after = family145ServedDebugCallerInverse(parts[1], after)
	}
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(family145NormalizeGateCountAssertions(key, after))
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(want, got) {
		return fmt.Errorf("failure read lost original business/NULL/refusal/lifecycle assertion: %s", key)
	}
	cursor := -1
	for _, cut := range family145GateFailureReadCuts[key] {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor {
			return fmt.Errorf("failure read lost exact context/owner/predicate/destination or cut order: %s", cut)
		}
		cursor = at
	}
	return nil
}

func TestFamily145GateFailureReadsPreserveCompleteConsumersAndRejectLostCuts(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	var originals []string
	for _, row := range rows {
		key := family145GateScalarReadKey(row.File, row.Function)
		cuts, ok := family145GateFailureReadCuts[key]
		if !ok {
			continue
		}
		if found[key] {
			t.Fatalf("ambiguous failure-read consumer: %s", key)
		}
		found[key] = true
		if err := family145GateFailureReadProofError(key, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range cuts {
			if family145GateFailureReadProofError(key, row.Before, strings.Replace(row.After, cut, "foreignFailureRead()", 1)) == nil {
				t.Fatalf("lost context/owner/scope/destination/order passed: %s", cut)
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.After)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if family145GateFailureReadProofError(key, row.Before, strings.Replace(row.After, condition, "false", 1)) == nil {
				t.Fatalf("lost original failure assertion passed: %s", condition)
			}
		}
		originals = append(originals, family145ConnectorSQL(t, "package proof\n"+row.Before)...)
	}
	if len(found) != len(family145GateFailureReadCuts) {
		t.Fatalf("failure-read consumers=%d/%d", len(found), len(family145GateFailureReadCuts))
	}
	assertFamily145GateFailurePhysicalSQL(t, originals)
}

func assertFamily145GateFailurePhysicalSQL(t *testing.T, originals []string) {
	t.Helper()
	for _, owner := range []string{"ReadGateEventFailure", "ReadGateSubscriberFailure", "ReadGateNodeFailure", "ReadGateSubscriberFailureOrEmptyObject", "ReadGateDeadLetterFailure"} {
		physical := family145ConversationForkOwnerSource(t, "internal/backend/delivery/read_projections.go", owner+"Tx")
		queries := family145ConnectorSQL(t, "package proof\n"+physical)
		if len(queries) != 1 {
			t.Fatalf("%s physical query count=%d", owner, len(queries))
		}
		found := false
		for _, query := range originals {
			if query == queries[0] {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s changed physical scope, subscriber type, casts, NULL or row ordering: %s", owner, queries[0])
		}
		bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_gate_recovery_fixture.go", owner+"ForTest")
		if strings.Count(bridge, "return readFanOutSemanticStorage(ctx, selected, []string{eventID},") != 1 || strings.Count(bridge, "return delivery."+owner+"Tx(ctx, tx, eventID") != 1 {
			t.Fatalf("%s bypassed original selected read coordinator", owner)
		}
	}
}
