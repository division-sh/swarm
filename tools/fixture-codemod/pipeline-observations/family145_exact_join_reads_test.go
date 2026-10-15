package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145ExactJoinReadCuts = map[string][]string{
	"assertExactJoinDeliveryStatus": {
		"deadline := time.Now().Add(5 * time.Second)",
		"storage, err = storetest.ReadExactJoinNodeDeliveryStorage(ctx, selected.events, eventID, subscriberKey)",
		"if err == nil {",
		"status, failure = storage.Status, storage.Failure",
		"if err == nil && status == want",
		"time.Sleep(10 * time.Millisecond)",
		"runtimeLog, _ := storetest.ReadLatestWorkflowTimerRuntimeLogText(ctx, selected.events)",
	},
	"assertExactJoinDeliveryCount": {
		"count, err := storetest.CountTemplateNodeDelivery(ctx, selected.events, eventID, subscriberKey)",
	},
	"exactJoinOccurrenceEventID": {
		"deadline := time.Now().Add(5 * time.Second)",
		"if eventID, err := storetest.ReadWorkflowTimerEventIdentity(ctx, selected.events, runID, eventName); err == nil",
		"return eventID",
		"time.Sleep(10 * time.Millisecond)",
		`t.Fatalf("durable join occurrence event was not published")`,
	},
}

func family145ExactJoinReadProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("join helper changed exact status/count/error assertions: %s: %v", function, err)
	}
	cursor := -1
	for _, cut := range family145ExactJoinReadCuts[function] {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("join helper changed original context, scope, deadline, poll or diagnostic cut: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "selected.db") || strings.Contains(after, "selected.postgres") || strings.Contains(after, "QueryRowContext") {
		return fmt.Errorf("join helper retained SQL/dialect authority: %s", function)
	}
	return nil
}

func TestFamily145ExactJoinSharedHelpersKeepPhysicalScopesAndAllWaitAssertions(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	found := map[string]string{}
	for _, row := range rows {
		cuts := family145ExactJoinReadCuts[row.Function]
		if cuts == nil {
			continue
		}
		if found[row.Function] != "" {
			t.Fatalf("ambiguous join helper: %s", row.Function)
		}
		found[row.Function] = row.Before
		if err := family145ExactJoinReadProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range cuts {
			if family145ExactJoinReadProofError(row.Function, row.Before, strings.Replace(row.After, cut, "foreignJoinEvidence()", 1)) == nil {
				t.Fatalf("lost join scope/wait/diagnostic binding passed: %s", cut)
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.After)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if family145ExactJoinReadProofError(row.Function, row.Before, strings.Replace(row.After, condition, "false", 1)) == nil {
				t.Fatalf("lost original join assertion passed: %s", condition)
			}
		}
	}
	if len(found) != len(family145ExactJoinReadCuts) {
		t.Fatalf("shared join helper inventory=%d/%d", len(found), len(family145ExactJoinReadCuts))
	}
	for _, tc := range []struct{ function, caller string }{
		{"ReadExactJoinNodeDeliveryStorageTx", "assertExactJoinDeliveryStatus"},
	} {
		owner := family145ConversationForkOwnerSource(t, "internal/backend/delivery/read_projections.go", tc.function)
		actual := family145A2ClaimLiteralQueries(t, owner)
		original := family145A2ClaimLiteralQueries(t, found[tc.caller])
		if tc.caller == "assertExactJoinDeliveryStatus" {
			if len(original) != 3 {
				t.Fatalf("join status lost its separate original log diagnostic: %v", original)
			}
			original = original[:2] // The independently qualified global log owner is reused.
		}
		if len(actual) != 2 || !reflect.DeepEqual(actual, original) {
			t.Fatalf("%s changed exact original dialects, node/event/subscriber predicates or text casts: %v/%v", tc.function, actual, original)
		}
	}
	count := family145ConversationForkOwnerSource(t, "internal/backend/delivery/read_projections.go", "CountTemplateNodeDeliveryTx")
	if queries := family145A2ClaimLiteralQueries(t, count); len(queries) != 1 || queries[0] != "SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node' AND subscriber_id=$2" || !strings.Contains(count, "event, node).Scan(&count)") {
		t.Fatalf("existing count owner changed exact node/event/subscriber scope or count destination: %v", queries)
	}
	actualCount := family145ConnectorSQL(t, "package proof\n"+count)
	originalCount := family145ConnectorSQL(t, "package proof\n"+found["assertExactJoinDeliveryCount"])
	if len(actualCount) != 1 || len(originalCount) != 2 || originalCount[0] != actualCount[0] || originalCount[1] != actualCount[0] {
		t.Fatalf("existing count owner is not both original dialects' exact physical predicate: %v/%v", actualCount, originalCount)
	}
	identity := family145ConversationForkOwnerSource(t, "internal/backend/eventrecord/template_storage_observation.go", "ReadWorkflowTimerEventIdentityTx")
	if actual, original := family145A2ClaimLiteralQueries(t, identity), family145A2ClaimLiteralQueries(t, found["exactJoinOccurrenceEventID"]); !reflect.DeepEqual(actual, original) || len(actual) != 2 {
		t.Fatalf("join identity did not consume the already-qualified exact run/name owner: %v/%v", actual, original)
	}
}
