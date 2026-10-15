package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145GateFinalDiagnosticCuts = map[string][]string{
	"TestA2AccumulatorSchemaAdmittedCanonicalEvidenceOnBothStores": {
		"if got := a2CanonicalAccumulatorEvidence(t, proof); got != want",
		"mutations, err := storetest.ReadGateDuplicateMutationDiagnostics(proof.ctx, proof.selected.events, duplicate.event.ID())",
		"for _, mutation := range mutations",
		"domain, path, oldValue, newValue := mutation.Domain, mutation.Path, mutation.OldValue, mutation.NewValue",
		"t.Logf(\"duplicate mutation: %s %s old=%s new=%s\", domain, path, oldValue, newValue)",
		"t.Errorf(\"canonical duplicate changed business/history/output: got=%#v want=%#v\", got, want)",
	},
	"runA2MapFanOutCompiledImmutableSource": {
		"if err != nil || a2AccumulatorJSONHash(t, written) != a2AccumulatorJSONHash(t, wantWritten)",
		"persisted, readErr := storetest.ReadGateRunRootFieldsStorage(p.ctx, p.selected.events, p.runID)",
		"if readErr != nil",
		"t.Fatal(readErr)",
		"t.Fatalf(\"compiled writer changed exact admitted map keys/values: %s; persisted=%s err=%v\", written, persisted, err)",
	},
}

func family145GateFinalDiagnosticProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("error-only read lost original schema/claim/replay/source/history assertions or diagnostic error: %s %v", function, err)
	}
	cursor := -1
	for _, cut := range family145GateFinalDiagnosticCuts[function] {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("error-only read changed original branch, owner/context/identity, text destination or failure: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "QueryRow") || strings.Contains(after, "QueryContext") {
		return fmt.Errorf("diagnostic consumer retains raw authority")
	}
	return nil
}

func TestFamily145GateFinalDiagnosticsRetainFailureBranchesAndExactTextScopes(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if family145GateFinalDiagnosticCuts[row.Function] == nil {
			continue
		}
		matched++
		if err := family145GateFinalDiagnosticProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range family145GateFinalDiagnosticCuts[row.Function] {
			if family145GateFinalDiagnosticProofError(row.Function, row.Before, strings.Replace(row.After, cut, "lostFailureCut()", 1)) == nil {
				t.Fatalf("lost error diagnostic branch/binding/destination passed: %s", cut)
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.After)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if family145GateFinalDiagnosticProofError(row.Function, row.Before, strings.Replace(row.After, condition, "false", 1)) == nil {
				t.Fatalf("lost diagnostic or original business assertion passed: %s", condition)
			}
		}
	}
	if matched != 2 {
		t.Fatalf("last diagnostic consumer inventory=%d/2", matched)
	}
	for _, tc := range []struct{ function, ids, call string }{
		{"ReadGateDuplicateMutationDiagnosticsForTest", "eventID", "mutationlog.ReadGateDuplicateMutationDiagnosticsTx(ctx, tx, eventID)"},
		{"ReadGateRunRootFieldsStorageForTest", "runID", "entitystore.ReadScenarioSetupFieldsStorageTx(ctx, tx, runID, runID)"},
	} {
		owner := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_scenario_setup_observation.go", tc.function)
		for _, cut := range []string{"readFanOutSemanticStorage(ctx, selected, []string{" + tc.ids + "},", tc.call} {
			if strings.Count(owner, cut) != 1 {
				t.Fatalf("diagnostic borrowed another owner, transaction or physical identity: %s", cut)
			}
		}
	}
	for _, tc := range []struct{ path, function, query, destinations string }{
		{"internal/backend/mutationlog/selected_cleanup_observation.go", "ReadGateDuplicateMutationDiagnosticsTx", "SELECT domain,path,CAST(old_value AS TEXT),CAST(new_value AS TEXT) FROM entity_mutations WHERE caused_by_event=$1", "rows.Scan(&row.Domain, &row.Path, &row.OldValue, &row.NewValue)"},
		{"internal/backend/entityruntime/scenario_setup_observation.go", "ReadScenarioSetupFieldsStorageTx", "SELECT fields FROM entity_state WHERE run_id=$1 AND entity_id=$2", "runID, entityID).Scan(&fields)"},
	} {
		owner := family145ConversationForkOwnerSource(t, tc.path, tc.function)
		if queries := family145A2ClaimLiteralQueries(t, owner); len(queries) != 1 || queries[0] != tc.query || strings.Count(owner, tc.destinations) != 1 {
			t.Fatalf("diagnostic narrowed rows, normalized NULL/JSON text, changed strict scans or borrowed another accessor: %s %v", tc.function, queries)
		}
	}
}
