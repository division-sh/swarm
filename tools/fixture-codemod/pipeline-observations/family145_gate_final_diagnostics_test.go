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
