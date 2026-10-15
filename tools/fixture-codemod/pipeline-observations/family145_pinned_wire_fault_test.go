package main

import (
	"encoding/json"
	"strings"
	"testing"
)

var family145PinnedWireFaultCuts = []string{
	"source := f.intent.Source",
	"count, err := storetest.SetPinnedAuthoredMutationWire(f.p.ctx, f.p.selected.events, source, raw)",
	"if err != nil {\n\t\treturn err\n\t}",
	"if err != nil || count != 1",
	"return fmt.Errorf(\"retained business-source setup affected %d rows: %v\", count, err)",
	"return nil",
}

func family145PinnedWireFaultProofError(after string) string {
	cursor := -1
	for _, cut := range family145PinnedWireFaultCuts {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return "pinned wire fault lost original scope, payload, context, exact-one or refusal: " + cut
		}
		cursor = at
	}
	if strings.Contains(after, "selected.db") || strings.Contains(after, "ExecContext") || strings.Contains(after, "RowsAffected") {
		return "pinned wire consumer retains its raw writer authority"
	}
	return ""
}

func TestFamily145PinnedWireFaultKeepsExactSourcePayloadRefusalAndNativeOwner(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.File != "internal/runtime/pipeline/a2_map_fan_out_source_corruption_external_test.go" || row.Function != "writeRaw" {
			continue
		}
		matched++
		if problem := family145PinnedWireFaultProofError(row.After); problem != "" {
			t.Fatal(problem)
		}
		for _, cut := range family145PinnedWireFaultCuts {
			if family145PinnedWireFaultProofError(strings.Replace(row.After, cut, "foreignPinnedMutation()", 1)) == "" {
				t.Fatalf("lost pinned fault binding/error/cardinality passed: %s", cut)
			}
		}
		query := "UPDATE entity_mutations SET new_value=$1\n\t\tWHERE mutation_id=$2 AND run_id=$3 AND entity_id=$4 AND domain='authored_field' AND path=$5"
		arguments := "string(raw), source.MutationID, source.RunID, source.EntityID, source.Field)"
		if strings.Count(row.Before, query) != 1 || strings.Count(row.Before, arguments) != 1 {
			t.Fatal("pinned fault source query or arguments differ from the exact historical writer")
		}
		owner := family145ConversationForkOwnerSource(t, "internal/backend/mutationlog/pinned_authored_wire_fault.go", "SetPinnedAuthoredMutationWireForTest")
		for _, binding := range []string{query, arguments, "changed, err := result.RowsAffected()", "if changed != 1", "return changed, nil"} {
			if strings.Count(owner, binding) != 1 {
				t.Fatalf("pinned wire owner lost original UPDATE, scan or exact-one settlement: %s", binding)
			}
		}
		bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_pinned_mutation_observation.go", "SetPinnedAuthoredMutationWireForTest")
		for _, binding := range []string{"validatePinnedAuthoredMutationSourceForTest(source)", "runWorkflowProjectionFault(ctx, selected", "mutationlog.SetPinnedAuthoredMutationWireForTest(ctx, tx, source, raw)", "if err != nil {\n\t\treturn 0, err\n\t}"} {
			if strings.Count(bridge, binding) != 1 {
				t.Fatalf("source fault bypasses shared validation/original coordinator or leaks a receipt: %s", binding)
			}
		}
	}
	if matched != 1 {
		t.Fatalf("shared pinned wire fault writer inventory=%d/1", matched)
	}
}
