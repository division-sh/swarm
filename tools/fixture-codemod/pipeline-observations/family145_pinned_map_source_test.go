package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func family145PinnedMapSourceRow(file, function string) bool {
	return file == "internal/runtime/pipeline/a2_map_fan_out_source_corruption_external_test.go" && function == "readRaw" ||
		file == "internal/runtime/pipeline/a2_map_fan_out_execution_external_test.go" && function == "runA2MapFanOutCompiledImmutableSource"
}

var family145PinnedMapSourceCuts = map[string][]string{
	"readRaw": {"source := f.intent.Source", "raw, err := storetest.ReadPinnedAuthoredMutationStorage(f.p.ctx, f.p.selected.events, source)", "return raw"},
	"runA2MapFanOutCompiledImmutableSource": {
		"frozen, err := storetest.ReadPinnedAuthoredMutationStorage(p.ctx, p.selected.events, intent.Source)",
		"wantFrozen, _ := json.Marshal(original)",
		"if a2AccumulatorJSONHash(t, frozen) != a2AccumulatorJSONHash(t, wantFrozen)",
	},
}

func family145PinnedMapSourceProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("pinned source reader lost original captured keys/values/claim, order, progress or completion assertions: %s: %v", function, err)
	}
	cursor := -1
	for _, cut := range family145PinnedMapSourceCuts[function] {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("pinned source read lost exact source/owner/context or physical-byte destination: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "SELECT new_value FROM entity_mutations") {
		return fmt.Errorf("pinned source consumer retained a second physical interpreter")
	}
	return nil
}

func TestFamily145PinnedMapSourceReadersReuseExactOwnerAndRetainEveryAssertion(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, row := range rows {
		if !family145PinnedMapSourceRow(row.File, row.Function) {
			continue
		}
		if found[row.Function] {
			t.Fatalf("ambiguous pinned map reader: %s", row.Function)
		}
		found[row.Function] = true
		if err := family145PinnedMapSourceProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range family145PinnedMapSourceCuts[row.Function] {
			if family145PinnedMapSourceProofError(row.Function, row.Before, strings.Replace(row.After, cut, "foreignPinnedSourceEvidence()", 1)) == nil {
				t.Fatalf("lost original captured source/bytes/ordering passed: %s", cut)
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.After)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if family145PinnedMapSourceProofError(row.Function, row.Before, strings.Replace(row.After, condition, "false", 1)) == nil {
				t.Fatalf("lost immutable-source execution assertion passed: %s", condition)
			}
		}
	}
	if len(found) != 2 {
		t.Fatalf("pinned authored-map reader inventory=%d/2", len(found))
	}
	owner := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_pinned_mutation_observation.go", "ReadPinnedAuthoredMutationStorageForTest")
	if queries := family145A2ClaimLiteralQueries(t, owner); len(queries) != 1 || queries[0] != "SELECT CAST(new_value AS TEXT) FROM entity_mutations WHERE mutation_id=$1 AND run_id=$2 AND entity_id=$3 AND domain='authored_field' AND path=$4" {
		t.Fatalf("existing source owner changed exact original physical row membership: %v", queries)
	}
	for _, cut := range []string{"validatePinnedAuthoredMutationSourceForTest(source)", "source.MutationID, source.RunID, source.EntityID, source.Field).Scan(&value)", "owner.backend.RunReadTransaction(ctx, read)"} {
		want := 1
		if cut == "owner.backend.RunReadTransaction(ctx, read)" {
			want = 2
		}
		if strings.Count(owner, cut) != want {
			t.Fatalf("existing pinned-source owner lost validation, native byte destination or original coordinator: %s", cut)
		}
	}
	validation := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_pinned_mutation_observation.go", "validatePinnedAuthoredMutationSourceForTest")
	for _, cut := range []string{"source.Kind != fanoutobligation.SourceEntityField", "source.Validate(true)", "source.RunID, source.EntityID, source.MutationID", "uuid.Parse(raw)", "id == uuid.Nil || id.String() != raw"} {
		if strings.Count(validation, cut) != 1 {
			t.Fatalf("shared pinned-source validation lost original source/canonical identity rule: %s", cut)
		}
	}
}
