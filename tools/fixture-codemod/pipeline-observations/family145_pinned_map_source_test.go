package main

import (
	"fmt"
	"reflect"
	"strings"
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
