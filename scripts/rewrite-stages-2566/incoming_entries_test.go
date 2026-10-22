package main

import (
	"reflect"
	"testing"
)

func TestRewrite2566IncomingOptInSourcesHaveIndependentEntryGoldens(t *testing.T) {
	entries, err := readCurrentEntryGoldens("../..")
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]entryGolden{
		"internal/releasee2e/testdata/workspace_mcp/schema.yaml":                                      {Entry: "pending", Order: []string{"pending", "working", "complete"}, Finals: []string{"complete"}},
		"internal/runtime/testfixtures/canonicalrouting/testdata/clock-deployment/finite/schema.yaml": {Entry: "pending", Order: []string{"pending", "done"}, Finals: []string{"done"}},
		"internal/serveapp/mock_docker_emission_test.go":                                              {Entry: "pending", Order: []string{"pending", "done"}, Finals: []string{"done"}, Function: "TestMockNormalRealDockerEmissionBothStores", Literal: 5},
	}
	for _, entry := range entries {
		want, found := expected[entry.File]
		if !found {
			continue
		}
		if entry.Entry != want.Entry || !reflect.DeepEqual(entry.Order, want.Order) || !reflect.DeepEqual(entry.Finals, want.Finals) || entry.Function != want.Function || entry.Literal != want.Literal {
			t.Fatalf("incoming source lost its independent entry/final decision: got=%+v want=%+v", entry, want)
		}
		if entry.Function != "" {
			// This reads the actual opt-in source even when Docker is unavailable.
			assertEmbeddedEntryGolden(t, entry)
		}
		delete(expected, entry.File)
	}
	if len(expected) != 0 {
		t.Fatalf("incoming/opt-in sources missing from permanent goldens: %v", expected)
	}
}
