package runforkrevision

import (
	"strings"
	"testing"
)

func TestTimerProjectionIncludesExactTypedLineageColumns(t *testing.T) {
	spec, ok := canonicalProjectionSpec(FamilyTimers)
	if !ok {
		t.Fatal("canonical timer projection is unavailable")
	}
	want := map[string]valueKind{
		"forked_from_point_kind": valueRaw, "forked_from_point_revision": valueRaw, "source_armed_at": valueTime,
	}
	for _, column := range spec.columns {
		if kind, ok := want[column.name]; ok {
			if column.kind != kind || !strings.Contains(spec.query, "t."+column.name) {
				t.Fatalf("typed lineage projection column %s: %+v", column.name, column)
			}
			delete(want, column.name)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing typed lineage columns: %v", want)
	}
}
