package tools

import (
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"testing"
)

func TestLeadReview6EntityToolNestedPresence(t *testing.T) {
	schema := testEntityFilterSchema()
	decl := schema.Contract.Types.Types["Metadata"]
	field := decl.Fields["region"]
	field.IsOptional = true
	decl.Fields["region"] = field
	schema.Contract.Types.Types["Metadata"] = decl
	materialized, err := entityruntime.Materialize(schema.Contract, map[string]any{"metadata": map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("materialized=%v", materialized)
	rows := []map[string]any{materialized}
	for _, expr := range []string{`metadata.region != ""`, `has(metadata.region) && metadata.region != ""`, `metadata.?region.orValue("") != ""`} {
		t.Run(expr, func(t *testing.T) {
			_, emptyErr := filterEntityStateRowsCEL(expr, nil, schema)
			got, runErr := filterEntityStateRowsCEL(expr, rows, schema)
			t.Logf("empty input=%v rows=%v error=%v", emptyErr, got, runErr)
			if expr == `metadata.region != ""` {
				if emptyErr == nil {
					t.Error("unsafe nested optional reader admitted before row execution")
				}
			} else if emptyErr != nil || runErr != nil {
				t.Fatal("supported presence decision rejected")
			}
		})
	}
}
