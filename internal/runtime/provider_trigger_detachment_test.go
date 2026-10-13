package runtime

import (
	"encoding/json"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestProviderTriggerCatalogRefinementCopyDetached(t *testing.T) {
	for _, bound := range []string{"length_min", "length_max", "range_min", "range_max"} {
		t.Run(bound, func(t *testing.T) {
			low, high := 1, 10
			start, end := 1.5, 9.5
			entry := c.EventCatalogEntry{Payload: c.EventPayloadSpec{Type: "object", Required: []string{"value"}, Properties: map[string]c.EventFieldSpec{
				"value": {Type: "text", IsOptional: true, Description: "provider copy", Refinements: c.SchemaRefinements{Pattern: "pattern", EqualTo: "peer", Length: c.SchemaLengthRefinement{Min: &low, Max: &high}, Range: c.SchemaRangeRefinement{Min: &start, Max: &end}}},
			}}}
			before, err := json.Marshal(entry)
			if err != nil {
				t.Fatal(err)
			}
			copied := cloneProviderTriggerEventCatalogEntry(entry)
			r := copied.Payload.Properties["value"].Refinements
			switch bound {
			case "length_min":
				*r.Length.Min = 2
			case "length_max":
				*r.Length.Max = 9
			case "range_min":
				*r.Range.Min = 2.5
			case "range_max":
				*r.Range.Max = 8.5
			}
			after, err := json.Marshal(entry)
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) || r.Pattern != "pattern" || r.EqualTo != "peer" {
				t.Fatalf("provider local copy changed retained bounds/metadata: before=%s after=%s", before, after)
			}
		})
	}
}
