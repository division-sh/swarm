package runforkrevision

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestProjectionFactKeyPreservesRawOwnerAdmission(t *testing.T) {
	for _, tc := range factKeyGoldens {
		t.Run(tc.name, func(t *testing.T) {
			var body map[string]any
			if err := json.Unmarshal([]byte(tc.raw), &body); err != nil {
				t.Fatal(err)
			}
			check := func(values map[string]any) {
				t.Helper()
				raw, err := json.Marshal(values)
				if err != nil {
					t.Fatal(err)
				}
				want, wantErr := FactKey(tc.family, raw)
				got, gotErr := projectionFactKey(tc.family, values)
				if got != want || (gotErr == nil) != (wantErr == nil) {
					t.Fatalf("projection key=%q/%v raw key=%q/%v: %s", got, gotErr, want, wantErr, raw)
				}
			}
			body["payload"] = map[string]any{"event_id": "not-authority", "deep": []any{7.5, "unchanged"}}
			check(body)
			fields, err := factKeyFields(tc.family)
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range fields {
				original, present := body[field]
				for _, value := range []any{nil, true, 7, "", " ", "not-an-identity", -1, 1.5} {
					body[field] = value
					check(body)
				}
				delete(body, field)
				check(body)
				if present {
					body[field] = original
				}
				alias := strings.ToUpper(field)
				body[alias] = original
				check(body)
				if _, err := projectionFactKey(tc.family, body); err == nil {
					t.Fatalf("case alias %s was omitted rather than rejected", alias)
				}
				delete(body, alias)
			}
		})
	}
	if _, err := projectionFactKey(Family("unknown"), map[string]any{}); err == nil {
		t.Fatal("unknown family accepted")
	}
}

func BenchmarkProjectionFactKey(b *testing.B) {
	values := map[string]any{"event_id": "11111111-1111-4111-8111-111111111111"}
	for i := 0; i < 30; i++ {
		values[strings.Repeat("field", i+1)] = strings.Repeat("frozen-evidence", 32)
	}
	raw, err := json.Marshal(values)
	if err != nil {
		b.Fatal(err)
	}
	for _, variant := range []string{"whole_body", "coordinates"} {
		b.Run(variant, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var err error
				if variant == "whole_body" {
					_, err = FactKey(FamilyEvents, raw)
				} else {
					_, err = projectionFactKey(FamilyEvents, values)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
