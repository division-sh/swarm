package runforkrevision

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func originalJSONEqual(left, right []byte) bool {
	var a, b any
	if json.Unmarshal(left, &a) != nil || json.Unmarshal(right, &b) != nil {
		return false
	}
	x, xe := json.Marshal(a)
	y, ye := json.Marshal(b)
	return xe == nil && ye == nil && bytes.Equal(x, y)
}

func TestRevisionEqualityPreservesExistingInterpretation(t *testing.T) {
	values := []string{`{}`, ` { } `, `null`, `[]`, `[1,2]`, `[2,1]`, `{"n":1}`, `{"n":1.0}`, `{"n":1e0}`, `{"n":1,"m":2}`, `{"m":2,"n":1}`, `{"n":0}`, `{"n":-0}`, `{"n":1e999}`, `{"n":1e-999}`, `{"n":1,"n":2}`, `{"n":2}`, `{"x":"\\u0061"}`, `{"x":"a"}`, `{"payload_base64":"Nw=="}`, `{"payload_base64":"Ny4w"}`, `{"n":9007199254740993}`, `{"n":9007199254740992}`, `{`, `{} {}`, `NaN`, "\xff"}
	for _, a := range values {
		for _, b := range values {
			if got, want := canonicalJSONEqual([]byte(a), []byte(b)), originalJSONEqual([]byte(a), []byte(b)); got != want {
				t.Fatalf("equality %q/%q=%v want %v", a, b, got, want)
			}
		}
	}
}

func FuzzRevisionEqualityPreservesExistingInterpretation(f *testing.F) {
	f.Add([]byte(`{"a":1}`), []byte(`{"a":1.0}`))
	f.Add([]byte(`{"n":1e999}`), []byte(`{"n":1e999}`))
	f.Add([]byte(`{"a":[null,true,{"x":-0}]}`), []byte(`{"a":[null,true,{"x":0}]}`))
	f.Add([]byte(`{"a":[null,false,{"x":"\u0061"}]}`), []byte(`{ "a": [null,false,{"x":"a"}] }`))
	f.Fuzz(func(t *testing.T, a, b []byte) {
		if canonicalJSONEqual(a, b) != originalJSONEqual(a, b) {
			t.Fatalf("changed equality %q/%q", a, b)
		}
		if canonicalJSONEqual(a, a) != originalJSONEqual(a, a) {
			t.Fatalf("changed reflexive admission %q", a)
		}
	})
}

func BenchmarkRevisionEqualityReordered(b *testing.B) {
	a := []byte(`{"revision":8,"source_evidence":"` + strings.Repeat("frozen-evidence", 32) + `","value":7.5}`)
	c := []byte(`{"value":7.5,"source_evidence":"` + strings.Repeat("frozen-evidence", 32) + `","revision":8}`)
	for _, method := range []struct {
		name  string
		equal func([]byte, []byte) bool
	}{{"former", originalJSONEqual}, {"decoded", canonicalJSONEqual}} {
		b.Run(method.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if !method.equal(a, c) {
					b.Fatal("not equal")
				}
			}
		})
	}
}

func BenchmarkRevisionEqualityIdentical(b *testing.B) {
	raw := []byte(`{"revision":8,"source_evidence":"` + strings.Repeat("frozen-evidence", 32) + `","value":7.5}`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !canonicalJSONEqual(raw, raw) {
			b.Fatal("not equal")
		}
	}
}

func TestComparedProjectionPrefixPreservesWriteDecisions(t *testing.T) {
	current := []canonicalFact{{key: "a", fact: []byte(`{"v":1}`)}, {key: "b", fact: []byte(`{"v":2}`)}, {key: "c", fact: []byte(`{"v":3}`)}}
	for _, mutation := range []string{"same", "first", "middle", "last", "missing", "tombstone", "extra", "invalid"} {
		t.Run(mutation, func(t *testing.T) {
			latest := map[string]ledgerFact{}
			for _, fact := range current {
				latest[fact.key] = ledgerFact{fact: fact.fact, present: true}
			}
			switch mutation {
			case "first":
				latest["a"] = ledgerFact{fact: []byte(`{"v":9}`), present: true}
			case "middle":
				latest["b"] = ledgerFact{fact: []byte(`{"v":9}`), present: true}
			case "last":
				latest["c"] = ledgerFact{fact: []byte(`{"v":9}`), present: true}
			case "missing":
				delete(latest, "b")
			case "tombstone":
				latest["b"] = ledgerFact{fact: []byte(`{}`), present: false}
			case "extra":
				latest["d"] = ledgerFact{fact: []byte(`{}`), present: true}
			case "invalid":
				latest["c"] = ledgerFact{fact: []byte(`{`), present: true}
			}
			equal, prefix := compareCanonicalProjection(current, latest)
			wantEqual := len(current) == countPresent(latest)
			for i, fact := range current {
				stored, exists := latest[fact.key]
				unchanged := exists && stored.present && originalJSONEqual(fact.fact, stored.fact)
				wantEqual = wantEqual && unchanged
				if i < prefix && !unchanged {
					t.Fatalf("prefix hides changed fact %q", fact.key)
				}
				newWrite := i >= prefix && (!exists || !stored.present || !canonicalJSONEqual(fact.fact, stored.fact))
				if newWrite != !unchanged {
					t.Fatalf("write decision changed for %q", fact.key)
				}
			}
			if equal != wantEqual {
				t.Fatalf("equal=%v want %v", equal, wantEqual)
			}
		})
	}
}
