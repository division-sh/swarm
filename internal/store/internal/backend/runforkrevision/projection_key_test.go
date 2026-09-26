package runforkrevision

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
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
				want, wantErr := legacyFactKey(tc.family, raw)
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
				if tc.family == FamilyFanOutObligations && (field == "origin_kind" || field == "deployment_feed_id") {
					continue
				}
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

type projectionKeyNamedString string
type projectionKeyJSON string

func (v projectionKeyJSON) MarshalJSON() ([]byte, error) { return []byte(v), nil }

func assertProjectionKeyLegacyEquivalent(t testing.TB, family Family, values map[string]any) {
	t.Helper()
	want, wantErr := legacyProjectionFactKey(family, values)
	got, gotErr := projectionFactKey(family, values)
	if got != want || (gotErr == nil) != (wantErr == nil) {
		t.Fatalf("family=%s values=%#v: projection=%q/%v legacy=%q/%v", family, values, got, gotErr, want, wantErr)
	}
}

func TestProjectionFactKeyHostileCarrierLegacyEquivalence(t *testing.T) {
	text := " AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA "
	values := []any{
		nil, "", " ", text, projectionKeyNamedString(text), &text, (*string)(nil),
		"opaque\xff\xfeid", "opaque\xed\xa0\x80id", "line\nzero\x00", "\ufffd", "\u2028",
		true, false, int(0), int8(-1), int32(7), int64(-1), int64(0), int64(math.MaxInt64),
		uint(1), uint64(math.MaxInt64), uint64(math.MaxInt64) + 1, uint64(math.MaxUint64),
		float32(1), float64(1), 1.5, math.Copysign(0, -1), float64(math.MaxInt64),
		math.Nextafter(float64(math.MaxInt64), 0), 1e21, math.Inf(1), math.NaN(),
		json.Number("0"), json.Number("-0"), json.Number("1.0"), json.Number("1e0"),
		json.Number("9007199254740993"), json.Number("9223372036854775807"),
		json.Number("9223372036854775808"), json.Number("-9223372036854775808"),
		json.Number("1e999"), json.Number("01"), json.Number(""),
		json.RawMessage(`null`), json.RawMessage(`1`), json.RawMessage(`1.0`),
		json.RawMessage(`"\ud800"`), json.RawMessage(`{} {}`),
		projectionKeyJSON(`null`), projectionKeyJSON(`"opaque"`), projectionKeyJSON(`-1`),
		projectionKeyJSON(`1e0`), projectionKeyJSON(`{`),
		[]byte("opaque"), []any{0}, map[string]any{"ordinal": 0}, make(chan int),
	}
	for _, tc := range factKeyGoldens {
		t.Run(tc.name, func(t *testing.T) {
			var body map[string]any
			if err := json.Unmarshal([]byte(tc.raw), &body); err != nil {
				t.Fatal(err)
			}
			// Non-key values must not become additional admission obligations.
			body["payload"] = make(chan int)
			fields, err := legacyFactKeyFields(tc.family)
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range fields {
				original, exists := body[field]
				for _, value := range values {
					body[field] = value
					assertProjectionKeyLegacyEquivalent(t, tc.family, body)
				}
				delete(body, field)
				assertProjectionKeyLegacyEquivalent(t, tc.family, body)
				if exists {
					body[field] = original
				}
				for _, alias := range []string{strings.ToUpper(field), strings.ReplaceAll(field, "s", "\u017f"), strings.ReplaceAll(field, "k", "\u212a")} {
					if alias == field {
						continue
					}
					body[alias] = original
					assertProjectionKeyLegacyEquivalent(t, tc.family, body)
					delete(body, alias)
				}
			}
		})
	}
	assertProjectionKeyLegacyEquivalent(t, Family("unknown"), nil)
}

func TestFactKeyWireHostileLegacyEquivalence(t *testing.T) {
	for _, tc := range factKeyGoldens {
		t.Run(tc.name, func(t *testing.T) {
			check := func(raw string) {
				t.Helper()
				want, wantErr := legacyFactKey(tc.family, []byte(raw))
				got, gotErr := FactKey(tc.family, []byte(raw))
				if got != want || (gotErr == nil) != (wantErr == nil) {
					t.Fatalf("raw=%q: key=%q/%v legacy=%q/%v", raw, got, gotErr, want, wantErr)
				}
			}
			for _, raw := range []string{tc.raw, "null", "[]", `"scalar"`, "{", tc.raw + "{}", strings.TrimSuffix(tc.raw, "}") + `,"payload":{"x":1,"x":2}}`} {
				check(raw)
			}
			fields, _ := legacyFactKeyFields(tc.family)
			for _, field := range fields {
				var body map[string]json.RawMessage
				if err := json.Unmarshal([]byte(tc.raw), &body); err != nil {
					t.Fatal(err)
				}
				for _, value := range []string{"null", "true", "[]", "{}", `""`, `"\ud800"`, `"raw` + "\xff\xfe" + `"`, "0", "-0", "-1", "1.0", "1e0", "1e999", "9223372036854775807", "9223372036854775808", "-9223372036854775808", `"0"`} {
					body[field] = json.RawMessage(value)
					raw, err := json.Marshal(body)
					if err != nil {
						t.Fatal(err)
					}
					check(string(raw))
				}
				delete(body, field)
				raw, err := json.Marshal(body)
				if err != nil {
					t.Fatal(err)
				}
				check(string(raw))
				for _, alias := range []string{field, strings.ToUpper(field), fmt.Sprintf(`\u%04x%s`, field[0], field[1:])} {
					check(strings.TrimSuffix(tc.raw, "}") + `,"` + alias + `":null}`)
				}
			}
		})
	}
}

func FuzzFactKeyWireAndProjectionLegacyEquivalence(f *testing.F) {
	for _, tc := range factKeyGoldens {
		f.Add([]byte(tc.raw))
	}
	for _, raw := range []string{`null`, `{}`, `{} {}`, `{"ordinal":1e0}`, `{"ordinal":9223372036854775808}`, `{"reply_context_id":"\ud800"}`, `{"reply_context_id":"a","REPLY_CONTEXT_ID":"b"}`, `{"reply_context_id":"a","reply_context_id":"b"}`, "{\"reply_context_id\":\"\xff\xfe\"}"} {
		f.Add([]byte(raw))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		for _, family := range append(AllFamilies(), Family("unknown")) {
			want, wantErr := legacyFactKey(family, raw)
			got, gotErr := FactKey(family, raw)
			if got != want || (gotErr == nil) != (wantErr == nil) {
				t.Fatalf("family=%s raw=%q: key=%q/%v legacy=%q/%v", family, raw, got, gotErr, want, wantErr)
			}
			for _, useNumber := range []bool{false, true} {
				decoder := json.NewDecoder(bytes.NewReader(raw))
				if useNumber {
					decoder.UseNumber()
				}
				var values map[string]any
				if decoder.Decode(&values) == nil {
					assertProjectionKeyLegacyEquivalent(t, family, values)
				}
			}
		}
	})
}

func BenchmarkProjectionFactKeyTypedCoordinates(b *testing.B) {
	for _, tc := range []factKeyGolden{factKeyGoldens[0], factKeyGoldens[11], factKeyGoldens[12], factKeyGoldens[13], factKeyGoldens[14]} {
		var values map[string]any
		if err := json.Unmarshal([]byte(tc.raw), &values); err != nil {
			b.Fatal(err)
		}
		if tc.family == FamilyFanOutObligations && values["ordinal"] != nil {
			values["ordinal"] = int64(0)
		}
		for i := 0; i < 30; i++ {
			values[strings.Repeat("field", i+1)] = strings.Repeat("frozen-evidence", 32)
		}
		for _, legacy := range []bool{true, false} {
			name := "typed"
			if legacy {
				name = "legacy"
			}
			b.Run(tc.name+"/"+name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var key string
					var err error
					if legacy {
						key, err = legacyProjectionFactKey(tc.family, values)
					} else {
						key, err = projectionFactKey(tc.family, values)
					}
					if err != nil || key != tc.want {
						b.Fatalf("key=%q err=%v", key, err)
					}
				}
			})
		}
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
