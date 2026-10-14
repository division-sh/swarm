package durabledata

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

type codecTestEntrance func(DeclarationRef, map[string]any, string, []byte) (CompiledVersion, []ValidationDefect)

func forEachCodecEntrance(t *testing.T, run func(*testing.T, codecTestEntrance)) {
	t.Helper()
	for _, entrance := range []struct {
		name    string
		compile codecTestEntrance
	}{{"import", CompileJSONL}, {"stored", CompileStoredJSONL}} {
		t.Run(entrance.name, func(t *testing.T) { run(t, entrance.compile) })
	}
}

func codecTestRef(t *testing.T) DeclarationRef {
	t.Helper()
	ref, err := ParseDeclarationRef(".", "company.lead")
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func requireCodecDefects(t *testing.T, compiled CompiledVersion, defects []ValidationDefect, codes ...string) {
	t.Helper()
	if !reflect.DeepEqual(compiled, CompiledVersion{}) || len(defects) != len(codes) {
		t.Fatalf("whole candidate=%+v defects=%+v, want %v", compiled, defects, codes)
	}
	for index, code := range codes {
		if defects[index].Code != code {
			t.Fatalf("defect %d=%+v, want %s", index, defects[index], code)
		}
	}
}

func TestResourceCodecC01EnvelopePrecedenceAndWireBoundary(t *testing.T) {
	ref := codecTestRef(t)
	schema := map[string]any{"type": "object"}
	forEachCodecEntrance(t, func(t *testing.T, compile codecTestEntrance) {
		for _, cell := range []struct {
			name  string
			ref   DeclarationRef
			input []byte
			code  string
		}{
			{"declaration_first", DeclarationRef{}, []byte{0xff}, "invalid_declaration"},
			{"utf8_before_bom", ref, []byte{0xef, 0xbb, 0xbf, 0xff}, "invalid_utf8"},
			{"bom_before_json", ref, []byte{0xef, 0xbb, 0xbf, '{', '}'}, "bom_forbidden"},
		} {
			t.Run(cell.name, func(t *testing.T) {
				compiled, defects := compile(cell.ref, schema, "", cell.input)
				requireCodecDefects(t, compiled, defects, cell.code)
				if defects[0].Row != 0 || defects[0].Path != "" {
					t.Fatalf("envelope defect gained a row=%+v", defects)
				}
			})
		}
		input := append([]byte("{}"), bytes.Repeat([]byte{' '}, MaxDecodedImportBytes-2)...)
		compiled, defects := compile(ref, schema, "", input)
		if len(defects) != 0 || string(compiled.CanonicalJSONL) != "{}\n" {
			t.Fatalf("exact wire limit=%+v defects=%+v", compiled, defects)
		}
		compiled, defects = compile(ref, schema, "", append(input, ' '))
		if t.Name() == "TestResourceCodecC01EnvelopePrecedenceAndWireBoundary/import" {
			requireCodecDefects(t, compiled, defects, "decoded_import_too_large")
		} else if len(defects) != 0 || string(compiled.CanonicalJSONL) != "{}\n" {
			t.Fatalf("stored wire-only bypass=%+v defects=%+v", compiled, defects)
		}
	})
}

func TestResourceCodecC02SchemaAndPhysicalLinePhase(t *testing.T) {
	ref := codecTestRef(t)
	forEachCodecEntrance(t, func(t *testing.T, compile codecTestEntrance) {
		schema := map[string]any{"type": "object", "description": "not identity", "additionalProperties": false,
			"properties": map[string]any{"id": map[string]any{"type": "string", "description": "not identity"}}, "required": []string{"id"}}
		compiled, defects := compile(ref, schema, " id ", []byte(`{"id":"a"}`))
		const want = `{"additionalProperties":false,"properties":{"id":{"type":"string"}},"required":["id"],"type":"object","x-swarm-dataset-key":"id"}`
		if len(defects) != 0 || string(compiled.CanonicalSchema) != want || string(compiled.Rows[0].BusinessKey) != `"a"` {
			t.Fatalf("schema/key projection=%+v defects=%+v", compiled, defects)
		}
		compiled, defects = compile(ref, map[string]any{"minLength": make(chan int)}, "", []byte("\n"))
		requireCodecDefects(t, compiled, defects, "invalid_schema")
		compiled, defects = compile(ref, map[string]any{"type": "object"}, "", []byte("{}\n\n"))
		requireCodecDefects(t, compiled, defects, "blank_line")
		if defects[0] != (ValidationDefect{Row: 2, Path: "$", Code: "blank_line", Message: "blank JSONL rows are forbidden"}) {
			t.Fatalf("only one final split segment may disappear=%+v", defects)
		}
	})
}

func TestResourceCodecC03AllRowDefectsRejectWholeCandidate(t *testing.T) {
	forEachCodecEntrance(t, func(t *testing.T, compile codecTestEntrance) {
		compiled, defects := compile(codecTestRef(t), map[string]any{"type": "object"}, "", []byte("{}\n\n{\"id\":1,\"id\":2}\nnull\n[]\n42\n}\n"))
		requireCodecDefects(t, compiled, defects, "blank_line", "invalid_json", "row_not_object", "row_not_object", "row_not_object", "invalid_json")
		for index, defect := range defects {
			if defect.Row != uint64(index+2) || defect.Path != "$" || defect.Message == "" {
				t.Fatalf("physical defect order=%+v", defects)
			}
			if defect.Code == "row_not_object" && defect.Message != "each JSONL row must be one object" {
				t.Fatalf("object refusal changed=%+v", defect)
			}
		}
	})
}

func TestResourceCodecC04PresenceAndRequiredCarriers(t *testing.T) {
	profile := map[string]any{"type": "object", "properties": map[string]any{"note": map[string]any{"type": "integer"}}, "additionalProperties": false}
	for _, required := range []any{[]string{"active", "zero", "empty", "profile", "matrix"}, []any{"active", "zero", "empty", "profile", "matrix"}} {
		schema := map[string]any{"type": "object", "additionalProperties": false, "required": required,
			"properties": map[string]any{"active": map[string]any{"type": "boolean"}, "zero": map[string]any{"type": "integer"}, "empty": map[string]any{"type": "string"},
				"profile": profile, "matrix": map[string]any{"type": "array", "items": map[string]any{"type": "array", "items": profile}}}}
		forEachCodecEntrance(t, func(t *testing.T, compile codecTestEntrance) {
			compiled, defects := compile(codecTestRef(t), schema, "", []byte(`{"active":false,"zero":0,"empty":"","profile":{"note":null},"matrix":[[{"note":null}]]}`))
			const want = "{\"active\":false,\"empty\":\"\",\"matrix\":[[{}]],\"profile\":{},\"zero\":0}\n"
			if len(defects) != 0 || string(compiled.CanonicalJSONL) != want {
				t.Fatalf("presence=%s defects=%+v", compiled.CanonicalJSONL, defects)
			}
			compiled, defects = compile(codecTestRef(t), schema, "", []byte(`{"active":false,"zero":null,"empty":"","profile":{},"matrix":[]}`))
			requireCodecDefects(t, compiled, defects, "schema_rejected")
		})
	}
	if got := schemaRequiredSet(map[string]any{"required": []any{"a", 7, "a"}}); !reflect.DeepEqual(got, map[string]struct{}{"a": {}}) {
		t.Fatalf("defensive required-array projection=%+v", got)
	}
}

func TestResourceCodecC05SchemaAcceptanceAndDefectOrder(t *testing.T) {
	schema := map[string]any{"type": "object", "additionalProperties": false, "required": []string{"value"},
		"properties": map[string]any{"value": map[string]any{"type": "string", "enum": []any{"green"}}}}
	forEachCodecEntrance(t, func(t *testing.T, compile codecTestEntrance) {
		good, defects := compile(codecTestRef(t), schema, "", []byte(`{"value":"green"}`))
		if len(defects) != 0 || string(good.CanonicalJSONL) != "{\"value\":\"green\"}\n" {
			t.Fatalf("valid enum=%+v defects=%+v", good, defects)
		}
		compiled, defects := compile(codecTestRef(t), schema, "", []byte("{\"value\":1}\n{\"value\":\"red\"}\n{\"value\":\"green\",\"other\":true}\n{}\n"))
		requireCodecDefects(t, compiled, defects, "schema_rejected", "schema_rejected", "schema_rejected", "schema_rejected")
		for index, defect := range defects {
			if defect.Row != uint64(index+1) || defect.Path != "$" || defect.Message == "" {
				t.Fatalf("schema defect order=%+v", defects)
			}
		}
	})
}

func TestResourceCodecC06ScalarKeysAndExactKeyByteLimit(t *testing.T) {
	for _, raw := range []string{`true`, `false`, `0`, `1.5`, `""`, `"a"`} {
		value, err := canonicaljson.Decode([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		key, err := canonicalBusinessKey(value)
		if err != nil || string(key) != raw {
			t.Fatalf("scalar %s key=%s error=%v", raw, key, err)
		}
	}
	for _, raw := range []string{`null`, `{}`, `[]`} {
		value, err := canonicaljson.Decode([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := canonicalBusinessKey(value); err == nil || err.Error() != "business key must be a non-null scalar" {
			t.Fatalf("nonscalar %s error=%v", raw, err)
		}
	}
	schema := map[string]any{"type": "object", "properties": map[string]any{"id": map[string]any{}}, "additionalProperties": false}
	forEachCodecEntrance(t, func(t *testing.T, compile codecTestEntrance) {
		compiled, defects := compile(codecTestRef(t), schema, "id", []byte(`{}`))
		requireCodecDefects(t, compiled, defects, "business_key_missing")
		if defects[0].Path != "$.id" || defects[0].Message != "business key field is missing" {
			t.Fatalf("missing key=%+v", defects)
		}
		for _, raw := range []string{`{"id":{}}`, `{"id":[]}`} {
			compiled, defects := compile(codecTestRef(t), schema, "id", []byte(raw))
			requireCodecDefects(t, compiled, defects, "business_key_invalid")
		}
		key := strings.Repeat("a", MaxBusinessKeyBytes-2)
		compiled, defects = compile(codecTestRef(t), schema, "id", []byte(`{"id":"`+key+`"}`))
		if len(defects) != 0 || len(compiled.Rows[0].BusinessKey) != MaxBusinessKeyBytes {
			t.Fatalf("exact key-byte limit=%+v defects=%+v", compiled, defects)
		}
		compiled, defects = compile(codecTestRef(t), schema, "id", []byte(`{"id":"`+key+`a"}`))
		requireCodecDefects(t, compiled, defects, "business_key_invalid")
		if defects[0].Message != fmt.Sprintf("business key exceeds %d bytes", MaxBusinessKeyBytes) {
			t.Fatalf("key limit diagnostic=%+v", defects)
		}
	})
}

func TestResourceCodecC07CanonicalDuplicatesKeepPhysicalPredecessor(t *testing.T) {
	schema := map[string]any{"type": "object", "properties": map[string]any{"id": map[string]any{"type": "number"}}, "required": []string{"id"}, "additionalProperties": false}
	forEachCodecEntrance(t, func(t *testing.T, compile codecTestEntrance) {
		compiled, defects := compile(codecTestRef(t), schema, "id", []byte("{\"id\":1}\n\n{\"id\":1.0}\n"))
		requireCodecDefects(t, compiled, defects, "blank_line", "duplicate_business_key")
		if defects[1] != (ValidationDefect{Row: 3, Path: "$.id", Code: "duplicate_business_key", Message: "business key 1 duplicates physical row 1"}) {
			t.Fatalf("canonical duplicate predecessor=%+v", defects)
		}
		sizedSchema := map[string]any{"type": "object", "properties": map[string]any{"id": map[string]any{"type": "number"}, "value": map[string]any{"type": "string"}}, "required": []string{"id"}, "additionalProperties": false}
		t.Run("key_reserved_before_row_size", func(t *testing.T) {
			input := "{\"id\":1,\"value\":\"" + strings.Repeat("a", MaxCanonicalRowBytes) + "\"}\n{\"id\":1}\n"
			compiled, defects := compile(codecTestRef(t), sizedSchema, "id", []byte(input))
			requireCodecDefects(t, compiled, defects, "row_too_large", "duplicate_business_key")
			if defects[1].Row != 2 || defects[1].Message != "business key 1 duplicates physical row 1" {
				t.Fatalf("late row refusal changed duplicate predecessor=%+v", defects)
			}
		})
		t.Run("schema_refusal_never_reserves_key", func(t *testing.T) {
			compiled, defects := compile(codecTestRef(t), sizedSchema, "id", []byte("{\"id\":1,\"value\":42}\n{\"id\":1}\n"))
			requireCodecDefects(t, compiled, defects, "schema_rejected")
			if defects[0].Row != 1 {
				t.Fatalf("early schema refusal changed physical row=%+v", defects)
			}
		})
	})
}

func TestResourceCodecC08ByteOrderAndPositionalMultiplicity(t *testing.T) {
	schema := map[string]any{"type": "object", "properties": map[string]any{"id": map[string]any{"type": "number"}}, "required": []string{"id"}, "additionalProperties": false}
	forEachCodecEntrance(t, func(t *testing.T, compile codecTestEntrance) {
		compiled, defects := compile(codecTestRef(t), schema, "id", []byte("{\"id\":2}\n{\"id\":10}\n{\"id\":-1}\n"))
		if len(defects) != 0 || string(compiled.CanonicalJSONL) != "{\"id\":-1}\n{\"id\":10}\n{\"id\":2}\n" {
			t.Fatalf("canonical numeric-byte sort=%+v defects=%+v", compiled, defects)
		}
		for index, key := range []BusinessKey{"-1", "10", "2"} {
			if compiled.Rows[index].Ordinal != uint64(index+1) || compiled.Rows[index].BusinessKey != key {
				t.Fatalf("keyed identity=%+v", compiled.Rows)
			}
		}
		const repeated = "{\"id\":2}\n{\"id\":10}\n{\"id\":2}\n"
		compiled, defects = compile(codecTestRef(t), schema, "", []byte(repeated))
		if len(defects) != 0 || string(compiled.CanonicalJSONL) != repeated || len(compiled.Rows) != 3 {
			t.Fatalf("positional multiplicity=%+v defects=%+v", compiled, defects)
		}
		for index, row := range compiled.Rows {
			if row.Ordinal != uint64(index+1) || row.BusinessKey != "" {
				t.Fatalf("positional identity=%+v", compiled.Rows)
			}
		}
	})
}

func TestResourceCodecC09LiteralDigestAndFramingVectors(t *testing.T) {
	forEachCodecEntrance(t, func(t *testing.T, compile codecTestEntrance) {
		keyed, defects := compile(codecTestRef(t), leadSchema(true), "slug", []byte("{\"slug\":\"zeta\",\"funding_round\":1}\n{\"funding_round\":2,\"slug\":\"alpha\"}\n"))
		if len(defects) != 0 || keyed.Manifest.ContentDigest != "resource-content-v1:sha256:13042dd832ba8f99c4f9f51f1857e119f984687474e81f6e14e22aea4191e989" ||
			keyed.VersionID != "resource-version-v1:sha256:c40744e4752a4b5e536091934696618dd36a741ef035771b7cf19b3e9bc6e839" {
			t.Fatalf("keyed literal vector=%+v defects=%+v", keyed.Manifest, defects)
		}
		positional, defects := compile(codecTestRef(t), leadSchema(false), "", []byte("{\"slug\":\"zeta\",\"funding_round\":1}\n{\"funding_round\":2,\"slug\":\"alpha\"}\n{\"slug\":\"zeta\",\"funding_round\":1}\n"))
		if len(defects) != 0 || positional.Manifest.ContentDigest != "resource-content-v1:sha256:2c83dcb78ea8e0ce6790f74a2ea861a870012e54a14194493acc574f1759b5d8" ||
			positional.VersionID != "resource-version-v1:sha256:01842395120adf1f7f5b4fa55e2ff91b93a76b3cb23c35d28284e851a12a596f" {
			t.Fatalf("positional literal vector=%+v defects=%+v", positional.Manifest, defects)
		}
	})
	var framed bytes.Buffer
	writeDigestField(&framed, []byte("x"))
	writeDigestUint64(&framed, 1)
	if want := []byte{0, 0, 0, 1, 'x', 0, 0, 0, 0, 0, 0, 0, 1}; !bytes.Equal(framed.Bytes(), want) {
		t.Fatalf("literal length/ordinal framing=%v, want=%v", framed.Bytes(), want)
	}
}

func TestResourceCodecC10RowBoundsEmptyAndIsolation(t *testing.T) {
	forEachCodecEntrance(t, func(t *testing.T, compile codecTestEntrance) {
		ref := codecTestRef(t)
		schema := map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}, "additionalProperties": false}
		empty, defects := compile(ref, schema, "", nil)
		if defects != nil || empty.Rows == nil || empty.CanonicalJSONL == nil || len(empty.Rows) != 0 || len(empty.CanonicalJSONL) != 0 || empty.Manifest.RowCount != 0 || empty.VersionID == "" {
			t.Fatalf("canonical empty=%+v defects=%+v", empty, defects)
		}
		const prefix, suffix = `{"value":"`, `"}`
		input := []byte(prefix + strings.Repeat("a", MaxCanonicalRowBytes-len(prefix)-len(suffix)) + suffix)
		compiled, defects := compile(ref, schema, "", input)
		if len(defects) != 0 || len(compiled.Rows[0].Canonical) != MaxCanonicalRowBytes {
			t.Fatalf("exact canonical-row boundary=%+v", defects)
		}
		oversize := []byte(prefix + strings.Repeat("a", MaxCanonicalRowBytes-len(prefix)-len(suffix)+1) + suffix)
		refused, defects := compile(ref, schema, "", oversize)
		requireCodecDefects(t, refused, defects, "row_too_large")
		if defects[0].Message != fmt.Sprintf("canonical row has %d bytes; limit is %d bytes", MaxCanonicalRowBytes+1, MaxCanonicalRowBytes) {
			t.Fatalf("canonical row-limit diagnostic=%+v", defects)
		}
		beforeSchema := append([]byte(nil), compiled.CanonicalSchema...)
		beforeJSONL := append([]byte(nil), compiled.CanonicalJSONL...)
		input[0] = '['
		schema["type"] = "array"
		compiled.Rows[0].Canonical[0] = '['
		if !bytes.Equal(compiled.CanonicalSchema, beforeSchema) || !bytes.Equal(compiled.CanonicalJSONL, beforeJSONL) {
			t.Fatal("input/schema/row mutation changed independently owned canonical bytes")
		}
	})
	compiled, defects := CompileJSONL(codecTestRef(t), map[string]any{"type": "object"}, "", []byte(strings.Repeat("{}\n", MaxResourceRows)))
	if len(defects) != 0 || len(compiled.Rows) != MaxResourceRows || compiled.Rows[MaxResourceRows-1].Ordinal != MaxResourceRows {
		t.Fatalf("exact row-count boundary rows=%d defects=%+v", len(compiled.Rows), defects)
	}
	compiled, defects = CompileStoredJSONL(codecTestRef(t), map[string]any{"type": "object"}, "", []byte(strings.Repeat("{}\n", MaxResourceRows+1)))
	requireCodecDefects(t, compiled, defects, "row_limit_exceeded")
}

func TestResourceCodecC11PositionalDeltaCountsMultiplicity(t *testing.T) {
	base := []Row{{Ordinal: 1, Canonical: []byte(`{"v":1}`)}, {Ordinal: 2, Canonical: []byte(`{"v":1}`)}, {Ordinal: 3, Canonical: []byte(`{"v":2}`)}}
	candidate := []Row{{Ordinal: 1, Canonical: []byte(`{"v":2}`)}, {Ordinal: 2, Canonical: []byte(`{"v":3}`)}, {Ordinal: 3, Canonical: []byte(`{"v":3}`)}}
	got, added, removed, changed := Delta(base, candidate, false)
	if got != (DeltaSummary{Added: 2, Removed: 2}) || added != nil || removed != nil || changed != nil {
		t.Fatalf("positional multiset delta=%+v keys=%v/%v/%v", got, added, removed, changed)
	}
	if counts := canonicalMultiplicity(base); !reflect.DeepEqual(counts, map[string]uint64{`{"v":1}`: 2, `{"v":2}`: 1}) {
		t.Fatalf("literal multiset=%+v", counts)
	}
}

func TestResourceCodecC12OrderChangedOnlyForEqualMultisets(t *testing.T) {
	base := []Row{{Ordinal: 1, Canonical: []byte(`{"v":1}`)}, {Ordinal: 2, Canonical: []byte(`{"v":2}`)}}
	reversed := []Row{{Ordinal: 1, Canonical: []byte(`{"v":2}`)}, {Ordinal: 2, Canonical: []byte(`{"v":1}`)}}
	got, _, _, _ := Delta(base, reversed, false)
	if got != (DeltaSummary{OrderChanged: true}) || sameCanonicalOrder(base, reversed) {
		t.Fatalf("equal multiset order=%+v", got)
	}
	got, _, _, _ = Delta(base, reversed[:1], false)
	if got != (DeltaSummary{Removed: 1}) || sameCanonicalOrder(base, reversed[:1]) {
		t.Fatalf("different multiset/cardinality order=%+v", got)
	}
	got, _, _, _ = Delta(nil, []Row{}, false)
	if got != (DeltaSummary{}) || !sameCanonicalOrder(nil, []Row{}) {
		t.Fatalf("empty sequence equivalence=%+v", got)
	}
}

func TestResourceCodecC13KeyedDeltaEvidenceIsSortedAndOrderFree(t *testing.T) {
	base := []Row{{BusinessKey: `"a"`, Canonical: []byte(`{"id":"a","v":1}`)}, {BusinessKey: `"m"`, Canonical: []byte(`{"id":"m","v":2}`)}, {BusinessKey: `"z"`, Canonical: []byte(`{"id":"z","v":3}`)}}
	candidate := []Row{{BusinessKey: `"o"`, Canonical: []byte(`{"id":"o","v":1}`)}, {BusinessKey: `"n"`, Canonical: []byte(`{"id":"n","v":1}`)}, {BusinessKey: `"m"`, Canonical: []byte(`{"id":"m","v":4}`)}}
	for iteration := 0; iteration < 2; iteration++ {
		got, added, removed, changed := Delta(base, candidate, true)
		if got != (DeltaSummary{Added: 2, Removed: 2, Changed: 1}) || !reflect.DeepEqual(added, []BusinessKey{`"n"`, `"o"`}) ||
			!reflect.DeepEqual(removed, []BusinessKey{`"a"`, `"z"`}) || !reflect.DeepEqual(changed, []BusinessKey{`"m"`}) {
			t.Fatalf("keyed delta=%+v evidence=%v/%v/%v", got, added, removed, changed)
		}
		candidate[0], candidate[2] = candidate[2], candidate[0]
	}
	got, added, removed, changed := Delta(base, append([]Row(nil), base...), true)
	if got != (DeltaSummary{}) || added != nil || removed != nil || changed != nil {
		t.Fatalf("unchanged keyed evidence=%+v %v/%v/%v", got, added, removed, changed)
	}
}

func TestResourceCodecC14StoredExpansionAndExactByteRevalidation(t *testing.T) {
	ref := codecTestRef(t)
	schema := map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}, "required": []string{"value"}, "additionalProperties": false}
	input := []byte(strings.Repeat("{\"value\":\""+strings.Repeat("<", 16_000)+"\"}\n", 8))
	compiled, defects := CompileJSONL(ref, schema, "", input)
	if len(defects) != 0 || len(compiled.CanonicalJSONL) <= MaxDecodedImportBytes {
		t.Fatalf("expanded admission bytes=%d defects=%+v", len(compiled.CanonicalJSONL), defects)
	}
	stored, defects := CompileStoredJSONL(ref, schema, "", compiled.CanonicalJSONL)
	if len(defects) != 0 || !reflect.DeepEqual(stored, compiled) {
		t.Fatalf("stored canonical expansion=%+v defects=%+v", stored.Manifest, defects)
	}
	head := VersionHead(compiled.VersionID)
	base := SourceEvaluationBase{State: "version", Head: HeadResult{Before: head, After: head, Revision: 1}, Manifest: &compiled.Manifest,
		CanonicalSchema: compiled.CanonicalSchema, CanonicalJSONL: compiled.CanonicalJSONL}
	if rows, err := base.Validate(ref); err != nil || len(rows) != 8 {
		t.Fatalf("exact historical base rows=%d error=%v", len(rows), err)
	}
	base.CanonicalJSONL = bytes.Replace(compiled.CanonicalJSONL, []byte(`\u003c`), []byte(`<`), 1)
	if _, err := base.Validate(ref); err == nil {
		t.Fatal("same-meaning noncanonical bytes acquired retained identity")
	}
	base.CanonicalJSONL = compiled.CanonicalJSONL
	base.Head.Before.VersionID = VersionID("resource-version-v1:sha256:" + strings.Repeat("f", 64))
	if _, err := base.Validate(ref); err == nil {
		t.Fatal("different retained version was accepted")
	}
}
