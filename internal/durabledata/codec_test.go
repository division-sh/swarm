package durabledata

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"strings"
	"testing"
)

func TestCompileJSONLKeylessBindingVectorPreservesOrderAndMultiplicity(t *testing.T) {
	declaration, err := ParseDeclarationRef(".", "company.lead")
	if err != nil {
		t.Fatal(err)
	}
	schema := leadSchema(false)
	input := []byte("{\"slug\":\"zeta\",\"funding_round\":1}\n{\"funding_round\":2,\"slug\":\"alpha\"}\n{\"slug\":\"zeta\",\"funding_round\":1}\n")
	compiled, defects := CompileJSONL(declaration, schema, "", input)
	if len(defects) != 0 {
		t.Fatalf("defects = %#v", defects)
	}
	if len(compiled.Rows) != 3 || compiled.Rows[0].Ordinal != 1 || compiled.Rows[1].Ordinal != 2 || compiled.Rows[2].Ordinal != 3 {
		t.Fatalf("rows = %#v", compiled.Rows)
	}
	if compiled.Rows[0].BusinessKey != "" || compiled.Rows[2].BusinessKey != "" {
		t.Fatalf("keyless rows grew business keys: %#v", compiled.Rows)
	}
	if got, want := string(compiled.CanonicalJSONL), "{\"funding_round\":1,\"slug\":\"zeta\"}\n{\"funding_round\":2,\"slug\":\"alpha\"}\n{\"funding_round\":1,\"slug\":\"zeta\"}\n"; got != want {
		t.Fatalf("canonical JSONL = %q, want %q", got, want)
	}
	if got, want := string(compiled.Manifest.ContentDigest), "resource-content-v1:sha256:2c83dcb78ea8e0ce6790f74a2ea861a870012e54a14194493acc574f1759b5d8"; got != want {
		t.Fatalf("content digest = %s, want %s", got, want)
	}
	if got, want := string(compiled.VersionID), "resource-version-v1:sha256:1b472fa93dd87ac43aa4d0cbcff4b9138d708cb8c895869af8a83700b3124649"; got != want {
		t.Fatalf("version ID = %s, want %s", got, want)
	}
}

func TestCompileJSONLEmptyInputProducesNonNilZeroRowPayload(t *testing.T) {
	declaration, err := ParseDeclarationRef(".", "company.lead")
	if err != nil {
		t.Fatal(err)
	}
	compiled, defects := CompileJSONL(declaration, leadSchema(false), "", nil)
	if len(defects) != 0 {
		t.Fatalf("defects = %#v", defects)
	}
	if compiled.CanonicalJSONL == nil || len(compiled.CanonicalJSONL) != 0 || len(compiled.Rows) != 0 || compiled.Manifest.RowCount != 0 {
		t.Fatalf("empty compiled version = %#v", compiled)
	}
}

func TestCompileJSONLKeyedSortsCanonicalTypedKeysAndRejectsDuplicates(t *testing.T) {
	declaration, _ := ParseDeclarationRef(".", "company.lead")
	schema := leadSchema(true)
	compiled, defects := CompileJSONL(declaration, schema, "slug", []byte("{\"slug\":\"zeta\",\"funding_round\":1}\n{\"funding_round\":2,\"slug\":\"alpha\"}\n"))
	if len(defects) != 0 {
		t.Fatalf("defects = %#v", defects)
	}
	if got, want := string(compiled.Rows[0].BusinessKey), "\"alpha\""; got != want {
		t.Fatalf("first business key = %s, want %s", got, want)
	}
	if got, want := string(compiled.CanonicalJSONL), "{\"funding_round\":2,\"slug\":\"alpha\"}\n{\"funding_round\":1,\"slug\":\"zeta\"}\n"; got != want {
		t.Fatalf("canonical JSONL = %q, want %q", got, want)
	}
	if got, want := string(compiled.Manifest.ContentDigest), "resource-content-v1:sha256:13042dd832ba8f99c4f9f51f1857e119f984687474e81f6e14e22aea4191e989"; got != want {
		t.Fatalf("content digest = %s, want %s", got, want)
	}
	if got, want := string(compiled.VersionID), "resource-version-v1:sha256:03f761b5d248a68772e30e819813cca57934aaba96319009a66f0626b26dca93"; got != want {
		t.Fatalf("version ID = %s, want %s", got, want)
	}
	if _, defects := CompileJSONL(declaration, schema, "slug", []byte("{\"slug\":\"same\",\"funding_round\":1}\n{\"slug\":\"same\",\"funding_round\":2}\n")); len(defects) != 1 || defects[0].Code != "duplicate_business_key" {
		t.Fatalf("duplicate defects = %#v", defects)
	}
}

func TestCompileJSONLExactJobflowCorpusBindingVector(t *testing.T) {
	compressed, err := os.Open("testdata/jobflow-gems.jsonl.gz")
	if err != nil {
		t.Fatal(err)
	}
	decompressor, err := gzip.NewReader(compressed)
	if err != nil {
		t.Fatal(err)
	}
	input, err := io.ReadAll(decompressor)
	if err != nil {
		t.Fatal(err)
	}
	if err := decompressor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := compressed.Close(); err != nil {
		t.Fatal(err)
	}
	if got, want := len(input), 666737; got != want {
		t.Fatalf("source bytes = %d, want %d", got, want)
	}
	digest := sha256.Sum256(input)
	if got, want := hex.EncodeToString(digest[:]), "7f91b3f892fd32e8605c9155bd4f7b4d90f4ff8dda1b556b1a68f0bb2d865a67"; got != want {
		t.Fatalf("source digest = %s, want %s", got, want)
	}
	if got, want := strings.Count(string(input), "\n"), 1362; got != want {
		t.Fatalf("physical rows = %d, want %d", got, want)
	}
	if got, want := strings.Count(string(input), `"ats": null`)+strings.Count(string(input), `"tvl": null`), 2042; got != want {
		t.Fatalf("external optional nulls = %d, want %d", got, want)
	}

	declaration, err := ParseDeclarationRef(".", "company.lead")
	if err != nil {
		t.Fatal(err)
	}
	compiled, defects := CompileJSONL(declaration, jobflowLeadSchema(), "slug", input)
	if len(defects) != 0 {
		t.Fatalf("jobflow defects = %#v", defects)
	}
	if got, want := len(compiled.Rows), 1362; got != want {
		t.Fatalf("canonical rows = %d, want %d", got, want)
	}
	if strings.Contains(string(compiled.CanonicalJSONL), ":null") {
		t.Fatal("canonical jobflow rows retain external null authority")
	}
	for index, row := range compiled.Rows {
		if row.Ordinal != uint64(index+1) || row.BusinessKey == "" {
			t.Fatalf("row %d identity = ordinal:%d key:%q", index, row.Ordinal, row.BusinessKey)
		}
		if index > 0 && compiled.Rows[index-1].BusinessKey >= row.BusinessKey {
			t.Fatalf("business keys are not strictly canonical at row %d: %q >= %q", index+1, compiled.Rows[index-1].BusinessKey, row.BusinessKey)
		}
	}
	if got, want := string(compiled.Manifest.ContentDigest), "resource-content-v1:sha256:4d89fe6a170b6f41ffdd251f2d64e229db533c989cacbec139641f999adeffdd"; got != want {
		t.Fatalf("jobflow content digest = %s, want %s", got, want)
	}
	if got, want := string(compiled.VersionID), "resource-version-v1:sha256:0077c74245132050e59c013d6d00f0cbdca71b1a10a99a29553ffd1caeb594c3"; got != want {
		t.Fatalf("jobflow version ID = %s, want %s", got, want)
	}
}

func TestCompileJSONLNormalizesOptionalNullToOmissionRecursively(t *testing.T) {
	declaration, _ := ParseDeclarationRef(".", "company.profile")
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"slug": map[string]any{"type": "string"},
			"profile": map[string]any{
				"type":                 "object",
				"properties":           map[string]any{"tvl": map[string]any{"type": "integer"}},
				"additionalProperties": false,
			},
		},
		"required":             []any{"slug", "profile"},
		"additionalProperties": false,
	}
	compiled, defects := CompileJSONL(declaration, schema, "", []byte("{\"slug\":\"a\",\"profile\":{\"tvl\":null}}\n"))
	if len(defects) != 0 {
		t.Fatalf("defects = %#v", defects)
	}
	if got, want := string(compiled.CanonicalJSONL), "{\"profile\":{},\"slug\":\"a\"}\n"; got != want {
		t.Fatalf("canonical JSONL = %q, want %q", got, want)
	}

	listSchema := map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"items": map[string]any{"type": "array", "items": map[string]any{"type": "string"}}},
		"required":             []any{"items"},
		"additionalProperties": false,
	}
	if _, defects := CompileJSONL(declaration, listSchema, "", []byte("{\"items\":[\"a\",null]}\n")); len(defects) == 0 || defects[0].Code != "schema_rejected" {
		t.Fatalf("null list element defects = %#v", defects)
	}
}

func TestDeltaDistinguishesKeylessMultiplicityOrderAndKeyedChanges(t *testing.T) {
	row := func(ordinal uint64, raw string) Row { return Row{Ordinal: ordinal, Canonical: []byte(raw)} }
	base := []Row{row(1, "{\"v\":1}"), row(2, "{\"v\":2}"), row(3, "{\"v\":1}")}
	reordered := []Row{row(1, "{\"v\":1}"), row(2, "{\"v\":1}"), row(3, "{\"v\":2}")}
	delta, added, removed, changed := Delta(base, reordered, false)
	if delta.Added != 0 || delta.Removed != 0 || delta.Changed != 0 || !delta.OrderChanged || len(added)+len(removed)+len(changed) != 0 {
		t.Fatalf("keyless reorder delta = %#v %#v %#v %#v", delta, added, removed, changed)
	}
	multiplicity := []Row{row(1, "{\"v\":1}"), row(2, "{\"v\":2}")}
	delta, _, _, _ = Delta(base, multiplicity, false)
	if delta.Removed != 1 || delta.Added != 0 || delta.OrderChanged {
		t.Fatalf("keyless multiplicity delta = %#v", delta)
	}

	key := func(raw string) BusinessKey { return BusinessKey(raw) }
	keyedBase := []Row{{Ordinal: 1, BusinessKey: key("\"a\""), Canonical: []byte("{\"slug\":\"a\",\"v\":1}")}}
	keyedNext := []Row{{Ordinal: 1, BusinessKey: key("\"a\""), Canonical: []byte("{\"slug\":\"a\",\"v\":2}")}}
	delta, _, _, changed = Delta(keyedBase, keyedNext, true)
	if delta.Changed != 1 || len(changed) != 1 || changed[0] != key("\"a\"") {
		t.Fatalf("keyed delta = %#v changed=%#v", delta, changed)
	}
}

func TestCompileJSONLRejectsWholeInputAndReportsDistantRows(t *testing.T) {
	declaration, _ := ParseDeclarationRef(".", "company.lead")
	schema := map[string]any{"type": "object", "properties": map[string]any{"slug": map[string]any{"type": "string"}}, "required": []any{"slug"}, "additionalProperties": false}
	compiled, defects := CompileJSONL(declaration, schema, "slug", []byte("{\"slug\":\"ok\"}\n\n{\"slug\":null}\n{\"slug\":\"a\",\"slug\":\"b\"}\n"))
	if len(defects) != 3 {
		t.Fatalf("defects = %#v, want 3", defects)
	}
	if compiled.VersionID != "" || len(compiled.Rows) != 0 {
		t.Fatalf("rejected input produced version %#v", compiled)
	}
}

func TestStaticAndResourcePathsAreOneWayIdentityProjections(t *testing.T) {
	staticID, err := NewStaticDataID(StaticDataRef{BundleHash: "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", CanonicalInputLabel: "bundle/flows/a/data/resume.md"})
	if err != nil {
		t.Fatal(err)
	}
	path, err := StaticMountPath(staticID)
	if err != nil {
		t.Fatal(err)
	}
	if len(path) != len("/data/.swarm/static/s_")+64+len(".data") {
		t.Fatalf("static path = %q", path)
	}
	declaration, _ := ParseDeclarationRef("flows/a", "company.lead")
	resourcePath, err := ResourceMountPath(declaration)
	if err != nil {
		t.Fatal(err)
	}
	if len(resourcePath) == 0 || resourcePath == path || strings.Contains(resourcePath, "company.lead") {
		t.Fatalf("resource path is not a one-way identity projection: %q", resourcePath)
	}
}

func leadSchema(keyed bool) map[string]any {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"slug":          map[string]any{"type": "string"},
			"funding_round": map[string]any{"type": "integer"},
		},
		"required":             []any{"slug", "funding_round"},
		"additionalProperties": false,
	}
	if keyed {
		schema["x-swarm-dataset-key"] = "slug"
	}
	return schema
}

func jobflowLeadSchema() map[string]any {
	stringField := func() map[string]any { return map[string]any{"type": "string"} }
	stringList := func() map[string]any { return map[string]any{"type": "array", "items": stringField()} }
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"slug":       stringField(),
			"name":       stringField(),
			"domain":     stringField(),
			"funds":      stringList(),
			"sources":    stringList(),
			"tvl":        map[string]any{"type": "number"},
			"category":   stringField(),
			"has_token":  map[string]any{"type": "boolean"},
			"note":       stringField(),
			"on_w3c":     map[string]any{"type": "boolean"},
			"eng_roles":  map[string]any{"type": "integer"},
			"fit_titles": stringList(),
			"gem_score":  map[string]any{"type": "number"},
			"ats": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"provider": stringField(), "ats_slug": stringField(), "titles": stringList(),
				},
				"required": []any{"provider", "ats_slug", "titles"}, "additionalProperties": false,
			},
		},
		"required":             []any{"slug", "name", "domain", "funds", "sources", "category", "has_token", "note", "on_w3c", "eng_roles", "fit_titles", "gem_score"},
		"additionalProperties": false,
	}
}
