package contracts

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func loadFlatEmbeddingFixture(t *testing.T, types, event, state string) (*WorkflowContractBundle, error) {
	t.Helper()
	root := t.TempDir()
	for name, body := range map[string]string{
		"schema.yaml": "name: embedding\nstages: {done: {final: true}}\n",
		"types.yaml":  types,
		"events.yaml": "company.lead:\n" + event,
		"state.yaml":  "company:\n" + state,
	} {
		writeFixtureFile(t, filepath.Join(root, name), body)
	}
	repo := repoRootForContractsTest(t)
	return LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
}

func TestFlatEmbeddingEventAndStateEquivalence(t *testing.T) {
	t.Run("expanded_business_key_refusals", func(t *testing.T) {
		for _, tc := range []struct{ key, fields, want string }{
			{"missing", "slug: text", "is not a declared payload field"},
			{"slug", "slug: 'text?'", "must be required"},
			{"detail", "detail: Detail", "must have boolean, number, or string semantics"},
		} {
			types := "types:\n  Detail: {value: text}\n  Lead: {" + tc.fields + "}\n"
			bundle, err := loadFlatEmbeddingFixture(t, types, "  _embed: [Lead]\n  _key: "+tc.key+"\n", "  own: text\n")
			if bundle != nil || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expanded key %s: bundle=%t err=%v", tc.key, bundle != nil, err)
			}
		}
	})
	t.Run("business_names_and_own_attributes", func(t *testing.T) {
		fields := ""
		for _, name := range []string{"key", "description", "required", "producer", "swarm", "type", "payload", "fields", "payload_type", "state_field"} {
			fields += "  " + name + ": text\n"
		}
		bundle, err := loadFlatEmbeddingFixture(t, "types:\n  Lead: {slug: text}\n", "  _embed: [Lead]\n"+fields,
			"  _embed: [Lead]\n"+fields+"  own: {type: text, initial: initial, immutable: true, indexed: true}\n")
		if err != nil {
			t.Fatal(err)
		}
		entry, _, _ := bundle.ResolveFlowEventCatalogEntry(".", "company.lead")
		if len(entry.Payload.Properties) != 11 || entry.BusinessKeyField != "" || entry.Description != "" {
			t.Fatalf("business names acquired metadata semantics: %+v", entry)
		}
		state := bundle.RootEntities["company"]
		if state.Fields["own"].Initial != "initial" || !state.Fields["own"].Immutable || !state.Fields["own"].Indexed || state.Fields["slug"].Initial != nil || state.Fields["slug"].Indexed {
			t.Fatalf("own role attributes changed: %+v", state.Fields)
		}
	})
	types := "types:\n  Lead:\n    slug: {type: text, description: Stable identity}\n    note: text?\n"
	inline := "  slug: {type: text, description: Stable identity}\n  note: text?\n  week: text\n"
	embedded := "  _embed: [Lead]\n  week: text\n"
	left, err := loadFlatEmbeddingFixture(t, types, "  _key: slug\n"+inline, inline)
	if err != nil {
		t.Fatal(err)
	}
	right, err := loadFlatEmbeddingFixture(t, types, "  _key: slug\n"+embedded, embedded)
	if err != nil {
		t.Fatal(err)
	}
	a, ok, err := left.ResolveCompiledFlowEventSchema(".", "company.lead")
	if err != nil || !ok {
		t.Fatalf("inline compiled event: %t %v", ok, err)
	}
	b, ok, err := right.ResolveCompiledFlowEventSchema(".", "company.lead")
	if err != nil || !ok || !bytes.Equal(a.CanonicalAcceptanceSchema(), b.CanonicalAcceptanceSchema()) {
		t.Fatalf("embedded acceptance differs: %t %v\ninline=%s\nembedded=%s", ok, err, a.CanonicalAcceptanceSchema(), b.CanonicalAcceptanceSchema())
	}
	pa, err := left.ResolveRootPrimaryEntity()
	if err != nil {
		t.Fatal(err)
	}
	pb, err := right.ResolveRootPrimaryEntity()
	if err != nil {
		t.Fatal(err)
	}
	sa, err := pa.StructuralType()
	if err != nil {
		t.Fatal(err)
	}
	sb, err := pb.StructuralType()
	if err != nil || !reflect.DeepEqual(sa, sb) {
		t.Fatalf("embedded state differs: %v\ninline=%+v\nembedded=%+v", err, sa, sb)
	}
}

func TestFlatEmbeddingTwentyTwoFieldsAndNestedDomains(t *testing.T) {
	// Start with the checked-in jobflow company's four fields. Additional
	// domains exercise reuse; this is not the user's registry-v5 acceptance.
	company, err := os.ReadFile(filepath.Join(repoRootForContractsTest(t), "internal/serveapp/testdata/verify-run-jobflow-registry/registry/state.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(company, []byte("company:\n")) {
		t.Fatal("checked jobflow company declaration changed")
	}
	inline := strings.TrimPrefix(string(company), "company:\n")
	inline = strings.Replace(inline, "  slug: text\n", "  slug: {type: text, description: Stable slug, pattern: '^[a-z]+$'}\n", 1)
	inline += "  status: Status\n  detail: Detail\n  details: [Detail]\n  by_name: map[text]Detail\n  optional: Label?\n"
	for i := 0; i < 13; i++ {
		inline += fmt.Sprintf("  value_%02d: numeric\n", i)
	}
	types := "scalars: {Label: text}\nenums:\n  Status: {values: [open, closed], default: open}\ntypes:\n  Detail:\n    label: Label\n  Lead:\n" + bytesToIndented(inline)
	a, err := loadFlatEmbeddingFixture(t, types, "  _key: slug\n"+inline, inline)
	if err != nil {
		t.Fatal(err)
	}
	b, err := loadFlatEmbeddingFixture(t, types, "  _key: slug\n  _embed: [Lead]\n", "  _embed: [Lead]\n")
	if err != nil {
		t.Fatal(err)
	}
	x, _, err := a.ResolveCompiledFlowEventSchema(".", "company.lead")
	if err != nil {
		t.Fatal(err)
	}
	y, _, err := b.ResolveCompiledFlowEventSchema(".", "company.lead")
	if err != nil || len(y.Fields()) != 22 || !bytes.Equal(x.CanonicalAcceptanceSchema(), y.CanonicalAcceptanceSchema()) {
		t.Fatalf("22-field event differs: %d %v", len(y.Fields()), err)
	}
	pa, err := a.ResolveRootPrimaryEntity()
	if err != nil {
		t.Fatal(err)
	}
	pb, err := b.ResolveRootPrimaryEntity()
	if err != nil {
		t.Fatal(err)
	}
	sa, err := pa.StructuralType()
	if err != nil {
		t.Fatal(err)
	}
	sb, err := pb.StructuralType()
	if err != nil || !StructuralCatalogTypesEqual(sa, sb) || len(sb.Fields) != 22 {
		t.Fatalf("22-field state differs: %v", err)
	}
	if b.RootEntities["company"].Fields["slug"].Description != "Stable slug" {
		t.Fatal("embedding dropped the defining description")
	}
}

func TestFlatEmbeddingRefusesInvalidReferencesAndCollisions(t *testing.T) {
	t.Run("reference_kind_is_not_retyped_by_declaration", testFlatEmbeddingReferenceKind)
	base := "scalars: {Label: text}\nenums:\n  Status: {values: [open], default: open}\ntypes:\n  Lead: {slug: text}\n  Other: {slug: text}\n  Empty: {}\n"
	for _, tc := range []struct{ name, declaration, want string }{
		{"scalar_sequence", "  _embed: Lead\n", "list of named types"},
		{"empty_sequence", "  _embed: []\n", "nonempty"},
		{"null_sequence", "  _embed: null\n", "list of named types"},
		{"mapping", "  _embed: {Lead: true}\n", "list of named types"},
		{"scalar_type", "  _embed: [text]\n", "named records"},
		{"alias_type", "  _embed: [Label]\n", "named records"},
		{"enum_type", "  _embed: [Status]\n", "named records"},
		{"list_type", "  _embed: ['[Lead]']\n", "named records"},
		{"map_type", "  _embed: ['map[text]Lead']\n", "named records"},
		{"optional", "  _embed: ['Lead?']\n", "named records"},
		{"unknown", "  _embed: [Missing]\n", "named records"},
		{"empty_record", "  _embed: [Empty]\n  own: text\n", "nonempty record"},
		{"repeat_type", "  _embed: [Lead, Lead]\n", "repeats named record"},
		{"embed_embed", "  _embed: [Lead, Other]\n", "field slug is declared by"},
		{"embed_own", "  _embed: [Lead]\n  slug: text\n", "own field"},
		{"description_annotation", "  _embed: [Lead]\n  slug: {type: text, description: Per-use annotation}\n", "own field"},
		{"annotate", "  _embed: [Lead]\n  slug: {type: text, indexed: true}\n", "own field"},
		{"unknown_metadata", "  _embeds: [Lead]\n", "_embeds"},
	} {
		for _, role := range []string{"event", "state"} {
			t.Run(role+"/"+tc.name, func(t *testing.T) {
				event, state := "  own: text\n", "  own: text\n"
				if role == "event" {
					event = tc.declaration
				} else {
					state = tc.declaration
				}
				bundle, err := loadFlatEmbeddingFixture(t, base, event, state)
				want := tc.want
				if role == "event" && tc.name == "annotate" {
					want = "indexed"
				}
				if bundle != nil || err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("want %q, got bundle=%t err=%v", tc.want, bundle != nil, err)
				}
				if strings.HasPrefix(tc.name, "embed_") || tc.name == "description_annotation" || (tc.name == "annotate" && role == "state") {
					file := "events.yaml"
					if role == "state" {
						file = "state.yaml"
					}
					if !strings.Contains(err.Error(), "types.yaml:") || (tc.name != "embed_embed" && !strings.Contains(err.Error(), file+":")) {
						t.Fatalf("collision lost defining/own coordinates: %v", err)
					}
				}
			})
		}
	}
}

func testFlatEmbeddingReferenceKind(t *testing.T) {
	for _, name := range []string{"[Lead]", "map[text]Lead", "Lead?"} {
		for _, role := range []string{"event", "state"} {
			t.Run(role+"/"+name, func(t *testing.T) {
				types := "types:\n  Lead: {slug: text}\n  '" + name + "': {wrong: text}\n"
				event, state := "  own: text\n", "  own: text\n"
				if role == "event" {
					event = "  _embed: ['" + name + "']\n"
				} else {
					state = "  _embed: ['" + name + "']\n"
				}
				bundle, err := loadFlatEmbeddingFixture(t, types, event, state)
				if bundle != nil || err == nil || !strings.Contains(err.Error(), "named records") {
					t.Fatalf("collection/optional expression admitted by a same-spelled declaration: bundle=%t err=%v", bundle != nil, err)
				}
			})
		}
	}
}

func TestFlatEmbeddingPermutationProvenanceAndDescription(t *testing.T) {
	types := "types:\n  Lead:\n    slug: text\n  Window:\n    week: text\n"
	var canonical []byte
	for _, refs := range []string{"Lead, Window", "Window, Lead"} {
		bundle, err := loadFlatEmbeddingFixture(t, types, "  _description: A published lead\n  _embed: ["+refs+"]\n", "  _embed: ["+refs+"]\n")
		if err != nil {
			t.Fatal(err)
		}
		compiled, _, err := bundle.ResolveCompiledFlowEventSchema(".", "company.lead")
		if err != nil {
			t.Fatal(err)
		}
		if canonical != nil && !bytes.Equal(canonical, compiled.CanonicalAcceptanceSchema()) {
			t.Fatal("embedding list order changed acceptance")
		}
		canonical = compiled.CanonicalAcceptanceSchema()
		if compiled.EventSchema().Description != "A published lead" {
			t.Fatal("event description was lost")
		}
		entry, _, ok := bundle.ResolveFlowEventCatalogEntry(".", "company.lead")
		if !ok {
			t.Fatal("missing event")
		}
		origin, ok := entry.FieldProvenance("slug")
		index := 0
		if strings.HasPrefix(refs, "Window") {
			index = 1
		}
		if !ok || origin.Origin != EffectiveValueOriginDerived || origin.SourceFile != "types.yaml" || origin.SourceLine != 3 || !reflect.DeepEqual(origin.InputPaths, []string{fmt.Sprintf("_embed[%d]", index)}) {
			t.Fatalf("provenance lost defining line/authored index: %+v", origin)
		}
		origin.InputPaths[0] = "changed"
		again, _ := entry.FieldProvenance("slug")
		if again.InputPaths[0] == "changed" {
			t.Fatal("returned provenance aliases the declaration")
		}
		entry.Description = "Different prose"
		rebuilt, err := newCompiledEventSchema(".", "company.lead", entry, bundle.RootTypes, "", CompiledEventSchemaSource{})
		if err != nil || !bytes.Equal(canonical, rebuilt.CanonicalAcceptanceSchema()) {
			t.Fatalf("description changed acceptance: %v", err)
		}
	}
}

func TestFlatEmbeddingCapturedCatalogAndRefinements(t *testing.T) {
	t.Run("declaring_scope_and_sibling_catalogs", testFlatEmbeddingDeclaringScope)
	t.Run("alias_and_merge_introductions", testFlatEmbeddingAliasIntroductions)
	types := "types:\n  Detail:\n    value: text\n  Lead:\n    slug: {type: text, length: {min: 2}, equal_to: peer}\n    peer: text\n    detail: Detail\n"
	bundle, err := loadFlatEmbeddingFixture(t, types, "  _embed: [Lead]\n  own: {type: text, equal_to: slug}\n", "  _embed: [Lead]\n  own: {type: text, equal_to: slug}\n")
	if err != nil {
		t.Fatal(err)
	}
	entry, _, _ := bundle.ResolveFlowEventCatalogEntry(".", "company.lead")
	consumer := cloneTypeCatalogDocument(bundle.RootTypes)
	consumer.Types["Detail"] = NamedTypeDecl{Fields: map[string]TypeFieldSpec{"value": {Type: "integer"}}}
	ref, ok := captureCatalogEventFieldReference(entry, consumer, "detail")
	if !ok {
		t.Fatal("missing captured nested reference")
	}
	resolved, err := ref.Resolve()
	if err != nil || resolved.Fields[0].Type.Kind != CatalogTypeText {
		t.Fatalf("consumer rebound producer field: %+v %v", resolved, err)
	}
	copy := cloneEventCatalogEntry(entry)
	*copy.Payload.Properties["slug"].Refinements.Length.Min = 99
	original := entry.Payload.Properties["slug"]
	if *original.Refinements.Length.Min != 2 {
		t.Fatal("event refinements alias a copied declaration")
	}
	primary, err := bundle.ResolveRootPrimaryEntity()
	if err != nil {
		t.Fatal(err)
	}
	copyState := cloneEntityContract(primary.Contract)
	*copyState.Fields["slug"].Refinements.Length.Min = 77
	if *primary.Contract.Fields["slug"].Refinements.Length.Min != 2 {
		t.Fatal("state refinements alias a copied declaration")
	}
	primary.Types.Types["Detail"] = NamedTypeDecl{Fields: map[string]TypeFieldSpec{"value": {Type: "integer"}}}
	shape, err := primary.StructuralType()
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range shape.Fields {
		if field.Name == "detail" && field.Type.Fields[0].Type.Kind != CatalogTypeText {
			t.Fatal("consumer catalog rebound captured state fields")
		}
	}
	for _, own := range []string{"  own: {type: integer, equal_to: slug}\n", "  own: {type: text, equal_to: missing}\n"} {
		if candidate, err := loadFlatEmbeddingFixture(t, types, "  _embed: [Lead]\n"+own, "  _embed: [Lead]\n"); candidate != nil || err == nil {
			t.Fatalf("invalid equality target admitted: %v", err)
		}
	}
}

func testFlatEmbeddingDeclaringScope(t *testing.T) {
	root := t.TempDir()
	for file, body := range map[string]string{
		"schema.yaml":       "name: scope\nstages: {done: {final: true}}\n",
		"types.yaml":        "types:\n  Detail: {value: text}\n  Lead: {detail: Detail}\n",
		"events.yaml":       "lead: {_embed: [Lead]}\n",
		"state.yaml":        "root_entity: {_embed: [Lead]}\n",
		"left/schema.yaml":  "name: left\nstages: {done: {final: true}}\n",
		"left/types.yaml":   "types:\n  Detail: {value: integer}\n  Lead: {detail: Detail}\n",
		"left/events.yaml":  "lead: {_embed: [Lead]}\n",
		"left/state.yaml":   "left_entity: {_embed: [Lead]}\n",
		"right/schema.yaml": "name: right\nstages: {done: {final: true}}\n",
		"right/types.yaml":  "types:\n  Detail: {value: boolean}\n  Lead: {detail: Detail}\n",
		"right/events.yaml": "lead: {_embed: [Lead]}\n",
		"right/state.yaml":  "right_entity: {_embed: [Lead]}\n",
	} {
		writeFixtureFile(t, filepath.Join(root, file), body)
	}
	repo := repoRootForContractsTest(t)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		flow string
		kind CatalogTypeKind
	}{
		{".", CatalogTypeText}, {"left", CatalogTypeInteger}, {"right", CatalogTypeBoolean},
	} {
		entry, _, found := bundle.ResolveFlowEventCatalogEntry(tc.flow, "lead")
		if !found {
			t.Fatalf("missing %s event", tc.flow)
		}
		// A consumer with another catalog must not rebind this producer.
		ref, found := captureCatalogEventFieldReference(entry, bundle.ResolvedTypeCatalogForFlow("right"), "detail")
		resolved, err := ref.Resolve()
		if !found || err != nil || len(resolved.Fields) != 1 || resolved.Fields[0].Type.Kind != tc.kind {
			t.Fatalf("%s producer scope changed: %+v %v", tc.flow, resolved, err)
		}
		primary, err := bundle.ResolveFlowPrimaryEntity(tc.flow)
		if err != nil {
			t.Fatal(err)
		}
		shape, err := primary.StructuralType()
		if err != nil || shape.Fields[0].Type.Fields[0].Type.Kind != tc.kind {
			t.Fatalf("%s state scope changed: %+v %v", tc.flow, shape, err)
		}
	}
	rootEntry, _, _ := bundle.ResolveFlowEventCatalogEntry(".", "lead")
	rootEntry.types.Types["Detail"] = NamedTypeDecl{Fields: map[string]TypeFieldSpec{"value": {Type: "uuid"}}}
	again, _, _ := bundle.ResolveFlowEventCatalogEntry(".", "lead")
	ref, _ := captureCatalogEventFieldReference(again, TypeCatalogDocument{}, "detail")
	resolved, err := ref.Resolve()
	if err != nil || resolved.Fields[0].Type.Kind != CatalogTypeText {
		t.Fatalf("returned catalog aliases the source: %+v %v", resolved, err)
	}
}

func testFlatEmbeddingAliasIntroductions(t *testing.T) {
	types := "types:\n  Base:\n    slug: &field {type: text, description: Named slug}\n  Lead:\n    alias: *field\n    merged:\n      <<: *field\n"
	for _, declaration := range []string{
		"  _embed: &record [Lead]\n",
		"  <<: &record\n    _embed: [Lead]\n",
	} {
		bundle, err := loadFlatEmbeddingFixture(t, types, declaration, declaration)
		if err != nil {
			t.Fatal(err)
		}
		entry, _, _ := bundle.ResolveFlowEventCatalogEntry(".", "company.lead")
		for _, tc := range []struct {
			name string
			line int
		}{
			{"alias", 5}, {"merged", 6},
		} {
			origin, ok := entry.FieldProvenance(tc.name)
			if !ok || origin.SourceFile != "types.yaml" || origin.SourceLine != tc.line || len(origin.InputPaths) != 1 || origin.InputPaths[0] != "_embed[0]" {
				t.Fatalf("%s introduction lost: %+v", tc.name, origin)
			}
			if entry.Payload.Properties[tc.name].Description != "Named slug" {
				t.Fatalf("%s lost aliased description", tc.name)
			}
		}
	}
	root := t.TempDir()
	for file, body := range map[string]string{
		"schema.yaml": "name: alias\nstages: {done: {final: true}}\n",
		"types.yaml":  types,
		"events.yaml": "first:\n  _embed: &records [Lead]\nsecond:\n  _embed: *records\n",
		"state.yaml":  "company:\n  _embed: &records [Lead]\n",
	} {
		writeFixtureFile(t, filepath.Join(root, file), body)
	}
	repo := repoRootForContractsTest(t)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	first, _, _ := bundle.ResolveFlowEventCatalogEntry(".", "first")
	second, _, _ := bundle.ResolveFlowEventCatalogEntry(".", "second")
	if !reflect.DeepEqual(first.Payload, second.Payload) {
		t.Fatal("sequence alias changed expanded fields")
	}
}

func TestFlatEmbeddingStateFilenameAndRetainedSource(t *testing.T) {
	for _, prefix := range []string{"", "child/"} {
		for _, oldAndNew := range []bool{false, true} {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: filenames\nstages: {done: {final: true}}\n")
			writeFixtureFile(t, filepath.Join(root, prefix+"entities.yaml"), "company: {slug: text}\n")
			if oldAndNew {
				writeFixtureFile(t, filepath.Join(root, prefix+"state.yaml"), "company: {slug: text}\n")
			}
			if admitted, err := sourceartifact.AdmitDirectory(root); admitted != nil || err == nil || !strings.Contains(err.Error(), "entities.yaml") {
				t.Fatalf("old filename layout admitted: prefix=%s both=%t err=%v", prefix, oldAndNew, err)
			}
		}
	}
	root := t.TempDir()
	for file, body := range map[string]string{
		"schema.yaml":       "name: retained\nstages: {done: {final: true}}\n",
		"types.yaml":        "types:\n  Lead: {slug: text}\n",
		"state.yaml":        "company: {_embed: [Lead]}\n",
		"events.yaml":       "company.lead: {_embed: [Lead]}\n",
		"child/schema.yaml": "name: child\nstages: {done: {final: true}}\n",
		"child/state.yaml":  "company: {_embed: [Lead]}\n",
	} {
		writeFixtureFile(t, filepath.Join(root, file), body)
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	restored, err := sourceartifact.DecodeLogical(artifact.LogicalBlob())
	if err != nil || restored.BundleHash() != artifact.BundleHash() {
		t.Fatalf("retained source changed: %v", err)
	}
	repo := repoRootForContractsTest(t)
	bundle, err := LoadWorkflowContractBundleFromArtifact(repo, restored, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
	if err != nil || bundle.RootEntities["company"].Fields["slug"].Type != "text" {
		t.Fatalf("retained root expansion: %v", err)
	}
	child, err := bundle.ResolveFlowPrimaryEntity("child")
	if err != nil || child.Contract.Fields["slug"].Type != "text" {
		t.Fatalf("retained child expansion: %v", err)
	}
}
