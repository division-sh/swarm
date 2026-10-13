package contracts

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func listRetirementField(t testing.TB, source string) yamlsource.Value {
	t.Helper()
	snapshot, err := yamlsource.Load([]byte("items: " + source + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	fields, err := snapshot.Document("list-proof.yaml").Root().Mapping()
	if err != nil || len(fields) != 1 {
		t.Fatalf("source fields: %v", err)
	}
	return fields[0].Value
}

func listRetirementDecode(t testing.TB, surface string, value yamlsource.Value) (string, bool, any, error) {
	t.Helper()
	switch surface {
	case "types":
		field, err := projectTypeFieldSpec(value)
		return field.Type, field.IsOptional, nil, err
	case "entities":
		field, err := projectEntityFieldDecl(value)
		return field.Type, field.IsOptional, field.Initial, err
	case "events":
		field, optional, _, err := admitEventPayloadField(value)
		return field.Type, optional, nil, err
	case "nodes":
		field, err := projectNodeStateFieldValue("items", value)
		return field.Type, false, field.Default, err
	default:
		t.Fatalf("unclassified surface %s", surface)
		return "", false, nil, nil
	}
}

func TestListSpellingRetirementTypedFields(t *testing.T) {
	for _, surface := range []string{"types", "entities", "events", "nodes"} {
		for _, source := range []string{
			"list<ProbeItem>", "'list<ProbeItem>'", "list<ProbeItem>?", "{type: list<ProbeItem>}",
			"map[text]list<ProbeItem>", "'[list<ProbeItem>]'", "{type: '[list<ProbeItem>]'}",
			"{type: list}", "{type: 'list?', of: ProbeItem}", "{<<: &base {type: list, of: ProbeItem}}",
			"'[ProbeItem]'", `"[ProbeItem]"`, "['ProbeItem']", `["ProbeItem"]`,
			"'  [ProbeItem]  '", "|-\n  [ProbeItem]", ">-\n  [ProbeItem]",
			"[]", "[ProbeItem, ProbeItem]", "[1]", "[true]", "[!!str ProbeItem]",
		} {
			t.Run(surface+"/"+source, func(t *testing.T) {
				_, _, _, err := listRetirementDecode(t, surface, listRetirementField(t, source))
				if err == nil || !errors.Is(err, ErrInvalidField) || !strings.Contains(err.Error(), "[T]") {
					t.Fatalf("expected typed [T] teaching: %v", err)
				}
			})
		}
		for _, source := range []string{"[ProbeItem]", "\n  - ProbeItem", "{type: '[ProbeItem]'}", `{type: "[ProbeItem]"}`} {
			t.Run(surface+"/positive/"+source, func(t *testing.T) {
				typeRef, optional, initial, err := listRetirementDecode(t, surface, listRetirementField(t, source))
				if err != nil || typeRef != "[ProbeItem]" || optional || initial != nil {
					t.Fatalf("declaration identity changed: type=%s optional=%v initial=%#v error=%v", typeRef, optional, initial, err)
				}
			})
		}
	}
}

func TestListSpellingRetirementPresenceMatrix(t *testing.T) {
	for _, surface := range []string{"types", "entities", "events", "nodes"} {
		for _, value := range []string{"", "null", "''", "ProbeItem", "0", "false", "[]", "[ProbeItem]", "{}", "{type: ProbeItem}"} {
			for _, prefix := range []string{"type: list, ", "type: text, ", ""} {
				source := "{" + prefix + "of: " + value + "}"
				t.Run(surface+"/"+source, func(t *testing.T) {
					_, _, _, err := listRetirementDecode(t, surface, listRetirementField(t, source))
					if err == nil || !errors.Is(err, ErrInvalidField) || !strings.Contains(err.Error(), "[T]") || !strings.Contains(err.Error(), "of") {
						t.Fatalf("retired presence was interpreted: %v", err)
					}
				})
			}
		}
	}
}

func TestListSpellingRetirementMetadataAndData(t *testing.T) {
	for _, surface := range []string{"types", "entities", "events"} {
		t.Run(surface, func(t *testing.T) {
			typeRef, optional, _, err := listRetirementDecode(t, surface, listRetirementField(t, "{type: '[text]?', length: {min: 0, max: 3}, description: 'list<T> is business text'}"))
			if err != nil || typeRef != "[text]" || !optional {
				t.Fatalf("metadata type slot changed: %q/%v/%v", typeRef, optional, err)
			}
			_, _, _, err = listRetirementDecode(t, surface, listRetirementField(t, "'[text]?'"))
			if err == nil || !errors.Is(err, ErrInvalidField) {
				t.Fatalf("bare quoted optional became a declaration: %v", err)
			}
		})
	}
	for _, test := range []struct{ surface, source string }{
		{"entities", "{type: '[text]', initial: ['list<T>', '[text]', 'text']}"},
		{"nodes", "{type: '[ProbeItem]', default: ['list<T>', '[text]', 'text']}"},
	} {
		t.Run(test.surface+"/data", func(t *testing.T) {
			_, _, initial, err := listRetirementDecode(t, test.surface, listRetirementField(t, test.source))
			if err != nil || !reflect.DeepEqual(initial, []any{"list<T>", "[text]", "text"}) {
				t.Fatalf("explicit data changed: %#v / %v", initial, err)
			}
		})
	}
}

func TestListSpellingRetirementBundleStyle(t *testing.T) {
	for _, surface := range []string{"types", "entities", "events", "nodes"} {
		for _, form := range []struct {
			name, source string
			reject       bool
		}{
			{"bare_plain", "[ProbeItem]", false},
			{"bare_quoted_bracket", "'[ProbeItem]'", true},
			{"bare_quoted_singleton", "['ProbeItem']", true},
			{"explicit_type", "{type: '[ProbeItem]'}", false},
			{"block_plain", "\n    - ProbeItem", false},
			{"merged_explicit_type", "{<<: &base {type: '[ProbeItem]'}}", false},
			{"angle", "list<ProbeItem>", true},
			{"long", "{type: list, of: ProbeItem}", true},
			{"merged_long", "{<<: &base {type: list, of: null}}", true},
		} {
			t.Run(surface+"/"+form.name, func(t *testing.T) {
				root := t.TempDir()
				files := map[string]string{
					"schema.yaml":   "name: list-proof\nstages:\n  ready: {}\n  done: {final: true}\n",
					"types.yaml":    "types:\n  ProbeItem:\n    value: text\n",
					"entities.yaml": "probe:\n  item: text\n",
				}
				switch surface {
				case "types":
					files["types.yaml"] += "  Container:\n    items: " + strings.ReplaceAll(form.source, "\n", "\n  ") + "\n"
				case "entities":
					files["entities.yaml"] = "probe:\n  items: " + form.source + "\n"
				case "events":
					files["events.yaml"] = "proof.received:\n  items: " + form.source + "\n"
				case "nodes":
					files["nodes.yaml"] = "proof:\n  state_schema:\n    fields:\n      items: " + strings.ReplaceAll(form.source, "\n", "\n    ") + "\n"
				}
				for path, source := range files {
					writeFixtureFile(t, root+"/"+path, source)
				}
				repo := repoRootForContractsTest(t)
				bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
				if form.reject {
					if err == nil || !strings.Contains(err.Error(), "[T]") || !strings.Contains(err.Error(), surface+".yaml") {
						t.Fatalf("full bundle failed to teach the offending source: %v", err)
					}
					artifact, err := sourceartifact.AdmitDirectory(root)
					if err != nil {
						t.Fatal(err)
					}
					persisted, err := sourceartifact.PersistedFromArtifact(artifact, time.Unix(1, 0))
					if err != nil {
						t.Fatal(err)
					}
					catalog, err := persisted.Decode()
					if err != nil {
						t.Fatal(err)
					}
					retained, err := sourceartifact.DecodeLogical(catalog.LogicalBlob())
					if err != nil {
						t.Fatal(err)
					}
					for _, source := range []*sourceartifact.AdmittedSourceArtifact{artifact, catalog, retained} {
						if _, err := LoadWorkflowContractBundleFromArtifact(repo, source, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{}); err == nil || !errors.Is(err, ErrInvalidField) || !strings.Contains(err.Error(), "[T]") || !strings.Contains(err.Error(), surface+".yaml") {
							t.Fatalf("retained source bypassed shape/style admission: %v", err)
						}
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				persisted, err := sourceartifact.PersistedFromArtifact(bundle.SourceArtifact, time.Unix(1, 0))
				if err != nil {
					t.Fatal(err)
				}
				catalog, err := persisted.Decode()
				if err != nil {
					t.Fatal(err)
				}
				retained, err := sourceartifact.DecodeLogical(catalog.LogicalBlob())
				if err != nil {
					t.Fatal(err)
				}
				for _, artifact := range []*sourceartifact.AdmittedSourceArtifact{catalog, retained} {
					loaded, err := LoadWorkflowContractBundleFromArtifact(repo, artifact, DefaultPlatformSpecFile(repo), WorkflowContractLoadOptions{})
					if err != nil {
						t.Fatal(err)
					}
					if artifact.BundleHash() != bundle.SourceArtifact.BundleHash() || !bytes.Equal(artifact.LogicalBlob(), bundle.SourceArtifact.LogicalBlob()) || !reflect.DeepEqual(loaded.ScopedNodeRecords(), bundle.ScopedNodeRecords()) || !reflect.DeepEqual(loaded.RootTypes, bundle.RootTypes) || !reflect.DeepEqual(loaded.RootEntities, bundle.RootEntities) || !reflect.DeepEqual(loaded.Events, bundle.Events) || !reflect.DeepEqual(loaded.EffectiveProvenance().Entries(), bundle.EffectiveProvenance().Entries()) {
						t.Fatal("retained source erased type/style/presence or provenance")
					}
				}
			})
		}
	}
}

func TestListSpellingRetirementNodeState(t *testing.T) {
	for _, source := range []string{
		"items: [ProbeItem]\n",
		"- name: items\n  type: '[ProbeItem]'\n",
	} {
		snapshot, err := yamlsource.Load([]byte(source))
		if err != nil {
			t.Fatal(err)
		}
		fields, err := projectNodeStateFieldsValue(snapshot.Document("nodes.yaml").Root())
		if err != nil || !reflect.DeepEqual(fields, []NodeStateField{{Name: "items", Type: "[ProbeItem]"}}) {
			t.Fatalf("FIELDS-container versus TYPE-sequence identity: %#v / %v", fields, err)
		}
	}
	for _, source := range []string{"- {name: items, type: list, of: null}\n", "- {name: items, type: list<ProbeItem>}\n"} {
		snapshot, err := yamlsource.Load([]byte(source))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := projectNodeStateFieldsValue(snapshot.Document("nodes.yaml").Root()); err == nil || !errors.Is(err, ErrInvalidField) || !strings.Contains(err.Error(), "[T]") {
			t.Fatalf("named FIELDS-container bypassed retirement: %v", err)
		}
	}
	for _, test := range []struct{ source, want string }{
		{"[text]", "[text]"},
		{"\n  - numeric(5,2)", "[numeric(5,2)]"},
		{"{type: '[numeric(5,2)]'}", "[numeric(5,2)]"},
		{"numeric(5,2)", "numeric(5,2)"},
		{"jsonb", "jsonb"},
	} {
		typeRef, _, initial, err := listRetirementDecode(t, "nodes", listRetirementField(t, test.source))
		if err != nil || typeRef != test.want || initial != nil {
			t.Fatalf("G-owned primitive meaning changed: %s -> %s initial=%#v err=%v", test.source, typeRef, initial, err)
		}
	}
	for _, source := range []string{"'[text]'", "['text']", "[numeric(5,2)]", "{type: 'text[]'}", "{type: 'numeric(5,2)[]'}", "{type: '[]ProbeItem'}"} {
		if _, _, _, err := listRetirementDecode(t, "nodes", listRetirementField(t, source)); err == nil || !errors.Is(err, ErrInvalidField) {
			t.Fatalf("authored declaration bypassed shape/G spelling owner: %s / %v", source, err)
		}
	}
}

func TestListSpellingRetirementAliasMergeDiagnostics(t *testing.T) {
	for _, surface := range []string{"types", "entities", "events", "nodes"} {
		for _, source := range []string{
			"definition: &value 'list<ProbeItem>'\nitems: *value\n",
			"definition: &value ['ProbeItem']\nitems: *value\n",
			"definition: &value {type: text, of: null}\nitems:\n  <<: *value\n",
		} {
			t.Run(surface+"/"+source, func(t *testing.T) {
				snapshot, err := yamlsource.Load([]byte(source))
				if err != nil {
					t.Fatal(err)
				}
				fields, err := snapshot.Document("alias-proof.yaml").Root().Mapping()
				if err != nil {
					t.Fatal(err)
				}
				_, _, _, err = listRetirementDecode(t, surface, fields[1].Value)
				if err == nil || !errors.Is(err, ErrInvalidField) || !strings.Contains(err.Error(), "[T]") || !strings.Contains(err.Error(), "items") || !strings.Contains(err.Error(), "introduced at alias-proof.yaml:") || !strings.Contains(err.Error(), "resolved at alias-proof.yaml:1:") {
					t.Fatalf("alias/merge lost source diagnostic: %v", err)
				}
			})
		}
	}
}

func TestListSpellingRetirementNestedReferences(t *testing.T) {
	for _, source := range []string{"list<text>", "[list<text>]", "map[text]list<text>", "map[text][list<text>]"} {
		if _, err := (CatalogTypeReference{Type: source}).Resolve(); err == nil || !errors.Is(err, ErrInvalidField) || !strings.Contains(err.Error(), "[T]") {
			t.Fatalf("programmatic reference retained angle authority: %q / %v", source, err)
		}
	}
	for _, source := range []string{"[text]", "[[text]]", "map[text][text]", "map[text][map[text]text]"} {
		if _, err := (CatalogTypeReference{Type: source}).Resolve(); err != nil {
			t.Fatalf("canonical nested reference rejected: %q / %v", source, err)
		}
	}
}
