package contracts

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/yamlsource"
)

var updateEventAdmission = flag.Bool("update-event-admission", false, "record the unchanged event-admission characterization")

type eventAdmissionObservation struct {
	Entries    map[string]EventCatalogEntry
	Provenance map[string]map[string]EffectiveValueProvenance
	Error      string
	ErrorType  string
}

func TestEventCatalogFactoringCharacterization(t *testing.T) {
	t.Run("document_missing", func(t *testing.T) {
		entries, err := admitEventCatalogDocument(yamlsource.MissingDocument("events.yaml"))
		if entries != nil || err == nil || err.Error() != "events.yaml source is missing" {
			t.Fatalf("missing document: %v %v", entries, err)
		}
	})
	cases := []struct{ name, source string }{
		{"document_null", ""}, {"document_scalar", "text\n"}, {"document_sequence", "[text]\n"}, {"document_empty", "{}\n"},
		{"bare", "test.event:\n"}, {"bare_mapping", "test.event: {}\n"}, {"bare_description", "test.event: {description: note}\n"},
		{"bare_null", "test.event: null\n"}, {"bare_tilde", "test.event: ~\n"}, {"bare_quoted", "test.event: ''\n"},
		{"bare_tag", "test.event: !!null\n"}, {"bare_alias", "first: &nil\ntest.event: *nil\n"},
		{"key_empty", "'':\n"}, {"key_normalized", "' Test.Event ':\n"}, {"key_path", "'/test.event':\n"},
		{"key_pattern", "'test.*':\n'**/test.event':\n"}, {"key_partial_glob", "'test.pre*':\n"},
		{"duplicate_declaration", "test.event:\ntest.event:\n"},
		{"duplicate_merge_declaration", "<<: &base\n  test.event:\ntest.event:\n"},
		{"retired_required", "test.event:\n  id: text\n  required: [id]\n"},
		{"retired_payload", "test.event:\n  payload: {fields: {id: text}}\n"},
		{"business_names", "test.event:\n  payload: text\n  source: text\n  status: text\n  note: text\n"},
		{"duplicate_fields", "test.event:\n  id: text\n  id: uuid\n"},
		{"duplicate_nested", "test.event:\n  id: {type: text, type: uuid}\n"},
		{"scalar_types", "test.event:\n  scalar: text\n  named: Customer\n  array: [text]\n  map: 'map[text]uuid'\n"},
		{"sequence_types", "test.event:\n  array: [text]\n"},
		{"sequence_multiple", "test.event:\n  array: [text, uuid]\n"},
		{"sequence_empty", "test.event:\n  array: []\n"},
		{"sequence_optional", "test.event:\n  array:\n    - text?\n"},
		{"sequence_wrong_kind", "test.event:\n  array: [{}]\n"},
		{"mapping_types", "test.event:\n  scalar: {type: text}\n  array: {type: '[Customer]'}\n"},
		{"mapping_missing_type", "test.event:\n  id: {description: note}\n"},
		{"mapping_missing_of", "test.event:\n  id: {type: list}\n"},
		{"mapping_foreign_of", "test.event:\n  id: {type: text, of: uuid}\n"},
		{"optionality", "test.event:\n  z: uuid\n  a: text?\n  list: {type: '[text]?'}\n  map: 'map[text]uuid?'\n"},
		{"invalid_optionality", "test.event:\n  id: te?xt\n"},
		{"double_optionality", "test.event:\n  id: text??\n"},
		{"spaced_optionality", "test.event:\n  id: 'text ?'\n"},
		{"nested_optionality", "test.event:\n  id: {type: '[text?]'}\n"},
		{"field_unknown", "test.event:\n  id: {type: text, typo: yes}\n"},
		{"field_null", "test.event:\n  id: null\n"}, {"field_empty", "test.event:\n  id: {}\n"},
		{"type_null", "test.event:\n  id: {type: null}\n"},
		{"text_refinements", "test.event:\n  id: {type: text, description: ' note ', pattern: ' ^[a-z]+$ '}\n"},
		{"pattern_bad", "test.event:\n  id: {type: text, pattern: '['}\n"},
		{"length", "test.event:\n  id: {type: text, length: {min: 0, max: 8}}\n"},
		{"length_null", "test.event:\n  id: {type: text, length: {min: null}}\n"},
		{"length_float", "test.event:\n  id: {type: text, length: {min: 1.0}}\n"},
		{"length_overflow", "test.event:\n  id: {type: text, length: {min: 18446744073709551616}}\n"},
		{"range", "test.event:\n  id: {type: number, range: {min: -0.5, max: 0}}\n"},
		{"range_null", "test.event:\n  id: {type: number, range: {max: null}}\n"},
		{"range_nonfinite", "test.event:\n  id: {type: number, range: {min: .inf}}\n"},
		{"equality", "test.event:\n  id: {type: text, equal_to: peer}\n  peer: text\n"},
		{"equality_empty", "test.event:\n  id: {type: text, equal_to: ''}\n"},
		{"citation", "test.event:\n  id: {type: text, citation: {criteria: ' note ', allowed_classes: [soft, hard, soft]}}\n"},
		{"citation_unknown", "test.event:\n  id: {type: text, citation: {unknown: value}}\n"},
		{"citation_wrong_kind", "test.event:\n  id: {type: text, citation: {allowed_classes: scalar}}\n"},
		{"business_key", "test.event:\n  _key: id\n  id: uuid\n  note: text?\n"},
		{"business_key_missing", "test.event:\n  _key: id\n  note: text\n"},
		{"business_key_optional", "test.event:\n  _key: id\n  id: uuid?\n"},
		{"business_key_space", "test.event:\n  _key: ' id '\n  id: uuid\n"},
		{"introductions", "test.event:\n  id: &field\n    type: text?\n    length: {min: 1, max: 8}\n  alias: *field\n  merged:\n    <<: *field\n"},
		{"provenance", "test.event:\n  _key: id\n  id: uuid\n  value:\n    type: text?\n    description: note\n    pattern: ^[a-z]+$\n    length: {min: 1, max: 8}\n    citation: {criteria: source, allowed_classes: [hard]}\n"},
		{"first_error_retirement", "test.event:\n  id: text\n  id: uuid\n  swarm: null\n"},
		{"first_error_field_order", "test.event:\n  z: {type: list}\n  a: text??\n"},
		{"first_error_refinement_order", "test.event:\n  id: {type: text, citation: {unknown: bad}, range: {min: null}, length: {min: null}, pattern: '['}\n"},
		{"first_error_business_key", "test.event:\n  _key: absent\n  id: text??\n"},
	}
	for _, reserved := range []string{
		"swarm", "emitter", "emitter_type", "producer", "_producer", "alternate_emitters", "consumer", "_consumer", "consumer_type", "_consumer_type", "_source", "_status", "_note", "intercepted", "passthrough", "runtime_handling", "owning_node", "delivery_channel", "author_summary_field",
	} {
		for _, spelling := range []string{"null", "{}", "''", "{nested: value}"} {
			cases = append(cases, struct{ name, source string }{"reserved_fields/" + reserved + "/" + spelling, "test.event:\n  " + reserved + ": " + spelling + "\n"})
		}
	}
	goldenPath := filepath.Join("testdata", "event_admission_characterization.json")
	want := map[string]eventAdmissionObservation{}
	if !*updateEventAdmission {
		body, err := os.ReadFile(goldenPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(body, &want); err != nil {
			t.Fatal(err)
		}
	}
	got := map[string]eventAdmissionObservation{}
	valid := toolSchemaStringSet([]string{"document_empty", "bare", "bare_description", "retired_required", "key_pattern", "business_names", "scalar_types", "sequence_types", "mapping_types", "optionality", "text_refinements", "length", "range", "equality", "citation", "business_key", "introductions", "provenance"})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot, err := yamlsource.Load([]byte(tc.source))
			var entries map[string]EventCatalogEntry
			if err == nil {
				entries, err = admitEventCatalogDocument(snapshot.Document("events.yaml"))
			}
			_, accepted := valid[tc.name]
			if accepted != (err == nil) {
				t.Fatalf("intended admission/refusal changed: accepted=%t error=%v", accepted, err)
			}
			observation := eventAdmissionObservation{Entries: entries}
			if err != nil {
				observation.Error = err.Error()
				observation.ErrorType = reflect.TypeOf(err).String()
			}
			if entries != nil {
				observation.Provenance = map[string]map[string]EffectiveValueProvenance{}
				for name, entry := range entries {
					observation.Provenance[name] = entry.admissionProvenance
				}
			}
			got[tc.name] = observation
			actual, err := json.Marshal(observation)
			if err != nil {
				t.Fatal(err)
			}
			expected, err := json.Marshal(want[tc.name])
			if err != nil {
				t.Fatal(err)
			}
			if !*updateEventAdmission && !bytes.Equal(actual, expected) {
				t.Fatalf("exact admission changed:\n got %s\nwant %s", actual, expected)
			}
		})
	}
	if *updateEventAdmission {
		body, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, append(body, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	} else if len(want) != len(cases) {
		t.Fatalf("characterization population changed: %d/%d", len(cases), len(want))
	}
}

func TestEventCatalogFactoringConsumers(t *testing.T) {
	t.Run("artifact_and_disk", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "events.yaml")
		const body = "test.event:\n  id: uuid\n  note: text?\n"
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		artifact, err := sourceartifact.AdmitDirectory(root)
		if err != nil {
			t.Fatal(err)
		}
		disk, err := loadOptionalEventCatalog(path)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := loadOptionalEventCatalogFromSource(artifact, "events.yaml")
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(disk["test.event"].Payload, actual["test.event"].Payload) || !reflect.DeepEqual(actual["test.event"].Payload.Required, []string{"id"}) {
			t.Fatal("disk/artifact admission changed")
		}
		for _, label := range []string{"", "missing.yaml"} {
			_, err := loadOptionalEventCatalogFromSource(artifact, label)
			if (label != "") != (err != nil) {
				t.Fatalf("optional artifact label %q: %v", label, err)
			}
		}
		absent, err := loadOptionalEventCatalog("")
		if err != nil || absent == nil || len(absent) != 0 {
			t.Fatalf("missing disk declaration: %v %v", absent, err)
		}
		if _, err := loadOptionalEventCatalog(filepath.Join(root, "absent.yaml")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("explicit missing disk path did not refuse: %v", err)
		}
		if err := os.WriteFile(filepath.Join(root, "empty.yaml"), nil, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadOptionalEventCatalog(filepath.Join(root, "empty.yaml")); err == nil {
			t.Fatal("present empty declaration became absent")
		}
		persisted, err := sourceartifact.PersistedFromArtifact(artifact, time.Unix(1, 0))
		if err != nil {
			t.Fatal(err)
		}
		rebuilt, err := persisted.Decode()
		if err != nil {
			t.Fatal(err)
		}
		actual["test.event"].Payload.Properties["id"] = EventFieldSpec{Type: "text"}
		fresh, err := loadOptionalEventCatalogFromSource(rebuilt, "events.yaml")
		if err != nil || fresh["test.event"].Payload.Properties["id"].Type != "uuid" || !bytes.Equal(artifact.LogicalBlob(), rebuilt.LogicalBlob()) {
			t.Fatalf("independent reconstruction/source nonmutation: %v", err)
		}
	})
	for _, tc := range []struct {
		name  string
		proof func(*testing.T)
	}{
		{"platform_marker", TestPlatformEventCatalogUsesRequiredByDefaultTypedOmission},
		{"platform_retired_required", TestPlatformEventCatalogRejectsRetiredRequiredList},
		{"type_marker", TestTypeCatalogDocumentDecode_PreservesNamedFieldOptionalityAndMergedMappings},
		{"entity_marker", TestEntityContractsDocumentDecode_PreservesOptionalTopLevelField},
		{"type_refusal", TestTypeCatalogDocumentDecode_RejectsInvalidOptionalMarkers},
		{"type_lowering", TestEventSchemaRegistryFromBundle_PreservesWave1TypeMeaning},
		{"business_key_lowering", TestEventCatalogBusinessKeyRejectsDynamicSemanticsAtBundleAdmission},
		{"provenance_ledger", TestNodeAdmissionProvenanceComposesWithEvents},
		{"pack_provenance", TestPackPlatformProvenanceComposesWithoutOverwrite},
		{"connected_rename", TestEffectiveEventResolutionPreservesConnectedRenameOwnership},
		{"shared_refinements", TestSharedRefinementBoundPresenceMatrix},
		{"shared_vocabulary", TestRemovedClosedFieldsUseCurrentVocabularyAcrossPresenceAndMerges},
	} {
		t.Run(tc.name, tc.proof)
	}
	for _, text := range []string{"text?", "[uuid]?", "map[text]uuid?"} {
		kind, optional, err := admitEventFieldTypeMarker(text, "shared")
		if err != nil || !optional || strings.Contains(kind, "?") {
			t.Fatalf("shared marker %q: %q %t %v", text, kind, optional, err)
		}
	}
}
