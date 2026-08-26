package yamlsource

import (
	"bytes"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestStoreCoalescesConcurrentContent(t *testing.T) {
	store := NewStore(Limits{MaxEntries: 8, MaxSourceBytes: 1024})
	raw := []byte("root:\n  value: one\n")
	const workers = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			snapshot, err := store.Load(raw)
			if err != nil {
				t.Errorf("Load: %v", err)
				return
			}
			var out map[string]any
			if err := snapshot.Decode(&out); err != nil {
				t.Errorf("Decode: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := store.Stats().ParseCount; got != 1 {
		t.Fatalf("parse count = %d, want 1", got)
	}
}

func TestStoreKeysByContentAndReturnsFreshValues(t *testing.T) {
	store := NewStore(Limits{MaxEntries: 8, MaxSourceBytes: 1024})
	first, err := store.Load([]byte("items: [one]\n"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Load([]byte("items: [one]\n"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest() != second.Digest() || store.Stats().ParseCount != 1 {
		t.Fatalf("same content did not reuse one parse: %+v", store.Stats())
	}
	var left, right map[string][]string
	if err := first.Decode(&left); err != nil {
		t.Fatal(err)
	}
	left["items"][0] = "mutated"
	if err := second.Decode(&right); err != nil {
		t.Fatal(err)
	}
	if right["items"][0] != "one" {
		t.Fatalf("later decode contaminated: %#v", right)
	}
	changed, err := store.Load([]byte("items: [two]\n"))
	if err != nil {
		t.Fatal(err)
	}
	if changed.Digest() == first.Digest() || store.Stats().ParseCount != 2 {
		t.Fatalf("changed content did not parse independently: %+v", store.Stats())
	}
}

func TestStoreRetainsErrorsAndRejectsMultipleDocuments(t *testing.T) {
	store := NewStore(Limits{MaxEntries: 8, MaxSourceBytes: 1024})
	bad := []byte("a: [\n")
	firstErr := loadError(store, bad)
	secondErr := loadError(store, append([]byte(nil), bad...))
	if firstErr != secondErr {
		t.Fatalf("retained error identity changed: %p != %p", firstErr, secondErr)
	}
	if store.Stats().ParseCount != 1 {
		t.Fatalf("parse count = %d, want 1", store.Stats().ParseCount)
	}
	if _, err := store.Load([]byte("a: one\n---\nb: two\n")); err == nil || !bytes.Contains([]byte(err.Error()), []byte("multiple documents")) {
		t.Fatalf("multi-document error = %v", err)
	}
}

func TestStoreBoundsEvictionAndOversizedEntries(t *testing.T) {
	store := NewStore(Limits{MaxEntries: 2, MaxSourceBytes: 32})
	a := []byte("a: one\n")
	b := []byte("b: two\n")
	c := []byte("c: three\n")
	for _, raw := range [][]byte{a, b} {
		if _, err := store.Load(raw); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Load(a); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(c); err != nil {
		t.Fatal(err)
	}
	if got := store.Stats().ParseCount; got != 3 {
		t.Fatalf("parse count after LRU touch = %d, want 3", got)
	}
	if _, err := store.Load(b); err != nil {
		t.Fatal(err)
	}
	if got := store.Stats().ParseCount; got != 4 {
		t.Fatalf("least-recently-used entry was retained: parse count = %d, want 4", got)
	}
	stats := store.Stats()
	if stats.Entries > 2 || stats.SourceBytes > 32 {
		t.Fatalf("cache bounds exceeded: %+v", stats)
	}
	oversized := bytes.Repeat([]byte("x"), 64)
	if _, err := store.Load(append([]byte("value: "), oversized...)); err != nil {
		t.Fatal(err)
	}
	if after := store.Stats(); after.Entries != stats.Entries {
		t.Fatalf("oversized entry retained: before=%+v after=%+v", stats, after)
	}
}

func TestStoreCoalescesOversizedInflightEntryWithoutRetainingIt(t *testing.T) {
	store := NewStore(Limits{MaxEntries: 1, MaxSourceBytes: 8})
	entered := make(chan struct{})
	release := make(chan struct{})
	parseSource := store.parse
	store.parse = func(raw []byte) (yaml.Node, error) {
		close(entered)
		<-release
		return parseSource(raw)
	}
	raw := []byte("value: oversized\n")
	firstDone := make(chan error, 1)
	go func() {
		_, err := store.Load(raw)
		firstDone <- err
	}()
	<-entered

	secondDone := make(chan error, 1)
	go func() {
		_, err := store.Load(raw)
		secondDone <- err
	}()
	deadline := time.Now().Add(time.Second)
	for store.Stats().Coalesced == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if store.Stats().Coalesced != 1 {
		close(release)
		t.Fatal("second load did not join the in-flight parse")
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if stats := store.Stats(); stats.ParseCount != 1 || stats.Entries != 0 || stats.Inflight != 0 {
		t.Fatalf("oversized in-flight stats = %+v", stats)
	}
}

func TestStoreRejectsMissingDocument(t *testing.T) {
	store := NewStore(Limits{MaxEntries: 8, MaxSourceBytes: 1024})
	if _, err := store.Load([]byte("# comments only\n")); err == nil || !bytes.Contains([]byte(err.Error()), []byte("no document")) {
		t.Fatalf("missing-document error = %v", err)
	}
}

func TestSnapshotReadOnlyTraversalAndCopiedBytes(t *testing.T) {
	store := NewStore(Limits{MaxEntries: 8, MaxSourceBytes: 1024})
	input := []byte("root:\n  value: &v tagged\n  alias: *v\n")
	snapshot, err := store.Load(input)
	if err != nil {
		t.Fatal(err)
	}
	input[0] = 'X'
	if snapshot.Bytes()[0] != 'r' {
		t.Fatal("snapshot retained caller-owned input bytes")
	}
	value, ok := snapshot.LookupMapPath("root", "value")
	if !ok || value.Kind() != ScalarNode || value.Value() != "tagged" {
		t.Fatalf("lookup = %#v, %v", value, ok)
	}
	alias, ok := snapshot.LookupMapPath("root", "alias")
	if !ok || alias.Kind() != AliasNode {
		t.Fatalf("alias lookup = %#v, %v", alias, ok)
	}
	target, ok := alias.Alias()
	if !ok || target != value {
		t.Fatalf("alias target = %#v, %v; want %#v", target, ok, value)
	}
	raw := snapshot.Bytes()
	raw[0] = 'X'
	if snapshot.Bytes()[0] != 'r' {
		t.Fatal("snapshot bytes were not copied")
	}
	copyRoot := snapshot.NodeCopy()
	copyRoot.Content[0].Content[1].Content[1].Value = "mutated"
	copyAlias := copyRoot.Content[0].Content[1].Content[3]
	if copyAlias.Alias != copyRoot.Content[0].Content[1].Content[1] {
		t.Fatal("node copy did not remap alias within copied tree")
	}
	if got, _ := snapshot.LookupMapPath("root", "value"); got.Value() != "tagged" {
		t.Fatalf("node copy mutation contaminated snapshot: %q", got.Value())
	}
}

type mutatingNodeTarget struct{}

func (*mutatingNodeTarget) UnmarshalYAML(node *yaml.Node) error {
	node.Value = "corrupted"
	node.Tag = "!!str"
	node.Content = nil
	node.Alias = nil
	return nil
}

func TestSnapshotDecodeDoesNotExposeRetainedNodeToUnmarshalers(t *testing.T) {
	store := NewStore(Limits{MaxEntries: 8, MaxSourceBytes: 1024})
	snapshot, err := store.Load([]byte("root:\n  value: pristine\n"))
	if err != nil {
		t.Fatal(err)
	}

	const workers = 24
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var target mutatingNodeTarget
			if err := snapshot.Decode(&target); err != nil {
				t.Errorf("Decode mutating target: %v", err)
			}
			if value, ok := snapshot.LookupMapPath("root", "value"); !ok || value.Value() != "pristine" {
				t.Errorf("retained node changed: value=%q found=%v", value.Value(), ok)
			}
		}()
	}
	close(start)
	wg.Wait()

	var decoded map[string]map[string]string
	if err := snapshot.Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["root"]["value"] != "pristine" {
		t.Fatalf("fresh decode = %#v", decoded)
	}
}

func loadError(store *Store, raw []byte) error {
	_, err := store.Load(raw)
	return err
}

func TestDocumentPreservesClosedPresenceVocabulary(t *testing.T) {
	snapshot, err := Load([]byte(`
null_value: null
empty_scalar: ""
scalar: value
empty_sequence: []
sequence: [value]
empty_mapping: {}
mapping: {field: value}
`))
	if err != nil {
		t.Fatal(err)
	}
	root := snapshot.Document("contract.yaml").Root()
	want := map[string]Presence{
		"missing":        PresenceMissing,
		"null_value":     PresenceNull,
		"empty_scalar":   PresenceEmptyScalar,
		"scalar":         PresenceScalar,
		"empty_sequence": PresenceEmptySequence,
		"sequence":       PresenceSequence,
		"empty_mapping":  PresenceEmptyMapping,
		"mapping":        PresenceMapping,
	}
	for field, presence := range want {
		lookup, err := root.Lookup(field)
		if err != nil {
			t.Fatalf("lookup %s: %v", field, err)
		}
		if lookup.Presence != presence {
			t.Errorf("%s presence = %s, want %s", field, lookup.Presence, presence)
		}
		if lookup.SemanticPath != `$["`+field+`"]` {
			t.Errorf("%s semantic path = %q", field, lookup.SemanticPath)
		}
	}
	if got := MissingDocument("optional.yaml").Presence(); got != PresenceMissing {
		t.Fatalf("missing document presence = %s", got)
	}
}

func TestDocumentMappingReportsAllDirectAndMergedOccurrences(t *testing.T) {
	snapshot, err := Load([]byte(`
defaults: &defaults
  retired: inherited
  stable: inherited
entry:
  <<: *defaults
  stable: direct
  retired: direct
`))
	if err != nil {
		t.Fatal(err)
	}
	entry, err := snapshot.Document("events.yaml").Root().Lookup("entry")
	if err != nil {
		t.Fatal(err)
	}
	retired, err := entry.Value.Lookup("retired")
	if err != nil {
		t.Fatal(err)
	}
	if len(retired.Occurrences) != 2 {
		t.Fatalf("retired occurrences = %#v", retired.Occurrences)
	}
	if !retired.Occurrences[0].FromMerge || retired.Occurrences[1].FromMerge {
		t.Fatalf("retired merge ownership = %#v", retired.Occurrences)
	}
	if retired.Occurrences[0].KeyLocation.Line != 3 || retired.Occurrences[1].KeyLocation.Line != 8 || retired.Occurrences[0].MergeLocation.Line != 6 {
		t.Fatalf("retired locations = %#v", retired.Occurrences)
	}
	stable, err := entry.Value.Lookup("stable")
	if err != nil {
		t.Fatal(err)
	}
	if got := []int{stable.Occurrences[0].KeyLocation.Line, stable.Occurrences[1].KeyLocation.Line}; !reflect.DeepEqual(got, []int{4, 7}) {
		t.Fatalf("stable occurrence lines = %v", got)
	}
}

func TestDocumentPreservesAliasUseAndResolvedTargetLocations(t *testing.T) {
	snapshot, err := Load([]byte(`
scalar_anchor: &scalar value
sequence_anchor: &sequence [one, two]
mapping_anchor: &mapping {field: value}
scalar_alias: *scalar
sequence_alias: *sequence
mapping_alias: *mapping
merge_target: &merge_target
  merged: *scalar
merge_alias:
  <<: *merge_target
`))
	if err != nil {
		t.Fatal(err)
	}
	root := snapshot.Document("contract.yaml").Root()
	for _, tc := range []struct {
		name         string
		aliasLine    int
		resolvedLine int
		presence     Presence
	}{
		{name: "scalar_alias", aliasLine: 5, resolvedLine: 2, presence: PresenceScalar},
		{name: "sequence_alias", aliasLine: 6, resolvedLine: 3, presence: PresenceSequence},
		{name: "mapping_alias", aliasLine: 7, resolvedLine: 4, presence: PresenceMapping},
	} {
		lookup, lookupErr := root.Lookup(tc.name)
		if lookupErr != nil {
			t.Fatal(lookupErr)
		}
		if lookup.Presence != tc.presence || lookup.Value.Location().Line != tc.aliasLine || lookup.Value.ResolvedLocation().Line != tc.resolvedLine {
			t.Fatalf("%s = presence:%s location:%s resolved:%s", tc.name, lookup.Presence, lookup.Value.Location(), lookup.Value.ResolvedLocation())
		}
		if len(lookup.Occurrences) != 1 || lookup.Occurrences[0].ValueLocation.Line != tc.aliasLine || lookup.Occurrences[0].ResolvedValueLocation.Line != tc.resolvedLine {
			t.Fatalf("%s occurrences = %#v", tc.name, lookup.Occurrences)
		}
	}

	scalarAlias, err := root.Lookup("scalar_alias")
	if err != nil {
		t.Fatal(err)
	}
	scalar, err := scalarAlias.Value.Scalar()
	if err != nil {
		t.Fatal(err)
	}
	if scalar.Location.Line != 5 || scalar.ResolvedLocation.Line != 2 {
		t.Fatalf("scalar alias locations = %s / %s", scalar.Location, scalar.ResolvedLocation)
	}

	mergeAlias, err := root.Lookup("merge_alias")
	if err != nil {
		t.Fatal(err)
	}
	merged, err := mergeAlias.Value.Lookup("merged")
	if err != nil {
		t.Fatal(err)
	}
	if len(merged.Occurrences) != 1 {
		t.Fatalf("merged occurrences = %#v", merged.Occurrences)
	}
	occurrence := merged.Occurrences[0]
	if !occurrence.FromMerge || occurrence.MergeLocation.Line != 11 || occurrence.MergeValueLocation.Line != 11 || occurrence.MergeResolvedValueLocation.Line != 8 {
		t.Fatalf("merge alias locations = %#v", occurrence)
	}
	if occurrence.ValueLocation.Line != 9 || occurrence.ResolvedValueLocation.Line != 2 {
		t.Fatalf("nested alias locations = %#v", occurrence)
	}
}

func TestDocumentValidateUniqueMappingsRejectsDirectAndMergedDuplicates(t *testing.T) {
	for _, tc := range []struct {
		source string
		lines  []string
	}{
		{source: "entry:\n  value: one\n  value: two\n", lines: []string{"contract.yaml:2", "contract.yaml:3"}},
		{source: "entry:\n  <<: &base\n    value: one\n  value: two\n", lines: []string{"contract.yaml:3", "contract.yaml:4"}},
	} {
		snapshot, err := Load([]byte(tc.source))
		if err != nil {
			t.Fatal(err)
		}
		err = snapshot.Document("contract.yaml").Root().ValidateUniqueMappings()
		if err == nil || !strings.Contains(err.Error(), "duplicate effective YAML key \"value\"") || !strings.Contains(err.Error(), tc.lines[0]) || !strings.Contains(err.Error(), tc.lines[1]) {
			t.Fatalf("duplicate error = %v", err)
		}
	}
}

func TestDocumentProjectReturnsCallerOwnedTypedValue(t *testing.T) {
	snapshot, err := Load([]byte("entry: {values: [one, two]}\n"))
	if err != nil {
		t.Fatal(err)
	}
	entry, err := snapshot.Document("contract.yaml").Root().Lookup("entry")
	if err != nil {
		t.Fatal(err)
	}
	var first struct {
		Values []string `yaml:"values"`
	}
	if err := entry.Value.Project(&first); err != nil {
		t.Fatal(err)
	}
	first.Values[0] = "changed"
	var second struct {
		Values []string `yaml:"values"`
	}
	if err := entry.Value.Project(&second); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(second.Values, []string{"one", "two"}) {
		t.Fatalf("second projection = %v", second.Values)
	}
}
