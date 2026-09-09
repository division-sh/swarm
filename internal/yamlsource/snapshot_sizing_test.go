package yamlsource

import (
	"strings"
	"testing"
)

func TestSnapshotSizingPreservesCyclicAliasIsolation(t *testing.T) {
	store := NewStore(Limits{MaxEntries: 1, MaxSourceBytes: 1024})
	snapshot, err := store.Load([]byte("root: &root [*root]\n"))
	if err != nil {
		t.Fatal(err)
	}
	nodes, edges := nodeTreeSize(&snapshot.entry.root)
	if snapshot.entry.nodes != nodes || snapshot.entry.edges != edges {
		t.Fatal("snapshot graph sizing differs from admitted graph")
	}
	// Eviction must not invalidate the immutable graph or its sizing metadata.
	if _, err := store.Load([]byte("other: value\n")); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		copy := snapshot.NodeCopy()
		sequence := copy.Content[0].Content[1]
		if sequence.Content[0].Alias != sequence {
			t.Fatal("cyclic alias was not remapped into the copied graph")
		}
		sequence.Content = nil
		sequence.Anchor = "changed"
	}
}

func BenchmarkSnapshotDecode(b *testing.B) {
	snapshot, err := Load([]byte("items:\n" + strings.Repeat("  - {name: item, values: [a, b, c]}\n", 128)))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var out map[string]any
		if err := snapshot.Decode(&out); err != nil {
			b.Fatal(err)
		}
	}
}
