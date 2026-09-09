package providerconnectors

import (
	"reflect"
	"sync"
	"testing"
)

func TestEmbeddedRegistryFixturesHaveIndependentMutableState(t *testing.T) {
	first := testPackRegistry(t)
	want, ok := first.Lookup("notion", "notion.append_block_children")
	if !ok {
		t.Fatal("Notion tool missing")
	}
	for _, tools := range first.byProvider {
		for id, pack := range tools {
			clear(pack.Manifest.Tools)
			clear(pack.ManifestBody)
			delete(tools, id)
		}
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, ok := testPackRegistry(t).Lookup("notion", "notion.append_block_children")
			if !ok || !reflect.DeepEqual(got, want) {
				t.Error("one fixture mutated the shared embedded registry template")
			}
		}()
	}
	wg.Wait()
}

func BenchmarkEmbeddedRegistryFixture(b *testing.B) {
	testPackRegistry(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		testPackRegistry(b)
	}
}
