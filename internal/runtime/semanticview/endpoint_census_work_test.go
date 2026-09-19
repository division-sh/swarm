package semanticview

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

type endpointHandlerCountingSource struct {
	Source
	reads    int
	withdraw bool
}

func (s *endpointHandlerCountingSource) ExecutableNodeEventHandlers(node runtimeidentity.ExecutableNode) map[string]runtimecontracts.SystemNodeEventHandler {
	s.reads++
	if s.withdraw {
		return nil
	}
	return s.Source.ExecutableNodeEventHandlers(node)
}

func TestEndpointCensusReadsExactHandlerMapsOncePerNode(t *testing.T) {
	base := endpointWorkScatterSource(t)
	source := &endpointHandlerCountingSource{Source: base}
	got := BuildAuthoredEventEndpointCensus(source)
	want := BuildAuthoredEventEndpointCensus(base)
	if !reflect.DeepEqual(got.Consumers(), want.Consumers()) || !reflect.DeepEqual(got.Producers(), want.Producers()) {
		t.Fatal("handler-map hoist changed endpoint evidence")
	}
	nodes := len(base.ExecutableNodeRecords())
	if nodes == 0 || source.reads != nodes {
		t.Fatalf("handler maps read %d times for %d nodes, want one per node", source.reads, nodes)
	}
	if len(got.Consumers()) <= nodes {
		t.Fatal("fixture does not exercise multiple handlers per node")
	}
	source.withdraw = true
	source.reads = 0
	next := BuildAuthoredEventEndpointCensus(source)
	for _, consumer := range next.Consumers() {
		if consumer.Kind == EventEndpointNodeHandler {
			t.Fatalf("new census reused withdrawn handler map: %+v", consumer)
		}
	}
}

func TestEndpointCensusHandlerMapReusePreservesLegacyClassification(t *testing.T) {
	for _, authored := range []string{"task.done", "task.*", "child/task.*", "sibling/task.*", "https://wrong.example/task.done", "missing", ""} {
		source := localWildcardEndpointCensusSource(authored)
		for _, record := range source.ExecutableNodeRecords() {
			node, err := record.Identity()
			if err != nil {
				t.Fatal(err)
			}
			handlers := source.ExecutableNodeEventHandlers(node)
			for _, event := range []string{authored, "task.done", "child/task.done", "sibling/task.done", "task.*", " missing ", ""} {
				want, matched := legacyEndpointHandlerProof(source, node, event)
				got, ok := resolveNodeHandlerProof(source, node, event, handlers)
				if got != want || ok != matched {
					t.Fatalf("authored=%q event=%q: got=%q/%t want=%q/%t", authored, event, got, ok, want, matched)
				}
			}
		}
	}
	if got, ok := resolveNodeHandlerProof(nil, runtimeidentity.ExecutableNode{}, "task.done", nil); got != "" || ok {
		t.Fatal("nil source/invalid node admitted")
	}
}

// Frozen pre-hoist behavior, including exact-name preference and canonical
// fallback. Kept test-only to detect classification changes, not as a runtime path.
func legacyEndpointHandlerProof(source Source, node runtimeidentity.ExecutableNode, eventType string) (string, bool) {
	if source == nil || !node.Valid() {
		return "", false
	}
	authored := eventidentity.Normalize(eventType)
	if admission := ClassifyExecutableNodeSubscription(source, node, authored); admission.Admitted() {
		for key := range source.ExecutableNodeEventHandlers(node) {
			if eventidentity.Normalize(key) == authored {
				return authored, true
			}
		}
	}
	resolution := ResolveExecutableNodeSubscriptionHandler(source, node, eventType)
	if resolution.Matched {
		return strings.TrimSpace(resolution.HandlerEventKey), true
	}
	return "", false
}

func endpointWorkScatterSource(t testing.TB) Source {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(repo, "internal/runtime/cataloge2e/testdata/scatter-gather-safety"), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return Wrap(bundle)
}

func BenchmarkEndpointCensusNodeHandlerProofs(b *testing.B) {
	source := endpointWorkScatterSource(b)
	records := source.ExecutableNodeRecords()
	for _, shared := range []bool{false, true} {
		name := "legacy_per_consumer"
		if shared {
			name = "operation_node_map"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for _, record := range records {
					node, _ := record.Identity()
					handlers := source.ExecutableNodeEventHandlers(node)
					for _, event := range sortedMapKeys(handlers) {
						var matched bool
						if shared {
							_, matched = resolveNodeHandlerProof(source, node, event, handlers)
						} else {
							_, matched = legacyEndpointHandlerProof(source, node, event)
						}
						if !matched {
							b.Fatalf("scatter handler %s did not match", event)
						}
					}
				}
			}
		})
	}
}
