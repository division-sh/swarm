package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodeComputeAndLoopValue(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte(`worker:
  event_handlers:
    scores.ready:
      compute:
        operation: weighted_average
        keys: {dimension_key: dimension, score_keys: [value]}
        tiers:
          - {dimensions: [a, b], weight: 0.6}
          - {dimensions: [c], weight: 0.4}
        store_as: entity.composite
      loop: {start: revise, from: payload.revision}
`))
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	handler := nodes["worker"].EventHandlers["scores.ready"]
	if handler.Compute == nil || len(handler.Compute.Tiers) != 2 || handler.Compute.Tiers[0].Weight != 0.6 ||
		handler.Compute.Keys.DimensionKey != "dimension" {
		t.Fatalf("compute projection lost typed data: %#v", handler.Compute)
	}
	if handler.Loop == nil || handler.Loop.Start != "revise" || handler.Loop.From != "payload.revision" {
		t.Fatalf("loop projection lost typed data: %#v", handler.Loop)
	}
}

func TestProjectNodeComputeValueRejectsInertParams(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("worker:\n  event_handlers:\n    scores.ready:\n      compute: {operation: sum, params: {ignored: true}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "nodes.yaml:4:") {
		t.Fatalf("expected source-located compute.params retirement, got %v", err)
	}
}

func TestProjectNodeLoopValueRejectsAmbiguousOperation(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("worker:\n  event_handlers:\n    task.ready:\n      loop: {start: revise, repeat: revise, from: payload.revision}\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err == nil || !strings.Contains(err.Error(), "exactly one") || !strings.Contains(err.Error(), "nodes.yaml:4:") {
		t.Fatalf("expected source-located operation rejection, got %v", err)
	}
}
