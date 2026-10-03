package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodeFanOutValuePreservesMetadataAndRejectsRetiredAddress(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("fan_out: {items_from: payload.items, as: component, max_items: 3, emit: {event: child.created, fields: {id: '${component.id}'}}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	field, err := snapshot.Document("nodes.yaml").Root().Lookup("fan_out")
	if err != nil {
		t.Fatal(err)
	}
	out, err := projectNodeFanOutValue(field.Value)
	if err != nil {
		t.Fatal(err)
	}
	if out.ItemsFrom != "payload.items" || out.As != "component" || !out.MaxItemsSet || out.MaxItems != 3 || !out.Emit.Fields["id"].HasCELValue() {
		t.Fatalf("wrong fan-out: %#v", out)
	}
	retired, err := yamlsource.Load([]byte("fan_out: {items_from: payload.items, as: component, target: null}\n"))
	if err != nil {
		t.Fatal(err)
	}
	field, _ = retired.Document("nodes.yaml").Root().Lookup("fan_out")
	_, err = projectNodeFanOutValue(field.Value)
	if err == nil || !strings.Contains(err.Error(), "is not supported") || !strings.Contains(err.Error(), "nodes.yaml:") {
		t.Fatalf("expected source-located address retirement, got %v", err)
	}
}

func TestProjectNodeAccumulateValueDoesNotAssignPinDefaults(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("accumulate: {into: arrivals}\n"))
	if err != nil {
		t.Fatal(err)
	}
	field, _ := snapshot.Document("nodes.yaml").Root().Lookup("accumulate")
	out, err := projectNodeAccumulateValue(field.Value)
	if err != nil {
		t.Fatal(err)
	}
	if out.Into != "arrivals" || out.Key != "" {
		t.Fatalf("node projection invented a business key: %#v", out)
	}
}

func TestProjectNodeAccumulateValueAdmitsOnlyAuthoredKey(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("accumulate: {into: arrivals, key: payload.record.id}\n"))
	if err != nil {
		t.Fatal(err)
	}
	field, _ := snapshot.Document("nodes.yaml").Root().Lookup("accumulate")
	out, err := projectNodeAccumulateValue(field.Value)
	if err != nil || out.Key != "payload.record.id" || len(out.KeyPath.Segments) != 2 {
		t.Fatalf("authored key = %#v, %v", out, err)
	}
	for _, retired := range []string{"window: payload.period", "window: null", "dedup_by: payload.id", "dedup_by: []", "key: null", "key: ''", "key: 12"} {
		snapshot, err := yamlsource.Load([]byte("accumulate: {into: arrivals, " + retired + "}\n"))
		if err != nil {
			t.Fatal(err)
		}
		field, _ := snapshot.Document("nodes.yaml").Root().Lookup("accumulate")
		if _, err := projectNodeAccumulateValue(field.Value); err == nil {
			t.Fatalf("retired/invalid field admitted: %s", retired)
		}
	}
}
