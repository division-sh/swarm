package contracts

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodeFanOutValuePreservesMetadataAndRejectsRetiredAddress(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("fan_out: {items_from: payload.items, as: component, max_items: 3, emit: {event: child.created, fields: {id: component.id}}}\n"))
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

func TestProjectNodeAccumulateValueRequiresKeyAndTeachesAppend(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("accumulate: {target: arrivals}\n"))
	if err != nil {
		t.Fatal(err)
	}
	field, _ := snapshot.Document("nodes.yaml").Root().Lookup("accumulate")
	_, err = projectNodeAccumulateValue(field.Value)
	if err == nil || !strings.Contains(err.Error(), "requires key") || !strings.Contains(err.Error(), "op: append") || !strings.Contains(err.Error(), "nodes.yaml:") {
		t.Fatalf("missing key must refuse with append teaching and source: %v", err)
	}
}

func TestProjectNodeAccumulateValueAdmitsOnlyAuthoredKey(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("accumulate: {target: arrivals, key: id}\n"))
	if err != nil {
		t.Fatal(err)
	}
	field, _ := snapshot.Document("nodes.yaml").Root().Lookup("accumulate")
	out, err := projectNodeAccumulateValue(field.Value)
	if err != nil || out.Target != "arrivals" || out.Key != "id" {
		t.Fatalf("authored key = %#v, %v", out, err)
	}
	for _, retired := range []string{"into: arrivals, key: id", "from: payload, key: id", "description: retired, key: id", "window: payload.period, key: id", "window: null, key: id", "dedup_by: payload.id, key: id", "dedup_by: [], key: id", "key: null", "key: ''", "key: 12", "key: payload.id"} {
		snapshot, err := yamlsource.Load([]byte("accumulate: {target: arrivals, " + retired + "}\n"))
		if err != nil {
			t.Fatal(err)
		}
		field, _ := snapshot.Document("nodes.yaml").Root().Lookup("accumulate")
		if _, err := projectNodeAccumulateValue(field.Value); err == nil {
			t.Fatalf("retired/invalid field admitted: %s", retired)
		}
	}
	for _, test := range []struct {
		name, handler, agents string
	}{
		{name: "second accumulate", handler: "    accumulate: {target: arrivals, key: id}\n"},
		{name: "direct write", handler: "    data_accumulation: {writes: [{target_field: arrivals, value: []}]}\n"},
		{name: "nested write", handler: "    data_accumulation: {writes: [{op: update, target: state.arrivals, index: 0, value: {id: overwritten}}]}\n"},
		{name: "rule write", handler: "    rules: [{when: true, data_accumulation: {writes: [{target_field: arrivals, value: []}]}}]\n"},
		{name: "compute", handler: "    compute: {operation: count, store_as: state.arrivals}\n"},
		{name: "query", handler: "    query: {source: state.arrivals, store_as: state.arrivals}\n"},
		{name: "clear", handler: "    clear: {targets: [state.arrivals]}\n"},
		{name: "payload lowering", handler: "    data_accumulation: {from: payload}\n"},
		{name: "agent grant", agents: "worker:\n  intent: {inline: Collect items}\n  model: regular\n  subscriptions: [item.changed]\n  emit_events: []\n  writes: [arrivals]\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			for file, body := range map[string]string{
				"manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
				"schema.yaml":   "stages: {active: {}}\ninputs: [item.arrived, item.changed]\n",
				"state.yaml":    "arrivals: [Item]\n",
				"types.yaml":    "Item:\n  id: text\n",
				"events.yaml":   "item.arrived:\n  _embed: [Item]\nitem.changed:\n  _embed: [Item]\n  arrivals: [Item]\n",
				"nodes.yaml":    "collector:\n  item.arrived:\n    accumulate: {target: arrivals, key: id}\nother:\n  item.changed:\n" + test.handler,
			} {
				if test.handler == "" && file == "nodes.yaml" {
					body += "    {}\n"
				}
				if test.name == "second accumulate" && file == "events.yaml" {
					body = "item.arrived:\n  _embed: [Item]\nitem.changed:\n  _embed: [Item]\n"
				}
				writeFixtureFile(t, filepath.Join(root, file), body)
			}
			if test.agents != "" {
				writeFixtureFile(t, filepath.Join(root, "agents.yaml"), test.agents)
			}
			repo := repoRootForContractsTest(t)
			_, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			if err == nil || !strings.Contains(err.Error(), "arrivals") || !strings.Contains(err.Error(), "nodes.yaml:") || !(strings.Contains(err.Error(), "two writers") || strings.Contains(err.Error(), "sole writer")) {
				t.Fatalf("conflicting writer must identify both declarations: %v", err)
			}
			if test.agents == "" && strings.Count(err.Error(), "nodes.yaml:") < 2 || test.agents != "" && !strings.Contains(err.Error(), "agents.yaml:") {
				t.Fatalf("conflicting writer location is missing: %v", err)
			}
		})
	}
}
