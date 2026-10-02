package scenariodocument

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// F139-F208 are the accepted census identities, not a new grammar inventory.
// Each row is exercised inside a valid branch with the eight retained shapes.
func TestAllPublicFieldContracts(t *testing.T) {
	type row struct {
		id               int
		base, path, kind string
		required         bool
		valid            any
	}
	object := func(raw string) any {
		var out any
		if err := json.Unmarshal([]byte(raw), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	rows := []row{
		{139, "derive", "connector_responses", "object", false, object(`{"tool":{"ok":true}}`)},
		{140, "derive", "connector_responses.tool", "literal", false, object(`{"ok":true}`)},
		{141, "derive", "derive", "object_nonempty", true, object(`{"flow":".","input":"request","payload":{"generate":true}}`)},
		{142, "derive", "derive.flow", "text", true, "."},
		{143, "derive", "derive.input", "text", true, "request"},
		{144, "derive", "derive.payload", "object_nonempty", true, object(`{"generate":true}`)},
		{145, "derive", "derive.payload.generate", "true", true, true},
		{146, "derive", "derive.payload.set", "object", false, object(`{"id":"x"}`)},
		{147, "ordinary", "expect", "closed_object", false, object(`{"events":["request"]}`)},
		{148, "ordinary", "expect.entities", "entity_list", false, object(`[{"ref":"item","fields":{}}]`)},
		{149, "count", "expect.entities.0.count", "count", true, int64(0)},
		{150, "ordinary", "expect.entities.0.current_state", "text", false, "ready"},
		{151, "ordinary", "expect.entities.0.fields", "object", false, object(`{"id":"x"}`)},
		{152, "ordinary", "expect.entities.0.gates", "object", false, object(`{"approved":true}`)},
		{153, "ordinary", "expect.entities.0.ref", "text", false, "item"},
		{154, "ordinary", "expect.entities.0.type", "text", false, "task"},
		{155, "ordinary", "expect.events", "events", false, object(`{"include":["request"]}`)},
		{156, "ordinary", "expect.events.exact", "list", false, object(`["request"]`)},
		{157, "ordinary", "expect.events.include", "list", false, object(`["request"]`)},
		{158, "ordinary", "expect.events.ordered", "list", false, object(`["request"]`)},
		{159, "ordinary", "expect.no_dead_letters", "bool", false, true},
		{160, "ordinary", "invalid", "object_nonempty", false, object(`{"base":{"publish":"request","payload":{}},"cases":[{"set":{"id":null}}]}`)},
		{161, "ordinary", "invalid.base", "object_nonempty", true, object(`{"publish":"request","payload":{}}`)},
		{162, "ordinary", "invalid.base.payload", "payload", true, object(`{"id":"x"}`)},
		{163, "ordinary", "invalid.base.publish", "text", true, "request"},
		{164, "ordinary", "invalid.cases", "list_nonempty", true, object(`[{"set":{"id":null}}]`)},
		{165, "ordinary", "invalid.cases.0.expect", "retired", false, "reject"},
		{166, "ordinary", "invalid.cases.0.name", "optional_text", false, "negative"},
		{167, "ordinary", "invalid.cases.0.set", "object", false, object(`{"id":null}`)},
		{168, "ordinary", "name", "optional_text", false, "scenario"},
		{169, "ordinary", "seed", "optional_text", false, "seed"},
		{170, "ordinary", "setup", "object_nonempty", false, object(`{"entities":[{"as":"item","type":"task"}]}`)},
		{171, "ordinary", "setup.entities", "list_nonempty", true, object(`[{"as":"item","type":"task"}]`)},
		{172, "ordinary", "setup.entities.0.as", "text", true, "item"},
		{173, "ordinary", "setup.entities.0.current_state", "text", false, "ready"},
		{174, "ordinary", "setup.entities.0.fields", "object", false, object(`{"id":"x"}`)},
		{175, "ordinary", "setup.entities.0.flow", "text", false, "."},
		{176, "ordinary", "setup.entities.0.gates", "object", false, object(`{"approved":true}`)},
		{177, "ordinary", "setup.entities.0.type", "text", true, "task"},
		{178, "ordinary", "steps", "list_nonempty", true, object(`[{"publish":"request","payload":{}}]`)},
		{179, "ordinary", "steps.0.emitter", "text", false, "operator"},
		{180, "ordinary", "steps.0.idempotency_key", "text", false, "key"},
		{181, "mailbox", "steps.0.mailbox.decide.fields", "object", false, object(`{"reason":"because"}`)},
		{182, "mailbox", "steps.0.mailbox.decide.idempotency_key", "text", false, "key"},
		{183, "mailbox", "steps.0.mailbox.decide.match", "object_nonempty", true, object(`{"anchor_kind":"stage_gate"}`)},
		{184, "mailbox", "steps.0.mailbox.decide.match.activity_id", "text", false, "activity"},
		{185, "mailbox", "steps.0.mailbox.decide.match.anchor_kind", "text", true, "stage_gate"},
		{186, "mailbox", "steps.0.mailbox.decide.match.card_id", "text", false, "card"},
		{187, "mailbox", "steps.0.mailbox.decide.match.category", "text", false, "task"},
		{188, "mailbox", "steps.0.mailbox.decide.match.decision", "text", false, "review"},
		{189, "mailbox", "steps.0.mailbox.decide.match.entity_id", "text", false, "entity"},
		{190, "mailbox", "steps.0.mailbox.decide.match.flow_instance", "text", false, "."},
		{191, "mailbox", "steps.0.mailbox.decide.match.request_event_id", "text", false, "request"},
		{192, "mailbox", "steps.0.mailbox.decide.match.requester_agent_id", "text", false, "agent"},
		{193, "mailbox", "steps.0.mailbox.decide.match.scope", "text", false, "global"},
		{194, "mailbox", "steps.0.mailbox.decide.match.stage", "text", false, "review"},
		{195, "defer", "steps.0.mailbox.defer.until", "text", true, "2030-01-01T00:00:00Z"},
		{196, "mailbox", "steps.0.mailbox.decide.verdict", "text", true, "approve"},
		{197, "mailbox", "steps.0.mailbox.decide", "object_nonempty", true, object(`{"match":{"anchor_kind":"stage_gate"},"verdict":"approve"}`)},
		{198, "defer", "steps.0.mailbox.defer", "object_nonempty", true, object(`{"match":{"anchor_kind":"stage_gate"},"until":"2030-01-01T00:00:00Z"}`)},
		{199, "ordinary", "steps.0.payload", "payload", true, object(`{"id":"x"}`)},
		{200, "fixture", "steps.0.payload.from", "text", false, "fixture.yaml"},
		{201, "ordinary", "steps.0.payload.set", "object", false, object(`{"id":"x"}`)},
		{202, "ordinary", "steps.0.publish", "text", true, "request"},
		{203, "ordinary", "steps.0.source_event_id", "text", false, "event"},
		{204, "ordinary", "steps.0.target", "text", false, "item"},
		{205, "target", "steps.0.target_flow_instance", "text", true, "."},
		{206, "target", "steps.0.target_entity_id", "text", true, "entity"},
		{207, "ordinary", "vars", "object", false, object(`{"id":"x"}`)},
		{208, "ordinary", "version", "retired", false, int64(1)},
	}
	if len(rows) != 70 {
		t.Fatalf("field coverage = %d, want 70", len(rows))
	}
	for i, row := range rows {
		if row.id != 139+i {
			t.Fatalf("census identity gap: %d", row.id)
		}
		t.Run(fmt.Sprintf("F%d_%s", row.id, row.path), func(t *testing.T) {
			shapes := []struct {
				name  string
				value any
			}{{"missing", nil}, {"null", nil}, {"empty_scalar", ""}, {"scalar", int64(0)}, {"empty_map", map[string]any{}}, {"empty_list", []any{}}, {"map", map[string]any{"unexpected": true}}, {"list", []any{"value"}}, {"canonical", row.valid}}
			for _, shape := range shapes {
				t.Run(shape.name, func(t *testing.T) {
					doc := fieldContractFixture(t, row.base)
					setContractField(t, doc, row.path, shape.value, shape.name == "missing")
					raw, err := json.Marshal(doc)
					if err != nil {
						t.Fatal(err)
					}
					_, err = Admit(raw, "tests/field.yaml")
					want := fieldShapeAccepted(row.kind, row.required, shape.name)
					if (err == nil) != want {
						t.Fatalf("accepted=%v want=%v: %s: %v", err == nil, want, raw, err)
					}
				})
			}
		})
	}
}

func fieldShapeAccepted(kind string, required bool, shape string) bool {
	if shape == "missing" {
		return !required
	}
	if kind == "retired" {
		return false
	}
	if shape == "canonical" {
		return true
	}
	switch kind {
	case "literal":
		return true
	case "optional_text":
		return shape == "empty_scalar"
	case "object", "payload":
		return shape == "empty_map" || shape == "map"
	case "closed_object":
		return shape == "empty_map"
	case "entity_list":
		return shape == "empty_list"
	case "events":
		return shape == "empty_map" || shape == "empty_list" || shape == "list"
	case "list":
		return shape == "empty_list" || shape == "list"
	case "count":
		return shape == "scalar"
	}
	return false
}

func fieldContractFixture(t *testing.T, branch string) map[string]any {
	t.Helper()
	raw := `{"name":"scenario","setup":{"entities":[{"as":"item","type":"task"}]},"steps":[{"publish":"request","payload":{}}],"expect":{"events":{},"entities":[{"ref":"item","type":"task","current_state":"ready","fields":{}}]},"invalid":{"base":{"publish":"request","payload":{}},"cases":[{"set":{}}]}}`
	if branch == "derive" {
		raw = `{"name":"profile","derive":{"flow":".","input":"request","payload":{"generate":true}},"connector_responses":{"tool":{}}}`
	}
	if branch == "mailbox" {
		raw = `{"steps":[{"mailbox.decide":{"match":{"anchor_kind":"stage_gate"},"verdict":"approve"}}]}`
	}
	if branch == "defer" {
		raw = `{"steps":[{"mailbox.defer":{"match":{"anchor_kind":"stage_gate"},"until":"2030-01-01T00:00:00Z"}}]}`
	}
	if branch == "count" {
		raw = `{"steps":[{"publish":"request","payload":{}}],"expect":{"entities":[{"type":"task","count":0}]}}`
	}
	if branch == "target" {
		raw = `{"steps":[{"publish":"request","payload":{},"target_flow_instance":".","target_entity_id":"entity"}]}`
	}
	if branch == "fixture" {
		raw = `{"steps":[{"publish":"request","payload":{"from":"fixture.yaml"}}]}`
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func setContractField(t *testing.T, root map[string]any, path string, value any, remove bool) {
	t.Helper()
	// Action names contain dots; preserve that exact grammar key in the test path.
	path = strings.ReplaceAll(strings.ReplaceAll(path, "mailbox.decide", "mailbox_decide"), "mailbox.defer", "mailbox_defer")
	parts := strings.Split(path, ".")
	var cursor any = root
	for i, part := range parts {
		part = strings.ReplaceAll(strings.ReplaceAll(part, "mailbox_decide", "mailbox.decide"), "mailbox_defer", "mailbox.defer")
		if list, ok := cursor.([]any); ok {
			index, err := strconv.Atoi(part)
			if err != nil {
				t.Fatal(err)
			}
			cursor = list[index]
			continue
		}
		mapping := cursor.(map[string]any)
		if i == len(parts)-1 {
			if remove {
				delete(mapping, part)
			} else {
				mapping[part] = value
			}
			return
		}
		cursor = mapping[part]
	}
}
