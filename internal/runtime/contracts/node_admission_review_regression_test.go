package contracts

import (
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestReviewer2492ClosedNodeAdmission(t *testing.T) {
	for name, body := range map[string]string{
		"compute_value_field":        "compute: {operation: pick_or_average, value_field: amount}",
		"compute_weight_field":       "compute: {operation: pick_or_average, weight_field: mass}",
		"clear_empty_source":         "data_accumulation: {writes: [{op: clear, target: entity.items, source_field: ''}]}",
		"clear_empty_field":          "data_accumulation: {writes: [{op: clear, target: entity.items, field: ''}]}",
		"mapped_empty_op":            "data_accumulation: {writes: [{source_field: items, target_field: items, op: ''}]}",
		"mapped_empty_second_target": "data_accumulation: {writes: [{source_field: items, target_field: items, target_path: ''}]}",
		"ordinary_empty_source":      "data_accumulation: {writes: [{target_field: items, value: 1, source_field: ''}]}",
		"fanout_empty_identity":      "fan_out: {items_from: payload.rows, as: row, identity: '', emit: {event: task.done, fields: {id: '${row}'}}}",
		"fanout_fractional_bound":    "fan_out: {items_from: payload.rows, as: row, max_items: 1.5, emit: {event: task.done, fields: {id: '${row}'}}}",
		"reduce_empty_source":        "reduce: {operation: sum, items_from: payload.rows, source: ''}",
		"count_empty_source":         "count: {items_from: payload.rows, source: ''}",
		"completion_activity":        "on_complete: [{activity: {tool: search}}]",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := admitReviewNode(t, body)
			if err == nil {
				t.Fatal("approved grammar requires source admission rejection")
			}
			if !strings.Contains(err.Error(), "nodes.yaml:") {
				t.Fatalf("source evidence lost: %v", err)
			}
		})
	}
}

func TestReviewer2492ValidControls(t *testing.T) {
	for _, body := range []string{
		"compute: {operation: pick_or_average}",
		"data_accumulation: {writes: [{op: clear, target: entity.items}]}",
		"data_accumulation: {writes: [{source_field: items, target_field: items}]}",
		"fan_out: {items_from: payload.rows, as: row, max_items: 2, emit: {event: task.done, fields: {id: '${row}'}}}",
		"reduce: {operation: sum, items_from: payload.rows}",
		"activity: {tool: search}",
	} {
		if _, err := admitReviewNode(t, body); err != nil {
			t.Fatalf("%s: %v", body, err)
		}
	}
}

func TestReviewer2492NestedDiagnosticPath(t *testing.T) {
	_, err := admitReviewNode(t, "rules:\n  - when: else")
	const path = `$["worker"]["event_handlers"]["task.ready"]["rules"][0]["when"]`
	if err == nil || !strings.Contains(err.Error(), path) {
		t.Fatalf("want semantic path %s, got %v", path, err)
	}
	snapshot, err := yamlsource.Load([]byte("worker:\n  execution_type: system_node\n  event_handlers:\n    start:\n      rules:\n        - when: else\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err == nil || !strings.Contains(err.Error(), `$["worker"]["event_handlers"]["start"]["rules"][0]["when"]`) {
		t.Fatalf("original motivating diagnostic must retain its exact path: %v", err)
	}
}

func TestReviewer2492AnnotationProvenanceBound(t *testing.T) {
	for _, depth := range []int{8, 12, 14, 24} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			var body strings.Builder
			body.WriteString("_note:\n  a0: &a0 [x, x]\n")
			for i := 1; i <= depth; i++ {
				fmt.Fprintf(&body, "  a%d: &a%d [*a%d, *a%d]\n", i, i, i-1, i-1)
			}
			node, err := admitReviewNode(t, body.String())
			if err != nil {
				t.Fatal(err)
			}
			if len(node.admissionProvenance) != 4 {
				t.Fatalf("annotation expanded into %d entries, want its outer occurrence only", len(node.admissionProvenance))
			}
		})
	}
}

func TestNodeWriteBranchPresenceContract(t *testing.T) {
	for _, branch := range []struct {
		name, valid string
		forbidden   []string
	}{
		{"mapped", "source_field: items, target_field: items", []string{"field", "target", "op", "key", "index", "value", "expression"}},
		{"ordinary", "target_path: entity.items, value: 1", []string{"field", "source_field", "target", "op", "key", "index", "expression"}},
		{"clear", "op: clear, target: entity.items", []string{"field", "source_field", "target_field", "target_path", "key", "index", "value", "expression"}},
		{"set", "op: set, target: entity.items, key: x, value: 1", []string{"field", "source_field", "target_field", "target_path", "index", "expression"}},
		{"merge", "op: merge, target: entity.items, key: x, value: {}", []string{"field", "source_field", "target_field", "target_path", "index", "expression"}},
		{"delete", "op: delete, target: entity.items, key: x", []string{"field", "source_field", "target_field", "target_path", "index", "value", "expression"}},
		{"append", "op: append, target: entity.items, value: 1", []string{"field", "source_field", "target_field", "target_path", "index", "expression"}},
		{"update", "op: update, target: entity.items, index: 0, value: 1", []string{"field", "source_field", "target_field", "target_path", "expression"}},
	} {
		t.Run(branch.name, func(t *testing.T) {
			if _, err := admitReviewNode(t, "data_accumulation: {writes: [{"+branch.valid+"}]}"); err != nil {
				t.Fatalf("valid branch: %v", err)
			}
			for _, key := range branch.forbidden {
				for _, state := range []string{"null", "''", "x", "{}", "{x: y}", "[]", "[x]"} {
					t.Run(key+"/"+state, func(t *testing.T) {
						body := "data_accumulation: {writes: [{" + branch.valid + ", " + key + ": " + state + "}]}"
						if _, err := admitReviewNode(t, body); err == nil {
							t.Fatalf("forbidden authored field was erased: %s", body)
						}
					})
				}
			}
		})
	}
	for _, write := range []string{
		"items", "{target_field: items, value: ''}", "{target_field: items, value: null}",
		"{source_field: items, target_path: entity.items}",
		"{op: append, target: entity.items, key: group, value: null}",
		"{op: update, target: entity.items, key: group, index: 0, value: ''}",
	} {
		if _, err := admitReviewNode(t, "data_accumulation: {writes: ["+write+"]}"); err != nil {
			t.Fatalf("valid literal/mapped/map-held-list admission %s: %v", write, err)
		}
	}
}

func TestNodeWriteRequiredTextAndExclusiveTargets(t *testing.T) {
	for _, state := range []string{"null", "''", "' '", "{}", "[]"} {
		for _, write := range []string{
			"{source_field: " + state + ", target_field: items}",
			"{source_field: items, target_field: " + state + "}",
			"{target_path: " + state + ", value: 1}",
			"{op: clear, target: " + state + "}",
			"{op: " + state + ", target: entity.items}",
			"{target_field: items, target_path: " + state + ", value: 1}",
		} {
			if _, err := admitReviewNode(t, "data_accumulation: {writes: ["+write+"]}"); err == nil {
				t.Fatalf("invalid branch admitted: %s", write)
			}
		}
	}
	for _, write := range []string{"''", "{field: items}", "{value: 1}", "{source_field: items}", "{target_field: items}", "{op: append, target: entity.items}", "{op: set, target: entity.items, value: 1}"} {
		if _, err := admitReviewNode(t, "data_accumulation: {writes: ["+write+"]}"); err == nil {
			t.Fatalf("required field/bare direct form admitted: %s", write)
		}
	}
}

func TestNodeFanOutNumericKindAndIdentityPresence(t *testing.T) {
	const base = "items_from: payload.rows, as: row, emit: task.done"
	for _, scalar := range []string{"1.5", "1.0", "'2'", "true", "null", "''", "0", "-1", "1001", "9999999999999999999999999999", "{}", "[]"} {
		if _, err := admitReviewNode(t, "fan_out: {"+base+", max_items: "+scalar+"}"); err == nil {
			t.Fatalf("non-integer/out-of-range max_items accepted: %s", scalar)
		}
	}
	for _, scalar := range []string{"1", "1000", "0x10"} {
		node, err := admitReviewNode(t, "fan_out: {"+base+", max_items: "+scalar+"}")
		if err != nil || !node.EventHandlers["task.ready"].FanOut.MaxItemsSet {
			t.Fatalf("positive integer %s: %v", scalar, err)
		}
	}
	for _, state := range []string{"null", "''", "' '", "{}", "[]"} {
		if _, err := admitReviewNode(t, "fan_out: {"+base+", identity: "+state+"}"); err == nil {
			t.Fatalf("invalid authored identity accepted: %s", state)
		}
	}
	for _, identity := range []string{"", ", identity: row.id"} {
		if _, err := admitReviewNode(t, "fan_out: {"+base+identity+"}"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNodeCollectionAuthoredSourcesAndPrecedence(t *testing.T) {
	for _, owner := range []string{"reduce", "count"} {
		for _, field := range []string{"source", "items_from"} {
			for _, state := range []string{"null", "''", "' '", "{}", "{x: y}", "[]", "[x]"} {
				if _, err := admitReviewNode(t, owner+": {"+field+": "+state+"}"); err == nil {
					t.Fatalf("%s.%s admits %s", owner, field, state)
				}
			}
		}
		for _, fields := range []string{"", "source: payload.old", "items_from: payload.rows", "source: payload.old, items_from: payload.rows"} {
			node, err := admitReviewNode(t, owner+": {"+fields+"}")
			if err != nil {
				t.Fatal(err)
			}
			handler := node.EventHandlers["task.ready"]
			if strings.Contains(fields, "items_from") && ((owner == "reduce" && handler.Reduce.ItemsFrom != "payload.rows") || (owner == "count" && handler.Count.ItemsFrom != "payload.rows")) {
				t.Fatal("items_from precedence/source evidence changed")
			}
		}
	}
}

func TestNodeClosedComputeAndCompletionPresence(t *testing.T) {
	for _, state := range []string{"null", "''", "x", "{}", "{tool: search}", "[]", "[x]"} {
		for _, body := range []string{
			"compute: {operation: pick_or_average, value_field: " + state + "}",
			"compute: {operation: pick_or_average, weight_field: " + state + "}",
			"on_complete: [{activity: " + state + "}]",
		} {
			if _, err := admitReviewNode(t, body); err == nil {
				t.Fatalf("closed field/context accepted: %s", body)
			}
		}
	}
	for _, body := range []string{"activity: {tool: search}", "rules: [{when: 'true', activity: {tool: search}}, {else: true}]", "on_complete: [{condition: 'true', emit: task.done}]", "sets_gate: done"} {
		if _, err := admitReviewNode(t, body); err != nil {
			t.Fatalf("supported context %s: %v", body, err)
		}
	}
}

func TestNodeAliasMergePresenceAndSemanticDiagnostics(t *testing.T) {
	for _, row := range []string{
		"_note: &bad ''\nfan_out: {items_from: payload.rows, as: row, identity: *bad}",
		"_note: &bad 1.5\nfan_out: {items_from: payload.rows, as: row, max_items: *bad}",
		"_note: &bad {source_field: ''}\ndata_accumulation: {writes: [{op: clear, target: entity.items, <<: *bad}]}",
		"_note: &bad {activity: null}\non_complete: [{<<: *bad}]",
		"_note: &bad {value_field: ''}\ncompute: {operation: pick_or_average, <<: *bad}",
		"_note: &bad ''\nreduce: {items_from: payload.rows, source: *bad}",
		"_note: &bad {source: ''}\ncount: {items_from: payload.rows, <<: *bad}",
	} {
		if _, err := admitReviewNode(t, row); err == nil {
			t.Fatalf("alias/merge erased authored presence: %s", row)
		}
	}
	for _, body := range []string{"rules: [{when: else}]", "_note: &bad else\nrules: [{when: *bad}]", "_note: &bad {when: else}\nrules: [{<<: *bad}]"} {
		_, err := admitReviewNode(t, body)
		if err == nil || !strings.Contains(err.Error(), `["rules"][0]["when"]`) || !strings.Contains(err.Error(), "introduced at nodes.yaml:") || !strings.Contains(err.Error(), "resolved at nodes.yaml:") {
			t.Fatalf("nested semantic-validator evidence: %v", err)
		}
	}
	if _, err := admitReviewNode(t, "_note: &max 2\nfan_out: {items_from: payload.rows, as: row, max_items: *max, emit: task.done}"); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{
		"_note: &source payload.rows\nreduce: {operation: sum, items_from: *source}",
		"_note: &source {items_from: payload.rows}\ncount: {source: payload.old, <<: *source}",
		"_note: &write {source_field: items, target_field: items}\ndata_accumulation: {writes: [*write]}",
	} {
		if _, err := admitReviewNode(t, body); err != nil {
			t.Fatalf("supported alias/merge must not regress: %s: %v", body, err)
		}
	}
}

func TestNodeAnnotationAliasDoesNotExemptActiveData(t *testing.T) {
	var body strings.Builder
	body.WriteString("_note:\n  a0: &a0 [x, x]\n")
	for i := 1; i <= 18; i++ {
		fmt.Fprintf(&body, "  a%d: &a%d [*a%d, *a%d]\n", i, i, i-1, i-1)
	}
	body.WriteString("emit: {event: task.done, fields: {data: *a18}}")
	if _, err := admitReviewNode(t, body.String()); err == nil || !strings.Contains(err.Error(), "YAML-EXPANSION-LIMIT") {
		t.Fatalf("active annotation alias must remain bounded: %v", err)
	}
	node, err := admitReviewNode(t, "emit: {event: task.done, fields: {_note: {business: value}}}")
	if err != nil {
		t.Fatal(err)
	}
	if _, found := node.admissionProvenance[`event_handlers["task.ready"].emit.fields._note.business`]; !found {
		t.Fatal("business data named _note is not a handler annotation")
	}
}

func admitReviewNode(t *testing.T, body string) (SystemNodeContract, error) {
	t.Helper()
	snapshot, err := yamlsource.Load([]byte("worker:\n  event_handlers:\n    task.ready:\n      " + strings.ReplaceAll(strings.TrimSuffix(body, "\n"), "\n", "\n      ") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	return nodes["worker"], err
}
