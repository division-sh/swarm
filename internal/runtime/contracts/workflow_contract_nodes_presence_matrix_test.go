package contracts

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type nodePresenceShape struct {
	name  string
	value string
}

func TestW4ScalarFieldPresenceAndTypedProjection(t *testing.T) {
	for _, group := range []struct {
		name, base string
		newTarget  func() any
		fields     map[string]string
	}{
		{"node", "event_handlers: {}\n", func() any { return new(SystemNodeContract) }, map[string]string{"description": "Description", "execution_type": "ExecutionType", "state_table": "StateTable"}},
		{"timer", "", func() any { return new(WorkflowTimerContract) }, map[string]string{"id": "ID", "stage": "Stage", "event": "Event", "owner": "Owner", "action": "Action", "cancellation": "Cancellation", "delay": "Delay", "start_on": "StartOn", "cancel_on": "CancelOn"}},
		{"handler", "create_entity: false\n", func() any { return new(SystemNodeEventHandler) }, map[string]string{"description": "Description", "advances_to": "AdvancesTo"}},
		{"state", "fields: {}\n", func() any { return new(NodeStateSchema) }, map[string]string{"description": "Description"}},
		{"activity", "input: {}\n", func() any { return new(ActivitySpec) }, map[string]string{"id": "ID", "tool": "Tool"}},
		{"guard", "checks: []\n", func() any { return new(GuardSpec) }, map[string]string{"id": "ID", "check": "Check", "policy_ref": "PolicyRef"}},
		{"query", "count: false\n", func() any { return new(QuerySpec) }, map[string]string{"source": "Source", "entities": "Entities", "filter": "Filter", "group_by": "GroupBy", "store_as": "StoreAs"}},
		{"group_by", "", func() any { return new(GroupBySpec) }, map[string]string{"items_from": "ItemsFrom", "key": "Key", "store_as": "StoreAs"}},
		{"filter", "", func() any { return new(FilterSpec) }, map[string]string{"source": "Source", "items_from": "ItemsFrom", "condition": "Condition", "store_as": "StoreAs"}},
		{"reduce", "", func() any { return new(ReduceSpec) }, map[string]string{"source": "Source", "items_from": "ItemsFrom", "operation": "Operation", "store_as": "StoreAs"}},
		{"count", "", func() any { return new(CountSpec) }, map[string]string{"source": "Source", "items_from": "ItemsFrom", "condition": "Condition", "store_as": "StoreAs"}},
		{"compute", "operation: pick_or_average\n", func() any { return new(ComputeSpec) }, map[string]string{"store_as": "StoreAs", "description": "Description", "value_field": "ValueField", "weight_field": "WeightField"}},
	} {
		for field, carrier := range group.fields {
			t.Run(group.name+"."+field, func(t *testing.T) {
				for index, shape := range nodePresenceShapes("payload.rows", "[payload.rows]", "{value: payload.rows}") {
					t.Run(shape.name, func(t *testing.T) {
						body := group.base
						if shape.name != "missing" {
							body += field + ": " + shape.value + "\n"
						} else if body == "" {
							body = "{}\n"
						}
						out := group.newTarget()
						err := decodeNodeTestYAML([]byte(body), out)
						want := index == 0 || index == 2 || index == 3
						if (err == nil) != want {
							t.Fatalf("admitted=%t want=%t: %v", err == nil, want, err)
						}
						if err == nil {
							value := reflect.ValueOf(out).Elem().FieldByName(carrier).String()
							expected := ""
							if index == 3 {
								expected = "payload.rows"
							}
							if value != expected {
								t.Fatalf("typed %s=%q want %q", carrier, value, expected)
							}
						}
					})
				}
			})
		}
	}
}

func TestW4QueryBooleanAndSelectionPresence(t *testing.T) {
	for _, field := range []string{"count", "select"} {
		t.Run(field, func(t *testing.T) {
			shapes := nodePresenceShapes("true", "[id]", "{value: id}")
			accepted := [8]bool{true, false, false, true, false, false, false, false}
			if field == "select" {
				accepted = [8]bool{true, false, false, false, true, true, false, false}
			}
			proveNodePresence(t, shapes, accepted, func(shape nodePresenceShape) string {
				body := "source: payload.rows\n"
				if shape.name != "missing" {
					body += field + ": " + shape.value + "\n"
				}
				return body
			}, decodeNodeTestYAML, func(t *testing.T, name string, out QuerySpec) {
				if out.Source != "payload.rows" {
					t.Fatalf("source=%q", out.Source)
				}
				if field == "count" && out.Count != (name == "scalar") {
					t.Fatalf("count=%t", out.Count)
				}
				if field == "select" && name == "sequence" && !reflect.DeepEqual(out.Select, []string{"id"}) {
					t.Fatalf("select=%v", out.Select)
				}
			})
		})
	}
}

func TestW4HandlerContainerPresence(t *testing.T) {
	for _, tc := range []struct {
		field, scalar, sequence, mapping string
		accepted                         [8]bool
		check                            func(*testing.T, SystemNodeEventHandler)
	}{
		{"emit", "task.done", "[task.done]", "{event: task.done}", [8]bool{true, true, true, true, false, false, true, true}, func(t *testing.T, h SystemNodeEventHandler) {
			if h.Emit.Event != "task.done" {
				t.Fatalf("emit=%#v", h.Emit)
			}
		}},
		{"activity", "task", "[task]", "{tool: search}", [8]bool{true, true, false, false, false, false, true, true}, func(t *testing.T, h SystemNodeEventHandler) {
			if h.Activity.Tool != "search" {
				t.Fatalf("activity=%#v", h.Activity)
			}
		}},
		{"guard", "ready", "[ready]", "{id: ready, check: true}", [8]bool{true, true, false, false, false, false, true, true}, func(t *testing.T, h SystemNodeEventHandler) {
			if h.Guard == nil || h.Guard.Check != "true" {
				t.Fatalf("guard=%#v", h.Guard)
			}
		}},
		{"on_success", "task.done", "[task.done]", "{emit: task.done}", [8]bool{true, true, false, false, false, false, true, true}, func(t *testing.T, h SystemNodeEventHandler) {
			if h.OnSuccess.Emit.Event != "task.done" {
				t.Fatalf("on_success=%#v", h.OnSuccess)
			}
		}},
		{"data_accumulation", "field", "[field]", "{writes: [field]}", [8]bool{true, false, false, false, false, false, true, true}, func(t *testing.T, h SystemNodeEventHandler) {
			if len(h.DataAccumulation.Writes) != 1 || h.DataAccumulation.Writes[0].Target() != "field" {
				t.Fatalf("writes=%#v", h.DataAccumulation.Writes)
			}
		}},
		{"loop", "revision", "[revision]", "{start: revision, from: queued}", [8]bool{true, false, false, false, false, false, false, true}, func(t *testing.T, h SystemNodeEventHandler) {
			if h.Loop == nil || h.Loop.Start != "revision" || h.Loop.From != "queued" {
				t.Fatalf("loop=%#v", h.Loop)
			}
		}},
		{"compute", "sum", "[sum]", "{operation: sum}", [8]bool{true, false, false, false, false, false, true, true}, func(t *testing.T, h SystemNodeEventHandler) {
			if h.Compute == nil || h.Compute.Operation != ComputeOpSum {
				t.Fatalf("compute=%#v", h.Compute)
			}
		}},
		{"query", "rows", "[{source: payload.rows}]", "{source: payload.rows}", [8]bool{true, false, false, false, false, false, false, true}, func(t *testing.T, h SystemNodeEventHandler) {
			if h.Query == nil || h.Query.Source != "payload.rows" {
				t.Fatalf("query=%#v", h.Query)
			}
		}},
		{"accumulate", "rows", "[rows]", "{into: rows}", [8]bool{true, false, false, false, false, false, true, true}, func(t *testing.T, h SystemNodeEventHandler) {
			if h.Accumulate == nil || h.Accumulate.Into != "rows" {
				t.Fatalf("accumulate=%#v", h.Accumulate)
			}
		}},
		{"fan_out", "rows", "[rows]", "{items_from: payload.rows, as: row, emit: task.ready}", [8]bool{true, false, false, false, false, false, false, true}, func(t *testing.T, h SystemNodeEventHandler) {
			if h.FanOut == nil || h.FanOut.As != "row" || h.FanOut.Emit.Event != "task.ready" {
				t.Fatalf("fan_out=%#v", h.FanOut)
			}
		}},
		{"group_by", "rows", "[rows]", "{items_from: payload.rows, key: id}", [8]bool{true, false, false, false, false, false, true, true}, func(t *testing.T, h SystemNodeEventHandler) {
			if h.GroupBy == nil || h.GroupBy.Key != "id" {
				t.Fatalf("group_by=%#v", h.GroupBy)
			}
		}},
		{"filter", "rows", "[rows]", "{items_from: payload.rows, condition: true}", [8]bool{true, false, false, false, false, false, true, true}, func(t *testing.T, h SystemNodeEventHandler) {
			if h.Filter == nil || h.Filter.Condition != "true" {
				t.Fatalf("filter=%#v", h.Filter)
			}
		}},
		{"reduce", "rows", "[rows]", "{items_from: payload.rows, operation: sum}", [8]bool{true, false, false, false, false, false, true, true}, func(t *testing.T, h SystemNodeEventHandler) {
			if h.Reduce == nil || h.Reduce.Operation != "sum" {
				t.Fatalf("reduce=%#v", h.Reduce)
			}
		}},
		{"count", "rows", "[rows]", "{items_from: payload.rows}", [8]bool{true, false, false, false, false, false, true, true}, func(t *testing.T, h SystemNodeEventHandler) {
			if h.Count == nil || h.Count.ItemsFrom != "payload.rows" {
				t.Fatalf("count=%#v", h.Count)
			}
		}},
		{"clear", "rows", "[rows]", "{targets: [entity.rows]}", [8]bool{true, false, false, false, false, false, true, true}, func(t *testing.T, h SystemNodeEventHandler) {
			if h.Clear == nil || !reflect.DeepEqual(h.Clear.Targets, []string{"entity.rows"}) {
				t.Fatalf("clear=%#v", h.Clear)
			}
		}},
		{"rules", "rule", "[{else: true, emit: task.done}]", "{else: true, emit: task.done}", [8]bool{true, false, false, false, true, true, false, true}, func(t *testing.T, h SystemNodeEventHandler) {
			if len(h.Rules) != 1 || h.Rules[0].Emit.Event != "task.done" {
				t.Fatalf("rules=%#v", h.Rules)
			}
		}},
		{"on_complete", "rule", "[{condition: true, emit: task.done}]", "{condition: true, emit: task.done}", [8]bool{true, false, false, false, true, true, false, false}, nil},
	} {
		t.Run(tc.field, func(t *testing.T) {
			proveNodePresence(t, nodePresenceShapes(tc.scalar, tc.sequence, tc.mapping), tc.accepted, func(shape nodePresenceShape) string {
				base := "description: ready\n"
				if tc.field == "on_success" {
					base += "rules: [{else: true}]\n"
				}
				if shape.name == "missing" {
					return base
				}
				return base + tc.field + ": " + shape.value + "\n"
			}, decodeNodeTestYAML, func(t *testing.T, name string, h SystemNodeEventHandler) {
				if name == "mapping" && tc.check != nil {
					tc.check(t, h)
				}
				if tc.field == "on_complete" && name == "sequence" && (len(h.OnComplete) != 1 || h.OnComplete[0].Condition != "true" || h.OnComplete[0].Emit.Event != "task.done") {
					t.Fatalf("on_complete=%#v", h.OnComplete)
				}
			})
		})
	}
}

func nodePresenceShapes(scalar, sequence, mapping string) []nodePresenceShape {
	return []nodePresenceShape{
		{"missing", ""}, {"null", "null"}, {"empty_scalar", "''"},
		{"scalar", scalar}, {"empty_sequence", "[]"}, {"sequence", sequence},
		{"empty_mapping", "{}"}, {"mapping", mapping},
	}
}

func proveNodePresence[T any](t *testing.T, shapes []nodePresenceShape, accepted [8]bool, render func(nodePresenceShape) string, decode func([]byte, any) error, check func(*testing.T, string, T)) {
	t.Helper()
	if len(shapes) != len(accepted) {
		t.Fatal("presence registry must enumerate all eight states")
	}
	for i, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			var out T
			err := decode([]byte(render(shape)), &out)
			if (err == nil) != accepted[i] {
				t.Fatalf("admitted=%t, want %t: %v", err == nil, accepted[i], err)
			}
			if err == nil && check != nil {
				check(t, shape.name, out)
			}
		})
	}
}

func TestW4HandlerFieldPresence(t *testing.T) {
	proveNodePresence(t, nodePresenceShapes("true", "[true]", "{value: true}"),
		[8]bool{true, false, false, true, false, false, false, false},
		func(shape nodePresenceShape) string {
			if shape.name == "missing" {
				return "description: ready\n"
			}
			return "create_entity: " + shape.value + "\n"
		}, decodeNodeTestYAML, func(t *testing.T, name string, out SystemNodeEventHandler) {
			if out.CreateEntity != (name == "scalar") {
				t.Fatalf("create_entity=%t", out.CreateEntity)
			}
		})
}

func TestW4SetsGatePresence(t *testing.T) {
	proveNodePresence(t, nodePresenceShapes("ready", "[ready]", "{name: ready}"),
		[8]bool{true, true, true, true, false, false, true, true},
		func(shape nodePresenceShape) string {
			if shape.name == "missing" {
				return "description: ready\n"
			}
			return "sets_gate: " + shape.value + "\n"
		}, decodeNodeTestYAML, func(t *testing.T, name string, out SystemNodeEventHandler) {
			wantGate := name == "scalar" || name == "mapping"
			if (out.SetsGate != nil) != wantGate {
				t.Fatalf("sets_gate=%#v", out.SetsGate)
			}
			if wantGate && (out.SetsGate.Name != "ready" || out.SetsGate.Value != true) {
				t.Fatalf("gate proof=%#v", out.SetsGate)
			}
		})
}

func TestW4NodeTimerPresence(t *testing.T) {
	proveNodePresence(t, nodePresenceShapes("true", "[true]", "{value: true}"),
		[8]bool{true, false, false, true, false, false, false, false},
		func(shape nodePresenceShape) string {
			body := "id: reminder\nevent: timer.reminder\ndelay: 1m\n"
			if shape.name != "missing" {
				body += "recurring: " + shape.value + "\n"
			}
			return body
		}, decodeNodeTestYAML, func(t *testing.T, name string, out WorkflowTimerContract) {
			if out.ID != "reminder" || out.Recurring != (name == "scalar") {
				t.Fatalf("timer=%#v", out)
			}
		})
}

func TestW4NodeStateFieldPresence(t *testing.T) {
	proveNodePresence(t, nodePresenceShapes("seen", "[{name: seen, type: boolean}]", "{seen: boolean}"),
		[8]bool{true, false, false, false, true, true, true, true},
		func(shape nodePresenceShape) string {
			if shape.name == "missing" {
				return "description: state\n"
			}
			return "fields: " + shape.value + "\n"
		}, decodeNodeTestYAML, func(t *testing.T, name string, out NodeStateSchema) {
			wantField := name == "sequence" || name == "mapping"
			if (len(out.Fields) == 1) != wantField {
				t.Fatalf("fields=%#v", out.Fields)
			}
			if wantField && (out.Fields[0].Name != "seen" || out.Fields[0].Type != "boolean") {
				t.Fatalf("typed field=%#v", out.Fields[0])
			}
		})
}

func TestW4NodeGateFieldPresence(t *testing.T) {
	proveNodePresence(t, nodePresenceShapes("ready", "[ready]", "{ready: complete}"),
		[8]bool{true, false, false, false, true, true, true, true},
		func(shape nodePresenceShape) string {
			if shape.name == "missing" {
				return "description: gates\n"
			}
			return "gates: " + shape.value + "\n"
		}, decodeNodeTestYAML, func(t *testing.T, name string, out NodeGateStateSchema) {
			wantGate := name == "sequence" || name == "mapping"
			if (len(out.Gates) == 1) != wantGate {
				t.Fatalf("gates=%#v", out.Gates)
			}
			if wantGate && out.Gates[0].Name != "ready" {
				t.Fatalf("typed gate=%#v", out.Gates[0])
			}
		})
}

func TestW4FanOutMaxItemsPresence(t *testing.T) {
	proveNodePresence(t, nodePresenceShapes("2", "[2]", "{value: 2}"),
		[8]bool{true, false, false, true, false, false, false, false},
		func(shape nodePresenceShape) string {
			body := "items_from: payload.items\nas: row\nidentity: row.id\nemit: item.ready\n"
			if shape.name != "missing" {
				body += "max_items: " + shape.value + "\n"
			}
			return body
		}, decodeNodeTestYAML, func(t *testing.T, name string, out FanOutSpec) {
			if out.MaxItemsSet != (name == "scalar") {
				t.Fatalf("max_items=%d set=%t", out.MaxItems, out.MaxItemsSet)
			}
			if name == "scalar" && out.MaxItems != 2 {
				t.Fatalf("max_items=%d", out.MaxItems)
			}
		})
}

func TestW4RetiredNodeFieldPresence(t *testing.T) {
	cases := []struct {
		name   string
		base   string
		field  string
		decode func([]byte) error
	}{
		{"handler.from", "description: ready\n", "from", func(body []byte) error { var out SystemNodeEventHandler; return decodeNodeTestYAML(body, &out) }},
		{"handler.dedup_by", "description: ready\n", "dedup_by", func(body []byte) error { var out SystemNodeEventHandler; return decodeNodeTestYAML(body, &out) }},
		{"sets_gate.value", "name: ready\n", "value", func(body []byte) error { var out GateSpec; return decodeNodeTestYAML(body, &out) }},
		{"compute.params", "operation: pick_or_average\n", "params", func(body []byte) error { var out ComputeSpec; return decodeNodeTestYAML(body, &out) }},
		{"query.operation", "source: payload.items\nstore_as: computed.items\n", "operation", func(body []byte) error { var out QuerySpec; return decodeNodeTestYAML(body, &out) }},
		{"filter.predicate", "items_from: payload.items\ncondition: item.ready\n", "predicate", func(body []byte) error { var out FilterSpec; return decodeNodeTestYAML(body, &out) }},
		{"reduce.params", "operation: sum\nitems_from: payload.items\n", "params", func(body []byte) error { var out ReduceSpec; return decodeNodeTestYAML(body, &out) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.decode([]byte(tc.base)); err != nil {
				t.Fatalf("omitted field rejected: %v", err)
			}
			for _, shape := range nodePresenceShapes("active", "[active]", "{value: active}")[1:] {
				t.Run(shape.name, func(t *testing.T) {
					body := tc.base + fmt.Sprintf("%s: %s\n", tc.field, shape.value)
					err := tc.decode([]byte(body))
					if err == nil || !strings.Contains(err.Error(), "RETIRED") {
						t.Fatalf("retired field admitted: %v", err)
					}
				})
			}
		})
	}
}
