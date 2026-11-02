package contracts

import (
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodeDataAccumulationValue(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte(`worker:
  task.requested:
    data_accumulation:
      from: payload
      source_event: task.requested
      writes:
        - name
        - target_field: dispatch_count
          value: payload.count
        - op: clear
          target: state.old_value
`))
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	accumulation := nodes["worker"].EventHandlers["task.requested"].DataAccumulation
	if accumulation.From != "payload" || accumulation.SourceEvent != "task.requested" || len(accumulation.Writes) != 3 ||
		accumulation.Writes[0].SourceField != "name" || accumulation.Writes[0].TargetField != "name" || accumulation.Writes[1].Value.CEL != "payload.count" ||
		accumulation.Writes[2].Operation != WorkflowDataOperationClear {
		t.Fatalf("write forms not preserved: %#v", accumulation)
	}
	for _, source := range []string{"null", "''", "state", "entity", "[payload]", "[payload, state]", "'*'", "{}"} {
		t.Run("source_"+source, func(t *testing.T) {
			var writes WorkflowDataAccumulation
			err := decodeNodeTestYAML([]byte("from: "+source+"\n"), &writes)
			if err == nil || !strings.Contains(err.Error(), "payload") {
				t.Fatalf("invalid write source %s admitted: %v", source, err)
			}
		})
	}
	for _, variant := range []string{"required", "explicit", "mismatch", "no_state", "no_match"} {
		t.Run("lower_"+variant, func(t *testing.T) {
			fields := map[string]EntityFieldDecl{"note": {Type: "text"}}
			properties := map[string]EventFieldSpec{"note": {Type: "text"}, "unmatched": {Type: "text"}}
			var required []string
			for i := 0; i < 22; i++ {
				name := fmt.Sprintf("field_%02d", i)
				fields[name] = EntityFieldDecl{Type: "integer"}
				properties[name] = EventFieldSpec{Type: "integer"}
				required = append(required, name)
			}
			required = append(required, "unmatched")
			writes := WorkflowDataAccumulation{From: "payload", fromLocation: accumulation.fromLocation}
			if variant == "explicit" || variant == "mismatch" {
				properties["field_00"] = EventFieldSpec{Type: "number"}
			}
			if variant == "explicit" {
				writes.Writes = []WorkflowDataWrite{{TargetField: "field_00", Value: LiteralExpression(7)}}
			}
			state := EntityContractsDocument{"state": {Fields: fields}}
			if variant == "no_state" {
				state = nil
			} else if variant == "no_match" {
				state = EntityContractsDocument{"state": {Fields: map[string]EntityFieldDecl{"elsewhere": {Type: "text"}}}}
			}
			bundle := &WorkflowContractBundle{
				RootSchema: &FlowSchemaDocument{}, RootEntities: state,
				Events: map[string]EventCatalogEntry{"task.requested": {Payload: EventPayloadSpec{Properties: properties, Required: required}}},
				Nodes: map[string]SystemNodeContract{"worker": {EventHandlers: map[string]SystemNodeEventHandler{
					"task.requested": {DataAccumulation: writes},
				}}},
			}
			root := &FlowContractView{
				Paths: FlowContractPaths{FlowPath: "."}, Path: ".", Schema: *bundle.RootSchema,
				Events: bundle.Events, Nodes: bundle.Nodes,
			}
			bundle.FlowTree = FlowTree{Root: root, ByID: map[string]*FlowContractView{".": root}, ByPath: map[string]*FlowContractView{".": root}}
			err := CompileWorkflowSemantics(bundle)
			if variant == "mismatch" {
				if err == nil || !strings.Contains(err.Error(), "payload.field_00") || !strings.Contains(err.Error(), "state.field_00") || !strings.Contains(err.Error(), "nodes.yaml:4:13") {
					t.Fatalf("missing located exact-type refusal: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			lowered := bundle.Nodes["worker"].EventHandlers["task.requested"].DataAccumulation
			want := 22
			if variant == "no_state" || variant == "no_match" {
				want = 0
			}
			if lowered.From != "" || len(lowered.Writes) != want {
				t.Fatalf("lowered writes = %#v, want %d explicit writes and no runtime source", lowered, want)
			}
			for i, write := range lowered.Writes {
				name := fmt.Sprintf("field_%02d", i)
				if write.TargetField != name {
					t.Fatalf("write %d = %#v; optional/unmatched field copied or explicit target duplicated", i, write)
				}
				if variant == "explicit" && i == 0 {
					if write.Value.Literal != 7 || write.SourceField != "" {
						t.Fatalf("explicit write lost precedence: %#v", write)
					}
				} else if write.SourceField != name {
					t.Fatalf("write %d did not lower to same-path payload copy: %#v", i, write)
				}
			}
			for _, handlers := range bundle.Semantics.NodeHandlers {
				if effective := handlers["task.requested"].DataAccumulation; effective.From != "" || len(effective.Writes) != want {
					t.Fatalf("executable handler lost admission lowering: %#v", effective)
				}
			}
		})
	}
}

func TestProjectNodeDataAccumulationValueRejectsRetiredExpression(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("worker:\n  task.requested:\n    data_accumulation:\n      writes: [{target_field: name, expression: payload.name}]\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err == nil || !strings.Contains(err.Error(), "is not supported") || !strings.Contains(err.Error(), "nodes.yaml:4:") {
		t.Fatalf("expected source-located write retirement, got %v", err)
	}
}
