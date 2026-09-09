package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/flowdata"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pythonmodule"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Companion to releasee2e.TestDurableDataInvocationInvarianceSQLitePostgres:
// public failed-delivery classification alone cannot identify mock schema rejection.
func TestStaticDataInvocationMockRequestsExactForeignID(t *testing.T) {
	repo := runtimepipeline.WorkflowRepoRoot()
	root := filepath.Join(repo, "internal/releasee2e/testdata/static_data_invocation")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	actors := map[string]models.AgentConfig{}
	var mock mockperformance.Performance
	for _, declaration := range semanticview.AgentDeclarations(source) {
		if declaration.LocalID != "reader" {
			continue
		}
		plan, err := semanticview.ScopedAgentNamePlan(source, declaration)
		if err != nil {
			t.Fatal(err)
		}
		actors[declaration.OwnerFlowID] = models.AgentConfig{
			ID: plan.AgentID, Role: declaration.Entry.Role, FlowID: declaration.OwnerFlowID,
			FlowPath: declaration.OwnerFlowID, ExecutionMode: "mock",
		}
		if declaration.OwnerFlowID == "." {
			mock = declaration.Entry.Mock
		}
	}
	executor := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})
	rootActor, childActor := actors["."], actors["registry"]
	own := flowdata.AllowedStaticData(source, rootActor)
	child := flowdata.AllowedStaticData(source, childActor)
	if len(own) != 1 || len(child) != 1 || own[0].StaticID == child[0].StaticID || len(mock.Source) == 0 {
		t.Fatalf("fixture must admit distinct root/child data and the real mock: own=%v child=%v", own, child)
	}
	var schema map[string]any
	var deliveredTools []map[string]any
	for _, tool := range executor.ToolDefinitionsForActor(rootActor) {
		deliveredTools = append(deliveredTools, map[string]any{"name": tool.Name, "schema": tool.Schema})
		if tool.Name == flowdata.ToolName {
			schema = tool.Schema.(map[string]any)
		}
	}
	if schema == nil {
		t.Fatal("fixture root reader has no generated read_flow_data")
	}
	for _, tc := range []struct{ name, foreign, expected string }{
		{"own", "", string(own[0].StaticID)},
		{"foreign", string(child[0].StaticID), string(child[0].StaticID)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame, err := json.Marshal(map[string]any{"event": map[string]any{
				"type": "read.work", "payload": map[string]any{"foreign_id": tc.foreign},
			}})
			if err != nil {
				t.Fatal(err)
			}
			request, err := json.Marshal(map[string]any{
				"messages": []map[string]any{{"role": "user", "content": string(frame)}},
				"tools":    deliveredTools, "tool_results": nil, "round": 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := pythonmodule.Execute(context.Background(), pythonmodule.Request{
				ModuleID: "agent.mock." + rootActor.ID, RowID: mock.SourcePath, Digest: mock.Digest,
				Entry: mockperformance.EntryHandle, Source: mock.Source, Input: request,
				Fuel: mockperformance.ExecutionFuel, MemoryPages: mockperformance.ExecutionMemoryPages,
				OutputBytes: mockperformance.ExecutionOutputBytes,
			})
			if err != nil {
				t.Fatal(err)
			}
			args, err := exactStaticInvocationRead(result.Output, tc.expected)
			if err != nil {
				t.Fatal(err)
			}
			// This is the validator called by llm.parseMockCompletionOutput, with
			// the actual executor-generated schema rather than a copied enum.
			err = eventschema.ValidateValueAgainstSchema(schema, args)
			if tc.foreign == "" {
				if err != nil {
					t.Fatalf("own static ID rejected: %v", err)
				}
			} else {
				want := "schema validation failed: $.static_id has invalid enum value " + tc.foreign
				if err == nil || err.Error() != want {
					t.Fatalf("foreign rejection = %v, want exactly %q", err, want)
				}
			}
		})
	}
}

func exactStaticInvocationRead(raw []byte, id string) (map[string]any, error) {
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		return nil, err
	}
	args := map[string]any{"kind": "static_file", "static_id": id}
	want := map[string]any{"calls": []any{map[string]any{"name": "read_flow_data", "arguments": args}}}
	if !reflect.DeepEqual(got, want) {
		return nil, fmt.Errorf("mock did not request the exact static read: got %#v, want %#v", got, want)
	}
	return args, nil
}

func TestStaticDataInvocationNegativeOracleRejectsUnrelatedCalls(t *testing.T) {
	for _, raw := range []string{
		`{"calls":[{"name":"review_nonexistent_tool","arguments":{}}]}`,
		`{"calls":[{"name":"read_flow_data","arguments":{"kind":"static_file","static_id":"own-id"}}]}`,
		`{"text":"No foreign read attempted."}`,
	} {
		if _, err := exactStaticInvocationRead([]byte(raw), "child-id"); err == nil || !strings.Contains(err.Error(), "exact static read") {
			t.Fatalf("unrelated mock output accepted: %s, error=%v", raw, err)
		}
	}
}
