package bootverify

import (
	"context"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestExecutableReaderCensusClassifiesEveryHandlerAndRuleField(t *testing.T) {
	assertExecutableReaderCensusFields(t, reflect.TypeOf(runtimecontracts.SystemNodeEventHandler{}), systemNodeEventHandlerExecutableReaderCensus)
	assertExecutableReaderCensusFields(t, reflect.TypeOf(runtimecontracts.HandlerRuleEntry{}), handlerRuleEntryExecutableReaderCensus)
}

func TestExecutableReaderCensusCoversEveryReaderFamily(t *testing.T) {
	entityRef := runtimecontracts.RefExpression("entity.verticals")
	entityCEL := runtimecontracts.CELExpression("entity.verticals")
	tests := []struct {
		name    string
		handler runtimecontracts.SystemNodeEventHandler
	}{
		{name: "action input", handler: runtimecontracts.SystemNodeEventHandler{Action: runtimecontracts.ActionSpec{InstanceIDFrom: "entity.verticals"}}},
		{name: "activity input", handler: runtimecontracts.SystemNodeEventHandler{Activity: runtimecontracts.ActivitySpec{Input: map[string]runtimecontracts.ExpressionValue{"value": entityRef}}}},
		{name: "select binding", handler: runtimecontracts.SystemNodeEventHandler{SelectEntity: &runtimecontracts.SelectEntitySpec{Bindings: []runtimecontracts.SelectEntityKeyBinding{{Field: "id", Ref: "entity.verticals"}}}}},
		{name: "select or create binding", handler: runtimecontracts.SystemNodeEventHandler{SelectOrCreateEntity: &runtimecontracts.SelectOrCreateEntitySpec{Bindings: []runtimecontracts.SelectEntityKeyBinding{{Field: "id", Ref: "entity.verticals"}}}}},
		{name: "emit field", handler: runtimecontracts.SystemNodeEventHandler{Emit: runtimecontracts.EmitSpec{Fields: map[string]runtimecontracts.ExpressionValue{"value": entityCEL}}}},
		{name: "guard", handler: runtimecontracts.SystemNodeEventHandler{Guard: &runtimecontracts.GuardSpec{Check: "size(entity.verticals) > 0"}}},
		{name: "direct write source", handler: runtimecontracts.SystemNodeEventHandler{DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{{SourceField: "entity.verticals", TargetRef: "metadata.copy"}}}}},
		{name: "direct write ref value", handler: runtimecontracts.SystemNodeEventHandler{DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{{TargetRef: "metadata.copy", Value: entityRef}}}}},
		{name: "condition", handler: runtimecontracts.SystemNodeEventHandler{Condition: "size(entity.verticals) > 0"}},
		{name: "logic", handler: runtimecontracts.SystemNodeEventHandler{Logic: "entity.verticals"}},
		{name: "loop source", handler: runtimecontracts.SystemNodeEventHandler{Loop: &runtimecontracts.LoopOperationSpec{From: "entity.verticals"}}},
		{name: "rule action", handler: runtimecontracts.SystemNodeEventHandler{Rules: []runtimecontracts.HandlerRuleEntry{{Action: runtimecontracts.ActionSpec{InstanceIDFrom: "entity.verticals"}}}}},
		{name: "on complete activity", handler: runtimecontracts.SystemNodeEventHandler{OnComplete: []runtimecontracts.HandlerRuleEntry{{Activity: runtimecontracts.ActivitySpec{Input: map[string]runtimecontracts.ExpressionValue{"value": entityRef}}}}}},
		{name: "accumulate", handler: runtimecontracts.SystemNodeEventHandler{Accumulate: &runtimecontracts.AccumulateSpec{From: "entity.verticals"}}},
		{name: "join members by", handler: runtimecontracts.SystemNodeEventHandler{Join: &runtimecontracts.JoinSpec{Members: runtimecontracts.JoinMembersSpec{By: "entity.verticals"}}}},
		{name: "join window", handler: runtimecontracts.SystemNodeEventHandler{Join: &runtimecontracts.JoinSpec{Window: &runtimecontracts.JoinWindowSpec{From: "entity.verticals"}}}},
		{name: "join completion", handler: runtimecontracts.SystemNodeEventHandler{Join: &runtimecontracts.JoinSpec{CompleteWhen: "size(entity.verticals) > 0"}}},
		{name: "compute lookup", handler: runtimecontracts.SystemNodeEventHandler{Compute: &runtimecontracts.ComputeSpec{Lookup: &runtimecontracts.ComputeLookupSpec{On: []string{"entity.verticals"}}}}},
		{name: "compute validation", handler: runtimecontracts.SystemNodeEventHandler{Compute: &runtimecontracts.ComputeSpec{Validation: &runtimecontracts.ComputeValidationSpec{Input: map[string]string{"value": "entity.verticals"}}}}},
		{name: "compute module", handler: runtimecontracts.SystemNodeEventHandler{Compute: &runtimecontracts.ComputeSpec{Module: &runtimecontracts.ComputeModuleSpec{Input: map[string]string{"value": "entity.verticals"}}}}},
		{name: "query select", handler: runtimecontracts.SystemNodeEventHandler{Query: &runtimecontracts.QuerySpec{Select: []string{"entity.verticals"}}}},
		{name: "fan out", handler: runtimecontracts.SystemNodeEventHandler{FanOut: &runtimecontracts.FanOutSpec{ItemsFrom: "entity.verticals"}}},
		{name: "group by key", handler: runtimecontracts.SystemNodeEventHandler{GroupBy: &runtimecontracts.GroupBySpec{Key: "entity.verticals"}}},
		{name: "filter", handler: runtimecontracts.SystemNodeEventHandler{Filter: &runtimecontracts.FilterSpec{Source: "entity.verticals"}}},
		{name: "reduce param", handler: runtimecontracts.SystemNodeEventHandler{Reduce: &runtimecontracts.ReduceSpec{Params: map[string]runtimecontracts.ExpressionValue{"value": entityRef}}}},
		{name: "count", handler: runtimecontracts.SystemNodeEventHandler{Count: &runtimecontracts.CountSpec{ItemsFrom: "entity.verticals"}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			readers := handlerExecutableReaderExpressionsForSource(nil, "coordinator", "node", "event", tc.handler)
			for _, reader := range readers {
				if reader.Expression == "entity.verticals" || reader.Expression == "size(entity.verticals) > 0" {
					return
				}
			}
			t.Fatalf("reader census omitted entity.verticals: %#v", readers)
		})
	}
}

func TestExecutableReaderCensusPreservesExecutionPhases(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{
		Accumulate: &runtimecontracts.AccumulateSpec{From: "entity.status"},
		GroupBy:    &runtimecontracts.GroupBySpec{Key: "entity.status"},
		Compute:    &runtimecontracts.ComputeSpec{Lookup: &runtimecontracts.ComputeLookupSpec{On: []string{"entity.status"}}},
		FanOut:     &runtimecontracts.FanOutSpec{ItemsFrom: "entity.items"},
		OnComplete: []runtimecontracts.HandlerRuleEntry{{Condition: "entity.status == 'done'"}},
	}
	want := map[string]runtimepipeline.WorkflowEntityFieldLifecyclePhase{
		"accumulate.from":          runtimepipeline.WorkflowEntityFieldLifecycleAccumulate,
		"group_by.key":             runtimepipeline.WorkflowEntityFieldLifecycleGroupBy,
		"compute.lookup.on[0]":     runtimepipeline.WorkflowEntityFieldLifecycleCompute,
		"fan_out.items_from":       runtimepipeline.WorkflowEntityFieldLifecycleFanOut,
		"on_complete[0].condition": runtimepipeline.WorkflowEntityFieldLifecycleOnComplete,
	}
	for _, reader := range handlerExecutableReaderExpressionsForSource(nil, "flow", "node", "event", handler) {
		phase, ok := want[reader.Kind]
		if !ok {
			continue
		}
		if reader.Phase != phase {
			t.Errorf("reader %s phase = %s, want %s", reader.Kind, reader.Phase, phase)
		}
		delete(want, reader.Kind)
	}
	if len(want) > 0 {
		t.Fatalf("reader census omitted phase rows: %#v", want)
	}
}

func TestRun_CompleteReaderCensusOwnsEntityReferenceValidation(t *testing.T) {
	tests := []struct {
		name     string
		handler  runtimecontracts.SystemNodeEventHandler
		wantKind string
	}{
		{
			name: "group by key",
			handler: runtimecontracts.SystemNodeEventHandler{GroupBy: &runtimecontracts.GroupBySpec{
				ItemsFrom: "payload.items",
				Key:       "entity.missing",
				StoreAs:   "metadata.grouped",
			}},
			wantKind: "group_by.key",
		},
		{
			name: "direct expression value ref",
			handler: runtimecontracts.SystemNodeEventHandler{DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
				Writes: []runtimecontracts.WorkflowDataWrite{{
					TargetRef: "metadata.copy",
					Value:     runtimecontracts.RefExpression("entity.missing"),
				}},
			}},
			wantKind: "data_accumulation.writes[0].value.ref",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			report := Run(context.Background(), semanticview.Wrap(handBuiltScopedReaderBundle(tc.handler)), Options{})
			if !findingContainsAll(report.Errors(), "expression_field_reference_validation", "entity.missing", tc.wantKind, "root-reader") {
				t.Fatalf("expression reference findings = %#v, want entity.missing in %s at root-reader", report.Errors(), tc.wantKind)
			}
		})
	}
}

func TestRun_ExpressionValidationPreservesDuplicateScopedNodeIDs(t *testing.T) {
	repoRoot := repoRootForBootverifyTest(t)
	root := canonicalrouting.CopyDuplicateScopedSingletonDemand(t)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "a", "nodes.yaml"), `
shared-node:
  id: shared-node
  execution_type: system_node
  subscribes_to: [item.received]
  event_handlers:
    item.received:
      group_by:
        items_from: payload.items
        key: entity.missing
        store_as: metadata.grouped
`)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !findingContainsAll(report.Errors(), "expression_field_reference_validation", "flow a", "shared-node", "group_by.key", "entity.missing") {
		t.Fatalf("expression reference findings = %#v, want exact flow a shared-node group_by.key failure", report.Errors())
	}
}

func TestHandBuiltScopedNodeRecordsReachReaderAndWriteConsumers(t *testing.T) {
	bundle := handBuiltScopedReaderBundle(runtimecontracts.SystemNodeEventHandler{
		GroupBy: &runtimecontracts.GroupBySpec{
			ItemsFrom: "payload.items",
			Key:       "entity.status",
			StoreAs:   "metadata.grouped",
		},
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{
			{
				Operation: runtimecontracts.WorkflowDataOperationDelete,
				TargetRef: "entity.items",
				Key:       runtimecontracts.LiteralExpression("expired"),
			},
			{SourceField: "status", TargetRef: "entity.status"},
		}},
	})
	source := semanticview.Wrap(bundle)
	if _, _, err := wave1ResolveEntityPathWithOwner(source, "", "entity.status"); err != nil {
		t.Fatalf("resolve root entity.status: %v", err)
	}

	coverage := wave1EntityReaderCoverageByFlow(source)
	if _, ok := coverage[""]["status"]; !ok {
		t.Fatalf("entity reader coverage = %#v, want root entity.status", coverage)
	}
	operations := wave1ContainedStateOperations(source)
	if len(operations) != 1 || operations[0].FlowID != "" || operations[0].NodeID != "root-reader" || operations[0].SourceFile != "root/nodes.yaml" || operations[0].Write.Target() != "entity.items" {
		t.Fatalf("contained operations = %#v, want exact hand-built root scope", operations)
	}
	writes := wave1AllEntityWriteTargets(source)
	foundDirectWrite := false
	for _, write := range writes {
		if write.FlowID == "" && write.NodeID == "root-reader" && write.SourceFile == "root/nodes.yaml" && write.Target == "entity.status" {
			foundDirectWrite = true
		}
	}
	if !foundDirectWrite {
		t.Fatalf("entity writes = %#v, want exact hand-built root entity.status write", writes)
	}
}

func handBuiltScopedReaderBundle(handler runtimecontracts.SystemNodeEventHandler) *runtimecontracts.WorkflowContractBundle {
	root := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{PackageKey: "root", NodesFile: "root/nodes.yaml"},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"root-reader": {
				ID:            "embedded-id-is-not-authority",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"item.received": handler},
			},
		},
	}
	return &runtimecontracts.WorkflowContractBundle{
		RootEntities: runtimecontracts.EntityContractsDocument{
			"state": {Fields: map[string]runtimecontracts.EntityFieldDecl{
				"items":  {Type: "map[text]text"},
				"status": {Type: "text"},
			}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"item.received": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
				"items":  {Type: "[text]"},
				"status": {Type: "text"},
			}}},
		},
		FlowTree: runtimecontracts.FlowTree{Root: &root},
	}
}

func findingContainsAll(findings []Finding, checkID string, values ...string) bool {
	for _, finding := range findings {
		if finding.CheckID != checkID {
			continue
		}
		text := finding.Message + " " + finding.Location
		matched := true
		for _, value := range values {
			if !strings.Contains(text, value) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func assertExecutableReaderCensusFields[T any](t *testing.T, typ reflect.Type, census map[string]T) {
	t.Helper()
	want := make([]string, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		want = append(want, typ.Field(i).Name)
	}
	got := make([]string, 0, len(census))
	for field := range census {
		got = append(got, field)
	}
	sort.Strings(want)
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reader census fields = %#v, want %#v", got, want)
	}
}
