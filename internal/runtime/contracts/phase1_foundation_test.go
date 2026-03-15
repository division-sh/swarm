package contracts

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestPhase1SemanticModelUsesTypedContracts(t *testing.T) {
	t.Parallel()

	expectFieldType(t, reflect.TypeOf(SystemNodeEventHandler{}), "Guard", reflect.TypeOf((*GuardSpec)(nil)))
	expectFieldType(t, reflect.TypeOf(SystemNodeEventHandler{}), "Accumulate", reflect.TypeOf((*AccumulateSpec)(nil)))
	expectFieldType(t, reflect.TypeOf(SystemNodeEventHandler{}), "Compute", reflect.TypeOf((*ComputeSpec)(nil)))
	expectFieldType(t, reflect.TypeOf(SystemNodeEventHandler{}), "FanOut", reflect.TypeOf((*FanOutSpec)(nil)))
	expectFieldType(t, reflect.TypeOf(SystemNodeEventHandler{}), "OnComplete", reflect.TypeOf([]HandlerRuleEntry{}))
	expectFieldType(t, reflect.TypeOf(SystemNodeEventHandler{}), "Rules", reflect.TypeOf([]HandlerRuleEntry{}))
	expectFieldType(t, reflect.TypeOf(SystemNodeEventHandler{}), "Filter", reflect.TypeOf((*FilterSpec)(nil)))
	expectFieldType(t, reflect.TypeOf(SystemNodeEventHandler{}), "Reduce", reflect.TypeOf((*ReduceSpec)(nil)))
	expectFieldType(t, reflect.TypeOf(SystemNodeEventHandler{}), "Count", reflect.TypeOf((*CountSpec)(nil)))
	expectFieldType(t, reflect.TypeOf(SystemNodeEventHandler{}), "Clear", reflect.TypeOf((*ClearSpec)(nil)))
	expectFieldType(t, reflect.TypeOf(SystemNodeEventHandler{}), "PayloadTransform", reflect.TypeOf((*PayloadTransformSpec)(nil)))
	expectFieldType(t, reflect.TypeOf(SystemNodeEventHandler{}), "Branch", reflect.TypeOf([]BranchSpec{}))
	expectFieldType(t, reflect.TypeOf(SystemNodeEventHandler{}), "SetsGate", reflect.TypeOf((*GateSpec)(nil)))
	expectFieldType(t, reflect.TypeOf(SystemNodeEventHandler{}), "ClearGates", reflect.TypeOf([]string{}))
	expectFieldType(t, reflect.TypeOf(SystemNodeEventHandler{}), "Query", reflect.TypeOf((*QuerySpec)(nil)))
	expectFieldType(t, reflect.TypeOf(SystemNodeEventHandler{}), "Action", reflect.TypeOf(ActionSpec{}))
	expectFieldType(t, reflect.TypeOf(HandlerTransitionSemantic{}), "Action", reflect.TypeOf(ActionSpec{}))
	expectMissingField(t, reflect.TypeOf(SystemNodeEventHandler{}), "Template")
	expectMissingField(t, reflect.TypeOf(SystemNodeEventHandler{}), "InstanceIDFrom")
	expectMissingField(t, reflect.TypeOf(SystemNodeEventHandler{}), "ConfigFrom")
	expectMissingField(t, reflect.TypeOf(HandlerTransitionSemantic{}), "Template")
	expectMissingField(t, reflect.TypeOf(HandlerTransitionSemantic{}), "InstanceIDFrom")
	expectMissingField(t, reflect.TypeOf(HandlerTransitionSemantic{}), "ConfigFrom")

	expectFieldType(t, reflect.TypeOf(ReduceSpec{}), "Params", reflect.TypeOf(map[string]ExpressionValue{}))
	expectFieldType(t, reflect.TypeOf(WorkflowTransitionContract{}), "From", reflect.TypeOf([]string{}))
	expectFieldType(t, reflect.TypeOf(WorkflowDataAccumulation{}), "Value", reflect.TypeOf(ExpressionValue{}))
	expectFieldType(t, reflect.TypeOf(WorkflowDataWrite{}), "Value", reflect.TypeOf(ExpressionValue{}))
	expectFieldType(t, reflect.TypeOf(FlowInstanceVariables{}), "Variables", reflect.TypeOf(map[string]FlowVariable{}))
	expectFieldType(t, reflect.TypeOf(ToolSchemaEntry{}), "InputSchema", reflect.TypeOf(ToolInputSchema{}))
	expectFieldType(t, reflect.TypeOf(SystemNodeContract{}), "StateSchema", reflect.TypeOf(NodeStateSchema{}))
	expectFieldType(t, reflect.TypeOf(EventCatalogEntry{}), "Emitter", reflect.TypeOf(EventEmitterRef{}))
	expectFieldType(t, reflect.TypeOf(EventCatalogEntry{}), "Consumer", reflect.TypeOf([]string{}))
	expectFieldType(t, reflect.TypeOf(EventCatalogEntry{}), "ConsumerType", reflect.TypeOf([]string{}))
	expectFieldType(t, reflect.TypeOf(EventCatalogEntry{}), "DeliveryChannel", reflect.TypeOf(""))
	expectFieldType(t, reflect.TypeOf(EventCatalogEntry{}), "Payload", reflect.TypeOf(EventPayloadSpec{}))
}

func TestPhase1SchemaRegistryUsesMASContractsSource(t *testing.T) {
	t.Parallel()

	bundle, err := LoadWorkflowContractBundle(repoRootForContractsTest(t))
	if err != nil {
		t.Skipf("no contract bundle: %v", err)
	}
	registry := EventSchemaRegistryFromCatalog(bundle.EventEntries())
	if len(registry) == 0 {
		t.Fatal("expected dynamic event schema registry entries from contract bundle")
	}
	for eventType, schema := range registry {
		if schema.Schema == nil {
			t.Fatalf("schema for %s has nil Schema map", eventType)
		}
		props, _ := schema.Schema["properties"].(map[string]any)
		if props == nil {
			t.Fatalf("schema for %s has no properties", eventType)
		}
	}
}

func TestPhase1DefaultContractPathsAreMASOnly(t *testing.T) {
	t.Parallel()

	repoRoot := repoRootForContractsTest(t)
	paths := ResolveWorkflowContractPaths(repoRoot)

	got := filepath.ToSlash(DefaultWorkflowContractsDir(repoRoot))
	if got == "" {
		t.Fatal("expected default workflow contracts dir to resolve from MAS product bundles")
	}
	if !strings.Contains(got, "/docs/specs/mas-platform/") && !strings.HasPrefix(got, "docs/specs/mas-platform/") {
		t.Fatalf("unexpected non-MAS default workflow contracts dir %q", got)
	}
	if strings.Contains(got, "/platform/contracts") || strings.HasSuffix(got, "/tests/test-guard-pass") || strings.Contains(got, "/tests/") {
		t.Fatalf("unexpected non-product workflow contracts dir %q", got)
	}
	if strings.Contains(filepath.ToSlash(paths.WorkflowDir), "/contracts/") && !strings.Contains(filepath.ToSlash(paths.WorkflowDir), "/docs/specs/mas-platform/") {
		t.Fatalf("unexpected legacy workflow dir %q", paths.WorkflowDir)
	}
	for _, candidate := range []string{
		paths.ProjectNodesFile,
		paths.ProjectEventsFile,
		paths.ProjectAgentsFile,
		paths.ProjectToolsFile,
		paths.ProjectPolicyFile,
	} {
		if strings.TrimSpace(candidate) == "" {
			t.Fatalf("unexpected empty contract candidate")
		}
	}
	if strings.TrimSpace(paths.DDLFile) != "" {
		t.Fatalf("expected no legacy canonical DDL authority path, got %q", paths.DDLFile)
	}
}

func expectFieldType(t *testing.T, typ reflect.Type, fieldName string, want reflect.Type) {
	t.Helper()

	field, ok := typ.FieldByName(fieldName)
	if !ok {
		t.Fatalf("%s missing field %s", typ.Name(), fieldName)
	}
	if field.Type != want {
		t.Fatalf("%s.%s type mismatch: got %s want %s", typ.Name(), fieldName, field.Type, want)
	}
}

func expectMissingField(t *testing.T, typ reflect.Type, fieldName string) {
	t.Helper()

	if _, ok := typ.FieldByName(fieldName); ok {
		t.Fatalf("%s unexpectedly still has field %s", typ.Name(), fieldName)
	}
}
