package semanticview_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"
)

var s1TriggerSelectorFrontDoors = []struct{ file, function, raw string }{
	{"internal/runtime/semanticview/scopes.go", "FlowScopeByID", "flowID"},
	{"internal/runtime/semanticview/event_proof.go", "ResolveFlowEventProof", "flowID"},
	{"internal/runtime/semanticview/event_proof.go", "CrossesDeclaredOutputBoundary", "p.FlowID"},
	{"internal/runtime/semanticview/subscription_admission.go", "ClassifyAuthoredSubscription", "req.FlowID"},
	{"internal/runtime/semanticview/subscription_admission.go", "ClassifyTimerSubscription", "flowID"},
	{"internal/runtime/semanticview/agent_subscription_admission.go", "AdmitFlowOwnedAgentSubscriptions", "req.FlowID"},
	{"internal/runtime/semanticview/endpoint_census.go", "ResolveTypedPubSubConsumerMatches", "producer.FlowID"},
	{"internal/runtime/semanticview/endpoint_census.go", "matchingEndpoints", "flowID"},
	{"internal/runtime/semanticview/endpoint_census.go", "ResolveDeclaredInputEndpoint", "flowID"},
	{"internal/runtime/core/pinrouting/pinrouting.go", "PinDeclaredOutput", "flowID"},
	{"internal/runtime/core/pinrouting/pinrouting.go", "ResolveEnvelope", "input.FlowID"},
	{"internal/runtime/bus/public_input_admission.go", "NewOrdinaryFlowAPIEventPublicationEndpoint", "flowID"},
	{"internal/runtime/pipeline/engine_adapter.go", "validatePipelineEmitPayload", "flowID"},
	{"internal/runtime/pipeline/engine_adapter.go", "pipelineEmitPayloadProperties", "flowID"},
	{"internal/runtime/manager/flow_activation.go", "validateAutoEmitPayload", "flowID"},
	{"internal/runtime/manager/agent_subscription_admission.go", "admitAgentConfigSubscriptions", "cfg.FlowID"},
	{"internal/runtime/core/actors/agent_config.go", "NormalizeRuntimeDescriptor", "cfg.FlowID"},
	{"internal/store/internal/backend/agentpersistence/projection.go", "marshalPersistedAgentRuntimeDescriptor", "cfg.FlowID"},
	{"internal/store/internal/backend/agentpersistence/projection.go", "decodePersistedAgentRuntimeDescriptor", "desc.FlowID"},
}

func TestS1TriggerSelectorFrontDoorsPreserveRawCoordinates(t *testing.T) {
	root := agentNameGuardRepoRoot(t)
	for _, owner := range s1TriggerSelectorFrontDoors {
		raw, err := os.ReadFile(filepath.Join(root, owner.file))
		if err != nil {
			t.Fatal(err)
		}
		if findings := s1TriggerSelectorNormalizations(t, raw, owner.function); len(findings) != 0 {
			t.Errorf("%s: %v", owner.file, findings)
		}
	}
}

func TestS1TriggerSelectorGuardRejectsOwnerAndWrapperNormalization(t *testing.T) {
	root := agentNameGuardRepoRoot(t)
	for _, owner := range s1TriggerSelectorFrontDoors {
		t.Run(owner.function, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(root, owner.file))
			if err != nil {
				t.Fatal(err)
			}
			file, err := parser.ParseFile(token.NewFileSet(), owner.file, raw, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, declaration := range file.Decls {
				fn, ok := declaration.(*ast.FuncDecl)
				if !ok || fn.Name.Name != owner.function {
					continue
				}
				cut := int(fn.Body.Lbrace)
				fault := string(raw[:cut]) + "\n _ = strings.TrimSpace(" + owner.raw + ")\n" + string(raw[cut:])
				if findings := s1TriggerSelectorNormalizations(t, []byte(fault), owner.function); len(findings) == 0 {
					t.Fatal("guard missed raw-coordinate normalization")
				}
				return
			}
			t.Fatal("tracked selector front door disappeared")
		})
	}
}

func TestS1TriggerSelectorGuardAllowsResolvedEventNormalization(t *testing.T) {
	raw := []byte(`package sample
func render(source Source, flowID, event string) {
    event = eventidentity.Normalize(source.ResolveFlowEventReference(flowID, event))
}`)
	if findings := s1TriggerSelectorNormalizations(t, raw, "render"); len(findings) != 0 {
		t.Fatalf("resolved event normalization classified as raw flow repair: %v", findings)
	}
}

func s1TriggerSelectorNormalizations(t *testing.T, raw []byte, function string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "selector.go", raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	var findings []string
	seen := 0
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Name.Name != function {
			continue
		}
		seen++
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !s1TriggerSelectorNormalizer(selector.Sel.Name) {
				return true
			}
			var rawFlow bool
			switch value := ast.Unparen(call.Args[0]).(type) {
			case *ast.Ident:
				rawFlow = value.Name == "flowID"
			case *ast.SelectorExpr:
				rawFlow = value.Sel.Name == "FlowID"
			}
			if rawFlow {
				findings = append(findings, function+":raw_flow_normalization")
			}
			return true
		})
	}
	if seen != 1 {
		findings = append(findings, fmt.Sprintf("%s:expected_one_front_door:got_%d", function, seen))
	}
	return findings
}

func s1TriggerSelectorNormalizer(name string) bool {
	switch name {
	case "TrimSpace", "Trim", "TrimPrefix", "TrimSuffix", "Normalize", "ToLower", "ToUpper", "Clean":
		return true
	default:
		return false
	}
}
