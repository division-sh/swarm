package cliapp

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// This test-only census is independent of bootverify's check registry. It pins
// the audited startup/refusal owners and their consumers, including local
// guards and post-registry clauses. It is not a production admission registry.
type admissionPredicateSurface struct {
	File   string
	Rows   string
	Proofs string
}

var admissionPredicateSurfaces = []admissionPredicateSurface{
	{"internal/cliapp/verify_runtime.go", "A01,A28,A29,A30", "P01,P02,P23,P24,P27,P29"},
	{"internal/cliapp/verify_deployment.go", "A02,A11,A12,A13,A15,A18,A26,A27,A28", "P02,P09,P10,P11,P13,P15,P16,P22,P24,P26"},
	{"internal/cliapp/verify_deployment_workspace.go", "A15,A16,A17", "P13,P14,P15"},
	{"internal/cliapp/verify_deployment_store.go", "A10,A19,A20,A21,A22,A23,A24,A27", "P08,P17,P18,P19,P20,P21,P27,P28"},
	{"internal/channelonboarding/retained_inspection.go", "A10,A27", "P08,P10,P21"},
	{"internal/channelonboarding/teardown.go", "A10,A27", "P08,P10,P21"},
	{"internal/serveapp/channel_onboarding.go", "A10,A27", "P08,P10,P21"},
	{"internal/cliapp/verify_retained_dependencies.go", "A11,A12,A13,A14,A22,A24,A25", "P09,P10,P11,P12,P15,P20,P21"},
	{"internal/cliapp/serve_admission.go", "A02,A26", "P02,P16"},
	{"internal/cliapp/serve_preflight_shared.go", "A02,A18,A26,A27", "P02,P15,P16"},
	{"internal/cliapp/local_preflight.go", "A15,A16,A17,A18,A27", "P13,P14,P15,P16"},
	{"internal/cliapp/workspace_backend.go", "A15", "P13,P14"},
	{"internal/cliapp/workspace_listener.go", "A15,A18", "P13,P15"},
	{"internal/serveapp/main.go", "A01,A02,A03,A04,A10,A19,A20,A21,A22,A26,A27", "P01,P02,P03,P08,P10,P16,P17,P18,P19,P20,P21,P22"},
	{"internal/runtime/runtime.go", "A05,A08,A11,A13,A18,A23,A24,A25", "P04,P08,P09,P11,P15,P20,P21"},
	{"internal/runtime/workflow_validation.go", "A05,A06,A07,A08,A09,A10,A13,A14,A29", "P00,P04,P05,P06,P07,P08,P11,P12"},
	{"internal/runtime/workflow_validation_admission.go", "A05,A06,A07,A08,A09,A10,A13,A14,A29", "P00,P04,P05,P06,P07,P08,P11,P12"},
	{"internal/runtime/bootverify/admission_report.go", "A28,A29,A30", "P23,P24"},
	{"internal/runtime/bootverify/admission_store_absence.go", "A19,A28", "2567-P02,P08,P09"},
	{"internal/runtime/runtime_claude_startup.go", "A11,A12,A18,A25", "P09,P10,P15,P21"},
	{"internal/runtime/runtime_activation_gateway.go", "A11,A18,A25", "P09,P15,P21"},
	{"internal/runtime/manager/activation_gateway.go", "A11,A18", "P09,P15"},
	{"internal/runtime/llm/mock_startup_probe.go", "A11,A18", "P09,P15"},
	{"internal/runtime/llm/workspace_mcp_admission.go", "A11,A18,A25", "P09,P15,P21"},
	{"internal/runtime/runtime_prepared_catalog.go", "A11,A13,A25", "P09,P11,P15,P21"},
	{"internal/runtime/startup_recovery_diagnostics.go", "A23,A24", "P20,P21"},
	{"internal/runtime/manager/agent_manager.go", "A11,A23,A24", "P09,P20,P21"},
	{"internal/runtime/manager/static_topology.go", "A11,A24", "P09,P20,P21"},
	{"internal/runtime/manager/retained_actor_inspection.go", "A11,A24", "P09,P20,P21"},
	{"internal/runtime/manager/recovery_inspection.go", "A23,A24", "P20,P21"},
	{"internal/runtime/runforkexecution/recovery.go", "A22,A24", "P03,P20,P21"},
	{"internal/runtime/runforkexecution/recovery_inspection.go", "A22,A24", "P03,P20,P21"},
	{"internal/runtime/runbundle/startup_admission.go", "A21", "P17,P18"},
	{"internal/runtime/startupownership/acquisition_admission.go", "A20", "P19,P22"},
	{"internal/runtime/workspace/capability_admission.go", "A15,A16,A17", "P13,P14,P15"},
	{"internal/runtime/workspace/source_admission.go", "A15", "P13,P14"},
	{"internal/runtime/llm/provider_admission.go", "A11,A12,A18,A25", "P09,P10,P15,P21"},
	{"internal/runtime/llm/cli_runtime_boot.go", "A12,A18,A25", "P09,P10,P15,P21"},
	{"internal/runtime/llm/provider_contract.go", "A11,A25", "P09,P15,P21"},
	{"internal/runtime/llm/provider_credentials.go", "A12", "P10,P21"},
	{"internal/runtime/tools/native_tools_admission.go", "A13", "P09,P11"},
	{"internal/store/selected/admission.go", "A10,A19,A20,A22,A23,A24,A27", "P08,P18,P19,P20,P21"},
	{"internal/store/internal/startupownership/sqlite_inspection_absence_unix.go", "A19,A28", "2567-P07,P08"},
	{"internal/store/internal/backend/postgres/backend.go", "A19,A20,A28", "P18,P19,P24"},
	{"internal/store/internal/backend/postgres/transaction.go", "A19,A28", "P18,P24"},
	{"internal/store/internal/backend/postgres/inspection.go", "A19,A28", "P18,P24"},
	{"internal/store/internal/backend/postgres/inspection_io.go", "A19,A28", "P18,P24"},
	{"internal/store/internal/backend/postgres/possession_observation.go", "A20,A28", "P19,P22,P24"},
	{"internal/store/internal/backend/runforkpersistence/selected_recovery.go", "A22,A24", "P20,P21"},
	{"internal/store/internal/backend/runforkpersistence/selected_recovery_inspection.go", "A22,A24", "P20,P21"},
	{"internal/store/internal/startupownership/possession_observation.go", "A20,A28", "P19,P22,P24"},
}

type admissionPredicateBody struct {
	Owner  string `json:"owner"`
	Rows   string `json:"audit_rows"`
	Proofs string `json:"proof_families"`
	SHA256 string `json:"body_sha256"`
}

func measureAdmissionPredicateBodies(root string) ([]admissionPredicateBody, error) {
	var result []admissionPredicateBody
	for _, surface := range admissionPredicateSurfaces {
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, surface.File), nil, 0)
		if err != nil {
			return nil, err
		}
		bodies, err := admissionPredicateFileBodies(surface, file)
		if err != nil {
			return nil, err
		}
		result = append(result, bodies...)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Owner < result[j].Owner })
	return result, nil
}

func admissionPredicateFileBodies(surface admissionPredicateSurface, file *ast.File) ([]admissionPredicateBody, error) {
	var result []admissionPredicateBody
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		var body bytes.Buffer
		if err := format.Node(&body, token.NewFileSet(), function); err != nil {
			return nil, err
		}
		name := function.Name.Name
		if function.Recv != nil {
			var receiver bytes.Buffer
			if err := format.Node(&receiver, token.NewFileSet(), function.Recv.List[0].Type); err != nil {
				return nil, err
			}
			name = receiver.String() + "." + name
		}
		result = append(result, admissionPredicateBody{
			Owner: surface.File + "::" + name, Rows: surface.Rows, Proofs: surface.Proofs,
			SHA256: fmt.Sprintf("%x", sha256.Sum256(body.Bytes())),
		})
	}
	return result, nil
}

func compareAdmissionPredicateBodies(expected, measured []admissionPredicateBody) error {
	want := make(map[string]admissionPredicateBody, len(expected))
	for _, body := range expected {
		if _, exists := want[body.Owner]; exists || body.Rows == "" || body.Proofs == "" {
			return fmt.Errorf("unclassified or duplicated startup predicate owner %s", body.Owner)
		}
		want[body.Owner] = body
	}
	for _, body := range measured {
		old, exists := want[body.Owner]
		if !exists || old != body {
			return fmt.Errorf("startup predicate/consumer changed: %s; refresh owner, callers and execution proof for %s (%s), not merely the check registry", body.Owner, body.Rows, body.Proofs)
		}
		delete(want, body.Owner)
	}
	if len(want) != 0 {
		return fmt.Errorf("audited startup owner/caller disappeared: %+v", want)
	}
	return nil
}

func TestVerifyBootStartupPredicateCensus(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	measured, err := measureAdmissionPredicateBodies(root)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("testdata", "verify_admission_predicate_census.json")
	if os.Getenv("SWARM_UPDATE_ADMISSION_PREDICATE_CENSUS") == "1" {
		data, err := json.MarshalIndent(measured, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(data, '\n'), 0644); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var expected []admissionPredicateBody
	if err := json.Unmarshal(data, &expected); err != nil {
		t.Fatal(err)
	}
	if err := compareAdmissionPredicateBodies(expected, measured); err != nil {
		t.Fatal(err)
	}
}

func TestAdmissionPredicateRatchetRejectsUncoveredStartupRefusal(t *testing.T) {
	surface := admissionPredicateSurface{"internal/runtime/runtime.go", "A11,A25", "P09,P15,P21"}
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join("..", "..", surface.File), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := admissionPredicateFileBodies(surface, file)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := parser.ParseExpr(`func() { if true { panic("unaccounted new startup refusal") } }`)
	if err != nil {
		t.Fatal(err)
	}
	changed := false
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok && function.Name.Name == "newRuntime" {
			function.Body.List = append(guard.(*ast.FuncLit).Body.List, function.Body.List...)
			changed = true
		}
	}
	if !changed {
		t.Fatal("production startup entrypoint was not found")
	}
	measured, err := admissionPredicateFileBodies(surface, file)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(expected, measured) {
		t.Fatal("production guard mutation did not affect the independent census")
	}
	if err := compareAdmissionPredicateBodies(expected, measured); err == nil || !strings.Contains(err.Error(), "runtime.go::newRuntime") || !strings.Contains(err.Error(), "P09") {
		t.Fatalf("uncovered boot-local guard escaped owner/caller/proof ratchet: %v", err)
	}
}

func TestAdmissionPredicateRatchetRejectsRemovedVerifyConsumer(t *testing.T) {
	surface := admissionPredicateSurface{"internal/cliapp/verify_deployment.go", "A11,A25", "P09,P15,P21"}
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join("..", "..", "internal", "cliapp", "verify_deployment.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := admissionPredicateFileBodies(surface, file)
	if err != nil {
		t.Fatal(err)
	}
	removed := false
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "ValidateDeclaredAgentModelAdmission" {
			call.Fun = ast.NewIdent("omittedSharedModelPredicate")
			removed = true
		}
		return true
	})
	if !removed {
		t.Fatal("actual shared verify consumer disappeared before the mutation proof")
	}
	measured, err := admissionPredicateFileBodies(surface, file)
	if err != nil {
		t.Fatal(err)
	}
	if err := compareAdmissionPredicateBodies(expected, measured); err == nil || !strings.Contains(err.Error(), "inspectVerifySourceAdmission") {
		t.Fatalf("removed verify consumer escaped independent census: %v", err)
	}
}

func TestAdmissionPredicateRatchetRejectsUncoveredProviderPrerequisite(t *testing.T) {
	var surface admissionPredicateSurface
	for _, candidate := range admissionPredicateSurfaces {
		if candidate.File == "internal/runtime/llm/cli_runtime_boot.go" {
			surface = candidate
			break
		}
	}
	if surface.File == "" {
		t.Fatal("provider prerequisite owner is missing from the independent census")
	}
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join("..", "..", surface.File), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := admissionPredicateFileBodies(surface, file)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := parser.ParseExpr(`func() { if true { panic("unaccounted provider prerequisite") } }`)
	if err != nil {
		t.Fatal(err)
	}
	changed := false
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok && function.Name.Name == "ValidateClaudeCLIRuntimeConfig" {
			function.Body.List = append(guard.(*ast.FuncLit).Body.List, function.Body.List...)
			changed = true
		}
	}
	if !changed {
		t.Fatal("production provider prerequisite entrypoint was not found")
	}
	measured, err := admissionPredicateFileBodies(surface, file)
	if err != nil {
		t.Fatal(err)
	}
	if err := compareAdmissionPredicateBodies(expected, measured); err == nil || !strings.Contains(err.Error(), "ValidateClaudeCLIRuntimeConfig") || !strings.Contains(err.Error(), "P15") {
		t.Fatalf("uncovered provider prerequisite escaped owner/caller/proof ratchet: %v", err)
	}
}
