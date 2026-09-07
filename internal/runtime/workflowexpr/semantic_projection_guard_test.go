package workflowexpr

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestWorkflowCELProjectionConsumersStayOnCanonicalOwner(t *testing.T) {
	runtimeRoot := workflowProjectionRuntimeRoot(t)
	files := map[string]string{
		"workflowexpr":   filepath.Join(runtimeRoot, "workflowexpr", "data_expression.go"),
		"pipeline":       filepath.Join(runtimeRoot, "pipeline", "workflow_expression_evaluator.go"),
		"pipeline_query": filepath.Join(runtimeRoot, "pipeline", "engine_adapter.go"),
		"engine_scope":   filepath.Join(runtimeRoot, "engine", "helpers.go"),
		"engine_sites":   filepath.Join(runtimeRoot, "engine", "executor.go"),
	}
	sources := map[string]string{}
	for name, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sources[name] = string(raw)
		for _, violation := range workflowProjectionGuardViolations(string(raw), name == "workflowexpr") {
			t.Errorf("%s: %s", name, violation)
		}
	}

	for name, required := range map[string][]string{
		"workflowexpr":   {"func ProjectCELValue", "ProjectCELValue(out)"},
		"pipeline":       {"projectWorkflowExpressionContext", "workflowexpr.ProjectCELValue"},
		"pipeline_query": {"func (e pipelineEngineEvaluator) queryEntityCount", "projectWorkflowExpressionContext(ctx)"},
		"engine_scope":   {"func newExecutionScope", "workflowexpr.ProjectCELValue", "compiledExecutionCondition"},
	} {
		for _, token := range required {
			if !strings.Contains(sources[name], token) {
				t.Errorf("%s stopped consuming canonical workflow/CEL projection %q", name, token)
			}
		}
	}
	if got := strings.Count(sources["engine_sites"], "newExecutionScope("); got != 5 {
		t.Errorf("engine list-consumer projection callsites = %d, want exact query-filter/query-group/filter/count/group set of 5", got)
	}
}

func TestWorkflowCELProjectionJSONSourcesPreserveNumberLexemes(t *testing.T) {
	runtimeRoot := workflowProjectionRuntimeRoot(t)
	repoRoot := filepath.Clean(filepath.Join(runtimeRoot, "..", ".."))
	boundaries := []struct {
		name, path, function string
		streaming            bool
		projects             bool
	}{
		{name: "ordinary event payload", path: filepath.Join(runtimeRoot, "engine", "executor.go"), function: "decodePayload", projects: true},
		{name: "persisted workflow state", path: filepath.Join(runtimeRoot, "pipeline", "workflow_instance_store.go"), function: "decodeWorkflowInstanceJSONMap", projects: true},
		{name: "deferred trigger payload", path: filepath.Join(runtimeRoot, "engine", "fan_out_evaluator.go"), function: "decodeFanOutPayload"},
		{name: "persisted fan-out capsule", path: filepath.Join(repoRoot, "internal", "store", "internal", "backend", "pipelinepersistence", "fan_out_owner.go"), function: "scanFanOutIntent"},
		{name: "run-fork fan-out capsule projection", path: filepath.Join(repoRoot, "internal", "store", "internal", "backend", "runforkpersistence", "run_fork_fan_out_projection.go"), function: "loadRunForkFanOutObligationsFromRevision"},
		{name: "materialized fork fan-out capsule verification", path: filepath.Join(repoRoot, "internal", "store", "internal", "backend", "runforkpersistence", "run_fork_fan_out_materializer.go"), function: "requireExactMaterializedRunForkFanOut"},
		{name: "exact entity revision", path: filepath.Join(repoRoot, "internal", "store", "internal", "backend", "pipelinepersistence", "fan_out_owner.go"), function: "collectionRangeFromJSON", streaming: true},
		{name: "pinned resource version", path: filepath.Join(repoRoot, "internal", "store", "internal", "backend", "pipelinepersistence", "fan_out_owner.go"), function: "collectionRangeFromJSONL", streaming: true},
	}
	for _, boundary := range boundaries {
		t.Run(boundary.name, func(t *testing.T) {
			raw, err := os.ReadFile(boundary.path)
			if err != nil {
				t.Fatal(err)
			}
			canonical, projected, useNumber, unmarshal, found := workflowProjectionSourceBoundaryFacts(string(raw), boundary.function)
			if !found || unmarshal || boundary.streaming && !useNumber || !boundary.streaming && !canonical || boundary.projects && !projected {
				t.Fatalf("source boundary %s facts = found:%v canonical:%v projected:%v use_number:%v unmarshal:%v", boundary.function, found, canonical, projected, useNumber, unmarshal)
			}
		})
	}
}

func TestWorkflowCELProjectionJSONWritersPreserveNumberKinds(t *testing.T) {
	runtimeRoot := workflowProjectionRuntimeRoot(t)
	repoRoot := filepath.Clean(filepath.Join(runtimeRoot, "..", ".."))
	boundaries := []struct {
		name, path, function string
	}{
		{name: "delivery payload projection", path: filepath.Join(repoRoot, "internal", "events", "semantic_boundary.go"), function: "NewDeliveryEvent"},
		{name: "external event publication", path: filepath.Join(repoRoot, "internal", "apiv1", "operator_event_publish.go"), function: "eventPublicationPayload"},
		{name: "ordinary event payload", path: filepath.Join(runtimeRoot, "engine", "executor.go"), function: "encodePayload"},
		{name: "dynamic flow auto-emit payload", path: filepath.Join(runtimeRoot, "manager", "flow_activation.go"), function: "buildDynamicFlowRuntimeCreationEventPlan"},
		{name: "activity result payload", path: filepath.Join(runtimeRoot, "pipeline", "activity_engine.go"), function: "publishActivityResultWithID"},
		{name: "workflow engine state", path: filepath.Join(runtimeRoot, "pipeline", "engine_mutation_commit.go"), function: "workflowEngineStateRecord"},
		{name: "workflow activation state", path: filepath.Join(runtimeRoot, "pipeline", "workflow_instance_activation.go"), function: "PersistenceRecord"},
		{name: "initial workflow projection", path: filepath.Join(runtimeRoot, "pipeline", "workflow_initial_materialization_commit.go"), function: "workflowInitialMaterializationRecord"},
		{name: "fan-out capsule", path: filepath.Join(repoRoot, "internal", "store", "internal", "backend", "pipelinepersistence", "fan_out_obligation.go"), function: "commitFanOutIntentTx"},
		{name: "entity source revision", path: filepath.Join(repoRoot, "internal", "store", "internal", "backend", "pipelinepersistence", "fan_out_obligation.go"), function: "insertFanOutEntitySourceRevisionTx"},
		{name: "fork fan-out capsule", path: filepath.Join(repoRoot, "internal", "store", "internal", "backend", "runforkpersistence", "run_fork_fan_out_materializer.go"), function: "materializeRunForkFanOutObligations"},
		{name: "selected-contract fork event route", path: filepath.Join(runtimeRoot, "runforkexecution", "runtime_container.go"), function: "projectSelectedContractSourceEventWorkflowStates"},
		{name: "selected-contract fork event mutation", path: filepath.Join(repoRoot, "internal", "store", "internal", "backend", "runforkpersistence", "run_fork_selected_contract_execution_mutation.go"), function: "projectRunForkSelectedContractSourceEventWorkflowState"},
	}
	for _, boundary := range boundaries {
		t.Run(boundary.name, func(t *testing.T) {
			raw, err := os.ReadFile(boundary.path)
			if err != nil {
				t.Fatal(err)
			}
			preserving, erasing, found := workflowProjectionWriterBoundaryFacts(string(raw), boundary.function)
			if !found || !preserving || erasing {
				t.Fatalf("writer boundary %s facts = found:%v preserving:%v erasing:%v", boundary.function, found, preserving, erasing)
			}
		})
	}
	spec, err := os.ReadFile(filepath.Join(repoRoot, "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, obligation := range []string{
		"production runtime-carrier JSON writer feeding this",
		"preserves integer-versus-double kind",
		"integral-float JSON encoding",
		"resource-version identity remains governed by semantic_json.contract",
		"Live-intent persistence and fork projection use",
	} {
		if !strings.Contains(string(spec), obligation) {
			t.Errorf("platform spec lost runtime numeric writer obligation %q", obligation)
		}
	}
}

func TestFanOutSummaryRetiredRejectedVocabularyStaysAbsent(t *testing.T) {
	runtimeRoot := workflowProjectionRuntimeRoot(t)
	repoRoot := filepath.Clean(filepath.Join(runtimeRoot, "..", ".."))
	paths := []string{
		filepath.Join(runtimeRoot, "fanoutobligation", "model.go"),
		filepath.Join(runtimeRoot, "..", "store", "internal", "backend", "pipelinepersistence", "fan_out_owner.go"),
		filepath.Join(runtimeRoot, "..", "cliapp", "diagnostics.go"),
		filepath.Join(runtimeRoot, "..", "apiv1", "operator_read.go"),
		filepath.Join(repoRoot, "platform-spec.yaml"),
		filepath.Join(repoRoot, "openrpc.json"),
	}
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		check := func(file string) {
			raw, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			source := string(raw)
			for _, retired := range []string{"json:\"rejected\"", "yaml:\"rejected\"", "\"rejected\":", "summary.Rejected", "RunSummary.Rejected"} {
				if strings.Contains(source, retired) {
					t.Errorf("%s reintroduced retired fan-out summary vocabulary %q", file, retired)
				}
			}
		}
		if !info.IsDir() {
			check(path)
			continue
		}
		err = filepath.WalkDir(path, func(file string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(file, ".go") || strings.HasSuffix(file, "_test.go") {
				return nil
			}
			check(file)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestWorkflowProjectionGuardDetectsArbitraryPrivateInterpreters(t *testing.T) {
	hostile := `package hostile
import "encoding/json"
func hidden(value json.Number) (float64, error) { return value.Float64() }
func workflowNormalizeCELInput(value any) any { return value }
`
	violations := workflowProjectionGuardViolations(hostile, false)
	if len(violations) != 3 {
		t.Fatalf("hostile projection violations = %#v, want json.Number, arbitrary Float64 receiver, and retired helper", violations)
	}
}

func TestWorkflowProjectionSourceGuardDetectsFloatErasingDecoder(t *testing.T) {
	hostile := `package hostile
import "encoding/json"
func arbitraryName(raw []byte, destination any) error { return json.Unmarshal(raw, destination) }
`
	canonical, projected, useNumber, unmarshal, found := workflowProjectionSourceBoundaryFacts(hostile, "arbitraryName")
	if !found || canonical || projected || useNumber || !unmarshal {
		t.Fatalf("hostile source facts = found:%v canonical:%v projected:%v use_number:%v unmarshal:%v", found, canonical, projected, useNumber, unmarshal)
	}
}

func TestWorkflowProjectionWriterGuardDetectsKindErasingEncoder(t *testing.T) {
	for name, hostile := range map[string]string{
		"json marshal": `package hostile
import "encoding/json"
func arbitraryName(value any) ([]byte, error) { return json.Marshal(value) }
`,
		"semantic canonicalization": `package hostile
import "github.com/division-sh/swarm/internal/runtime/canonicaljson"
func arbitraryName(value any) ([]byte, error) { return canonicaljson.Bytes(value) }
`,
	} {
		t.Run(name, func(t *testing.T) {
			preserving, erasing, found := workflowProjectionWriterBoundaryFacts(hostile, "arbitraryName")
			if !found || preserving || !erasing {
				t.Fatalf("hostile writer facts = found:%v preserving:%v erasing:%v", found, preserving, erasing)
			}
		})
	}
}

func workflowProjectionWriterBoundaryFacts(source, functionName string) (preserving, erasing, found bool) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "source.go", source, 0)
	if err != nil {
		return false, false, false
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name == nil || function.Name.Name != functionName {
			continue
		}
		found = true
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel == nil {
				return true
			}
			owner, _ := selector.X.(*ast.Ident)
			switch {
			case owner != nil && owner.Name == "canonicaljson" && selector.Sel.Name == "MarshalPreservingNumberKinds":
				preserving = true
			case owner != nil && owner.Name == "fanoutobligation" && selector.Sel.Name == "MarshalCapsule":
				preserving = true
			case owner != nil && owner.Name == "json" && selector.Sel.Name == "Marshal":
				erasing = true
			case owner != nil && owner.Name == "canonicaljson" && (selector.Sel.Name == "Bytes" || selector.Sel.Name == "Encode"):
				erasing = true
			}
			return true
		})
		break
	}
	return preserving, erasing, found
}

func workflowProjectionSourceBoundaryFacts(source, functionName string) (canonical, projected, useNumber, unmarshal, found bool) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "source.go", source, 0)
	if err != nil {
		return false, false, false, false, false
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name == nil || function.Name.Name != functionName {
			continue
		}
		found = true
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel == nil {
				return true
			}
			owner, _ := selector.X.(*ast.Ident)
			switch {
			case owner != nil && owner.Name == "canonicaljson" && selector.Sel.Name == "DecodePreservingNumberLexemes":
				canonical = true
			case owner != nil && owner.Name == "workflowexpr" && selector.Sel.Name == "ProjectCELValue":
				projected = true
			case selector.Sel.Name == "UseNumber":
				useNumber = true
			case owner != nil && owner.Name == "json" && selector.Sel.Name == "Unmarshal":
				unmarshal = true
			}
			return true
		})
		break
	}
	return canonical, projected, useNumber, unmarshal, found
}

func workflowProjectionGuardViolations(source string, canonicalOwner bool) []string {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "guard.go", source, 0)
	if err != nil {
		return []string{"parse source: " + err.Error()}
	}
	violations := []string{}
	if !canonicalOwner && strings.Contains(source, "json.Number") {
		violations = append(violations, "declares a private json.Number interpreter")
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		switch function.Name.Name {
		case "NormalizeCELValue", "NormalizeCELInputMap", "normalizeCELValue", "normalizedCELInputMap", "workflowNormalizeCELInput":
			violations = append(violations, "declares retired projection helper "+function.Name.Name)
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel == nil || selector.Sel.Name != "Int64" && selector.Sel.Name != "Float64" {
				return true
			}
			if canonicalOwner && function.Name.Name == "projectCELValue" && selector.Sel.Name == "Int64" {
				return true
			}
			violations = append(violations, "calls private numeric parser "+selector.Sel.Name+" on a receiver")
			return true
		})
	}
	return violations
}

func workflowProjectionRuntimeRoot(t *testing.T) string {
	t.Helper()
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate workflow projection guard")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(current), ".."))
}
