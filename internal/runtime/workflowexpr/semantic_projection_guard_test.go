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
