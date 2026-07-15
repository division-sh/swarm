package apispec

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var userFacingDiagnosticInternalRefPattern = regexp.MustCompile(`#\d{3,}|\bimplemented_[A-Za-z0-9_#-]+\b|\b(?:tracked by|split to|unsupported by|first-slice source authority)\b`)

func TestUserFacingDiagnosticIssueRefPatternRejectsInternalIssues(t *testing.T) {
	for _, text := range []string{
		"tracked by #1234",
		"implemented_#1614",
		"implemented_first_slice",
		"split to #1176",
		"first-slice source authority",
	} {
		if !userFacingDiagnosticInternalRefPattern.MatchString(text) {
			t.Fatalf("internal-ref guard did not match %q", text)
		}
	}
}

func TestUserFacingDiagnosticSourcesDoNotLeakInternalIssueRefs(t *testing.T) {
	files := []string{
		"internal/serveapp/main.go",
		"internal/cliapp/target_resolution.go",
		"internal/cliapp/local_context_registry.go",
		"internal/runtime/bootverify/workflow_composition_connect_checks.go",
		"internal/runtime/bootverify/workflow_output_pin_key_carries_checks.go",
		"internal/runtime/contracts/workflow_contract_tree.go",
		"internal/runtime/contracts/workflow_contract_types.go",
		"internal/runtime/contracts/workflow_contract_yaml_flow.go",
		"internal/apiv1/operator_runtime_control.go",
		"internal/apiv1/operator_agent_control.go",
		"internal/apiv1/operator_mailbox.go",
	}

	var problems []string
	for _, rel := range files {
		path := filepath.Join(repoRoot(t), rel)
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", rel, err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			lit, ok := node.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			if !userFacingDiagnosticInternalRefPattern.MatchString(value) {
				return true
			}
			pos := fset.Position(lit.Pos())
			problems = append(problems, fmt.Sprintf("%s:%d: user-facing diagnostic source leaks internal tracker ref in %q", rel, pos.Line, value))
			return true
		})
	}
	if len(problems) > 0 {
		t.Fatalf("user-facing diagnostic issue/tracker refs are forbidden by platform-spec.yaml#cli_specification.foundations.output_contract.diagnostic_convention:\n%s", strings.Join(problems, "\n"))
	}
}
