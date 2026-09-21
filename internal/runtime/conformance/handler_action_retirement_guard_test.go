package conformance

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"gopkg.in/yaml.v3"
)

var retiredHandlerActionSymbols = map[string]bool{
	"ActionSpec": true, "ConfigFromSpec": true, "ConfigBinding": true,
	"MailboxWriteSpec": true, "ArtifactRepoSpec": true, "ArtifactRepoFileSpec": true,
	"ArtifactRepoSchemaSpec": true, "ArtifactRepoOutputSpec": true, "ArtifactRepoLimitsSpec": true,
	"ActionInstruction": true, "ActionFromContract": true,
	"ActionRegistry": true, "ActionRunner": true, "StepAction": true, "selectedActionSpec": true,
	"NewContractActionRegistry": true, "contractActionRegistry": true, "pipelineEngineActionRegistry": true,
	"MailboxMaterializer": true, "createFlowInstance": true,
	"ActionEntries": true, "ActionEntryByID": true, "ActionInstructions": true, "ActionInstructionByID": true,
	"decodeActionSpecNode": true, "decodeConfigFromSpecNode": true, "configFromKeyContainsSystemPrompt": true,
	"NormalizeHandlerActionID": true, "ParseHandlerActionID": true, "IsSupportedHandlerActionID": true,
	"HandlerHasAmbiguousTopLevelAction": true, "HandlerRuleActionIDs": true, "deriveWorkflowActionEntries": true,
	"appendActionExecutableReaders": true, "validateWorkflowActionSpec": true,
	"ArtifactRepoResultPublication": true, "ArtifactRepoResultPayloadFieldReserved": true,
	"handlerActionExecutable": true, "actionResultEvents": true,
}

func TestNoRetiredHandlerActionInterpreters(t *testing.T) {
	root := conformanceRepoRoot(t)
	var violations []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" || entry.Name() == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		found, err := retiredHandlerInterpreterViolations(filepath.ToSlash(relative), raw)
		violations = append(violations, found...)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(violations)
	if len(violations) != 0 {
		t.Fatalf("retired handler action authority remains or was restored:\n%s", strings.Join(violations, "\n"))
	}
}

// Scan every production Go file, not a list of former interpreter locations.
// Exact homonym exceptions identify a declaration, never a package or directory.
func retiredHandlerInterpreterViolations(path string, raw []byte) ([]string, error) {
	files := token.NewFileSet()
	file, err := parser.ParseFile(files, path, raw, 0)
	if err != nil {
		return nil, err
	}
	var violations []string
	report := func(node ast.Node, reason string) {
		violations = append(violations, fmt.Sprintf("%s:%d: %s", path, files.Position(node.Pos()).Line, reason))
	}
	for _, declaration := range file.Decls {
		owner := ""
		if function, ok := declaration.(*ast.FuncDecl); ok {
			owner = function.Name.Name
		}
		ast.Inspect(declaration, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.Ident:
				if retiredHandlerActionSymbols[value.Name] {
					report(value, "retired symbol "+value.Name)
				}
			case *ast.Field:
				if value.Tag == nil {
					break
				}
				tag, err := strconv.Unquote(value.Tag.Value)
				if err != nil {
					break
				}
				key := strings.Split(reflect.StructTag(tag).Get("yaml"), ",")[0]
				if !retiredHandlerField(key) {
					break
				}
				if !retainedActionHomonymField(path, declaration, value, key) {
					report(value, "retired YAML field "+key)
				}
			case *ast.CaseClause:
				for _, expr := range value.List {
					key, ok := actionRetirementConstantString(expr)
					// Template lifecycle modes are not handler options.
					if ok && key != "template" && retiredHandlerField(key) && !(path == "internal/runtime/contracts/workflow_contract_action_retirement.go" && owner == "retiredHandlerActionFieldError") {
						report(expr, "retired field dispatch "+key)
					}
				}
			case ast.Expr:
				text, ok := actionRetirementConstantString(value)
				if !ok || !retiredHandlerActionID(text) {
					break
				}
				if path == "internal/runtime/contracts/loader_diagnostics.go" && owner == "NewUndefinedFieldDiagnostic" && text == "mailbox_write" {
					break
				}
				if path == "internal/runtime/tools/permissions.go" && text == "create_flow_instance" && actionRetirementValueDeclaration(declaration, "defaultPlatformPermissions") {
					break
				}
				// This test-source ownership detector classifies retired routing as
				// authored too; it neither decodes nor executes the action.
				if path == "internal/runtime/testfixtures/canonicalrouting/fixture.go" && owner == "yamlNodeContainsAuthoredRoutingAt" && text == "create_flow_instance" {
					break
				}
				report(value, "retired action identity "+text)
				return false
			}
			return true
		})
	}
	return violations, nil
}

func actionRetirementValueDeclaration(declaration ast.Decl, name string) bool {
	group, ok := declaration.(*ast.GenDecl)
	if !ok {
		return false
	}
	for _, spec := range group.Specs {
		if value, ok := spec.(*ast.ValueSpec); ok {
			for _, id := range value.Names {
				if id.Name == name {
					return true
				}
			}
		}
	}
	return false
}

func retainedActionHomonymField(path string, declaration ast.Decl, field *ast.Field, key string) bool {
	group, ok := declaration.(*ast.GenDecl)
	if !ok {
		return false
	}
	for _, spec := range group.Specs {
		typ, ok := spec.(*ast.TypeSpec)
		if !ok || field.Pos() < typ.Pos() || field.End() > typ.End() {
			continue
		}
		if path == "internal/runtime/contracts/workflow_contract_types.go" && key == "action" && (typ.Name.Name == "WorkflowTimerContract" || typ.Name.Name == "GuardFailureSpec") {
			return true
		}
		return path == "internal/providertriggers/providertriggers.go" && typ.Name.Name == "EventNameManifest" && key == "template"
	}
	return false
}

func actionRetirementConstantString(expr ast.Expr) (string, bool) {
	switch value := expr.(type) {
	case *ast.BasicLit:
		if value.Kind == token.STRING {
			text, err := strconv.Unquote(value.Value)
			return text, err == nil
		}
	case *ast.BinaryExpr:
		if value.Op == token.ADD {
			left, lok := actionRetirementConstantString(value.X)
			right, rok := actionRetirementConstantString(value.Y)
			return left + right, lok && rok
		}
	case *ast.ParenExpr:
		return actionRetirementConstantString(value.X)
	}
	return "", false
}

func retiredHandlerField(key string) bool {
	switch key {
	case "action", "template", "evidence_target", "instance_id_from", "config_from":
		return true
	}
	return false
}

func retiredHandlerActionID(id string) bool {
	switch id {
	case "create_flow_instance", "record_evidence", "mailbox_write", "artifact_repo_commit":
		return true
	}
	return false
}

func TestNoRetiredHandlerActionInterpretersRejectsHostileRestoration(t *testing.T) {
	for name, source := range map[string]string{
		"renamed_dto":      "type Renamed struct { Value any `yaml:\"action\"` }",
		"original_dto":     "type ActionSpec struct{}",
		"runtime_runner":   "type ActionRunner interface { Run() error }",
		"engine_step":      "const StepAction = 1",
		"decoder":          "func decodeActionSpecNode() {}",
		"renamed_dispatch": "func run(s string) { switch s { case \"record_evidence\": } }",
		"split_dispatch":   "func run(s string) bool { return s == (\"artifact_repo_\" + \"commit\") }",
		"registry":         "var handlers = map[string]int{\"mailbox_write\": 1}",
		"raw_decoder":      "func decode(k string) { switch k {case \"config_from\": } }",
		"renamed_option":   "type NewOptions struct { Bindings any `yaml:\"config_from\"` }",
	} {
		t.Run(name, func(t *testing.T) {
			for _, path := range []string{"internal/runtime/nested/new.go", "cmd/newowner/main.go", "internal/runtime/testfixtures/new.go"} {
				found, err := retiredHandlerInterpreterViolations(path, []byte("package probe\n"+source))
				if err != nil || len(found) == 0 {
					t.Fatalf("%s escaped repository guard: findings=%v err=%v", path, found, err)
				}
			}
		})
	}
}

func TestCanonicalFormsRegistryPinsHandlerActionRetirement(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(conformanceRepoRoot(t), canonicalFormsRegistryPath))
	if err != nil {
		t.Fatal(err)
	}
	var record struct {
		Retirement struct {
			Issue     int      `yaml:"issue"`
			Status    string   `yaml:"status"`
			Ruling    string   `yaml:"ruling"`
			Rows      []string `yaml:"rows"`
			Fields    []string `yaml:"retired_fields"`
			Decoders  []string `yaml:"retired_decoders"`
			Preserved []string `yaml:"preserved"`
		} `yaml:"action_retirement"`
	}
	if err := yaml.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	r := record.Retirement
	if r.Issue != 2307 || r.Status != "grammar_retired_source_dispositions_reviewed" || r.Ruling != "5754202888" || !reflect.DeepEqual(r.Fields, []string{"action", "evidence_target", "template", "instance_id_from", "config_from"}) || !reflect.DeepEqual(r.Decoders, []string{"ActionSpec", "MailboxWriteSpec", "ArtifactRepoSpec", "ArtifactRepoFileSpec", "ArtifactRepoSchemaSpec", "ArtifactRepoOutputSpec", "ArtifactRepoLimitsSpec"}) {
		t.Fatalf("action retirement registry drift: %#v", r)
	}
	if !reflect.DeepEqual(r.Rows, []string{"handler.container", "handler.rules_shape", "handler.emit_action_activity", "handler.expression", "handler.collection_compute", "handler.join", "handler.mailbox_artifact_repo"}) || !reflect.DeepEqual(r.Preserved, []string{"emit_template_specialization", "timer_action", "guard_failure_action", "activity_tool", "provider_event_template", "data_accumulation_source_event"}) {
		t.Fatalf("retirement scope or preserved homonyms changed: %#v", r)
	}
	for _, typ := range []reflect.Type{reflect.TypeOf(contracts.SystemNodeEventHandler{}), reflect.TypeOf(contracts.HandlerRuleEntry{}), reflect.TypeOf(contracts.HandlerTransitionSemantic{})} {
		for _, name := range []string{"Action", "EvidenceTarget", "Template", "InstanceIDFrom", "ConfigFrom"} {
			if _, ok := typ.FieldByName(name); ok {
				t.Errorf("%s restored retired field %s", typ, name)
			}
		}
	}
}
