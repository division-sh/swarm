package semanticview_test

import (
	"fmt"
	"go/ast"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

var s1ActivityToolConsumers = map[string]string{
	"internal/runtime/contracts/workflow_contract_activity.go":  "generatedActivityDeclarationRecords",
	"internal/runtime/activity_validation.go":                   "validateActivitySpec",
	"internal/runtime/engine/executor.go":                       "stepActivity",
	"internal/runtime/pipeline/activity_engine.go":              "executeActivityIntent",
	"internal/runtime/pipeline/activity_lineage.go":             "FreshActivityRequestLineage",
	"internal/runtime/pipeline/workflow_executable_readers.go":  "activityInputResultType",
	"internal/runtime/bootverify/workflow_expression_checks.go": "executableReaderExpressions",
	"internal/runtime/authoringview/view.go":                    "buildApprovalPoints",
}

func TestS1ActivityToolConsumersUseScopedOwner(t *testing.T) {
	if findings := activityToolOwnerFindings(t, nil); len(findings) != 0 {
		t.Fatal(strings.Join(findings, "\n"))
	}
}

func TestS1ActivityToolOwnerGuardRejectsGlobalLookupAndReaderAliases(t *testing.T) {
	root := agentNameGuardRepoRoot(t)
	validation := filepath.Join(root, "internal/runtime/activity_validation.go")
	body, err := os.ReadFile(validation)
	if err != nil {
		t.Fatal(err)
	}
	marker := "source.ActivityToolEntry(node, toolID)"
	if strings.Count(string(body), marker) != 1 {
		t.Fatal("exact activity validation owner not found")
	}
	overlay := map[string][]byte{validation: []byte(strings.Replace(string(body), marker, "source.ToolEntries()[toolID]", 1))}
	readers := filepath.Join(root, "internal/runtime/pipeline/workflow_executable_readers.go")
	body, err = os.ReadFile(readers)
	if err != nil {
		t.Fatal(err)
	}
	marker = "func activityInputResultType(source semanticview.Source, node runtimeidentity.ExecutableNode, toolID, field string) (runtimecontracts.ResolvedCatalogType, bool, bool, error) {"
	if strings.Count(string(body), marker) != 1 {
		t.Fatal("exact activity reader owner not found")
	}
	overlay[readers] = []byte(strings.Replace(string(body), marker, marker+"\n _ = source.ToolEntries", 1))
	findings := strings.Join(activityToolOwnerFindings(t, overlay), "\n")
	for _, owner := range []string{"validateActivitySpec", "activityInputResultType"} {
		if !strings.Contains(findings, owner+":global_tool_alias") {
			t.Fatalf("guard missed %s: %s", owner, findings)
		}
	}
}

func activityToolOwnerFindings(t *testing.T, overlay map[string][]byte) []string {
	t.Helper()
	root := agentNameGuardRepoRoot(t)
	pkgs, err := packages.Load(&packages.Config{Dir: root, Overlay: overlay,
		Mode: packages.NeedName | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo},
		"./internal/runtime/contracts", "./internal/runtime/semanticview", "./internal/runtime/engine", "./internal/runtime/pipeline",
		"./internal/runtime/bootverify", "./internal/runtime/authoringview", "./internal/runtime")
	if err != nil || packages.PrintErrors(pkgs) != 0 {
		t.Fatalf("activity owner census requires resolved production types: %v", err)
	}
	var findings []string
	seen := map[string]int{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			name, err := filepath.Rel(root, pkg.Fset.Position(file.Pos()).Filename)
			if err != nil {
				t.Fatal(err)
			}
			name = filepath.ToSlash(name)
			owner, tracked := s1ActivityToolConsumers[name]
			if !tracked {
				continue
			}
			for _, declaration := range file.Decls {
				fn, ok := declaration.(*ast.FuncDecl)
				if !ok || fn.Name.Name != owner {
					continue
				}
				seen[name]++
				usesOwner := false
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					selector, ok := node.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					object := pkg.TypesInfo.Uses[selector.Sel]
					if object == nil || object.Pkg() == nil {
						return true
					}
					path := object.Pkg().Path()
					if path != "github.com/division-sh/swarm/internal/runtime/contracts" && path != "github.com/division-sh/swarm/internal/runtime/semanticview" {
						return true
					}
					if object.Name() == "ToolEntries" {
						findings = append(findings, owner+":global_tool_alias")
					}
					usesOwner = usesOwner || object.Name() == "ActivityToolEntry"
					return true
				})
				if !usesOwner {
					findings = append(findings, owner+":missing_scoped_tool_owner")
				}
			}
		}
	}
	for file, owner := range s1ActivityToolConsumers {
		if seen[file] != 1 {
			findings = append(findings, fmt.Sprintf("%s:expected_one_owner:got_%d", owner, seen[file]))
		}
	}
	return findings
}
