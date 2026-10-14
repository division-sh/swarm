package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

func TestFamily145DeploymentResourceCarrierRetirementHasCompleteNativeCallers(t *testing.T) {
	root := filepath.Clean("../../..")
	pins, err := selectedreadrole.ReadDeploymentResourcePins(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := selectedreadrole.ValidateDeploymentResourceCurrent(root, pins); err != nil {
		t.Fatal(err)
	}
	paths := map[string]bool{}
	for _, pin := range pins {
		paths[pin.File] = true
		if strings.Contains(pin.After, ".db") || strings.Contains(pin.After, "AdmitPostgresRuntimeStore") || strings.Contains(pin.After, "DatabaseForTest") {
			t.Fatal("raw carrier capability survived:", pin.Function)
		}
	}
	if len(paths) != 17 {
		t.Fatal("whole caller/source inventory changed:", len(paths))
	}
	path := filepath.Join(root, "internal/runtime/conformance/fan_out_resource_source_proof_test.go")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := deploymentResourceCarrierClosed(raw); err != nil {
		t.Fatal(err)
	}
	for _, addition := range []string{"db *sql.DB", "db any", "pool interface{}"} {
		bad := strings.Replace(string(raw), "type deploymentResourceFixture struct {", "type deploymentResourceFixture struct {\n"+addition, 1)
		if err := deploymentResourceCarrierClosed([]byte(bad)); err == nil {
			t.Fatal("carrier admitted a new escape field:", addition)
		}
	}
	for _, pin := range pins {
		if pin.Function != "newDeploymentResourceFixtureWithSource" && pin.Function != "selectedDeploymentResourceFixtureConfigured" {
			continue
		}
		if !strings.Contains(pin.After, "StartSQLiteRuntimeStore") || !strings.Contains(pin.After, "StartPostgresRuntimeStore") || !strings.Contains(pin.After, "newNotifyAllChildrenProcessTopology") {
			t.Fatal("native constructor lost source/topology or backend:", pin.Function)
		}
	}
}

func deploymentResourceCarrierClosed(raw []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
	if err != nil {
		return err
	}
	for _, declaration := range file.Decls {
		decl, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range decl.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != "deploymentResourceFixture" {
				continue
			}
			shape, ok := typeSpec.Type.(*ast.StructType)
			if !ok || len(shape.Fields.List) != 6 {
				return os.ErrInvalid
			}
			want := []string{"selected", "postgresDSN", "source", "runtime", "topology", "ctx"}
			for index, field := range shape.Fields.List {
				if len(field.Names) != 1 || field.Names[0].Name != want[index] {
					return os.ErrInvalid
				}
			}
			return nil
		}
	}
	return os.ErrNotExist
}

func TestFamily145DeploymentResourceFiniteRecipesKeepHistoricalAssertionsAndOneOwner(t *testing.T) {
	root := filepath.Clean("../../..")
	pins, err := selectedreadrole.ReadDeploymentResourcePins(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
		input := []byte("package conformance\n" + pin.Before)
		output, changed, err := rewriteFunction(pin.File, input, finite)
		if err != nil || !changed || !strings.Contains(string(output), pin.Function) {
			t.Fatal("finite exact transform failed:", pin.Function, err)
		}
		if _, again, err := rewriteFunction(pin.File, output, finite); err != nil || again {
			t.Fatal("finite transform was not idempotent:", pin.Function, err)
		}
		bad := strings.Replace(pin.Before, pin.Function, pin.Function+"Foreign", 1)
		if _, changed, err := rewriteFunction(pin.File, []byte("package conformance\n"+bad), finite); err == nil || changed {
			t.Fatal("foreign declaration accepted:", pin.Function)
		}
	}
}
