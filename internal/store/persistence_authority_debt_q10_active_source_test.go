package store_test

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
)

func TestPersistenceAuthorityDebtQ10AncestrySourceIsActiveAndRequired(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	const path = "internal/serveapp/a2_nested_portfolio_isolation_test.go"
	const name = "TestA2NestedPortfolioSamePeriodAndMemberIsolationBothStores"
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, raw, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	for _, group := range file.Comments {
		for _, comment := range group.List {
			if strings.HasPrefix(comment.Text, "//go:build") || strings.HasPrefix(comment.Text, "// +build") {
				t.Fatal("required native ancestry source became conditional")
			}
		}
	}
	var body *ast.BlockStmt
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok && function.Name.Name == name {
			if body != nil {
				t.Fatal("duplicate ancestry proof")
			}
			body = function.Body
		}
	}
	if body == nil {
		t.Fatal("required native ancestry proof is missing")
	}
	ast.Inspect(body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
				if selector.Sel.Name == "Skip" || selector.Sel.Name == "Skipf" || selector.Sel.Name == "SkipNow" {
					t.Fatal("required native ancestry proof can skip")
				}
			}
		}
		return true
	})
	for _, assertion := range []string{
		"periods[index].Entity.CurrentState == nil || *periods[index].Entity.CurrentState",
		"periods[0].Entity.EntityID == periods[1].Entity.EntityID || periods[0].Entity.FlowInstance == periods[1].Entity.FlowInstance",
		"!reflect.DeepEqual(otherBefore, requireA2PortfolioEntity(t, rt, runID, periods[1-index].Entity.FlowInstance))",
		"!reflect.DeepEqual(results, expected)",
		"join.JoinRef().StageEntry().InstancePath != initial[index].Entity.FlowInstance",
	} {
		if strings.Count(string(raw), assertion) != 1 {
			t.Fatalf("required native ancestry assertion changed: %s", assertion)
		}
	}
	if strings.Count(string(raw), "waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, runID)") != 2 ||
		strings.Contains(string(raw), "waitServedRunDeliveryQuiescence(t, rt.DB,") {
		t.Fatal("both delivery-settlement reads must retain the owned reader")
	}
	policyFile, err := os.Open(filepath.Join(root, ".github/test-proof-plan.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer policyFile.Close()
	policy, err := testplanning.LoadPolicy(policyFile)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := testplanning.DiscoverRootInventory(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	proofs, err := testplanning.LoadParityProofs(filepath.Join(root, "internal/apiv1/testdata/public_surface_backend_matrix.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var packages []string
	for pkg := range inventory.Packages {
		packages = append(packages, pkg)
	}
	model := testplanning.WeightModel{Version: testplanning.WeightModelVersion, SourceRunID: "q10-active-ancestry"}
	for _, venue := range []string{testplanning.VenueCI, testplanning.VenueLocal} {
		plan, err := testplanning.BuildPlan(policy, model, packages, testplanning.ProfileLifecycle, "required native ancestry", "test-head", testplanning.BuildOptions{Venue: venue})
		if err != nil {
			t.Fatal(err)
		}
		if err := testplanning.BindExecution(&plan, inventory, proofs, policy); err != nil {
			t.Fatal(err)
		}
		unit, err := plan.Unit("serveapp-other")
		if err != nil || unit.Skip != "" {
			t.Fatalf("%s ancestry unit is not runnable: %+v %v", venue, unit, err)
		}
		found := false
		for _, required := range unit.RequiredTests {
			if required.Package == "github.com/division-sh/swarm/internal/serveapp" && required.Name == name {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s lifecycle does not require the native ancestry root", venue)
		}
	}
}
