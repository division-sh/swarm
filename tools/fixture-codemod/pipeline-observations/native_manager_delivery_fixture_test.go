package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"golang.org/x/tools/go/ast/astutil"
)

func rewriteManagerDelivery(entry recipe, source []byte) ([]byte, recipe, error) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, entry.File, source, parser.ParseComments|parser.AllErrors)
	if err != nil {
		return nil, recipe{}, err
	}
	var found *ast.FuncDecl
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if ok && (fn.Name.Name == entry.Function || fn.Name.Name == "ProveNative"+strings.TrimPrefix(entry.Function, "Test")) {
			if found != nil {
				return nil, recipe{}, fmt.Errorf("ambiguous family caller %s", entry.Function)
			}
			found = fn
		}
	}
	if found == nil {
		return nil, recipe{}, fmt.Errorf("missing family caller %s", entry.Function)
	}
	start, end := set.Position(found.Pos()).Offset, set.Position(found.End()).Offset
	before := string(source[start:end])
	if found.Name.Name != entry.Function {
		var out bytes.Buffer
		if err := format.Node(&out, token.NewFileSet(), found); err != nil {
			return nil, recipe{}, err
		}
		after := out.String()
		return bytes.Join([][]byte{source[:start], []byte(after), source[end:]}, nil), recipe{File: filepath.ToSlash(entry.File), Function: entry.Function, Before: before, After: after}, nil
	}
	found.Name.Name = "ProveNative" + strings.TrimPrefix(entry.Function, "Test")
	found.Type.Params.List = append(found.Type.Params.List, &ast.Field{Names: []*ast.Ident{ast.NewIdent("newNativeDelivery")}, Type: ast.NewIdent("managerDeliveryNativeFactory")})
	constructors := 0
	seeded := map[string]bool{}
	ast.Inspect(found.Body, func(node ast.Node) bool {
		block, ok := node.(*ast.BlockStmt)
		if !ok {
			return true
		}
		statements := []ast.Stmt{}
		for _, statement := range block.List {
			owner := false
			astutil.Apply(statement, func(cursor *astutil.Cursor) bool {
				switch item := cursor.Node().(type) {
				case *ast.FuncLit, *ast.BlockStmt:
					return false
				case *ast.CallExpr:
					if managerDeliveryNodeName(item.Fun) == "newManagerDeliveryTestStore" {
						constructors++
						owner = true
						cursor.Replace(ast.NewIdent("nativeDelivery"))
					}
				}
				return true
			}, nil)
			if owner {
				statements = append(statements, managerDeliveryStatement("nativeDelivery := newNativeDelivery(t)"))
			}
			explicitPublication := ""
			astutil.Apply(statement, func(cursor *astutil.Cursor) bool {
				switch item := cursor.Node().(type) {
				case *ast.FuncLit, *ast.BlockStmt:
					return false
				case *ast.KeyValueExpr:
					if managerDeliveryNodeName(item.Key) == "managerDeliveryTestStore" {
						item.Key = ast.NewIdent("ManagerDeliveryNativeFixture")
					}
				case *ast.SelectorExpr:
					if item.Sel.Name == "authority" {
						item.Sel.Name = "Authority"
					}
				case *ast.CallExpr:
					event, agent := managerDeliveryPublicationInput(item)
					if event != "" && !seeded[event] {
						seeded[event] = true
						explicitPublication += "nativeDelivery.seedAgentDeliveries(t, " + agent + ", []events.Event{" + event + "});"
					}
				}
				return true
			}, nil)
			if explicitPublication != "" {
				statements = append(statements, managerDeliveryStatements(explicitPublication)...)
			}
			statements = append(statements, statement)
		}
		block.List = statements
		return true
	})
	if constructors != 1 {
		return nil, recipe{}, fmt.Errorf("%s constructors=%d,want1", entry.Function, constructors)
	}
	var out bytes.Buffer
	if err := format.Node(&out, token.NewFileSet(), found); err != nil {
		return nil, recipe{}, err
	}
	after := out.String()
	updated := bytes.Join([][]byte{source[:start], []byte(after), source[end:]}, nil)
	return updated, recipe{File: filepath.ToSlash(entry.File), Function: entry.Function, Before: before, After: after}, nil
}

func managerDeliveryPublicationInput(call *ast.CallExpr) (string, string) {
	if managerDeliveryNodeName(call.Fun) == "managerClaimedDeliveryContext" && len(call.Args) == 5 {
		return managerDeliveryNodeName(call.Args[3]), managerDeliveryNodeName(call.Args[4])
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", ""
	}
	if selector.Sel.Name == "claimExact" && len(call.Args) == 3 {
		route, ok := call.Args[2].(*ast.CallExpr)
		if ok && managerDeliveryNodeName(route.Fun) == "managerAgentDeliveryRouteForRun" && len(route.Args) == 2 {
			return managerDeliveryNodeName(call.Args[1]), managerDeliveryNodeName(route.Args[1])
		}
	}
	if selector.Sel.Name == "Publish" && managerDeliveryNodeName(selector.X) == "eventBus" && len(call.Args) == 2 {
		return managerDeliveryNodeName(call.Args[1]), "agent.ID()"
	}
	return "", ""
}

func managerDeliveryNodeName(node ast.Node) string {
	var out bytes.Buffer
	if err := format.Node(&out, token.NewFileSet(), node); err != nil {
		panic(err)
	}
	return out.String()
}

func managerDeliveryStatements(source string) []ast.Stmt {
	file, err := parser.ParseFile(token.NewFileSet(), "statement.go", "package probe;func recipe(){"+source+"}", parser.AllErrors)
	if err != nil {
		panic(err)
	}
	return file.Decls[0].(*ast.FuncDecl).Body.List
}
func managerDeliveryStatement(source string) ast.Stmt { return managerDeliveryStatements(source)[0] }

func TestManagerDeliveryWholeFamilyRecipesPreserveMechanicalConsumers(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-manager-delivery-caller" {
			continue
		}
		count++
		_, got, err := rewriteManagerDelivery(row, []byte("package probe\n"+row.Before))
		if err != nil {
			t.Fatal(err)
		}
		want, err := canonicalFunction(row.After)
		transformed, transformErr := canonicalFunction(got.After)
		if err != nil || transformErr != nil || want != transformed {
			t.Fatalf("nonmechanical caller rewrite: %s", row.Function)
		}
		checkManagerDeliveryRecipe(t, row)
	}
	if count != 18 {
		t.Fatalf("mechanical manager roots=%d,want18", count)
	}
}

func checkManagerDeliveryRecipe(t *testing.T, row recipe) {
	t.Helper()
	actualName := "ProveNative" + strings.TrimPrefix(row.Function, "Test")
	actual, err := canonicalFunction(selectedCausalObservationBody(t, row.File, actualName))
	expected, wantErr := canonicalFunction(row.After)
	if err != nil || wantErr != nil || actual != expected {
		t.Fatalf("manager caller source diverged: %s", row.Function)
	}
	mutant := strings.Replace(row.After, "newNativeDelivery(t)", "newForeignDelivery(t)", 1)
	if mutant == row.After {
		t.Fatal("manager authority mutant stopped matching")
	}
	if _, changed, err := rewriteFunction(row.File, []byte("package probe\n"+mutant), row); err == nil || changed {
		t.Fatalf("foreign construction accepted: %s", row.Function)
	}
}

func TestManagerDeliveryWholeFamilySemanticRepairsStaySourcePinned(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-manager-delivery-owner" {
			continue
		}
		count++
		checkManagerDeliveryRecipe(t, row)
	}
	if count != 10 {
		t.Fatalf("semantic manager roots=%d,want10", count)
	}
}

func TestManagerDeliveryFakePersistenceCapabilityIsRetired(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal", "runtime", "manager"), func(path string, entry fs.DirEntry, failure error) error {
		if failure != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return failure
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, data, parser.AllErrors)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if id, ok := node.(*ast.Ident); ok && (id.Name == "managerDeliveryTestStore" || id.Name == "newManagerDeliveryTestStore" || id.Name == "ensureDelivery" || id.Name == "seedSelectedExecution") {
				t.Errorf("retired manager persistence escape remains: %s/%s", path, id.Name)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
