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
)

func servedDeliveryCallerSource(source, name string) (string, error) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "caller.go", "package probe\n"+source, parser.AllErrors)
	if err != nil {
		return "", err
	}
	var failure error
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || formattedNativeReadNode(call.Fun) != "waitServedRunDeliveryQuiescence" {
			return true
		}
		if len(call.Args) != 4 {
			failure = fmt.Errorf("unexpected original delivery wait arity: %s", name)
			return false
		}
		if selector, ok := call.Args[1].(*ast.SelectorExpr); ok && selector.Sel.Name == "DB" {
			call.Args[1] = &ast.SelectorExpr{X: selector.X, Sel: ast.NewIdent("ReadRunDeliveries")}
		} else if name == "TestServedMailboxCompletionProcessBoundariesBothStores" && formattedNativeReadNode(call.Args[1]) == "db" {
			call.Args[1] = &ast.SelectorExpr{X: ast.NewIdent("rt"), Sel: ast.NewIdent("ReadRunDeliveries")}
		} else {
			failure = fmt.Errorf("unreviewed delivery-owner handoff: %s", name)
			return false
		}
		call.Args = []ast.Expr{call.Args[0], call.Args[1], call.Args[3]}
		return true
	})
	if failure != nil {
		return "", failure
	}
	var out bytes.Buffer
	if err := format.Node(&out, set, file.Decls[0]); err != nil {
		return "", err
	}
	return out.String(), nil
}

func TestServedDeliveryQuiescenceMechanicalCallerRecipesPreserveAllOtherWork(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-served-delivery-quiescence-caller" {
			continue
		}
		matched++
		transformed, err := servedDeliveryCallerSource(row.Before, row.Function)
		if err != nil {
			t.Fatal(err)
		}
		want, err := canonicalFunction(transformed)
		got, afterErr := canonicalFunction(row.After)
		current := row.After
		if row.Successor != "" {
			current = row.Successor
		}
		currentPin, pinErr := canonicalFunction(current)
		actual, actualErr := canonicalFunction(selectedCausalObservationBody(t, row.File, row.Function))
		if err != nil || afterErr != nil || pinErr != nil || actualErr != nil || want != got || actual != currentPin {
			t.Fatalf("delivery caller changed setup, temporal cuts or assertions: %s", row.Function)
		}
		mutant := strings.Replace(row.After, "ReadRunDeliveries", "ForeignRunDeliveries", 1)
		if mutant == row.After {
			t.Fatal("delivery caller hostile owner stopped matching")
		}
		if _, changed, err := rewriteFunction(row.File, []byte("package probe\n"+mutant), row); err == nil || changed {
			t.Fatalf("foreign owner admitted by finite rewrite: %s", row.Function)
		}
	}
	if matched != 73 {
		t.Fatalf("mechanical delivery caller recipes=%d,want73; construction is separate", matched)
	}
}

func TestServedDeliveryQuiescenceCallerInventoryConsumesOnlyDomainReadPorts(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	counts := map[string]int{}
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal", "serveapp"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return walkErr
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok || formattedNativeReadNode(call.Fun) != "waitServedRunDeliveryQuiescence" {
					return true
				}
				counts[fn.Name.Name]++
				if len(call.Args) != 3 || strings.Contains(formattedNativeReadNode(call.Args[1]), ".DB") || formattedNativeReadNode(call.Args[1]) == "db" {
					t.Errorf("delivery wait regained raw/dialect authority: %s/%s", path, formattedNativeReadNode(call))
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, count := range counts {
		total += count
	}
	if len(counts) != 81 || total != 150 || counts["TestServedRunDeliveryQuiescenceReaderPreservesStableStatusCut"] != 1 {
		t.Fatalf("delivery family inventory functions=%d,calls=%d,want80/149 plus one unit witness", len(counts), total)
	}
}

func TestServedDeliveryQuiescenceOwnerRecipesRemainSourcePinned(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-served-delivery-quiescence-owner" {
			continue
		}
		matched++
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		current := row.After
		if row.Successor != "" {
			current = row.Successor
		}
		want, err := canonicalFunction(current)
		got, actualErr := canonicalFunction(actual)
		if err != nil || actualErr != nil || want != got {
			t.Fatalf("construction/lifetime/delivery predicate changed: %s", row.Function)
		}
		if row.Function != "waitServedRunDeliveryQuiescence" && !strings.Contains(current, "p.deps.DeliveryStore.SummarizeRun") && !strings.Contains(current, "InspectionDeliveryReader(t,") {
			t.Fatalf("constructor did not bind an original delivery or inspection owner: %s", row.Function)
		}
	}
	if matched != 20 {
		t.Fatalf("delivery owner recipes=%d,want20", matched)
	}
}
