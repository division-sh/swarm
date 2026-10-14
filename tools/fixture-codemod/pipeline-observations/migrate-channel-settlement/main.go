package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"golang.org/x/tools/go/packages"
)

var helpers = map[string]bool{
	"waitObjectCardReceipt": true, "waitObjectRequestedResponse": true,
	"waitObjectIntentResponse": true, "waitChannelDraftPromptSettlement": true,
	"waitChannelDraftStatus": true, "waitChannelDeliverySendsSettled": true,
	"waitNativeIntentCount": true, "selectedChannelDraftState": true,
	"beginObjectInputCard": true, "logUnsettledChannelDeliveryPlans": true,
	"readChannelDeliverySettlementDiagnostic": true, "readChannelDeliveryDiagnosticRows": true,
	"CountUnsettledChannelDeliveryResponsibilities": true,
}

type declaration struct {
	File, Function, Before string
	Calls                  []string
}

func main() {
	generate := flag.Bool("ports", false, "generate the finite approved selected-observation ports")
	rewrite := flag.Bool("rewrite", false, "validate the whole signed caller transformation")
	write := flag.Bool("write", false, "write the whole checked caller candidate")
	history := flag.Bool("history", false, "compose the finite signed source/history stage")
	flag.Parse()
	if flag.NArg() != 0 {
		panic("unexpected channel-settlement argument")
	}
	if *generate {
		generatePorts()
		return
	}
	root, err := os.Getwd()
	must(err)
	if *history {
		generateHistory(root, *write)
		return
	}
	if *rewrite {
		migrate(root, *write)
		return
	}
	pkgs, err := packages.Load(&packages.Config{Dir: root, Tests: true,
		Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedSyntax},
		"./internal/serveapp", "./internal/store/storetest")
	must(err)
	if len(pkgs) == 0 || packages.PrintErrors(pkgs) != 0 {
		panic("channel settlement inventory does not type-check")
	}
	rows := map[string]declaration{}
	active := map[string]bool{}
	for _, pkg := range pkgs {
		covered := map[*ast.Ident]bool{}
		for _, file := range pkg.Syntax {
			active[filepath.Clean(pkg.Fset.Position(file.Pos()).Filename)] = true
			path, err := filepath.Rel(root, pkg.Fset.Position(file.Pos()).Filename)
			must(err)
			for _, node := range file.Decls {
				fn, ok := node.(*ast.FuncDecl)
				if !ok {
					continue
				}
				calls, err := references(pkg, fn, covered)
				must(err)
				if len(calls) == 0 && !helpers[fn.Name.Name] {
					continue
				}
				key := filepath.ToSlash(path) + ":" + fn.Name.Name
				row := declaration{filepath.ToSlash(path), fn.Name.Name, requestkey.Node(fn), calls}
				if old, ok := rows[key]; ok && old.Before != row.Before {
					panic("conflicting typed source: " + key)
				}
				rows[key] = row
			}
		}
		must(completeReferences(pkg, covered))
	}
	must(checkExcludedSources(root, active))
	keys := make([]string, 0, len(rows))
	for key := range rows {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	result := make([]declaration, 0, len(keys))
	for _, key := range keys {
		result = append(result, rows[key])
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	must(encoder.Encode(result))
}

func checkExcludedSources(root string, active map[string]bool) error {
	for _, directory := range []string{"internal/serveapp", "internal/store/storetest"} {
		err := checkoutsource.WalkDir(root, filepath.Join(root, directory), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || active[filepath.Clean(path)] {
				return err
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
			if err != nil {
				return err
			}
			ast.Inspect(file, func(node ast.Node) bool {
				if ident, ok := node.(*ast.Ident); ok && helpers[ident.Name] {
					err = fmt.Errorf("excluded owned source has an unclassified channel helper reference: %s/%s", path, ident.Name)
				}
				return true
			})
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func helperReference(object types.Object) bool {
	fn, ok := object.(*types.Func)
	if !ok || fn.Pkg() == nil || !helpers[fn.Name()] {
		return false
	}
	owner := fn.Pkg().Path()
	return owner == "github.com/division-sh/swarm/internal/serveapp" || owner == "github.com/division-sh/swarm/internal/store/storetest"
}

func completeReferences(pkg *packages.Package, covered map[*ast.Ident]bool) error {
	for ident, object := range pkg.TypesInfo.Uses {
		if helperReference(object) && !covered[ident] {
			return fmt.Errorf("channel settlement helper reference outside its caller declaration: %s", pkg.Fset.Position(ident.Pos()))
		}
	}
	return nil
}

func references(pkg *packages.Package, fn *ast.FuncDecl, covered map[*ast.Ident]bool) ([]string, error) {
	var result []string
	direct := map[*ast.Ident]bool{}
	ast.Inspect(fn, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			switch called := call.Fun.(type) {
			case *ast.Ident:
				direct[called] = true
			case *ast.SelectorExpr:
				direct[called.Sel] = true
			}
		}
		return true
	})
	var failure error
	ast.Inspect(fn, func(node ast.Node) bool {
		ident, ok := node.(*ast.Ident)
		if !ok || !helperReference(pkg.TypesInfo.Uses[ident]) {
			return true
		}
		covered[ident] = true
		if !direct[ident] {
			failure = fmt.Errorf("channel settlement indirect helper consumer: %s", pkg.Fset.Position(ident.Pos()))
		}
		result = append(result, fmt.Sprintf("%s:%d:%s", fn.Name.Name, pkg.Fset.Position(ident.Pos()).Line, ident.Name))
		return true
	})
	return result, failure
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
