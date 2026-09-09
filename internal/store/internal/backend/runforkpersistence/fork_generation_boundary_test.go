package runforkpersistence

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const generationBoundaryLoop = "runtime/loopruntime::"

// G25 guards the audited generation consumers, not whole-program taint flow.
// Behavioral tests remain responsible for values erased into maps or reflection.
func TestForkGenerationConsumerBoundary(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	patterns := []string{
		"./internal/runtime/loopruntime", "./internal/runtime/runfork",
		"./internal/runtime/runforkexecution", "./internal/runtime/runforkreadiness",
		"./internal/runtime/semanticview", "./internal/runtime/bootverify",
		"./internal/store/internal/backend/runforkpersistence",
		"./internal/store/internal/backend/pipelinepersistence",
	}
	loaded, err := packages.Load(&packages.Config{Dir: root, Tests: false,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
	}, patterns...)
	if err != nil || packages.PrintErrors(loaded) != 0 || len(loaded) != len(patterns) {
		t.Fatalf("generation guard requires every audited package to compile: %v", err)
	}
	imports := historicalBoundaryImports{}
	packages.Visit(loaded, func(pkg *packages.Package) bool {
		if pkg.Types != nil {
			imports[pkg.PkgPath] = pkg.Types
		}
		return true
	}, nil)
	var findings []historicalBoundaryFinding
	for _, pkg := range loaded {
		for _, file := range pkg.Syntax {
			findings = append(findings, generationBoundaryCollect(pkg.Types, pkg.TypesInfo, pkg.Fset, file)...)
		}
	}
	t.Run("production", func(t *testing.T) {
		if problems := historicalBoundaryProblems(findings, generationBoundaryAllowances(), true); len(problems) != 0 {
			t.Fatalf("generation ownership bypass or stale census:\n%s", strings.Join(problems, "\n"))
		}
	})
	t.Run("hostile", func(t *testing.T) { generationBoundaryHostile(t, imports) })
	for _, pkg := range loaded {
		if pkg.PkgPath == historicalBoundaryModule+"runtime/loopruntime" {
			t.Run("captured_owner_extra_coordinate", func(t *testing.T) { generationBoundaryCapturedOwnerHostile(t, pkg, imports) })
		}
	}
}

func generationBoundaryCollect(pkg *types.Package, info *types.Info, fset *token.FileSet, file *ast.File) []historicalBoundaryFinding {
	var findings []historicalBoundaryFinding
	for _, declaration := range file.Decls {
		scope := strings.TrimPrefix(pkg.Path(), historicalBoundaryModule) + "::<package>"
		if fn, ok := declaration.(*ast.FuncDecl); ok {
			resolved, ok := info.Defs[fn.Name].(*types.Func)
			if !ok {
				panic("generation boundary requires compiler-resolved functions")
			}
			scope = historicalBoundaryFunction(resolved)
		}
		add := func(node ast.Node, kind string) {
			findings = append(findings, historicalBoundaryFinding{Scope: scope, Kind: kind, Site: fset.Position(node.Pos()).String()})
		}
		ast.Inspect(declaration, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.Ident:
				if fn, ok := info.Uses[n].(*types.Func); ok {
					callee := historicalBoundaryFunction(fn)
					if callee == generationBoundaryLoop+"Fork" || callee == generationBoundaryLoop+"ForkGeneration" {
						add(n, "reference:"+callee)
					}
				}
			case *ast.SelectorExpr:
				selection := info.Selections[n]
				if selection == nil || selection.Kind() != types.FieldVal {
					break
				}
				if generationBoundarySelection(selection) {
					kind := "coordinate:" + selection.Obj().Name()
					if pkg.Path() == historicalBoundaryModule+"runtime/loopruntime" {
						kind = "canonical_coordinate"
					}
					add(n, kind)
				}
			case *ast.CompositeLit:
				if generationBoundaryType(info.TypeOf(n)) && len(n.Elts) != 0 {
					add(n, "generation_construction")
				}
			}
			return true
		})
	}
	return findings
}

func generationBoundarySelection(selection *types.Selection) bool {
	typ := selection.Recv()
	for _, index := range selection.Index() {
		if generationBoundaryType(typ) {
			return true
		}
		typ = types.Unalias(typ)
		if pointer, ok := typ.(*types.Pointer); ok {
			typ = types.Unalias(pointer.Elem())
		}
		structure, ok := typ.Underlying().(*types.Struct)
		if !ok || index >= structure.NumFields() {
			return false
		}
		typ = structure.Field(index).Type()
	}
	return false
}

func generationBoundaryType(typ types.Type) bool {
	if typ == nil {
		return false
	}
	typ = types.Unalias(typ)
	if pointer, ok := typ.(*types.Pointer); ok {
		typ = types.Unalias(pointer.Elem())
	}
	named, ok := typ.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Pkg().Path() == historicalBoundaryModule+"runtime/core/attemptgeneration" && named.Obj().Name() == "Generation" ||
		named.Obj().Pkg().Path() == historicalBoundaryModule+"runtime/loopruntime" && named.Obj().Name() == "Activation"
}

func generationBoundaryAllowances() map[string]historicalBoundaryAllowance {
	allowed := map[string]historicalBoundaryAllowance{
		generationBoundaryLoop + "NewForkCorrespondence/reference:" + generationBoundaryLoop + "Fork":             {1, "canonical relation constructs the sole expected child activation"},
		generationBoundaryLoop + "ForkCorrespondence.Bind/reference:" + generationBoundaryLoop + "ForkGeneration": {1, "canonical relation translates an admitted source reference"},
		generationBoundaryLoop + "Activation.CapturedContext/canonical_coordinate":                                {2, "projects only captured attempt/revision after exact historical and current owner validation"},
		historicalBoundaryOwner + "forkPendingProposedEffect/coordinate:RevisionID":                               {1, "fresh continuation projects the admitted child reference"},
		historicalBoundaryOwner + "projectForkRevisionPayload/coordinate:RevisionField":                           {2, "bound payload projection checks and replaces only the admitted field"},
		historicalBoundaryOwner + "projectForkRevisionPayload/coordinate:RevisionID":                              {2, "bound payload projection checks and replaces the exact admitted revision"},
		historicalBoundaryOwner + "runForkActivityFact/coordinate:RevisionID":                                     {1, "activity story readback projects admitted journal evidence"},
	}
	for _, function := range []string{"prepareRunForkSelectedContractSourceEvent", "projectRunForkFanOutCapsule"} {
		for _, coordinate := range []string{"FlowID", "LoopID", "RevisionField"} {
			allowed[historicalBoundaryOwner+function+"/coordinate:"+coordinate] = historicalBoundaryAllowance{1, "compare original declared role with the already admitted exact generation"}
		}
	}
	for function, count := range map[string]int{
		"Activation.Key": 2, "Activation.Validate": 24, "Activation.Admit": 3,
		"Activation.ownsRevision": 2, "Activation.Close": 6, "Activation.OwnsGeneration": 13,
		"Activation.AdvanceWithin": 4, "Activation.Repeat": 13, "Activation.Generation": 6,
		"Activation.Context": 7, "PublicActivations": 7, "GenerationCurrent": 4,
		"ForkGeneration": 8, "Fork": 7,
	} {
		allowed[generationBoundaryLoop+function+"/canonical_coordinate"] = historicalBoundaryAllowance{count, "existing canonical loop identity, admission, lifecycle or public projection owner"}
	}
	for function, count := range map[string]int{
		"forkActivationInventory": 18, "ForkCorrespondence.AdmitSourceKey": 5,
		"ForkCorrespondence.AdmitSourceRevision": 5, "ForkCorrespondence.AdmitSource": 2,
		"ForkCorrespondence.ProjectedActivations": 6, "ForkChildReference.Context": 2,
		"ForkCorrespondence.AdmitChild": 7, "ForkCorrespondence.ValidateChild": 8,
		"ForkCorrespondence.AdmitSourceContext": 1,
	} {
		allowed[generationBoundaryLoop+function+"/canonical_coordinate"] = historicalBoundaryAllowance{count, "exact source/child relation admission and projection, never caller-local election"}
	}
	for _, function := range []string{"New", "Activation.Generation", "ForkGeneration", "ForkCorrespondence.AdmitSourceContext"} {
		allowed[generationBoundaryLoop+function+"/generation_construction"] = historicalBoundaryAllowance{1, "canonical generation construction followed by exact owner validation"}
	}
	return allowed
}

func generationBoundaryCapturedOwnerHostile(t *testing.T, loaded *packages.Package, imports historicalBoundaryImports) {
	t.Helper()
	for _, coordinate := range []string{"Attempt", "RevisionID"} {
		t.Run(coordinate, func(t *testing.T) {
			fset := token.NewFileSet()
			var files []*ast.File
			changed := false
			for _, path := range loaded.CompiledGoFiles {
				body, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				text := string(body)
				const signature = "func (a Activation) CapturedContext(g attemptgeneration.Generation) (map[string]any, error) {"
				if strings.Contains(text, signature) {
					if changed || strings.Count(text, signature) != 1 {
						t.Fatal("expected one captured projection owner")
					}
					// Same approved method/file, arbitrary local receiver name: an
					// extra current-owner read must exceed the exact projection budget.
					text = strings.Replace(text, signature, signature+"\n arbitraryFruit := a; _ = arbitraryFruit."+coordinate, 1)
					changed = true
				}
				file, err := parser.ParseFile(fset, path, text, 0)
				if err != nil {
					t.Fatal(err)
				}
				files = append(files, file)
			}
			if !changed {
				t.Fatal("captured owner hostile injection did not land")
			}
			info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
			pkg, err := (&types.Config{Importer: imports}).Check(loaded.PkgPath, fset, files, info)
			if err != nil {
				t.Fatal(err)
			}
			var findings []historicalBoundaryFinding
			for _, file := range files {
				findings = append(findings, generationBoundaryCollect(pkg, info, fset, file)...)
			}
			problems := historicalBoundaryProblems(findings, generationBoundaryAllowances(), false)
			if len(problems) != 1 || !strings.Contains(problems[0], "Activation.CapturedContext/canonical_coordinate") {
				t.Fatalf("approved owner admitted extra current-generation %s read: %v", coordinate, problems)
			}
		})
	}
}

func generationBoundaryHostile(t *testing.T, imports historicalBoundaryImports) {
	const source = `package runforkpersistence
import (
    generation "github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
    loops "github.com/division-sh/swarm/internal/runtime/loopruntime"
)
type alias = generation.Generation
type arbitraryReceiver struct{}
func (banana *arbitraryReceiver) selectFirst(values []alias, wanted string) alias {
    for _, value := range values { if value.LoopID == wanted { return value } }
    return alias{}
}
func (banana *arbitraryReceiver) remap(value alias) (alias, error) {
    hidden := loops.ForkGeneration
    return hidden(value, "child", "entity")
}
func forged() alias { return alias{LoopID: "same-label"} }
type wrapped struct { alias }
func promoted(value wrapped) string { return value.RevisionID }
func ordinaryBusiness(value struct{ LoopID string }) string { return value.LoopID }
`
	fset := token.NewFileSet()
	// An otherwise approved filename grants no authority to these functions.
	file, err := parser.ParseFile(fset, "run_fork_activity_generation.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	pkg, err := (&types.Config{Importer: imports}).Check(historicalBoundaryModule+"store/internal/backend/runforkpersistence", fset, []*ast.File{file}, info)
	if err != nil {
		t.Fatal(err)
	}
	findings := generationBoundaryCollect(pkg, info, fset, file)
	if len(findings) != 4 || len(historicalBoundaryProblems(findings, generationBoundaryAllowances(), false)) != 4 {
		t.Fatalf("hostile aliases/receiver/construction must all be detected: %+v", findings)
	}
}
