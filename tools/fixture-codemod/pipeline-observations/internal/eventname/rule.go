package eventname

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

var Helpers = map[string]bool{
	"servedEventNameCount":        true,
	"servedEventNameCountForRun":  true,
	"requireServedEventNameCount": true,
}

const GlobalCounter = `func servedEventNameCount(t *testing.T, reader receiverProofStateReader, backend, eventName string) int {
	t.Helper()
	switch backend {
	case "postgres", "sqlite":
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		count, err = storetest.CountEventNameStorage(ctx, reader, eventName)
		return err
	})
	if err != nil {
		t.Fatalf("%s count event name %s: %v", backend, eventName, err)
	}
	return count
}`

const RunCounter = `func servedEventNameCountForRun(t *testing.T, reader receiverProofStateReader, backend, runID, eventName string) int {
	t.Helper()
	switch backend {
	case "postgres", "sqlite":
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		count, err = storetest.CountRunNamedEventStorage(ctx, reader, runID, eventName)
		return err
	})
	if err != nil {
		t.Fatalf("%s count run %s event name %s: %v", backend, runID, eventName, err)
	}
	return count
}`

type edit struct {
	start, end int
	value      string
}

func Transform(before string) (string, int, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "scope.go", "package proof\n"+before, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return "", 0, fmt.Errorf("invalid event name declaration: %v", err)
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok {
		return before, 0, nil
	}
	switch fn.Name.Name {
	case "servedEventNameCount":
		after, err := requestkey.Normalize(GlobalCounter)
		return after, 0, err
	case "servedEventNameCountForRun":
		after, err := requestkey.Normalize(RunCounter)
		return after, 0, err
	}
	changes, err := callEdits(fset, fn)
	if err != nil {
		return "", 0, err
	}
	slices.SortFunc(changes, func(a, b edit) int { return b.start - a.start })
	after := before
	for _, change := range changes {
		after = after[:change.start] + change.value + after[change.end:]
	}
	if fn.Name.Name == "requireServedEventNameCount" {
		after = strings.Replace(after, "db *sql.DB", "reader receiverProofStateReader", 1)
	}
	after, err = requestkey.Normalize(after)
	return after, len(changes), err
}

func callEdits(fset *token.FileSet, fn *ast.FuncDecl) ([]edit, error) {
	var changes []edit
	var failure error
	start := fset.Position(fn.Pos()).Offset
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		id, ok := call.Fun.(*ast.Ident)
		if !ok || !Helpers[id.Name] {
			return true
		}
		value, err := transformedCall(fn, call, id)
		if err != nil {
			failure = err
			return false
		}
		changes = append(changes, edit{fset.Position(call.Pos()).Offset - start, fset.Position(call.End()).Offset - start, value})
		return true
	})
	return changes, failure
}

func transformedCall(fn *ast.FuncDecl, call *ast.CallExpr, id *ast.Ident) (string, error) {
	want := 4
	if id.Name != "servedEventNameCount" {
		want = 5
	}
	if len(call.Args) != want || call.Ellipsis.IsValid() || requestkey.Node(call.Args[0]) != "t" || id.Obj != nil && id.Obj.Kind != ast.Fun {
		return "", fmt.Errorf("unclassified event name call: %s", fn.Name.Name)
	}
	var args []string
	for _, argument := range call.Args {
		args = append(args, requestkey.Node(argument))
	}
	switch args[1] {
	case "rt.DB":
		args[1] = "rt.ReceiverStateReader"
	case "db":
		if fn.Name.Name != "requireServedEventNameCount" {
			return "", fmt.Errorf("event name caller has no original read role: %s", fn.Name.Name)
		}
		args[1] = "reader"
	default:
		return "", fmt.Errorf("unclassified event name read owner: %s", fn.Name.Name)
	}
	return id.Name + "(" + strings.Join(args, ", ") + ")", nil
}
