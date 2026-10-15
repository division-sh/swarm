package workflowstate

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
	"requireLifecycleFlowEntity":    true,
	"requirePendingInputStateCount": true,
}

type edit struct {
	start, end int
	value      string
}

func Transform(before string) (string, int, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "scope.go", "package proof\n"+before, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return "", 0, fmt.Errorf("invalid workflow state declaration: %v", err)
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok {
		return before, 0, nil
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
	if Helpers[fn.Name.Name] {
		after = strings.Replace(after, "rt servedControlProofRuntime", "reader receiverProofStateReader", 1)
		after = strings.ReplaceAll(after, "rt.ReceiverStateReader", "reader")
		after, err = nativeBody(fn.Name.Name, after)
		if err != nil {
			return "", 0, err
		}
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
	want := 5
	if len(call.Args) != want || call.Ellipsis.IsValid() || requestkey.Node(call.Args[0]) != "t" || id.Obj != nil && id.Obj.Kind != ast.Fun {
		return "", fmt.Errorf("unclassified workflow state call: %s", fn.Name.Name)
	}
	var args []string
	for _, argument := range call.Args {
		args = append(args, requestkey.Node(argument))
	}
	if args[1] != "rt" {
		return "", fmt.Errorf("unclassified workflow state carrier: %s", fn.Name.Name)
	}
	args[1] = "rt.ReceiverStateReader"
	return id.Name + "(" + strings.Join(args, ", ") + ")", nil
}

func replaceExact(source, before, after string) (string, error) {
	if strings.Count(source, before) != 1 {
		return "", fmt.Errorf("workflow state source escaped exact query cut")
	}
	return strings.Replace(source, before, after, 1), nil
}

func nativeBody(function, source string) (string, error) {
	switch function {
	case "requirePendingInputStateCount":
		return replaceExact(source,
			"if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND current_state=$2`, runID, state).Scan(&count); err != nil {",
			"err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error { var err error; count, err = storetest.CountRunStateEntityStorage(ctx, reader, runID, state); return err })\nif err != nil {")
	case "requireLifecycleFlowEntity":
		return replaceExact(source,
			"err := rt.DB.QueryRow(`SELECT entity_id FROM flow_instances WHERE run_id=$1 AND instance_path=$2 AND current_state=$3`, runID, instance, state).Scan(&entityID)\n\t\tif err == nil {\n\t\t\treturn entityID\n\t\t}\n\t\tif err != sql.ErrNoRows {\n\t\t\tt.Fatal(err)\n\t\t}",
			"var found bool\nerr := reader.InspectSnapshot(context.Background(),func(ctx context.Context) error {var err error; entityID, found, err = storetest.ReadWorkflowStateEntityStorage(ctx, reader, runID, instance, state); return err })\nif err != nil { t.Fatal(err) }\nif found { return entityID }")
	}
	return source, nil
}
