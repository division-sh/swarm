package deliverystatus

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
	"servedEventPublishDeliveryStatusCount":           true,
	"waitServedEventPublishDeliveryStatusCount":       true,
	"waitServedEventPublishDeliveryStatusCountForRun": true,
	"requireNoServedDeliveryStatusDuring":             true,
}

const Counter = `func servedEventPublishDeliveryStatusCount(t *testing.T, reader receiverProofStateReader, backend, eventID, subscriberType, subscriberID string, statuses ...string) int {
	t.Helper()
	switch backend {
	case "postgres", "sqlite":
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		count, err = storetest.ReadServedDeliveryStatusCount(ctx, reader, eventID, subscriberType, subscriberID, statuses...)
		return err
	})
	if err != nil {
		t.Fatalf("%s count deliveries for event=%s subscriber=%s/%s statuses=%#v: %v", backend, eventID, subscriberType, subscriberID, statuses, err)
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
		return "", 0, fmt.Errorf("invalid delivery status declaration: %v", err)
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok {
		return before, 0, nil
	}
	if fn.Name.Name == "servedEventPublishDeliveryStatusCount" {
		after, err := requestkey.Normalize(Counter)
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
	if fn.Name.Name == "waitServedEventPublishDeliveryStatusCountForRun" {
		after = strings.Replace(after, "db *sql.DB, reader receiverProofStateReader", "reader receiverProofStateReader", 1)
	} else if Helpers[fn.Name.Name] {
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
	if len(call.Args) < 6 || requestkey.Node(call.Args[0]) != "t" || id.Obj != nil && id.Obj.Kind != ast.Fun {
		return "", fmt.Errorf("unclassified status call: %s", fn.Name.Name)
	}
	var args []string
	for _, argument := range call.Args {
		args = append(args, requestkey.Node(argument))
	}
	if id.Name == "waitServedEventPublishDeliveryStatusCountForRun" {
		if len(args) != 10 || call.Ellipsis.IsValid() {
			return "", fmt.Errorf("unclassified status run cut: %s", fn.Name.Name)
		}
		args = append(args[:1], args[2:]...) // Keep its existing exact read owner.
	} else {
		reader, err := callReader(fn, args[1])
		if err != nil {
			return "", err
		}
		args[1] = reader
	}
	if call.Ellipsis.IsValid() {
		args[len(args)-1] += "..."
	}
	return id.Name + "(" + strings.Join(args, ", ") + ")", nil
}

func callReader(fn *ast.FuncDecl, argument string) (string, error) {
	if argument == "rt.DB" {
		return "rt.ReceiverStateReader", nil
	}
	if argument == "db" {
		if Helpers[fn.Name.Name] {
			return "reader", nil
		}
		for _, field := range fn.Type.Params.List {
			if requestkey.Node(field.Type) == "receiverProofStateReader" {
				for _, id := range field.Names {
					if id.Name == "reader" {
						return "reader", nil
					}
				}
			}
		}
	}
	return "", fmt.Errorf("status caller has no original native read role: %s/%s", fn.Name.Name, argument)
}
