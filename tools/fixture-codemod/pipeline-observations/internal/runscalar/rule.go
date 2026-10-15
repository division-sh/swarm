package runscalar

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

const Counter = `func servedEventPublishScalarCount(t *testing.T, reader receiverProofStateReader, backend, scope, runID, eventID string) int {
	t.Helper()
	switch backend {
	case "postgres", "sqlite":
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		switch scope {
		case "runs":
			count, err = storetest.CountPhysicalRunIdentity(ctx, reader, runID)
		case "application_events_by_run":
			count, err = storetest.CountApplicationRunEvents(ctx, reader, runID)
		case "event_deliveries":
			count, err = storetest.CountExactRunEventDeliveries(ctx, reader, runID, eventID)
		default:
			return fmt.Errorf("unknown %s proof count scope %q", backend, scope)
		}
		return err
	})
	if err != nil {
		t.Fatalf("%s count %s: %v", backend, scope, err)
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
		return "", 0, fmt.Errorf("invalid scalar declaration: %v", err)
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok {
		return before, 0, nil
	}
	if fn.Name.Name == "servedEventPublishScalarCount" {
		after, err := requestkey.Normalize(Counter)
		return after, 0, err
	}
	if fn.Name.Name == "servedEventPublishRowCount" {
		return "", 1, nil // Its two callers consume the single canonical counter directly.
	}
	var changes []edit
	var failure error
	start := fset.Position(fn.Pos()).Offset
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		id, ok := call.Fun.(*ast.Ident)
		if !ok || id.Name != "servedEventPublishScalarCount" && id.Name != "servedEventPublishRowCount" {
			return true
		}
		if len(call.Args) != 6 || call.Ellipsis.IsValid() || requestkey.Node(call.Args[0]) != "t" || id.Obj != nil && id.Obj.Kind != ast.Fun {
			failure = fmt.Errorf("unclassified scalar call: %s", fn.Name.Name)
			return false
		}
		reader, err := callReader(fn, call.Args[1])
		if err != nil {
			failure = err
			return false
		}
		var args []string
		for i, arg := range call.Args {
			value := requestkey.Node(arg)
			if i == 1 {
				value = reader
			}
			args = append(args, value)
		}
		changes = append(changes, edit{fset.Position(call.Pos()).Offset - start, fset.Position(call.End()).Offset - start, "servedEventPublishScalarCount(" + strings.Join(args, ", ") + ")"})
		return true
	})
	if failure != nil {
		return "", 0, failure
	}
	slices.SortFunc(changes, func(a, b edit) int { return b.start - a.start })
	after := before
	for _, change := range changes {
		after = after[:change.start] + change.value + after[change.end:]
	}
	after, err = requestkey.Normalize(after)
	return after, len(changes), err
}

func callReader(fn *ast.FuncDecl, argument ast.Expr) (string, error) {
	name := requestkey.Node(argument)
	if name == "db" {
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
	if name == "observer" && (fn.Name.Name == "TestServePostgresLossAndRestartFromDurableState" || fn.Name.Name == "TestServePostgresLostCommitResponseRecoversDurablePublication") && strings.Contains(requestkey.Node(fn), "var reader receiverProofStateReader") {
		return "reader", nil
	}
	return "", fmt.Errorf("scalar caller has no original native read role: %s/%s", fn.Name.Name, name)
}
