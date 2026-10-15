package channeleventcount

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"slices"
	"strconv"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

var Consumers = map[string]bool{
	"TestChannelActionAcknowledgmentAbruptProcessDeathPublicJourney": true,
	"TestChannelDraftTerminalRestartPublicJourney":                   true,
	"TestChannelDeliveryAbruptProcessDeathPublicJourney":             true,
	"TestChannelDeliveryHumanTemporalReceiptPublicJourney":           true,
	"TestChannelDeliveryUncertainCopyAuthorityPublicJourney":         true,
	"TestChannelLearnedObjectRealAnchorProducersPublicJourney":       true,
	"TestChannelLearnedObjectPublicJourney":                          true,
	"proveObjectChannelRecovery":                                     true,
	"TestChannelLearnedObjectHumanTemporalReceiptPublicJourney":      true,
	"TestChannelSelectedForkInputBoundaryMatrix":                     true,
	"TestChannelSourceLifecyclePublicJourney":                        true,
}

var predicate = regexp.MustCompile(`^SELECT COUNT\(\*\) FROM events WHERE run_id=\$1 AND event_name='([a-z_.]+)'$`)

type countRead struct{ destination, run, name string }
type edit struct {
	start, end int
	after      string
}

func Transform(before string) (string, int, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "scope.go", "package proof\n"+before, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return "", 0, fmt.Errorf("invalid channel event-count declaration: %v", err)
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok || fn.Body == nil {
		return before, 0, nil
	}
	var changes []edit
	var failure error
	start := fset.Position(fn.Pos()).Offset
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		after, owned, err := statement(fn.Name.Name, node)
		if err != nil {
			failure = err
			return false
		}
		if owned {
			changes = append(changes, edit{fset.Position(node.Pos()).Offset - start, fset.Position(node.End()).Offset - start, after})
			return false
		}
		return true
	})
	if failure != nil {
		return "", 0, failure
	}
	slices.SortFunc(changes, func(a, b edit) int { return b.start - a.start })
	after := before
	for _, change := range changes {
		after = after[:change.start] + change.after + after[change.end:]
	}
	after, err = requestkey.Normalize(after)
	return after, len(changes), err
}

func statement(function string, node ast.Node) (string, bool, error) {
	switch node := node.(type) {
	case *ast.IfStmt:
		init, ok := node.Init.(*ast.AssignStmt)
		if !ok {
			return "", false, nil
		}
		read, errName, owned, err := assignment(function, init)
		if err != nil || !owned {
			return "", owned, err
		}
		if init.Tok != token.DEFINE {
			return "", false, fmt.Errorf("channel event count has unreviewed if binding")
		}
		copy := *node
		copy.Init = nil
		return "{\nvar " + errName + " error\n" + read.destination + ", " + errName + " = " + expression(read) + "\n" + requestkey.Node(&copy) + "\n}", true, nil
	case *ast.AssignStmt:
		read, errName, owned, err := assignment(function, node)
		if err != nil || !owned {
			return "", owned, err
		}
		prefix := ""
		if node.Tok == token.DEFINE {
			prefix = "var " + errName + " error\n"
		}
		return prefix + read.destination + ", " + errName + " = " + expression(read), true, nil
	}
	return "", false, nil
}

func assignment(function string, node *ast.AssignStmt) (countRead, string, bool, error) {
	if len(node.Rhs) != 1 {
		return countRead{}, "", false, nil
	}
	read, owned, err := scan(node.Rhs[0])
	if err != nil || !owned {
		return read, "", owned, err
	}
	if !Consumers[function] || len(node.Lhs) != 1 {
		return read, "", false, fmt.Errorf("unreviewed channel event count consumer/binding: %s", function)
	}
	name, ok := node.Lhs[0].(*ast.Ident)
	if !ok || name.Name == "_" || name.Name == read.destination || (node.Tok != token.DEFINE && node.Tok != token.ASSIGN) {
		return read, "", false, fmt.Errorf("unreviewed channel event count error binding")
	}
	return read, name.Name, true, nil
}

func scan(expr ast.Expr) (countRead, bool, error) {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return countRead{}, false, nil
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Scan" {
		return countRead{}, false, nil
	}
	query, ok := sel.X.(*ast.CallExpr)
	if !ok || len(query.Args) == 0 {
		return countRead{}, false, nil
	}
	literal, ok := query.Args[0].(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return countRead{}, false, nil
	}
	text, err := strconv.Unquote(literal.Value)
	if err != nil {
		return countRead{}, false, err
	}
	match := predicate.FindStringSubmatch(text)
	if match == nil {
		return countRead{}, false, nil
	}
	return scanCoordinates(call, query, match[1])
}

func scanCoordinates(call, query *ast.CallExpr, name string) (countRead, bool, error) {
	if requestkey.Node(query.Fun) != "db.QueryRow" || len(query.Args) != 2 || len(call.Args) != 1 || query.Ellipsis.IsValid() || call.Ellipsis.IsValid() {
		return countRead{}, false, fmt.Errorf("unreviewed channel event count carrier/arity")
	}
	address, ok := call.Args[0].(*ast.UnaryExpr)
	if !ok || address.Op != token.AND {
		return countRead{}, false, fmt.Errorf("channel event count destination is not the exact local")
	}
	destination, ok := address.X.(*ast.Ident)
	if !ok || destination.Name == "_" {
		return countRead{}, false, fmt.Errorf("channel event count has unreviewed destination")
	}
	return countRead{destination.Name, requestkey.Node(query.Args[1]), name}, true, nil
}

func expression(read countRead) string {
	return "channelAnchorStorageObservation(t, h).CountRunNamedEventStorage(context.Background(), " + read.run + ", " + strconv.Quote(read.name) + ")"
}
