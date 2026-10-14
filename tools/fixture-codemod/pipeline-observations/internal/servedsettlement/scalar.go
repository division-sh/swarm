package servedsettlement

import (
	"fmt"
	"go/ast"
	"go/token"
	"regexp"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

const ScalarPredecessor = "9ee3ba6663d789b5292b3770f12e7233469ddfc0"
const ScalarPath = "tools/fixture-codemod/pipeline-observations/family145-served-setup-scalar-preimages.json"

// These are complete, existing-owner query shapes, not arbitrary SQL routing.
var scalarOwners = map[string]struct {
	port, result string
	arguments    int
}{
	"SELECT COUNT(*) FROM RUNS":                                               {"CountGlobalRunStorage", "int", 0},
	"SELECT COUNT(*) FROM FLOW_INSTANCES WHERE RUN_ID=$1":                     {"CountRunFlowInstanceStorage", "int", 1},
	"SELECT COUNT(*) FROM ENTITY_STATE WHERE RUN_ID=$1":                       {"CountWorkflowRunFieldRows", "int", 1},
	"SELECT COUNT(*) FROM EVENTS WHERE RUN_ID=$1":                             {"CountPhysicalRunEvents", "int", 1},
	"SELECT COUNT(*) FROM EVENTS WHERE RUN_ID=$1 AND EVENT_NAME=$2":           {"CountRunNamedEventStorage", "int", 2},
	"SELECT COUNT(*) FROM EVENTS WHERE RUN_ID=$1 AND EVENT_ID=$2":             {"CountRunExactEventStorage", "int", 2},
	"SELECT COUNT(*) FROM ENTITY_STATE WHERE RUN_ID=$1 AND CURRENT_STATE=$2":  {"CountRunStateEntityStorage", "int", 2},
	"SELECT STATUS FROM RUNS WHERE RUN_ID=$1":                                 {"ReadRunStatusStorage", "string", 1},
	"SELECT CURRENT_STATE FROM ENTITY_STATE WHERE RUN_ID=$1 AND ENTITY_ID=$2": {"ReadCLIMaterializedEntityState", "string", 2},
	"SELECT COUNT(*) FROM TIMERS WHERE RUN_ID=$1":                             {"CountRunTimerStorage", "int", 1},
}

// ScalarCall is used only after the driver proves the original selected fixture
// field and destination types. The splice leaves the surrounding oracle intact.
func ScalarCall(set *token.FileSet, scan *ast.CallExpr, resultType string) (Splice, string, error) {
	query, receiver, destination, err := scalarQuery(scan)
	if err != nil {
		return Splice{}, "", err
	}
	querySelector := query.Fun.(*ast.SelectorExpr)
	ctx, index := "context.Background()", 0
	if querySelector.Sel.Name == "QueryRowContext" {
		if len(query.Args) == 0 {
			return Splice{}, "", fmt.Errorf("scalar context query has no context")
		}
		ctx, index = requestkey.Node(query.Args[0]), 1
	}
	if len(query.Args) <= index {
		return Splice{}, "", fmt.Errorf("scalar query has no literal")
	}
	literal, ok := query.Args[index].(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return Splice{}, "", fmt.Errorf("scalar query is not a complete literal")
	}
	sql, err := strconv.Unquote(literal.Value)
	if err != nil {
		return Splice{}, "", err
	}
	key, fixedArgument := scalarQueryKey(sql)
	owner, ok := scalarOwners[key]
	argumentsCount := len(query.Args) - index - 1
	if fixedArgument != "" {
		argumentsCount++
	}
	if !ok || owner.result != resultType || argumentsCount != owner.arguments || query.Ellipsis.IsValid() {
		return Splice{}, "", fmt.Errorf("scalar query/type/arity has no exact existing owner")
	}
	reader := requestkey.Node(receiver.X) + ".ReceiverStateReader"
	arguments := []string{"ctx", reader}
	for _, arg := range query.Args[index+1:] {
		arguments = append(arguments, requestkey.Node(arg))
	}
	if fixedArgument != "" {
		arguments = append(arguments, fixedArgument)
	}
	after := fmt.Sprintf("%s.InspectSnapshot(%s,func(ctx context.Context)error{var err error;%s,err=storetest.%s(%s);return err})", reader, ctx, requestkey.Node(destination), owner.port, strings.Join(arguments, ","))
	return Splice{set.Position(scan.Pos()).Offset, set.Position(scan.End()).Offset, after}, owner.port, nil
}

var scalarFixedValue = regexp.MustCompile(`(?i)^(SELECT (?:COUNT\(\*\)|CURRENT_STATE|STATUS) FROM (?:EVENTS|ENTITY_STATE|RUNS) WHERE RUN_ID\s*=\s*\$1 AND (?:EVENT_NAME|CURRENT_STATE)\s*=\s*)'([a-zA-Z0-9_.:/-]+)'$`)

func scalarQueryKey(sql string) (string, string) {
	key := strings.Join(strings.Fields(sql), " ")
	fixed := ""
	if match := scalarFixedValue.FindStringSubmatch(key); match != nil {
		key, fixed = match[1]+"$2", strconv.Quote(match[2])
	}
	key = strings.ReplaceAll(strings.ReplaceAll(key, " =", "="), "= ", "=")
	return strings.ToUpper(key), fixed
}

func scalarQuery(scan *ast.CallExpr) (*ast.CallExpr, *ast.SelectorExpr, ast.Expr, error) {
	selector, ok := scan.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Scan" || len(scan.Args) != 1 || scan.Ellipsis.IsValid() {
		return nil, nil, nil, fmt.Errorf("scalar query requires one exact Scan destination")
	}
	query, ok := selector.X.(*ast.CallExpr)
	if !ok {
		return nil, nil, nil, fmt.Errorf("scalar Scan lost its original query")
	}
	querySelector, ok := query.Fun.(*ast.SelectorExpr)
	if !ok || (querySelector.Sel.Name != "QueryRow" && querySelector.Sel.Name != "QueryRowContext") {
		return nil, nil, nil, fmt.Errorf("scalar reader is not the original row query")
	}
	receiver, ok := querySelector.X.(*ast.SelectorExpr)
	if !ok || receiver.Sel.Name != "DB" {
		return nil, nil, nil, fmt.Errorf("scalar reader is not the served fixture's DB field")
	}
	address, ok := scan.Args[0].(*ast.UnaryExpr)
	if !ok || address.Op != token.AND {
		return nil, nil, nil, fmt.Errorf("scalar reader destination is not addressable")
	}
	return query, receiver, address.X, nil
}
