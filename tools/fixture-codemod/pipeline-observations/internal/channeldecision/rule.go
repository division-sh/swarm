package channeldecision

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

var Helpers = map[string]int{"waitUncertainCopyDecision": 3}
var Readers = map[string]string{
	"TestChannelLearnedObjectPublicJourney":                  "channelAnchorStorageObservation(t, h)",
	"TestChannelDeliveryUncertainCopyAuthorityPublicJourney": "channelAnchorStorageObservation(t, h)",
	"proveObjectChannelLostPrompt":                           "channelAnchorStorageObservation(t, h)",
}

type edit struct {
	start, end int
	value      string
}

func Transform(before string) (string, int, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "scope.go", "package proof\n"+before, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return "", 0, fmt.Errorf("invalid channel-decision declaration: %v", err)
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
	if Helpers[fn.Name.Name] != 0 {
		after = strings.Replace(after, "db *sql.DB", "reader storetest.ChannelAnchorObservation", 1)
	}
	after, err = nativeBody(fn.Name.Name, after)
	if err != nil {
		return "", 0, err
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
		if !ok || Helpers[id.Name] == 0 {
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
	if len(call.Args) != Helpers[id.Name] || call.Ellipsis.IsValid() || requestkey.Node(call.Args[0]) != "t" ||
		requestkey.Node(call.Args[1]) != "db" || id.Obj != nil && id.Obj.Kind != ast.Fun {
		return "", fmt.Errorf("unclassified channel-decision call: %s", fn.Name.Name)
	}
	reader := Readers[fn.Name.Name]
	if Helpers[fn.Name.Name] != 0 {
		reader = "reader"
	}
	if reader == "" {
		return "", fmt.Errorf("channel-decision caller lacks exact reader provenance: %s", fn.Name.Name)
	}
	var args []string
	for _, arg := range call.Args {
		args = append(args, requestkey.Node(arg))
	}
	args[1] = reader
	return id.Name + "(" + strings.Join(args, ", ") + ")", nil
}

func replaceExact(source, before, after string) (string, error) {
	if strings.Count(source, before) != 1 {
		return "", fmt.Errorf("channel-decision source escaped exact rewrite cut: %s", before)
	}
	return strings.Replace(source, before, after, 1), nil
}

func nativeBody(function, source string) (string, error) {
	if function == "waitUncertainCopyDecision" {
		return replaceExact(source,
			"var status, verdict string\n\t\tif err := db.QueryRow(`SELECT status,COALESCE(verdict,'') FROM decision_cards WHERE card_id=$1`, cardID).Scan(&status, &verdict); err != nil {\n\t\t\tt.Fatal(err)\n\t\t}",
			"observed, err := reader.ReadChannelAnchorDecisionStorage(context.Background(), cardID)\nif err != nil { t.Fatal(err) }\nstatus, verdict := observed.Status, observed.Verdict")
	}
	if function != "TestChannelSourceLifecyclePublicJourney" {
		return source, nil
	}
	source, err := replaceExact(source,
		"var status, verdict string\n\t\t\t\t\t\t\tcardErr := db.QueryRow(`SELECT status,COALESCE(verdict,'') FROM decision_cards WHERE card_id=$1`, child).Scan(&status, &verdict)",
		"observed, cardErr := channelAnchorStorageObservation(t, h).ReadChannelAnchorDecisionStorage(context.Background(), child)\nstatus, verdict := observed.Status, observed.Verdict")
	if err != nil {
		return "", err
	}
	return replaceExact(source,
		"if err := db.QueryRow(`SELECT status,COALESCE(verdict,'') FROM decision_cards WHERE card_id=$1`, child).Scan(&status, &verdict); err != nil ||",
		"replayedDecision, decisionErr := channelAnchorStorageObservation(t, h).ReadChannelAnchorDecisionStorage(context.Background(), child)\nstatus, verdict = replayedDecision.Status, replayedDecision.Verdict\nif err := decisionErr; err != nil ||")
}
