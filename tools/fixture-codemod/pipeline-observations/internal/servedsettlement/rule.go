package servedsettlement

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

const Predecessor = "871d459b5a21a13cc79de9eefba843c5416f9f1f"
const Path = "tools/fixture-codemod/pipeline-observations/family145-served-settlement-preimages.json"
const Digest = "dbf467316e449a09a2670d61cfe437bd6f492b32aaf97d9897e7dbda613ba217"

func Decode(raw []byte) ([]runphysical.Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("served settlement whole-family manifest changed")
	}
	var pins []runphysical.Pin
	if err := json.Unmarshal(raw, &pins); err != nil || len(pins) != 34 {
		return nil, fmt.Errorf("served settlement declaration inventory changed: %v", err)
	}
	return pins, nil
}

type Change struct {
	Argument int
	Remove   bool
}

var Changes = map[string]Change{
	"servedEventPublishNodeDeliveryCountValue":              {1, false},
	"servedEventPublishNodeDeliveryCount":                   {1, false},
	"servedEventPublishReceiptOutcomeCount":                 {1, false},
	"servedEventPublishBlockingHandlerAuthorityHook":        {0, false},
	"requireNoServedReceiptOutcomeDuring":                   {1, false},
	"requireServedEventPublishCommittedReplayScope":         {1, true},
	"waitServedEventPublishReceiptOutcomeCount":             {1, true},
	"waitServedDeliveryOutcomeCount":                        {1, true},
	"waitForServedEventPublishNodeDeliveryLifecycleForNode": {1, true},
	"waitForServedEventPublishNodeDeliveryLifecycle":        {1, true},
	"runServedEventPublishFollowUpProof":                    {2, true},
}

type Splice struct {
	Start, End int
	After      string
}

func Call(set *token.FileSet, call *ast.CallExpr, caller, callee string) (Splice, error) {
	change, ok := Changes[callee]
	if !ok || len(call.Args) <= change.Argument {
		return Splice{}, fmt.Errorf("unknown served settlement call shape: %s", callee)
	}
	arg := call.Args[change.Argument]
	start, end := set.Position(arg.Pos()).Offset, set.Position(arg.End()).Offset
	if change.Remove {
		if len(call.Args) <= change.Argument+1 {
			return Splice{}, fmt.Errorf("served settlement reader missing: %s", callee)
		}
		return Splice{Start: start, End: set.Position(call.Args[change.Argument+1].Pos()).Offset}, nil
	}
	if callee == "servedEventPublishBlockingHandlerAuthorityHook" {
		return Splice{start, end, "&reader"}, nil
	}
	reader, err := callerReader(caller, requestkey.Node(arg))
	return Splice{start, end, reader}, err
}

func callerReader(caller, argument string) (string, error) {
	switch argument {
	case "rt.DB":
		return "rt.ReceiverStateReader", nil
	case "proof.DB":
		return "proof.ReceiverStateReader", nil
	case "db":
		if caller == "seedServedRunControlPendingRunWithAgentDelivery" {
			return "rt.ReceiverStateReader", nil
		}
		if caller == "servedEventPublishBlockingHandlerAuthorityHook" {
			return "currentReader", nil
		}
		return "reader", nil
	default:
		return "", fmt.Errorf("unclassified served settlement reader: %s/%s", caller, argument)
	}
}

func Signature(set *token.FileSet, fn *ast.FuncDecl) (Splice, error) {
	change, ok := Changes[fn.Name.Name]
	if !ok {
		return Splice{}, fmt.Errorf("unknown served settlement declaration")
	}
	for index, field := range fn.Type.Params.List {
		if len(field.Names) != 1 || field.Names[0].Name != "db" {
			continue
		}
		if requestkey.Node(field.Type) != "*sql.DB" {
			return Splice{}, fmt.Errorf("served settlement declaration lost original SQL carrier")
		}
		start, end := set.Position(field.Pos()).Offset, set.Position(field.End()).Offset
		if change.Remove {
			if index+1 >= len(fn.Type.Params.List) {
				return Splice{}, fmt.Errorf("served settlement declaration lost reader parameter")
			}
			return Splice{Start: start, End: set.Position(fn.Type.Params.List[index+1].Pos()).Offset}, nil
		}
		role := "reader servedEventCountReader"
		if fn.Name.Name == "servedEventPublishBlockingHandlerAuthorityHook" {
			role = "reader *receiverProofStateReader"
		}
		return Splice{start, end, role}, nil
	}
	return Splice{}, fmt.Errorf("served settlement declaration has no unique SQL parameter: %s", fn.Name.Name)
}

func OwnerBody(name, source string) (string, error) {
	after := source
	switch name {
	case "servedEventPublishNodeDeliveryCountValue":
		return `func servedEventPublishNodeDeliveryCountValue(ctx context.Context,reader servedEventCountReader,backend,runID,eventID,subscriberID string)(int,error){if backend!="postgres"&&backend!="sqlite"{return 0,fmt.Errorf("unknown proof backend %q",backend)};return readServedExactRunNodeDeliveryCount(ctx,reader,runID,eventID,subscriberID)}`, nil
	case "servedEventPublishReceiptOutcomeCount":
		return `func servedEventPublishReceiptOutcomeCount(t *testing.T,reader servedEventCountReader,backend,eventID,subscriberType,subscriberID,outcome string)int{t.Helper();if backend!="postgres"&&backend!="sqlite"{t.Fatalf("unknown proof backend %q",backend)};count,err:=readServedSubscriberReceiptOutcomeCount(context.Background(),reader,eventID,subscriberType,subscriberID,outcome);if err!=nil{t.Fatalf("%s receipt count query failed: %v",backend,err)};return count}`, nil
	case "requireServedEventPublishCommittedReplayScope":
		start, end, err := statementRange(source, "switch", "var count int")
		if err != nil {
			return "", err
		}
		prefix := source[:start]
		declarations := strings.Index(prefix, "var (")
		if declarations < 0 {
			return "", fmt.Errorf("replay scope declaration changed")
		}
		after = prefix[:declarations] + `if backend!="postgres"&&backend!="sqlite"{t.Fatalf("unknown proof backend %q",backend)};` + source[end:]
		after = strings.Replace(after, "if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil", "count,err:=readServedExactReplayScopeCount(context.Background(),reader,runID,eventID,strings.TrimSpace(wantScope)); if err!=nil", 1)
		after = strings.Replace(after, "var count int", "", 1)
		after = strings.Replace(after, `"%s committed replay scope query failed: %v\n%s", backend, err, query`, `"%s committed replay scope query failed: %v", backend, err`, 1)
	case "waitServedDeliveryOutcomeCount":
		start, end, err := statementRange(source, "var query string", "if got == want")
		if err != nil {
			return "", err
		}
		after = source[:start] + `if backend!="postgres"&&backend!="sqlite"{t.Fatalf("unknown proof backend %q",backend)};var err error;got,err=readServedCurrentDeliveryOutcomeCount(context.Background(),reader,eventID,subscriberType,subscriberID,outcome);if err!=nil{t.Fatalf("%s delivery outcome count query failed: %v",backend,err)};` + source[end:]
	case "seedServedRunControlPendingRunWithAgentDelivery":
		if strings.Count(source, "db := rt.DB") != 1 {
			return "", fmt.Errorf("seeded receipt carrier no longer exact")
		}
		after = strings.Replace(source, "db := rt.DB", "", 1)
	case "servedEventPublishBlockingHandlerAuthorityHook":
		cut := "once.Do(func() {"
		if strings.Count(source, cut) != 1 {
			return "", fmt.Errorf("handler observation lost exact once window")
		}
		after = strings.Replace(source, cut, cut+"\nvar currentReader receiverProofStateReader; if reader!=nil { currentReader=*reader }", 1)
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "source.go", "package proof\n"+after, parser.AllErrors); err != nil {
		return "", err
	}
	return after, nil
}

func statementRange(source, first, next string) (int, int, error) {
	start, end := strings.Index(source, first), strings.Index(source, next)
	if start < 0 || end <= start || strings.Count(source, next) != 1 {
		return 0, 0, fmt.Errorf("served settlement owner block changed: %s", first)
	}
	return start, end, nil
}

func Transform(source string) (string, int, error) {
	const prefix = "package proof\n"
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "source.go", prefix+source, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return "", 0, fmt.Errorf("served settlement requires one complete declaration: %v", err)
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok || fn.Recv != nil || fn.Body == nil {
		return "", 0, fmt.Errorf("served settlement requires a complete named function")
	}
	edits, calls, err := declarationEdits(set, fn)
	if err != nil {
		return "", 0, err
	}
	for index := range edits {
		edits[index].Start -= len(prefix)
		edits[index].End -= len(prefix)
	}
	raw, err := Apply([]byte(source), edits)
	if err != nil {
		return "", 0, err
	}
	after, err := OwnerBody(fn.Name.Name, string(raw))
	if err != nil {
		return "", 0, err
	}
	after, err = requestkey.Normalize(after)
	return after, calls, err
}

func declarationEdits(set *token.FileSet, fn *ast.FuncDecl) ([]Splice, int, error) {
	var edits []Splice
	if _, ok := Changes[fn.Name.Name]; ok {
		edit, err := Signature(set, fn)
		if err != nil {
			return nil, 0, err
		}
		edits = append(edits, edit)
	}
	called := map[*ast.Ident]bool{}
	var err error
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		ident, ok := call.Fun.(*ast.Ident)
		if !ok {
			return true
		}
		if _, ok := Changes[ident.Name]; !ok {
			return true
		}
		called[ident] = true
		edit, cause := Call(set, call, fn.Name.Name, ident.Name)
		if cause != nil || ident.Obj != nil {
			err = fmt.Errorf("unclassified/shadowed served settlement call: %s: %v", ident.Name, cause)
		} else {
			edits = append(edits, edit)
		}
		return true
	})
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if ident, ok := node.(*ast.Ident); ok {
			if _, target := Changes[ident.Name]; target && !called[ident] {
				err = fmt.Errorf("served settlement callable alias/reference is not a direct typed call: %s", ident.Name)
			}
		}
		return true
	})
	return edits, len(called), err
}

func Apply(raw []byte, edits []Splice) ([]byte, error) {
	slices.SortFunc(edits, func(a, b Splice) int { return b.Start - a.Start })
	last := len(raw)
	for _, edit := range edits {
		if edit.Start < 0 || edit.End > last || edit.Start >= edit.End {
			return nil, fmt.Errorf("served settlement overlapping/invalid AST splices")
		}
		raw = append(append(append([]byte{}, raw[:edit.Start]...), edit.After...), raw[edit.End:]...)
		last = edit.Start
	}
	return raw, nil
}
