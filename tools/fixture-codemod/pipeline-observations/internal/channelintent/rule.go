package channelintent

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"slices"
	"strconv"
	"strings"
)

const Waiter = `func waitObjectChannelDisposition(t *testing.T, reader channeldelivery.Observer, query channeldelivery.IntentObservationQuery, allowed ...string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		settled, result := observeChannelDisposition(t, reader, query, allowed...)
		if settled {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("object intent %+v not settled: %+v", query, result)
		}
		time.Sleep(20 * time.Millisecond)
	}
}`

const Query = `func objectIntentQuery(identity channeldelivery.IntentObservationQuery, kind channeldelivery.IntentKind, occurrence string) channeldelivery.IntentObservationQuery {
	identity.Kind, identity.ProviderEventID = kind, occurrence
	return identity
}`

const Close = `func closeObjectIntentObservation(t *testing.T, p *objectChannelProvider) {
	t.Helper()
	if p.intentReader != nil {
		if err := p.intentReader.Close(); err != nil {
			t.Fatal(err)
		}
		p.intentReader = nil
	}
}`

const CrashWait = `func waitChannelInputCommitDispositions(t *testing.T, p *objectChannelProvider, mode string, partial bool) {
	t.Helper()
	if mode != "skip" {
		disposition := "input_complete"
		if partial {
			disposition = "input_progressed"
		}
		if mode == "chosen" {
			disposition = "chooser"
		}
		waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(p.intentIdentity, channeldelivery.IntentText, "crash-answer"), disposition)
	}
	if mode != "plain" {
		waitObjectChannelDisposition(t, p.intentReader, objectIntentQuery(p.intentIdentity, channeldelivery.IntentAction, "crash-input-action"), "applied")
	}
}`

type edit struct {
	start, end int
	after      string
}

func Normalize(source string) (string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "rule.go", "package proof\n"+source, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return "", fmt.Errorf("invalid channel-intent declaration: %w", err)
	}
	return Node(file.Decls[0]), nil
}

func Node(node ast.Node) string {
	var out bytes.Buffer
	if err := format.Node(&out, token.NewFileSet(), node); err != nil {
		panic(err)
	}
	return out.String()
}

func Transform(before string) (string, int, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "rule.go", "package proof\n"+before, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return "", 0, fmt.Errorf("invalid intent predecessor: %w", err)
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok {
		return transformCarrier(before)
	}
	if fn.Name.Name == "waitObjectChannelDisposition" {
		after, err := Normalize(Waiter)
		return after, 0, err
	}
	after, calls, err := rewriteCalls(fset, fn, before)
	if err != nil {
		return "", 0, err
	}
	if fn.Name.Name == "assertChannelInputCommit" {
		after, err = removeCrashWait(after)
	} else {
		after = captureAndJoin(fn.Name.Name, after)
	}
	if err != nil {
		return "", 0, err
	}
	after, err = Normalize(after)
	return after, calls, err
}

func transformCarrier(before string) (string, int, error) {
	if !strings.HasPrefix(before, "type objectChannelProvider struct {\n") {
		return before, 0, nil
	}
	after := strings.Replace(before, "struct {\n", "struct {\n\tintentIdentity channeldelivery.IntentObservationQuery\n\tintentReader storetest.ChannelObservation\n", 1)
	normal, err := Normalize(after)
	return normal, 0, err
}

func rewriteCalls(fset *token.FileSet, fn *ast.FuncDecl, before string) (string, int, error) {
	var edits []edit
	var failure error
	start := fset.Position(fn.Pos()).Offset
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		id, ok := call.Fun.(*ast.Ident)
		if !ok || id.Name != "waitObjectChannelDisposition" {
			return true
		}
		after, err := rewriteCall(fn.Name.Name, call, id)
		if err != nil {
			failure = err
			return false
		}
		edits = append(edits, edit{fset.Position(call.Pos()).Offset - start, fset.Position(call.End()).Offset - start, after})
		return true
	})
	if failure != nil {
		return "", 0, failure
	}
	slices.SortFunc(edits, func(a, b edit) int { return b.start - a.start })
	for _, e := range edits {
		before = before[:e.start] + e.after + before[e.end:]
	}
	return before, len(edits), nil
}

func rewriteCall(function string, call *ast.CallExpr, id *ast.Ident) (string, error) {
	if len(call.Args) < 5 || call.Ellipsis.IsValid() || Node(call.Args[0]) != "t" || Node(call.Args[1]) != "db" || id.Obj != nil && id.Obj.Kind != ast.Fun {
		return "", fmt.Errorf("unclassified intent call: %s", function)
	}
	table, err := strconv.Unquote(Node(call.Args[2]))
	if err != nil {
		return "", fmt.Errorf("nonliteral intent table: %s", function)
	}
	var kind string
	switch table {
	case "operator_channel_text_intents":
		kind = "channeldelivery.IntentText"
	case "operator_channel_action_intents":
		kind = "channeldelivery.IntentAction"
	default:
		return "", fmt.Errorf("unknown intent table: %s", function)
	}
	carrier := "p"
	if function == "TestChannelLearnedObjectPublicJourney" {
		carrier = "provider"
	}
	identity := carrier + ".intentIdentity"
	if function == "proveObjectChannelRebindRetirement" || function == "proveChannelInputAuthorityRestart" && strings.HasPrefix(Node(call.Args[3]), `"authority-old-`) {
		identity = "successorIntent"
	}
	args := []string{"t", carrier + ".intentReader", "objectIntentQuery(" + identity + ", " + kind + ", " + Node(call.Args[3]) + ")"}
	for _, arg := range call.Args[4:] {
		args = append(args, Node(arg))
	}
	return "waitObjectChannelDisposition(" + strings.Join(args, ", ") + ")", nil
}

var lifetimeCarriers = map[string]string{
	"TestChannelLearnedObjectPublicJourney":                     "provider",
	"proveObjectChannelRebindRetirement":                        "p",
	"proveObjectChannelChooserRestart":                          "p",
	"proveObjectChannelLostPrompt":                              "p",
	"TestChannelLearnedObjectRecoveryPagingPublicJourney":       "p",
	"proveObjectAllRecoveryChoices":                             "p",
	"proveObjectResponseEditLoss":                               "p",
	"TestChannelLearnedObjectHumanTemporalReceiptPublicJourney": "p",
	"proveChannelInputAuthorityRestart":                         "p",
	"proveChannelInputCommitDeath":                              "p",
}

func captureAndJoin(function, source string) string {
	carrier := lifetimeCarriers[function]
	stopped := false
	captured := false
	cleanup := cleanupLines(source)
	var out []string
	for index, line := range strings.Split(source, "\n") {
		if cleanup[index] {
			out = append(out, line)
			continue
		}
		plain := strings.TrimSpace(line)
		indent := line[:len(line)-len(strings.TrimLeft(line, "\t"))]
		if carrier != "" && (plain == "h.stop(t)" || strings.HasPrefix(plain, "if err := first.kill();") || strings.HasPrefix(plain, "if err := second.stop();")) {
			out = append(out, indent+"closeObjectIntentObservation(t, "+carrier+")")
			stopped = true
		}
		if function == "proveChannelInputCommitDeath" && plain == "assertChannelInputCommit(t, db, card, mode, partial)" {
			out = append(out, indent+"waitChannelInputCommitDispositions(t, p, mode, partial)")
		}
		out = append(out, line)
		out = append(out, captureLines(function, carrier, stopped, captured, plain, indent)...)
		if plain == "waitObjectNativeQualification(t, h, begun.Operation.OperationID)" && len(readyCapture(function, indent)) != 0 {
			captured = true
		}
	}
	return strings.Join(out, "\n")
}

func cleanupLines(source string) map[int]bool {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "rule.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		panic(err)
	}
	lines := map[int]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) != 1 {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "Cleanup" || Node(selector.X) != "t" {
			return true
		}
		fn, ok := call.Args[0].(*ast.FuncLit)
		if ok {
			for line := fset.Position(fn.Pos()).Line - 2; line <= fset.Position(fn.End()).Line-2; line++ {
				lines[line] = true
			}
		}
		return true
	})
	return lines
}

func captureLines(function, carrier string, stopped, captured bool, plain, indent string) []string {
	var out []string
	if carrier != "" && (plain == "h.start(t)" && (function != "TestChannelLearnedObjectPublicJourney" || stopped) || plain == "h.endpoint = first.endpoint(t)" || plain == "h.endpoint = second.endpoint(t)") {
		out = append(out, indent+carrier+".intentReader = openChannelAnchorObservation(t, h)")
	}
	if plain == "old := getChannelOnboardingRPC(t, h, operationID)" && function == "proveChannelInputAuthorityRestart" {
		out = append(out, indent+"successorIntent := p.intentIdentity")
	}
	if !captured && plain == "waitObjectNativeQualification(t, h, begun.Operation.OperationID)" {
		out = append(out, readyCapture(function, indent)...)
	}
	return out
}

func readyCapture(function, indent string) []string {
	const tuple = "channeldelivery.IntentObservationQuery{Provider: ready.Operation.Provider, InterfaceKey: ready.Operation.Interface.Key()}"
	switch function {
	case "TestChannelLearnedObjectPublicJourney":
		return []string{indent + "provider.intentIdentity = " + tuple, indent + "provider.intentReader = openChannelAnchorObservation(t, h)"}
	case "startObjectChannelJourney":
		return []string{indent + "p.intentIdentity = " + tuple, indent + "p.intentReader = openChannelAnchorObservation(t, h)"}
	case "proveObjectChannelRebindRetirement":
		return []string{indent + "successorIntent := " + tuple}
	case "proveChannelInputAuthorityRestart":
		return []string{indent + "successorIntent = " + tuple}
	}
	return nil
}

func removeCrashWait(source string) (string, error) {
	const marker = "\tvar index, decisions int\n"
	index := strings.Index(source, marker)
	if index < 0 {
		return "", fmt.Errorf("crash assertion tail disappeared")
	}
	const header = "func assertChannelInputCommit(t *testing.T, db *sql.DB, card, mode string, partial bool) {\n\tt.Helper()\n"
	if !strings.HasPrefix(source, header) {
		return "", fmt.Errorf("crash assertion signature changed")
	}
	prefix := source[:index] + "}"
	want := strings.Replace(CrashWait, "func waitChannelInputCommitDispositions(t *testing.T, p *objectChannelProvider, mode string, partial bool)", "func assertChannelInputCommit(t *testing.T, db *sql.DB, card, mode string, partial bool)", 1)
	if prefix != want {
		return "", fmt.Errorf("crash wait has changed conditions/dispositions/cuts")
	}
	return header + source[index:], nil
}
