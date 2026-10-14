package main

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/imports"
)

const predecessor = "5ffa94a6fd9646b3ee7f38d138ebf4db965415f2"
const manifest = "tools/fixture-codemod/pipeline-observations/family145-channel-settlement-preimages.json"

var readerHelpers = map[string]bool{
	"waitObjectCardReceipt": true, "waitObjectRequestedResponse": true, "waitObjectIntentResponse": true,
	"waitChannelDraftPromptSettlement": true, "waitChannelDraftStatus": true, "waitNativeIntentCount": true,
	"selectedChannelDraftState": true, "waitObjectFreshCopyCount": true, "waitObjectDeliveryState": true,
	"readChannelHistoricalRender": true, "assertChannelHistoricalRender": true, "waitUncertainCopyPlan": true,
	"proveObjectChannelPages": true,
}
var derivativeHelpers = map[string]bool{
	"beginObjectInputCard": true, "requestObjectRecoveryInbox": true, "proveObjectResponseEditLoss": true,
	"proveObjectAllRecoveryChoices": true, "proveObjectChannelChooserRestart": true,
	"createObjectUncertainCards": true, "proveObjectChannelRecovery": true, "proveObjectChannelLostPrompt": true,
}

type splice struct {
	start, end int
	after      string
}
type projection struct {
	method string
	fields []string
}

func originalOverlay(root string) map[string][]byte {
	raw, err := exec.Command("git", "-C",root,"archive", predecessor, "--", "internal/serveapp").Output()
	must(err)
	reader := tar.NewReader(bytes.NewReader(raw))
	files := map[string][]byte{}
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		must(err)
		if header.Typeflag != tar.TypeReg || !strings.HasSuffix(header.Name, ".go") {
			continue
		}
		raw, err := io.ReadAll(reader)
		must(err)
		files[filepath.Join(root, header.Name)] = raw
	}
	if len(files) == 0 {
		panic("empty signed source inventory")
	}
	return files
}

func checkedLoad(root string, overlay map[string][]byte) []*packages.Package {
	pkgs, err := packages.Load(&packages.Config{Dir: root, Tests: true, Overlay: overlay, Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedSyntax | packages.NeedImports}, "./internal/serveapp")
	must(err)
	if len(pkgs) == 0 || packages.PrintErrors(pkgs) != 0 {
		panic("complete channel candidate does not type-check")
	}
	return pkgs
}

func migrate(root string, write bool) {
	original := originalOverlay(root)
	pkgs := checkedLoad(root, original)
	queries := projectionQueries(root)
	outputs := map[string][]byte{}
	pins := map[string]runphysical.Pin{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			fileCandidate(root, pkg, file, original, queries, outputs, pins)
		}
	}
	for path, raw := range outputs {
		formatted, err := imports.Process(path, raw, nil)
		must(err)
		outputs[path] = formatted
	}
	checkedLoad(root, outputs)
	var keys []string
	for key := range pins {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	var ordered []runphysical.Pin
	for _, key := range keys {
		ordered = append(ordered, pins[key])
	}
	encoded, err := json.MarshalIndent(ordered, "", "  ")
	must(err)
	encoded = append(encoded, '\n')
	fmt.Printf("declarations=%d files=%d sha256=%x\n", len(ordered), len(outputs), sha256.Sum256(encoded))
	if write {
		for path, raw := range outputs {
			must(os.WriteFile(path, raw, 0644))
		}
		must(os.WriteFile(manifest, encoded, 0644))
	}
}

func fileCandidate(root string, pkg *packages.Package, file *ast.File, original map[string][]byte, queries map[string]projection, outputs map[string][]byte, pins map[string]runphysical.Pin) {
	path := pkg.Fset.Position(file.Pos()).Filename
	raw, owned := original[path]
	if !owned {
		return
	}
	var edits []splice
	afterBodies := map[string]string{}
	filePins := map[string]runphysical.Pin{}
	for _, node := range file.Decls {
		fn, ok := node.(*ast.FuncDecl)
		if !ok {
			continue
		}
		originalBody := string(raw[pkg.Fset.Position(fn.Pos()).Offset:pkg.Fset.Position(fn.End()).Offset])
		after, calls := transform(originalBody, queries)
		if after == originalBody {
			continue
		}
		before, err := requestkey.Normalize(originalBody)
		must(err)
		afterBody, err := requestkey.Normalize(after)
		must(err)
		relative, err := filepath.Rel(root, path)
		must(err)
		relative = filepath.ToSlash(relative)
		pin := runphysical.Pin{File: relative, Function: fn.Name.Name, Before: before, After: afterBody, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(before))), Calls: calls}
		key := relative + "/" + fn.Name.Name
		if old, ok := pins[key]; ok && old.After != afterBody {
			panic("inconsistent typed candidate")
		}
		pins[key] = pin
		afterBodies[fn.Name.Name] = after
		filePins[fn.Name.Name] = pin
		edits = append(edits, splice{pkg.Fset.Position(fn.Pos()).Offset, pkg.Fset.Position(fn.End()).Offset, after})
	}
	if len(edits) != 0 {
		outputs[path] = destination(path, afterBodies, filePins)
	}
}

func destination(path string, afterBodies map[string]string, pins map[string]runphysical.Pin) []byte {
	raw, err := os.ReadFile(path)
	must(err)
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, raw, parser.AllErrors)
	must(err)
	seen := map[string]bool{}
	var edits []splice
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		pin, ok := pins[fn.Name.Name]
		if !ok {
			continue
		}
		if seen[fn.Name.Name] {
			panic("duplicate channel destination")
		}
		seen[fn.Name.Name] = true
		current := requestkey.Node(fn)
		if current == pin.After {
			continue
		}
		if current != pin.Before && !exactIntermediate(pin, current) {
			panic("channel destination changed outside finite transform: " + fn.Name.Name)
		}
		edits = append(edits, splice{set.Position(fn.Pos()).Offset, set.Position(fn.End()).Offset, afterBodies[fn.Name.Name]})
	}
	if len(seen) != len(pins) {
		panic("incomplete channel destination")
	}
	return apply(raw, edits)
}

func exactIntermediate(pin runphysical.Pin, current string) bool {
	raw, err := os.ReadFile(manifest)
	count := map[string]int{"30d405d24082fb20eb1b8059da2d2e54df35de3a114fa049dcb0b9c4248b8d46": 36, "9a8cd6a49fc9a86b1f67ef9732a5e51e34e28655cfc544bc9c3b9b9a3ac57113": 38}[fmt.Sprintf("%x", sha256.Sum256(raw))]
	if err != nil || count == 0 {
		return false
	}
	var prior []runphysical.Pin
	if json.Unmarshal(raw, &prior) != nil || len(prior) != count {
		return false
	}
	for _, row := range prior {
		if row.File == pin.File && row.Function == pin.Function && row.Before == pin.Before && row.After == current {
			return true
		}
	}
	return false
}

func apply(raw []byte, edits []splice) []byte {
	slices.SortFunc(edits, func(a, b splice) int { return b.start - a.start })
	for _, edit := range edits {
		raw = append(append(append([]byte{}, raw[:edit.start]...), []byte(edit.after)...), raw[edit.end:]...)
	}
	return raw
}

func parseDeclaration(source string) (*token.FileSet, *ast.FuncDecl) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "source.go", "package proof\n"+source, parser.AllErrors)
	must(err)
	if len(file.Decls) != 1 {
		panic("channel transform requires exactly one declaration")
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok {
		panic("channel transform requires a function")
	}
	return set, fn
}

func transform(source string, queries map[string]projection) (string, int) {
	set, fn := parseDeclaration(source)
	if !readerHelpers[fn.Name.Name] && !derivativeHelpers[fn.Name.Name] && !declarationHasHarness(fn) {
		return source, 0
	}
	offset := set.Position(fn.Pos()).Offset
	edits := signatureEdits(set, fn, offset)
	calls := 0
	reader := "channelSettlementStorageObservation(t,h)"
	if readerHelpers[fn.Name.Name] {
		reader = "reader"
	}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if edit, ok := helperCallEdit(set, fn, call, offset, reader); ok {
			edits = append(edits, edit)
			calls++
			return true
		}
		query, targets, ok := scan(call)
		if !ok {
			return true
		}
		rule, ok := queries[normalize(query)]
		if !ok {
			return true
		}
		edits = append(edits, splice{set.Position(call.Pos()).Offset - offset, set.Position(call.End()).Offset - offset, projectionCall(reader, rule, queryArgs(call), targets)})
		calls++
		return false
	})
	if len(edits) == 0 && fn.Name.Name != "waitNativeIntentCount" && fn.Name.Name != "waitObjectIntentResponse" {
		return source, 0
	}
	after := string(apply([]byte(source), edits))
	after = specialOwners(fn.Name.Name, after)
	if readerHelpers[fn.Name.Name] || derivativeHelpers[fn.Name.Name] {
		_, rewritten := parseDeclaration(after)
		ast.Inspect(rewritten, func(node ast.Node) bool {
			if ident, ok := node.(*ast.Ident); ok && ident.Name == "db" {
				panic("unconverted raw channel dependency: " + fn.Name.Name)
			}
			return true
		})
	}
	after = removeUnusedDB(after)
	formatted, err := format.Source([]byte("package proof\n" + after))
	must(err)
	return strings.TrimSpace(strings.TrimPrefix(string(formatted), "package proof\n")), calls
}

func declarationHasHarness(fn *ast.FuncDecl) bool {
	found := false
	ast.Inspect(fn, func(node ast.Node) bool {
		if ident, ok := node.(*ast.Ident); ok && ident.Name == "h" {
			found = true
		}
		return true
	})
	return found
}

func signatureEdits(set *token.FileSet, fn *ast.FuncDecl, offset int) []splice {
	if !readerHelpers[fn.Name.Name] && !derivativeHelpers[fn.Name.Name] {
		return nil
	}
	for index, field := range fn.Type.Params.List {
		if len(field.Names) != 1 || field.Names[0].Name != "db" {
			continue
		}
		if requestkey.Node(field.Type) != "*sql.DB" {
			panic("channel SQL carrier changed")
		}
		start, end := set.Position(field.Pos()).Offset-offset, set.Position(field.End()).Offset-offset
		after := "reader storetest.ChannelSettlementObservation"
		if derivativeHelpers[fn.Name.Name] {
			if index+1 >= len(fn.Type.Params.List) {
				panic("missing derivative parameter")
			}
			end = set.Position(fn.Type.Params.List[index+1].Pos()).Offset - offset
			after = ""
		}
		return []splice{{start, end, after}}
	}
	panic("channel declaration lost its original raw carrier")
}

func helperCallEdit(set *token.FileSet, fn *ast.FuncDecl, call *ast.CallExpr, offset int, reader string) (splice, bool) {
	ident, ok := call.Fun.(*ast.Ident)
	if ok && (readerHelpers[ident.Name] || derivativeHelpers[ident.Name]) && ident.Obj != nil && ident.Obj.Kind != ast.Fun {
		panic("shadowed channel helper")
	}
	if ok && (ident.Name == "waitChannelDeliverySendsSettled" || ident.Name == "readChannelDeliverySettlementDiagnostic") && fn.Name.Name != "TestChannelDeliverySettlementWaitIncludesPlannedWork" {
		if len(call.Args) != 3 || requestkey.Node(call.Args[1]) != "db" {
			panic("unclassified selected settlement call")
		}
		if ident.Name == "waitChannelDeliverySendsSettled" {
			argument := requestkey.Node(call.Args[2])
			if argument != "backend" && argument != "h.backend" && argument != "servedparity.BackendDefaultSQLite" && argument != "servedparity.BackendExplicitPostgres" {
				panic("unsafe backend argument evaluation")
			}
		}
		name := "waitSelectedChannelDeliverySendsSettled"
		args := requestkey.Node(call.Args[0]) + "," + reader
		if ident.Name == "readChannelDeliverySettlementDiagnostic" {
			name = "readSelectedChannelDeliverySettlementDiagnostic"
			args += "," + requestkey.Node(call.Args[2])
		}
		return splice{set.Position(call.Pos()).Offset - offset, set.Position(call.End()).Offset - offset, name + "(" + args + ")"}, true
	}
	if !ok || !readerHelpers[ident.Name] && !derivativeHelpers[ident.Name] {
		return splice{}, false
	}
	index := 1
	if derivativeHelpers[ident.Name] {
		index = 2
	}
	if len(call.Args) <= index || requestkey.Node(call.Args[index]) != "db" || call.Ellipsis.IsValid() {
		panic("unclassified channel helper arguments: " + fn.Name.Name + "/" + ident.Name)
	}
	start, end := set.Position(call.Args[index].Pos()).Offset-offset, set.Position(call.Args[index].End()).Offset-offset
	after := reader
	if derivativeHelpers[ident.Name] {
		if len(call.Args) <= index+1 {
			panic("truncated derivative call")
		}
		end = set.Position(call.Args[index+1].Pos()).Offset - offset
		after = ""
	}
	return splice{start, end, after}, true
}

func normalize(value string) string { return strings.Join(strings.Fields(value), " ") }

func scan(call *ast.CallExpr) (string, []string, bool) {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Scan" {
		return "", nil, false
	}
	query, ok := selector.X.(*ast.CallExpr)
	if !ok {
		return "", nil, false
	}
	method, ok := query.Fun.(*ast.SelectorExpr)
	if !ok || requestkey.Node(method.X) != "db" || method.Sel.Name != "QueryRow" {
		return "", nil, false
	}
	if len(query.Args) == 0 {
		return "", nil, false
	}
	literal, ok := query.Args[0].(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return "", nil, false
	}
	sql, err := strconv.Unquote(literal.Value)
	must(err)
	var targets []string
	for _, arg := range call.Args {
		pointer, ok := arg.(*ast.UnaryExpr)
		if !ok || pointer.Op != token.AND {
			panic("non-address channel scan target")
		}
		targets = append(targets, requestkey.Node(pointer.X))
	}
	return sql, targets, true
}

func queryArgs(call *ast.CallExpr) []string {
	query := call.Fun.(*ast.SelectorExpr).X.(*ast.CallExpr)
	var args []string
	for _, arg := range query.Args[1:] {
		args = append(args, requestkey.Node(arg))
	}
	return args
}

func projectionCall(reader string, rule projection, args, targets []string) string {
	if len(targets) != len(rule.fields) {
		panic("channel projection changed column cardinality")
	}
	text := "func()error{observed,err:=" + reader + "." + rule.method + "(context.Background()" + prefixed(strings.Join(args, ",")) + ");"
	for index, target := range targets {
		text += target + "=observed" + rule.fields[index] + ";"
	}
	return text + "return err}()"
}

func projectionQueries(root string) map[string]projection {
	fields := map[string][]string{"ReadLatestCardSentReceiptStorage": {""}, "ReadAcknowledgedCardRenderStorage": {".Input", ".Hash"}, "ReadChannelDraftStatusStorage": {""}, "ReadChannelDraftCardPhysicalStorage": {"[0]", "[1]", "[2]", "[3]", "[4]", "[5]", "[6]"}, "ReadDeliveryStateStorage": {""}, "CountSentFreshCardCopiesStorage": {""}, "ReadCardCopyCountsStorage": {".Total", ".Uncertain"}, "ReadChannelRenderStorage": {".Input", ".Hash"}, "ReadChannelCardRenderStorage": {".RenderID", ".Input", ".Hash", ".Snapshot"}, "CountUncertainSourcePlansStorage": {""}, "CountDecidedCardsStorage": {""}, "CountThreeDecidedCardsStorage": {""}, "ReadCardRunStorage": {""}, "ReadCardSnapshotStorage": {""}}
	result := map[string]projection{}
	for _, domain := range []string{"channeldelivery/settlement_observation.go", "decisioncard/channel_draft_observation.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, "internal/store/internal/backend", domain), nil, parser.AllErrors)
		must(err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			collectProjectionQueries(fn, fields, result)
		}
	}
	result[normalize(`SELECT CAST(c.snapshot AS TEXT),p.current_render_id,CAST(r.render_input AS TEXT),r.render_hash FROM decision_cards c JOIN channel_delivery_plans p ON p.source_id=c.card_id JOIN channel_delivery_renders r ON r.render_id=p.current_render_id WHERE c.card_id=$1`)] = projection{"ReadChannelCardRenderStorage", []string{".Snapshot", ".RenderID", ".Input", ".Hash"}}
	result[normalize(`SELECT status FROM decision_cards WHERE card_id=$1`)] = projection{"ReadChannelAnchorDecisionStorage", []string{".Status"}}
	return result
}

func collectProjectionQueries(fn *ast.FuncDecl, fields map[string][]string, result map[string]projection) {
	for _, port := range ports {
		columns, ok := fields[port.Name]
		if !ok || port.Operation != fn.Name.Name {
			continue
		}
		ast.Inspect(fn, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "QueryRowContext" || len(call.Args) < 2 {
				return true
			}
			literal, ok := call.Args[1].(*ast.BasicLit)
			if !ok {
				return true
			}
			query, err := strconv.Unquote(literal.Value)
			must(err)
			result[normalize(query)] = projection{port.Name, columns}
			return true
		})
	}
}

func specialOwners(name, source string) string {
	switch name {
	case "proveObjectChannelLostPrompt":
		cut := `waitObjectMessageText(t, p, oldReceipt, "Input: reason (text) required")`
		source = replaceExact(source, cut, `if _,err:=channelSettlementStorageObservation(t,h).ReadLatestCardSentReceiptStorage(context.Background(),card);err!=sql.ErrNoRows{t.Fatalf("uncertain plan retained a current sent receipt: %v",err)};requireHistoricalObjectReceipt(t,h,card,oldReceipt);`+cut)
		cut = `waitObjectMessageText(t, p, fresh, "Input: reason (text) required")`
		source = replaceExact(source, cut, `requireHistoricalObjectReceipt(t,h,card,oldReceipt);`+cut)
	case "waitNativeIntentCount":
		start := strings.Index(source, "query :=")
		end := strings.Index(source, "for {")
		if start < 0 || end <= start {
			panic("intent-count query setup changed")
		}
		source = source[:start] + source[end:]
		source = replaceExact(source, "db.QueryRow(query).Scan(&count)", projectionCall("reader", projection{"CountTextIntentDispositionStorage", []string{""}}, []string{"disposition"}, []string{"count"}))
	case "waitObjectIntentResponse":
		source = replaceExact(source, "table, eventID string", "kind channeldelivery.IntentKind, eventID string")
		start := strings.Index(source, "if table !=")
		end := strings.Index(source, "deadline :=")
		if start < 0 || end <= start {
			panic("response kind validation changed")
		}
		source = source[:start] + `if kind!=channeldelivery.IntentText&&kind!=channeldelivery.IntentAction{t.Fatalf("unsupported response intent kind %q",kind)};` + source[end:]
		source = rewriteResponseScan(source)
	}
	if strings.Contains(source, "waitObjectIntentResponse(") {
		source = strings.ReplaceAll(source, `"operator_channel_text_intents"`, "channeldelivery.IntentText")
		source = strings.ReplaceAll(source, `"operator_channel_action_intents"`, "channeldelivery.IntentAction")
	}
	return source
}

func rewriteResponseScan(source string) string {
	set, fn := parseDeclaration(source)
	base := set.Position(fn.Pos()).Offset
	var edits []splice
	ast.Inspect(fn, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Scan" {
			return true
		}
		query, ok := sel.X.(*ast.CallExpr)
		if !ok {
			return true
		}
		method, ok := query.Fun.(*ast.SelectorExpr)
		if !ok || requestkey.Node(method.X) != "db" {
			return true
		}
		replacement := projectionCall("reader", projection{"ReadIntentSentResponseStorage", []string{".DeliveryID", ".Reference"}}, []string{"kind", "eventID"}, []string{"deliveryID", "raw"})
		edits = append(edits, splice{set.Position(call.Pos()).Offset - base, set.Position(call.End()).Offset - base, replacement})
		return false
	})
	if len(edits) != 1 {
		panic("response exact projection missing")
	}
	return string(apply([]byte(source), edits))
}

func replaceExact(source, before, after string) string {
	if strings.Count(source, before) != 1 {
		panic("channel predecessor block changed: " + before)
	}
	return strings.Replace(source, before, after, 1)
}

func removeUnusedDB(source string) string {
	set, fn := parseDeclaration(source)
	uses := 0
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if ident, ok := node.(*ast.Ident); ok && ident.Name == "db" {
			uses++
		}
		return true
	})
	if uses != 1 {
		return source
	}
	var edits []splice
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		assignment, ok := node.(*ast.AssignStmt)
		if !ok || assignment.Tok != token.DEFINE {
			return true
		}
		for _, node := range assignment.Lhs {
			if ident, ok := node.(*ast.Ident); ok && ident.Name == "db" {
				edits = append(edits, splice{set.Position(ident.Pos()).Offset - set.Position(fn.Pos()).Offset, set.Position(ident.End()).Offset - set.Position(fn.Pos()).Offset, "_"})
			}
		}
		return true
	})
	return string(apply([]byte(source), edits))
}
