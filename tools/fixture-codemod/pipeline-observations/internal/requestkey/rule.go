package requestkey

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"slices"
	"strings"
)

const APICounter = `func servedEventPublishAPIIdempotencyCount(t *testing.T, reader receiverProofStateReader, backend, method, idempotencyKey string) int {
	t.Helper()
	switch backend {
	case "postgres", "sqlite":
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		count, err = storetest.ReadAPIIdempotencyCardinality(ctx, reader, method, idempotencyKey)
		return err
	})
	if err != nil {
		t.Fatalf("%s count api idempotency method=%q key=%q: %v", backend, method, idempotencyKey, err)
	}
	return count
}`

const EventCounter = `func servedEventPublishEventCountByIdempotencyKey(t *testing.T, reader receiverProofStateReader, backend, idempotencyKey string) int {
	t.Helper()
	switch backend {
	case "postgres", "sqlite":
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		count, err = storetest.ReadEventIdempotencyCardinality(ctx, reader, idempotencyKey)
		return err
	})
	if err != nil {
		t.Fatalf("%s count events by idempotency key %q: %v", backend, idempotencyKey, err)
	}
	return count
}`

const SourceCounter = `func servedEventPublishSourcedEventCount(t *testing.T, reader receiverProofStateReader, backend, sourceEventID string) int {
	t.Helper()
	switch backend {
	case "postgres", "sqlite":
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		count, err = storetest.CountSourceEventPublications(ctx, reader, sourceEventID)
		return err
	})
	if err != nil {
		t.Fatalf("%s sourced event count query failed: %v", backend, err)
	}
	return count
}`

const RollbackAssertion = `if got := servedEventPublishAPIIdempotencyCount(t, reader, servedBackendLabel(backend), "event.publish", idempotencyKey); got != 0 {
	t.Fatalf("%s API idempotency completion rows after public-input rollback = %d, want 0", backend, got)
}`

const ReaderCapture = `var reader receiverProofStateReader
captureSelectedRuntimePersistence(t, func(persistence serveRuntimePersistence) {
	reader = requireReceiverProofStateReader(t, persistence.deps.EventStore)
})
`

type edit struct {
	start, end int
	after      string
}

func Node(node ast.Node) string {
	var out bytes.Buffer
	if err := format.Node(&out, token.NewFileSet(), node); err != nil {
		panic(err)
	}
	return out.String()
}

func Normalize(source string) (string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "rule.go", "package proof\n"+source, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return "", fmt.Errorf("invalid request-key declaration: %v", err)
	}
	return Node(file.Decls[0]), nil
}

func Key(fn *ast.FuncDecl) string {
	if fn.Recv == nil {
		return fn.Name.Name
	}
	return fn.Name.Name + "/" + Node(fn.Recv.List[0].Type)
}

func Transform(before string) (string, int, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "rule.go", "package proof\n"+before, parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return "", 0, fmt.Errorf("invalid request-key predecessor: %v", err)
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok {
		return before, 0, nil
	}
	if Key(fn) == "start/*issue2394StopRig" {
		return captureStopRigReader(before)
	}
	switch fn.Name.Name {
	case "servedEventPublishAPIIdempotencyCount":
		after, err := Normalize(APICounter)
		return after, 0, err
	case "servedEventPublishEventCountByIdempotencyKey":
		after, err := Normalize(EventCounter)
		return after, 0, err
	case "servedEventPublishSourcedEventCount":
		after, err := Normalize(SourceCounter)
		return after, 0, err
	case "requirePublicInputRollbackNoResidue":
		return removeRollbackCounter(before)
	case "startServedLiveAgentProofRuntimeWithLLMAndDirectiveFaults", "startServedControlProofRuntimeWithFixture":
		return captureMissingServingReader(before)
	}
	after, calls, err := rewriteCalls(fset, fn, before)
	if err != nil {
		return "", 0, err
	}
	if readerParameter(fn.Name.Name) {
		const parameter = "db *sql.DB"
		if strings.Count(after, parameter) != 1 {
			return "", 0, fmt.Errorf("request-key helper has changed raw carrier: %s", fn.Name.Name)
		}
		after = strings.Replace(after, parameter, "reader receiverProofStateReader", 1)
	}
	if rollbackCaller(fn.Name.Name) {
		const start = "endpoint, db, bundleHash := startPublicInputRollbackRuntime(t, backend)"
		if strings.Count(after, start) != 1 || strings.Contains(before, "var reader ") {
			return "", 0, fmt.Errorf("rollback reader capture cut changed: %s", fn.Name.Name)
		}
		after = strings.Replace(after, start, ReaderCapture+start, 1)
	}
	after, err = Normalize(after)
	return after, calls, err
}

func captureStopRigReader(before string) (string, int, error) {
	const start = "r.process = startServeRuntimeTestProcess(t, r.opts)"
	const result = "DB: r.db, Backend: r.backend, BundleHash: r.bundleHash, Runtime: rt,"
	if strings.Count(before, start) != 1 || strings.Count(before, result) != 1 || strings.Contains(before, "ReceiverStateReader:") {
		return "", 0, fmt.Errorf("stop rig reader capture/result cut changed")
	}
	after := strings.Replace(before, start, ReaderCapture+start, 1)
	after = strings.Replace(after, result, result+" ReceiverStateReader: reader,", 1)
	after, err := Normalize(after)
	return after, 0, err
}

func captureMissingServingReader(before string) (string, int, error) {
	const declaration = "var deliveryReader servedRunDeliveryReader"
	const capture = "deliveryReader = p.deps.DeliveryStore.SummarizeRun"
	const result = "ReadRunDeliveries: deliveryReader,"
	if strings.Count(before, declaration) != 1 || strings.Count(before, capture) != 1 || strings.Count(before, result) != 2 || strings.Contains(before, "ReceiverStateReader:") {
		return "", 0, fmt.Errorf("shared serving reader capture/result inventory changed")
	}
	after := strings.Replace(before, declaration, declaration+"\nvar receiverReader receiverProofStateReader", 1)
	after = strings.Replace(after, capture, capture+"\nreceiverReader = requireReceiverProofStateReader(t, p.deps.EventStore)", 1)
	after = strings.ReplaceAll(after, result, result+" ReceiverStateReader: receiverReader,")
	after, err := Normalize(after)
	return after, 0, err
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
		if !ok || !requestKeyHelper(id.Name) {
			return true
		}
		after, err := rewriteCall(fn, call, id)
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

func requestKeyHelper(name string) bool {
	switch name {
	case "requireServedControlAPIIdempotencyRows", "servedEventPublishAPIIdempotencyCount", "servedEventPublishEventCountByIdempotencyKey", "requirePublicInputRollbackNoResidue", "requireServedReplayNoDeliveryHistoryNoMutation", "servedEventPublishSourcedEventCount":
		return true
	}
	return false
}

func readerParameter(name string) bool {
	return name == "requireServedControlAPIIdempotencyRows" || name == "requireServedReplayNoDeliveryHistoryNoMutation"
}

func rewriteCall(fn *ast.FuncDecl, call *ast.CallExpr, id *ast.Ident) (string, error) {
	want := map[string]int{"requireServedControlAPIIdempotencyRows": 6, "servedEventPublishAPIIdempotencyCount": 5,
		"servedEventPublishEventCountByIdempotencyKey": 4, "requirePublicInputRollbackNoResidue": 6,
		"requireServedReplayNoDeliveryHistoryNoMutation": 6, "servedEventPublishSourcedEventCount": 4}[id.Name]
	if len(call.Args) != want || call.Ellipsis.IsValid() || Node(call.Args[0]) != "t" || id.Obj != nil && id.Obj.Kind != ast.Fun {
		return "", fmt.Errorf("unclassified request-key call: %s/%s", fn.Name.Name, id.Name)
	}
	if id.Name == "requirePublicInputRollbackNoResidue" {
		return liftRollbackCounter(fn.Name.Name, call)
	}
	carrier := 1
	if id.Name == "requireServedReplayNoDeliveryHistoryNoMutation" {
		carrier = 2
	}
	reader, err := callReader(fn, call.Args[carrier])
	if err != nil {
		return "", err
	}
	var args []string
	for i, arg := range call.Args {
		value := Node(arg)
		if i == carrier {
			value = reader
		}
		args = append(args, value)
	}
	return id.Name + "(" + strings.Join(args, ", ") + ")", nil
}

func callReader(fn *ast.FuncDecl, argument ast.Expr) (string, error) {
	if Node(argument) == "rt.DB" {
		return "rt.ReceiverStateReader", nil
	}
	if Node(argument) == "db" && readerParameter(fn.Name.Name) {
		return "reader", nil
	}
	if Node(argument) == "db" {
		for _, field := range fn.Type.Params.List {
			if Node(field.Type) != "receiverProofStateReader" {
				continue
			}
			for _, name := range field.Names {
				if name.Name == "reader" {
					return "reader", nil
				}
			}
		}
	}
	return "", fmt.Errorf("request-key caller has no original selected read role: %s/%s", fn.Name.Name, Node(argument))
}

func rollbackCaller(name string) bool {
	return name == "TestRootConnectedTemplatePublicationRollbackIsAtomicAcrossSelectedStores" ||
		name == "TestRootConnectedTemplatePublicationRollsBackAfterDeliveryAndCompletionAcrossSelectedStores"
}

func liftRollbackCounter(function string, call *ast.CallExpr) (string, error) {
	if !rollbackCaller(function) {
		return "", fmt.Errorf("unclassified rollback caller: %s", function)
	}
	var args []string
	for _, arg := range call.Args {
		args = append(args, Node(arg))
	}
	if strings.Join(args, ", ") != "t, db, backend, idempotencyKey, eventID, runID" {
		return "", fmt.Errorf("rollback arguments or observation cut changed: %s", function)
	}
	return "requirePublicInputRollbackNoResidue(t, db, backend, eventID, runID)\n" + RollbackAssertion, nil
}

func removeRollbackCounter(before string) (string, int, error) {
	const header = "idempotencyKey, eventID, runID string"
	original := strings.Replace(RollbackAssertion, "t, reader,", "t, db,", 1)
	indented := "\t" + strings.ReplaceAll(original, "\n", "\n\t")
	if strings.Count(before, header) != 1 || !strings.HasSuffix(before, indented+"\n}") {
		return "", 0, fmt.Errorf("rollback API tail or signature changed")
	}
	after := strings.TrimSuffix(before, indented+"\n}") + "}"
	after = strings.Replace(after, header, "eventID, runID string", 1)
	after, err := Normalize(after)
	return after, 1, err
}
