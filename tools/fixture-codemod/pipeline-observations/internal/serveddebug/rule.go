package serveddebug

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
	"slices"
	"strings"
)

type helper struct {
	DBIndex int
	Replace bool
}

var helpers = map[string]helper{
	"waitServedJoinSourceTimer": {1, false}, "servedJoinTarget": {1, false},
	"requireServedEventPublishPreHandlerProof":              {1, true},
	"waitForServedEventPublishNodeDeliveryLifecycleForNode": {1, false},
	"waitForServedEventPublishNodeDeliveryLifecycle":        {1, false},
	"waitServedEventPublishDeliveryStatusCountForRun":       {1, false},
	"requireServedEventPublishCommittedReplayScope":         {1, false},
	"requireServedRunStatusWithDebug":                       {2, true},
	"requireServedParitySettlementPostconditions":           {2, true},
	"requireServedParitySettlementPostconditionsWithDebug":  {2, true},
	"waitServedEventPublishEventID":                         {1, false},
	"servedEventPublishDebugSummaryForEvent":                {1, true},
	"requireServedStoppedPendingDelivery":                   {1, false},
	"waitServedEventPublishReceiptOutcomeCount":             {1, false},
	"waitServedDeliveryOutcomeCount":                        {1, false}, "semanticNumericOutput": {2, false},
	"runServedCreateCarryProjectionProof": {2, false},
}

var captureRoots = map[string]bool{
	"TestRunServeRuntimeJoinFailureReachesAPIAndCLI":                           true,
	"TestRunServeRuntimeJoinForkReplayRejectsTimerBearingSourceBeforeMutation": true,
	"startServedSessionCleanupProof":                                           true,
	"runServedCreateCarryProjectionSQLiteProof":                                true,
	"runServedCreateCarryProjectionPostgresProof":                              true,
	"TestServedSemanticNumericIdempotencyReplay":                               true,
	"runServedStandingServiceLifecycleBackendProof":                            true,
	"runServedGeneratedInputFixtureBackendProof":                               true,
	"runServedPublicMockApprovalBackendProof":                                  true,
}

const EventDebugSource = `func servedEventPublishDebugSummaryForEvent(t *testing.T, reader receiverProofStateReader, backend, eventID string) string {
	t.Helper()
	var runID string
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		runID, err = storetest.ReadServedEventRunIDStorage(ctx, reader, eventID)
		return err
	})
	if err != nil {
		return fmt.Sprintf("%s event %s run lookup failed: %v", backend, eventID, err)
	}
	return servedInspectionRunDebugSummary(reader, runID)
}`

type edit struct {
	start, end int
	after      string
}

// TransformFunction owns only this finite argument/capture propagation. The
// command and its proofs additionally require complete signed preimages.
func TransformFunction(fset *token.FileSet, function *ast.FuncDecl, source string) (string, int, int, bool) {
	start := fset.Position(function.Pos()).Offset
	debug, calls, readerUsed := 0, 0, false
	var edits []edit
	ast.Inspect(function.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := nodeString(call.Fun)
		if name == "servedEventPublishDebugQuery" {
			if len(call.Args) != 5 || nodeString(call.Args[0]) != "t" || nodeString(call.Args[3]) != `"runtime_logs"` {
				panic("unclassified independent diagnostic section")
			}
			reader := nativeReader(function.Name.Name, nodeString(call.Args[1]))
			after := "servedInspectionRuntimeLogDebugSummary(" + reader + ", " + nodeString(call.Args[4]) + ")"
			edits = append(edits, edit{fset.Position(call.Pos()).Offset - start, fset.Position(call.End()).Offset - start, after})
			calls++
			return false
		}
		if name == "servedEventPublishDebugSummary" {
			if len(call.Args) != 4 || nodeString(call.Args[0]) != "t" {
				panic("foreign diagnostic call")
			}
			reader := nativeReader(function.Name.Name, nodeString(call.Args[1]))
			readerUsed = readerUsed || reader == "reader"
			after := "servedInspectionRunDebugSummary(" + reader + ", " + nodeString(call.Args[3]) + ")"
			edits = append(edits, edit{fset.Position(call.Pos()).Offset - start, fset.Position(call.End()).Offset - start, after})
			debug++
			return false
		}
		if rule, ok := helpers[name]; ok {
			if len(call.Args) <= rule.DBIndex || nodeString(call.Args[0]) != "t" {
				panic("foreign shared diagnostic helper call")
			}
			argument := call.Args[rule.DBIndex]
			reader := nativeReader(function.Name.Name, nodeString(argument))
			readerUsed = readerUsed || reader == "reader"
			from, to := fset.Position(argument.Pos()).Offset-start, fset.Position(argument.End()).Offset-start
			if !rule.Replace {
				from = to
				reader = ", " + reader
			}
			edits = append(edits, edit{from, to, reader})
			calls++
			return true
		}
		return true
	})
	after := string(apply([]byte(source), edits))
	after, needs := finishSource(function.Name.Name, after, readerUsed)
	return after, debug, calls, needs
}

func finishSource(function, after string, readerUsed bool) (string, bool) {
	if function == "servedEventPublishDebugSummaryForEvent" {
		return EventDebugSource, false
	}
	if rule, ok := helpers[function]; ok {
		old, replacement := "db *sql.DB", "db *sql.DB, reader receiverProofStateReader"
		if rule.Replace {
			replacement = "reader receiverProofStateReader"
		}
		if !strings.Contains(after, old) {
			panic("shared helper's original raw parameter disappeared")
		}
		after = strings.Replace(after, old, replacement, 1)
	}
	needs := readerUsed && !strings.Contains(after, "reader receiverProofStateReader") && !strings.Contains(after, "var reader ") && !strings.Contains(after, "reader :=")
	if needs && captureRoots[function] {
		capture := "\n\tvar reader receiverProofStateReader\n\tcaptureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { reader = requireReceiverProofStateReader(t, p.deps.EventStore) })"
		brace := strings.Index(after, "{\n") + 1
		if strings.Contains(after, "\n\tt.Helper()") {
			brace = strings.Index(after, "\n\tt.Helper()") + len("\n\tt.Helper()")
		}
		after = after[:brace] + capture + after[brace:]
		needs = false
	}
	if function == "startServedSessionCleanupProof" {
		after = strings.Replace(after, "Endpoint: endpoint, DB: db,", "Endpoint: endpoint, DB: db, ReceiverStateReader: reader,", 1)
	}
	if function == "runServedStandingServiceLifecycleBackendProof" {
		after = strings.Replace(after, "\n\tdebugBackend := \"postgres\"\n\tif backend == servedparity.BackendDefaultSQLite {\n\t\tdebugBackend = \"sqlite\"\n\t}", "", 1)
	}
	if function == "runServedPublicMockApprovalBackendProof" {
		after = strings.Replace(after, "\n\tvar db *sql.DB", "", 1)
		after = strings.Replace(after, "\n\t\tif err == nil {\n\t\t\tdb = selectedStoreDatabaseForTest(t, stores)\n\t\t}", "", 1)
	}
	return after, needs
}

func nativeReader(function, db string) string {
	if function == "seedServedRunControlPendingRunWithAgentDelivery" && db == "db" {
		return "rt.ReceiverStateReader"
	}
	switch db {
	case "rt.DB":
		return "rt.ReceiverStateReader"
	case "f.rt.DB":
		return "f.rt.ReceiverStateReader"
	case "proof.DB":
		return "proof.ReceiverStateReader"
	case "db", "servedDB", "observer", "reader":
		return "reader"
	case "selectedStoreDatabaseForTest(t, stores)":
		return "requireReceiverProofStateReader(t, candidates[1].runtime.Bus.Store())"
	default:
		panic("unclassified diagnostic owner in " + function + ": " + db)
	}
}

func apply(source []byte, edits []edit) []byte {
	slices.SortFunc(edits, func(a, b edit) int { return b.start - a.start })
	for _, e := range edits {
		source = append(source[:e.start], append([]byte(e.after), source[e.end:]...)...)
	}
	return source
}
func nodeString(node ast.Node) string {
	var out bytes.Buffer
	if err := format.Node(&out, token.NewFileSet(), node); err != nil {
		panic(err)
	}
	return out.String()
}

func TransformCarrier(before string) string {
	const old = "type servedSessionCleanupProof struct {"
	if !strings.HasPrefix(before, old) {
		panic(fmt.Sprintf("unclassified carrier: %s", before))
	}
	return strings.Replace(before, old, old+"\n\tReceiverStateReader receiverProofStateReader", 1)
}
