package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"go/ast"
	"strings"
	"testing"
)

//go:embed family145-fork-completion-preimages.json
var family145ForkCompletionPreimages []byte

func family145ForkCompletionInverse(function, after string) string {
	after = family145ServedDebugCallerInverse(function, after)
	if function != "waitForkReceiverSourceCompletion" {
		return strings.ReplaceAll(after, "waitForkReceiverSourceCompletion(t, rt.ReceiverStateReader, ", "waitForkReceiverSourceCompletion(t, rt, ")
	}
	const native = `reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
			var err error
			incomplete, err = storetest.ReadServedIncompletePipelineHandoffCount(ctx, reader, runID)
			return err
		})`
	const original = "rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries d WHERE d.run_id=$1 AND\n\t\t\t(d.status IN ('pending','in_progress') OR d.continuation_handoff_at IS NULL OR NOT EXISTS\n\t\t\t(SELECT 1 FROM event_receipts r WHERE r.event_id=d.event_id AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'))`, runID).Scan(&incomplete)"
	return strings.NewReplacer(
		"reader receiverProofStateReader", "rt servedControlProofRuntime",
		native, original,
		"servedInspectionRunDebugSummary(reader, runID)", "servedEventPublishDebugSummary(t, rt.DB, rt.Backend, runID)",
	).Replace(after)
}

func family145CheckForkCompletionWholeRule(t *testing.T, file, function, after string) bool {
	t.Helper()
	var pins []writerStagePreimage
	if err := json.Unmarshal(family145ForkCompletionPreimages, &pins); err != nil {
		t.Fatal(err)
	}
	for _, pin := range pins {
		if pin.File == file && pin.Function == function {
			if fmt.Sprintf("%x", sha256.Sum256([]byte(family145ForkCompletionInverse(function, after)))) != pin.SHA256 {
				t.Fatalf("completion transform changed complete signed predecessor: %s/%s", file, function)
			}
			return true
		}
	}
	return false
}

func family145ForkCompletionCallError(call *ast.CallExpr) error {
	if len(call.Args) != 3 || family145NodeString(call.Args[0]) != "t" || family145NodeString(call.Args[1]) != "rt.ReceiverStateReader" {
		return fmt.Errorf("completion call retained raw/foreign authority or changed arity")
	}
	return nil
}

func family145ForkCompletionRecordCall(t *testing.T, function string, call *ast.CallExpr, counts map[string]int) {
	t.Helper()
	if family145NodeString(call.Fun) != "waitForkReceiverSourceCompletion" {
		return
	}
	if err := family145ForkCompletionCallError(call); err != nil {
		t.Fatal(err)
	}
	counts["fork-source-completion/"+function]++
}
