package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
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

func TestFamily145ForkCompletionWholeDependencyPreservesAll30CallsAndTemporalCuts(t *testing.T) {
	if fmt.Sprintf("%x", sha256.Sum256([]byte(strings.TrimSuffix(string(family145ForkCompletionPreimages), "\n")))) != "30bef9d6edb24f9d59b3f8c828fff01607d4fbae6788af1b73e05549a937e9b2" {
		t.Fatal("signed completion preimages changed")
	}
	var pins []writerStagePreimage
	if err := json.Unmarshal(family145ForkCompletionPreimages, &pins); err != nil {
		t.Fatal(err)
	}
	owned := family145ForkReceiverJoinedOwnedInventory(t)
	calls, helpers := 0, 0
	for _, pin := range pins {
		after := family145ConversationForkOwnerSource(t, "../serveapp/"+strings.TrimPrefix(pin.File, "internal/serveapp/"), pin.Function)
		if !family145CheckForkCompletionWholeRule(t, pin.File, pin.Function, after) {
			t.Fatal("completion preimage not owned")
		}
		calls += owned["fork-source-completion/"+pin.Function]
		if pin.Function == "waitForkReceiverSourceCompletion" {
			helpers++
		}
		for _, cut := range []string{"ReceiverStateReader", "t.Fatal", "t.Fatalf", "reflect.DeepEqual", "InspectSnapshot", "return err", "incomplete == 0", "10 * time.Millisecond", "servedProofPollDeadline", "servedInspectionRunDebugSummary", "ReadServedIncompletePipelineHandoffCount", "runForkCLI", "requireForkReceiverDelivery"} {
			if !strings.Contains(after, cut) {
				continue
			}
			hostile := strings.Replace(after, cut, "foreignCompletionCut()", 1)
			if fmt.Sprintf("%x", sha256.Sum256([]byte(family145ForkCompletionInverse(pin.Function, hostile)))) == pin.SHA256 {
				t.Fatalf("completion transform accepted lost owner/assertion/temporal cut: %s/%s", pin.Function, cut)
			}
		}
	}
	if len(pins) != 17 || calls != 30 || helpers != 1 {
		t.Fatalf("completion body/caller/helper inventory = %d/%d/%d, want 17/30/1", len(pins), calls, helpers)
	}
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

func TestFamily145ForkCompletionRejectsRawAndForeignNearMatches(t *testing.T) {
	for _, source := range []string{
		"waitForkReceiverSourceCompletion(t, rt, runID)",
		"waitForkReceiverSourceCompletion(t, rt.DB, runID)",
		"waitForkReceiverSourceCompletion(t, foreign.ReceiverStateReader, runID)",
		"waitForkReceiverSourceCompletion(t, rt.ReceiverStateReader)",
		"waitForkReceiverSourceCompletion(t, rt.ReceiverStateReader, runID, foreign)",
		"waitForkReceiverSourceCompletion(foreign, rt.ReceiverStateReader, runID)",
	} {
		expression, err := parser.ParseExpr(source)
		if err != nil {
			t.Fatal(err)
		}
		if err := family145ForkCompletionCallError(expression.(*ast.CallExpr)); err == nil {
			t.Fatalf("completion call accepted non-equivalent source: %s", source)
		}
	}
}
