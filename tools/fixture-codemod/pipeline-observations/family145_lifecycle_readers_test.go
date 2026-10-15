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

//go:embed family145-lifecycle-reader-preimages.json
var family145LifecycleReaderPreimages []byte

func family145LifecycleReadersInverse(after string) string {
	return strings.NewReplacer(
		"requireLifecycleEventCount(t, rt.ReceiverStateReader, ", "requireLifecycleEventCount(t, rt, ",
		"readLifecycleLoop(t, rt.Endpoint, ", "readLifecycleLoop(t, rt, ",
	).Replace(after)
}

func family145CheckLifecycleReaderWholeRule(t *testing.T, file, function, after string) bool {
	t.Helper()
	var rows []writerStagePreimage
	if err := json.Unmarshal(family145LifecycleReaderPreimages, &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.File == file && row.Function == function {
			after = family145ServedDebugCallerInverse(function, after)
			if fmt.Sprintf("%x", sha256.Sum256([]byte(family145LifecycleReadersInverse(after)))) != row.SHA256 {
				t.Fatalf("lifecycle reader rule changed complete signed predecessor: %s/%s", file, function)
			}
			return true
		}
	}
	return false
}

func TestFamily145LifecycleReadersKeepAll50CallsAndWholeBodies(t *testing.T) {
	if fmt.Sprintf("%x", sha256.Sum256([]byte(strings.TrimSuffix(string(family145LifecycleReaderPreimages), "\n")))) != "7202f3b970cab9e979d8821ace9dd803bb6024350949c3288863f033c98b51aa" {
		t.Fatal("lifecycle reader signed preimage ledger changed")
	}
	var rows []writerStagePreimage
	if err := json.Unmarshal(family145LifecycleReaderPreimages, &rows); err != nil {
		t.Fatal(err)
	}
	owned := family145ForkReceiverJoinedOwnedInventory(t)
	counts, loops := 0, 0
	for _, row := range rows {
		after := family145ConversationForkOwnerSource(t, "../serveapp/"+strings.TrimPrefix(row.File, "internal/serveapp/"), row.Function)
		after = family145ServedDebugCallerInverse(row.Function, after)
		if !family145CheckLifecycleReaderWholeRule(t, row.File, row.Function, after) {
			t.Fatal("missing lifecycle reader rule")
		}
		counts += owned["lifecycle-event-count/"+row.Function]
		loops += owned["lifecycle-public-loop/"+row.Function]
		for _, cut := range []string{"ReceiverStateReader", "rt.Endpoint", "t.Fatal", "reflect.DeepEqual", "requireLifecycleEventCount", "readLifecycleLoop", "releaseOnce.Do", "first.kill"} {
			if !strings.Contains(after, cut) {
				continue
			}
			hostile := strings.Replace(after, cut, "foreignLifecycleReadCut()", 1)
			if fmt.Sprintf("%x", sha256.Sum256([]byte(family145LifecycleReadersInverse(hostile)))) == row.SHA256 {
				t.Fatalf("lifecycle propagation accepted lost owner/assertion/temporal cut: %s/%s", row.Function, cut)
			}
		}
	}
	if len(rows) != 21 || counts != 31 || loops != 19 {
		t.Fatalf("lifecycle whole bodies=%d/21 count calls=%d/31 public loop calls=%d/19", len(rows), counts, loops)
	}
}

func family145LifecycleReadersRecordCall(t *testing.T, function string, call *ast.CallExpr, counts map[string]int) {
	t.Helper()
	name := family145NodeString(call.Fun)
	switch name {
	case "requireLifecycleEventCount":
		if len(call.Args) != 5 || family145NodeString(call.Args[1]) != "rt.ReceiverStateReader" {
			t.Fatal("lifecycle count retained raw or foreign authority")
		}
		counts["lifecycle-event-count/"+function]++
	case "readLifecycleLoop":
		if len(call.Args) != 4 || family145NodeString(call.Args[1]) != "rt.Endpoint" {
			t.Fatal("public loop read retained raw or foreign authority")
		}
		counts["lifecycle-public-loop/"+function]++
	}
}

func TestFamily145LifecycleReaderHelpersPreserveExactQueriesAndPublicAssertions(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	helpers := 0
	for _, row := range rows {
		if row.Function != "requireLifecycleEventCount" && row.Function != "readLifecycleLoop" {
			continue
		}
		helpers++
		if actual := family145ConversationForkOwnerSource(t, "../serveapp/lifecycle_emitter_test.go", row.Function); actual != row.After {
			t.Fatalf("lifecycle helper escaped its finite current source: %s", row.Function)
		}
		inverse := family145LifecycleReadHelperInverse(row.Function, row.After)
		if inverse != row.Before {
			t.Fatalf("lifecycle helper lost original run/entity/event/cardinality/error/public API cut: %s", row.Function)
		}
		if row.Function == "requireLifecycleEventCount" {
			physical := family145ConversationForkOwnerSource(t, "internal/backend/eventrecord/run_name_cardinality.go", "CountRunNamedEventStorageTx")
			want := family145WriterStageNormalizedQueries(t, row.Before)
			got := family145WriterStageNormalizedQueries(t, physical)
			if len(want) != 1 || len(got) != 1 || want[0] != got[0] {
				t.Fatal("lifecycle event count changed exact run/name/count SQL")
			}
		}
		for _, pair := range [][2]string{{"return err", "return nil"}, {"count != want", "count < want"}, {"len(result.Loops) != 1", "len(result.Loops) < 1"}, {"result.Loops[0]", "result.Loops[1]"}} {
			hostile := strings.Replace(row.After, pair[0], pair[1], 1)
			if hostile != row.After && family145LifecycleReadHelperInverse(row.Function, hostile) == row.Before {
				t.Fatalf("lifecycle helper accepted lost exact assertion/read error: %s", pair[0])
			}
		}
	}
	if helpers != 2 {
		t.Fatalf("lifecycle helpers=%d/2", helpers)
	}
}

func family145LifecycleReadHelperInverse(function, after string) string {
	if function == "readLifecycleLoop" {
		return strings.NewReplacer("endpoint, runID, entityID string", "rt servedControlProofRuntime, runID, entityID string", "requireServedJSONRPCResult(t, endpoint,", "requireServedJSONRPCResult(t, rt.Endpoint,").Replace(after)
	}
	const native = `reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		count, err = storetest.CountRunNamedEventStorage(ctx, reader, runID, event)
		return err
	})`
	return strings.NewReplacer("reader receiverProofStateReader", "rt servedControlProofRuntime", native, "rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id = $1 AND event_name = $2`, runID, event).Scan(&count)").Replace(after)
}
