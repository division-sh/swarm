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
