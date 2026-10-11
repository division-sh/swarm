package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func assertFamily145ReplayScopeHistoryProofs(t testing.TB, name, before, after string) {
	t.Helper()
	if err := family145ReplayScopeHistoryProofError(name, before, after); err != nil {
		t.Fatal(err)
	}
}

func family145ReplayScopeHistoryProofError(name, before, after string) error {
	for _, raw := range []string{"*sql.DB", "ExecContext", "context.Background()", "authoractivityfixture.Dialect", "reasonCode"} {
		if strings.Contains(after, raw) {
			return fmt.Errorf("replay-scope setup retained %s", raw)
		}
	}
	if name == "seedSelectedExecutionSourceReplayScopeMarker" {
		if !strings.Contains(after, "storetest.StampHistoricalReplayScope(ctx, selected, sourceRunID, sourceEventID, scope, at)") ||
			!strings.Contains(after, "scope runtimepipelineobligation.CommittedScope") ||
			!strings.Contains(before, "case \"replay_scope_direct\":\n\t\tscope = runtimepipelineobligation.ScopeDirect") ||
			!strings.Contains(before, "case \"replay_scope_subscribed\":\n\t\tscope = runtimepipelineobligation.ScopeSubscribed") {
			return fmt.Errorf("marker changed original typed scope/owner/coordinates/clocks")
		}
		return nil
	}
	original := diagnosticHistoryCallArgs(before, "InsertExistingRunRootEventRecord")
	replacement := diagnosticHistoryCallArgs(after, "SeedPostForkSourceEvent")
	if len(original) != 11 || strings.Join(original[4:7], ",") != `sourceEventID,sourceRunID,"source.after"` ||
		original[7] != `eventtest.Producer(events.EventProducerExternal, "source-runtime")` ||
		original[8] != "[]byte(`{}`)" ||
		original[9] != `events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "flow-a/1")` || original[10] != "at" ||
		strings.Join(replacement, ",") != "t,ctx,selected,sourceRunID,sourceEventID,entityID,at" {
		return fmt.Errorf("post-fork source setup changed original event facts or selected owner")
	}
	return nil
}

func TestFamily145ReplayScopeHistoryControlsRejectLostCoordinates(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var covered int
	for _, row := range rows {
		if row.Function != "seedSelectedExecutionSourceReplayScopeMarker" && row.Function != "seedSelectedExecutionPostForkSourceEvent" {
			continue
		}
		covered++
		after := row.After
		if row.Successor != "" {
			after = row.Successor
		}
		if err := family145ReplayScopeHistoryProofError(row.Function, row.Before, after); err != nil {
			t.Fatal(err)
		}
		for _, lost := range []string{"ctx", "selected", "sourceRunID", "sourceEventID", "at"} {
			if err := family145ReplayScopeHistoryProofError(row.Function, row.Before, strings.ReplaceAll(after, lost, "unreviewed")); err == nil {
				t.Fatalf("lost %s accepted in %s", lost, row.Function)
			}
		}
	}
	if covered != 2 {
		t.Fatalf("replay-scope/post-fork source owner proof inventory=%d, want two", covered)
	}
}

func TestFamily145ReplayScopeHistoryCompleteCallerInventory(t *testing.T) {
	path := "../../../internal/runtime/runforkexecution/execution_test.go"
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := call.Fun.(*ast.Ident)
		if !ok || (name.Name != "seedSelectedExecutionSourceReplayScopeMarker" && name.Name != "seedSelectedExecutionPostForkSourceEvent") {
			return true
		}
		counts[name.Name]++
		if len(call.Args) != 7 || family145NodeString(call.Args[1]) != "ctx" || family145NodeString(call.Args[2]) != "pg" || family145NodeString(call.Args[3]) != "sourceRunID" {
			t.Fatalf("%s caller lost original source/context/selected owner", name.Name)
		}
		if name.Name == "seedSelectedExecutionSourceReplayScopeMarker" {
			event, scope, at := family145NodeString(call.Args[4]), family145NodeString(call.Args[5]), family145NodeString(call.Args[6])
			if event == "sourceEventID" && scope == "runtimepipelineobligation.ScopeSubscribed" && at == "at" {
				return true
			}
			if event == "afterEventID" && scope == "runtimepipelineobligation.ScopeDirect" && at == "at.Add(time.Second)" {
				return true
			}
			t.Fatalf("marker caller changed original typed scope or clock: %s/%s/%s", event, scope, at)
		}
		if family145NodeString(call.Args[4]) != "afterEventID" || family145NodeString(call.Args[5]) != "entityID" || family145NodeString(call.Args[6]) != "at.Add(time.Second)" {
			t.Fatal("post-fork source caller changed original event/entity/clock")
		}
		return true
	})
	if counts["seedSelectedExecutionSourceReplayScopeMarker"] != 2 || counts["seedSelectedExecutionPostForkSourceEvent"] != 1 {
		t.Fatalf("replay scope/post-fork source inventory changed: %+v", counts)
	}
}
