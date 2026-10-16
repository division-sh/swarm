package main

import (
	"fmt"
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
