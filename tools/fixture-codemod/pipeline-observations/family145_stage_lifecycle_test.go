package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestFamily145StageLifecycleDiagnosticRetainsExactPhysicalQueryAndOwner(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var original string
	for _, row := range rows {
		if row.Function == "stageLifecycleRuntimeLogs" {
			original = row.Before
		}
	}
	owner := family145ConversationForkOwnerSource(t, "internal/backend/eventrecord/template_storage_observation.go", "ReadStageLifecycleRuntimeLogPayloadsTx")
	want, got := family145ConnectorSQL(t, "package proof\n"+original), family145ConnectorSQL(t, "package proof\n"+owner)
	if len(want) != 1 || !reflect.DeepEqual(got, want) {
		t.Fatalf("physical diagnostic name/scope/text/order/limit changed: got=%v want=%v", got, want)
	}
	for _, cut := range []string{"var payload string", "rows.Scan(&payload)", "out = append(out, payload)", "return out, rows.Err()", "errors.Join(err, rows.Close())", "out = nil"} {
		if !strings.Contains(owner, cut) {
			t.Fatalf("physical diagnostic rendering/error settlement changed: %s", cut)
		}
	}
	bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_event_support.go", "ReadStageLifecycleRuntimeLogPayloadsForTest")
	if !strings.Contains(bridge, "readFanOutSemanticStorage(ctx, selected, nil,") || !strings.Contains(bridge, "eventrecord.ReadStageLifecycleRuntimeLogPayloadsTx(ctx, tx)") {
		t.Fatal("diagnostic bridge lost original selected coordinator or widened its selector")
	}
}
