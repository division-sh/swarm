package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func family145RetainedReplyFaultProofError(before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("retained reply fault lost original shape, refusal or complete-record assertion: %v", err)
	}
	for _, cut := range []string{
		"raw, err := original.EncodeOrigin()",
		`entry := wire["join_admissions"].([]any)[0].(map[string]any)["ref"].(map[string]any)["stage_entry"].(map[string]any)`,
		"raw, err = json.Marshal(wire)",
		"if err := storetest.CorruptOpenReplyOrigin(ctx, selected.events, original.RunID, original.ID, string(raw)); err != nil {",
		"corrupted, err := selected.events.LoadReplyContext(ctx, original.ID)",
		"restored.ReturnJoins = original.ReturnJoins",
	} {
		if strings.Count(after, cut) != 1 {
			return fmt.Errorf("retained reply fault lost exact source/context/row/run/bytes or readback: %s", cut)
		}
	}
	for _, forbidden := range []string{"selected.db", "selected.postgres", "eventfixture.", "authoractivityfixture.", "runforkrevisionfixture.", "WithSQL", "ExecContext", "AddFact"} {
		if strings.Contains(after, forbidden) {
			return fmt.Errorf("retained reply fault kept escaped transaction or semantic ownership: %s", forbidden)
		}
	}
	return nil
}

func TestFamily145RetainedReplyFaultRetiresReconstructionAndPreservesExactHostility(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var original, current string
	for _, row := range rows {
		if row.Function == "a2CorruptBoundReplyEntry" {
			if original != "" {
				t.Fatal("ambiguous retained-reply fault consumer")
			}
			original, current = row.Before, row.After
		}
	}
	if original == "" {
		t.Fatal("missing retained-reply whole-helper transition")
	}
	if err := family145RetainedReplyFaultProofError(original, current); err != nil {
		t.Fatal(err)
	}
	for _, lost := range []string{"original.RunID", "original.ID, string(raw)", "restored.ReturnJoins = original.ReturnJoins"} {
		if family145RetainedReplyFaultProofError(original, strings.Replace(current, lost, "foreignOrigin", 1)) == nil {
			t.Fatalf("wrong scope/bytes or incomplete-row proof passed: %s", lost)
		}
	}
	physical := family145ConversationForkOwnerSource(t, "internal/backend/replycontext/test_retained_origin_fault.go", "CorruptOpenReplyOriginForTest")
	beforeSQL := family145GateRecoveryStatements(t, "package proof\n"+original, "UPDATE")
	afterSQL := family145GateRecoveryStatements(t, "package proof\n"+physical, "UPDATE")
	if len(beforeSQL) != 2 || !reflect.DeepEqual(beforeSQL, afterSQL) {
		t.Fatalf("retained origin changed exact dialect SQL, casts, open/unaccepted/run/id conjunction: %v/%v", beforeSQL, afterSQL)
	}
	for _, cut := range []string{
		"attempt.WithSQL(ctx,",
		"result, err := tx.ExecContext(ctx, query, origin, contextID, runID)",
		"rows, err := result.RowsAffected()",
		"if err != nil || rows != 1 {",
		"return attempt.AddFact(runID, runforkrevision.FamilyReplyContexts, contextID)",
	} {
		if strings.Count(physical, cut) != 1 {
			t.Fatalf("retained origin lost exact affected row, fact or atomicity: %s", cut)
		}
	}
	assertFamily145RetainedReplyNativeProtocol(t)
}

func assertFamily145RetainedReplyNativeProtocol(t *testing.T) {
	t.Helper()
	bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_reply_context_storage.go", "CorruptOpenReplyOriginForTest")
	for _, cut := range []string{
		"dialect, err := eventFixtureDialectForTest(selected)",
		"replycontextstore.CorruptOpenReplyOriginForTest(ctx, attempt, dialect == authoractivityfixture.DialectPostgres, runID, contextID, origin)",
		"mutationprotocol.RunPostgres(ctx, owner.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, owner.runLifecycleCandidates, apply).Err()",
		"mutationprotocol.RunSQLite(ctx, owner.backend, \"canonical event fixture\", mutationprotocol.Story, mutationprotocol.Ordinary, nil, owner.runLifecycleCandidates, apply).Err()",
	} {
		if strings.Count(bridge, cut) != 1 {
			t.Fatalf("retained origin changed original native owner, Story, revision or candidate coordinator: %s", cut)
		}
	}
}
