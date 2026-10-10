package main

import (
	"encoding/json"
	"strings"
	"testing"
)

const committedScopeReadShape = `func loadCommittedPipelineScopePostgres(t *testing.T,ctx context.Context,selected any,eventID string) runtimepipelineobligation.CommittedScope {
t.Helper()
scope,err := storetest.ReadCommittedPipelineScope(ctx,selected,eventID)
if err != nil { t.Fatalf("load committed pipeline scope for %s: %v",eventID,err) }
return scope
}`

func committedScopeReadPreserved(source string) bool {
	want, err := canonicalFunction(committedScopeReadShape)
	got, parseErr := canonicalFunction(source)
	return err == nil && parseErr == nil && want == got
}

func TestNativeBusCommittedScopePreservesFullPostCommitCuts(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-committed-pipeline-scope" {
			continue
		}
		matched++
		switch row.Function {
		case "loadCommittedPipelineScopePostgres":
			if !committedScopeReadPreserved(row.After) {
				t.Fatal("scope reader lost original owner/context/event, typed value or failure refusal")
			}
			for _, pair := range [][2]string{
				{"ctx, selected, eventID", "ctx, foreignStore, eventID"},
				{"ctx, selected, eventID", "ctx, selected, otherEvent"},
				{"if err != nil", "if false"}, {"return scope", "return runtimepipelineobligation.ScopeDirect"},
			} {
				mutant := strings.Replace(row.After, pair[0], pair[1], 1)
				if mutant == row.After || committedScopeReadPreserved(mutant) {
					t.Fatalf("weakened committed-scope observation admitted: %v", pair)
				}
			}
		case "Intercept":
			before := strings.Replace(row.Before, "storetest.DatabaseForTest(i.store)", "i.store", 1)
			if normalizedNativePostCommitMethod(before) != row.After {
				t.Fatal("post-commit transaction absence, persistence, recipients, scope or completion signal changed")
			}
		case "TestEventBusPublishAcknowledgedReturnsBeforePostCommitDispatchCompletes":
			before := strings.Replace(row.Before, "loadCommittedPipelineScopePostgres(t, ctx, db, eventID)", "loadCommittedPipelineScopePostgres(t, ctx, pg, eventID)", 1)
			if normalizedBusRunConstruction(t, before) != normalizedBusRunConstruction(t, row.After) {
				t.Fatal("held dispatch, early acknowledgment, scope, quiescence deadline or eventual delivery assertion changed")
			}
		default:
			t.Fatalf("unknown committed-scope recipe: %s", row.Function)
		}
	}
	if matched != 3 {
		t.Fatalf("committed-scope recipes=%d, want3", matched)
	}
	owner := selectedCausalObservationBody(t, "internal/store/internal/runtimepersistence/test_event_readback.go", "ReadCommittedPipelineScopeForTest")
	for _, required := range []string{
		"validateSelectedForkStorageIdentity(eventID)", "validateChannelObservationOwner(selected)",
		"readServedDeliveryObservation(ctx, selected,", "pipelinepersistence.LoadCommittedScope(ctx, tx, eventID, postgres)",
		`return "", err`,
	} {
		if !strings.Contains(owner, required) {
			t.Fatalf("canonical scope owner or fail-closed boundary missing: %s", required)
		}
	}
	if strings.Contains(owner, "SELECT ") || strings.Contains(owner, "ParseCommittedScope(") {
		t.Fatal("parallel SQL/scope interpreter introduced outside existing pipeline owner")
	}
}
