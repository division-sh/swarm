package main

import (
	"encoding/json"
	"strings"
	"testing"
)

const nativeConversationTurnFixtureShape = "func persistConformanceAgentTurnReadbackFixture(\n\tt testing.TB,\n\tctx context.Context,\n\tselected *store.PostgresStore,\n\trec runtimellm.AgentTurnRecord,\n) error {\n\tt.Helper()\n\tif rec.Identity.IsZero() {\n\t\trec.Identity = conformanceAgentMemoryIdentity(t, rec.RunID, rec.AgentID)\n\t\trec.FlowInstance = rec.Identity.FlowInstance()\n\t}\n\teventID := uuid.NewString()\n\teventType := events.EventType(\"conformance.turn.requested\")\n\tevent := eventtest.ExistingRunRootIngressWithRoutingSource(\n\t\teventID, eventType, \"conformance\", \"\", []byte(`{}`), 0, rec.RunID,\n\t\tevents.EventEnvelope{}, events.NoRoutingSource(), time.Now().UTC(),\n\t)\n\tprobe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})\n\tstoretest.CommitSemanticEvent(t, ctx, selected, event)\n\tif counts := probe.Snapshot(); counts.Total.WriteCommits == 0 || counts.Active != 0 {\n\t\tt.Fatalf(\"conversation trigger publication escaped original selected coordinator: %+v\", counts)\n\t}\n\tstoretest.PersistManagedAgentTurnFixture(t, ctx, storetest.ManagedAgentTurnFixture{\n\t\tStore: selected, Selected: selected, Identity: rec.Identity,\n\t\tRunID: rec.RunID, SessionID: rec.SessionID, TurnID: uuid.NewString(), Memory: rec.Memory,\n\t\tEntityID: rec.EntityID, TaskID: rec.TaskID, Event: event, TurnBlocks: rec.TurnBlocks,\n\t\tParseOK: rec.ParseOK, Latency: rec.Latency, CreatedAt: time.Now().UTC(),\n\t})\n\treturn nil\n}"

func TestNativeConversationTurnPublicationRetainsAllPublicReadbackAssertions(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-conversation-turn-publication" {
			continue
		}
		matched++
		expected := strings.ReplaceAll(row.Before, "persistConformanceAgentTurnReadbackFixture(t, ctx, db, pg,", "persistConformanceAgentTurnReadbackFixture(t, ctx, pg,")
		expected = strings.Replace(expected, "\t_, db, _ := testutil.StartPostgres(t)\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "\tpg := storetest.StartPostgresRuntimeStore(t)", 1)
		if row.Function == "persistConformanceAgentTurnReadbackFixture" {
			expected = nativeConversationTurnFixtureShape
		}
		want, err := canonicalFunction(expected)
		got, actualErr := canonicalFunction(row.After)
		actual, sourceErr := canonicalFunction(selectedCausalObservationBody(t, row.File, row.Function))
		if err != nil || actualErr != nil || sourceErr != nil || want != got || got != actual {
			t.Fatalf("conversation record/payload/session/public projection assertions changed: %s", row.Function)
		}
	}
	if matched != 4 {
		t.Fatalf("conversation publication recipes=%d,want4", matched)
	}
	for _, pair := range [][2]string{
		{"events.NoRoutingSource()", "eventtest.RootRoutingSource(rec.RunID)"},
		{"storetest.CommitSemanticEvent(t, ctx, selected, event)", "storetest.CommitSemanticEvent(t, ctx, anotherOwner, event)"},
		{"Store: selected, Selected: selected", "Store: selected, Selected: anotherOwner"},
		{"counts.Total.WriteCommits == 0 || counts.Active != 0", "false"},
		{"SessionID: rec.SessionID", "SessionID: uuid.NewString()"},
	} {
		mutant := strings.Replace(nativeConversationTurnFixtureShape, pair[0], pair[1], 1)
		want, _ := canonicalFunction(nativeConversationTurnFixtureShape)
		got, err := canonicalFunction(mutant)
		if mutant == nativeConversationTurnFixtureShape || (err == nil && got == want) {
			t.Fatalf("foreign/changed conversation trigger admitted: %v", pair)
		}
	}
}
