package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeConversationSessionRecipeKeepsOriginalRegistryAndWatchdogAssertions(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-conversation-session-owner" {
			continue
		}
		matched++
		before := row.Before
		if row.Function == "acquireLiveConversationSession" {
			before = strings.Replace(before, "db *sql.DB", "registry runtimesessions.Registry", 1)
			before = strings.Replace(before, "\tregistry := storetest.AdmitPostgresRuntimeStore(t, db)\n", "", 1)
		} else {
			before = strings.Replace(before, "\t_, db, _ := testutil.StartPostgres(t)\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "\tpg := storetest.StartPostgresRuntimeStore(t)", 1)
			before = strings.Replace(before, "\tlease := acquireLiveConversationSession(t, ctx, db, identity)\n\tsessionID := lease.SessionID", "\tprobe := storetest.CollectTransactions(t, pg, storetest.TransactionProbeOptions{})\n\tlease := acquireLiveConversationSession(t, ctx, pg, identity)", 1)
			before = strings.Replace(before, "\tif err := pg.UpsertConversation", "\tif counts := probe.Snapshot(); counts.Total.WriteCommits != 1 || counts.Active != 0 {\n\t\tt.Fatalf(\"conversation acquisition escaped original selected coordinator: %+v\", counts)\n\t}\n\tsessionID := lease.SessionID\n\tif err := pg.UpsertConversation", 1)
		}
		want, err := canonicalFunction(before)
		got, actualErr := canonicalFunction(row.After)
		if err != nil || actualErr != nil || want != got {
			t.Fatalf("conversation source/identity/lease/release/watchdog proof changed: %s", row.Function)
		}
	}
	if matched != 2 {
		t.Fatalf("original session recipes=%d,want2", matched)
	}
}
