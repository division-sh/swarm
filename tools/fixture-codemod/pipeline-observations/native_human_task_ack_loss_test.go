package main

import (
	"encoding/json"
	"strings"
	"testing"
)

const humanTaskAckLossOldCount = "\t\t\tquery := `SELECT COUNT(*) FROM events WHERE event_id = ?`\n\t\t\targ := any(committed.DecisionEventID)\n\t\t\tif backend == \"postgres\" {\n\t\t\t\tquery = `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid`\n\t\t\t}\n\t\t\tvar eventCount int\n\t\t\tif err := db.QueryRowContext(ctx, query, arg).Scan(&eventCount); err != nil || eventCount != 1 {\n\t\t\t\tt.Fatalf(\"durable decision event count = %d, %v; want exactly one\", eventCount, err)\n\t\t\t}\n"
const humanTaskAckLossNativeCount = "\t\t\tdecisionEvent, found, err := storetest.ReadCanonicalEventRecord(ctx, cardStore, committed.DecisionEventID)\n\n\t\t\tif err != nil || !found || decisionEvent.ID() != committed.DecisionEventID || decisionEvent.RunID() != runID {\n\t\t\t\tt.Fatalf(\"durable decision event = %s/%s, found=%t, %v; want exactly one %s/%s\", decisionEvent.RunID(), decisionEvent.ID(), found, err, runID, committed.DecisionEventID)\n\t\t\t}\n"

func nativeHumanTaskAckLossSource(source string) string {
	source = strings.Replace(source, "cardStore, humanStore, idempotency, mailbox, workflowStore, db :=", "cardStore, humanStore, idempotency, mailbox, workflowStore :=", 1)
	source = strings.Replace(source, humanTaskAckLossOldCount, humanTaskAckLossNativeCount, 1)
	source = strings.Replace(source, "DecisionCardAuthority, *sql.DB)", "DecisionCardAuthority)", 1)
	source = strings.Replace(source, "\t\t_, db, cleanup := testutil.StartPostgres(t)\n\t\tt.Cleanup(cleanup)\n\t\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "\t\tpg := storetest.StartPostgresRuntimeStore(t)", 1)
	source = strings.Replace(source, "newHumanTaskAckLossDecisionAuthority(t, db, pg,", "newHumanTaskAckLossDecisionAuthority(t, pg,", 1)
	source = strings.Replace(source, "pg, pg), db", "pg, pg)", 1)
	source = strings.Replace(source, "newHumanTaskAckLossDecisionAuthority(t, storetest.Database(sqliteStore), sqliteStore,", "newHumanTaskAckLossDecisionAuthority(t, sqliteStore,", 1)
	source = strings.Replace(source, "sqliteStore, sqliteStore), storetest.Database(sqliteStore)", "sqliteStore, sqliteStore)", 1)
	return strings.Replace(source, "\tdb *sql.DB,\n", "", 1)
}

func TestNativeHumanTaskAcknowledgmentFixturePreservesReplayAndExactEventIdentity(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-human-task-acknowledgment-owner" {
			continue
		}
		matched++
		want, err := canonicalFunction(nativeHumanTaskAckLossSource(row.Before))
		got, afterErr := canonicalFunction(row.After)
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		value, sourceErr := canonicalFunction(actual)
		if err != nil || afterErr != nil || sourceErr != nil || want != got || value != want {
			t.Fatalf("%s changed outside native construction/exact primary-key observation", row.Function)
		}
		for _, raw := range []string{"*sql.DB", "db.Query", "storetest.Database(", "AdmitPostgresRuntimeStore", "testutil.StartPostgres"} {
			if strings.Contains(actual, raw) {
				t.Fatalf("human acknowledgment fixture still escapes: %s", raw)
			}
		}
		for _, cut := range []string{"authority.successfulMutations != 2", "!found", "decisionEvent.ID() != committed.DecisionEventID", "decisionEvent.RunID() != runID", "ReadCanonicalEventRecord(ctx, cardStore,", "reloaded.Verdict !=", "pg, pg)", "HumanTasks:          humanTasks"} {
			mutant := strings.Replace(row.After, cut, "unreviewedAcknowledgmentCut", 1)
			if mutant == row.After {
				continue
			}
			changed, mutantErr := canonicalFunction(mutant)
			if mutantErr == nil && changed == want {
				t.Fatalf("lost replay/identity/owner assertion accepted: %s", cut)
			}
		}
	}
	if matched != 3 {
		t.Fatalf("human task acknowledgment recipes=%d,want3", matched)
	}
}
