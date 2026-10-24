package main

import (
	"strings"
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
