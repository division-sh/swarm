package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func normalizedNativeStandaloneSetup(source string) string {
	for _, replacement := range [][2]string{
		{"_, db, cleanup := testutil.StartPostgres(t)\n\tt.Cleanup(cleanup)\n\n\tctx := testAuthorActivityContext(context.Background())\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "pg := storetest.StartPostgresRuntimeStore(t)\n\tctx := testAuthorActivityContext(context.Background())"},
		{"executeStandaloneCompletionCandidate(t, ctx, db, pg,", "executeStandaloneCompletionCandidate(t, ctx, pg,"},
		{"loadRunStateForEvent(t, ctx, db,", "loadRunStateForEvent(t, ctx, pg,"},
		{"loadAgentDeliveryForEvent(t, ctx, db,", "loadAgentDeliveryForEvent(t, ctx, pg,"},
		{"runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{\n\t\t\t\tOrigin:     runlifecyclefixture.EventOrigin(t, tc.eventID, string(tc.eventType)),", "storetest.RequireRun(t, ctx, pg, storetest.RunFixture{\n\t\t\t\tOrigin:     storetest.EventOrigin(t, tc.eventID, string(tc.eventType)),"},
	} {
		source = strings.ReplaceAll(source, replacement[0], replacement[1])
	}
	return source
}
func TestNativeBusStandaloneStoragePreservesCompleteHelpersAndCandidateExecution(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-standalone-storage" {
			continue
		}
		matched++
		before := strings.Replace(row.Before, "db *sql.DB, pg *store.PostgresStore", "pg *store.PostgresStore", 1)
		before = strings.Replace(before, "db *sql.DB", "selected any", 1)
		switch row.Function {
		case "loadRunStateForEvent":
			before = strings.Replace(before, "var runID, runStatus, triggerEventType string\n\tif err := db.QueryRowContext(ctx, `\n\t\tSELECT\n\t\t\tCOALESCE(r.run_id::text, ''),\n\t\t\tCOALESCE(r.status, ''),\n\t\t\tCOALESCE(r.trigger_event_type, '')\n\t\tFROM events e\n\t\tINNER JOIN runs r ON r.run_id = e.run_id\n\t\tWHERE e.event_id = $1::uuid\n\t`, eventID).Scan(&runID, &runStatus, &triggerEventType); err != nil {", "out, err := storetest.ReadStandaloneRunStorage(ctx, selected, eventID)\n\tif err != nil {", 1)
			before = strings.Replace(before, "\treturn runID, runStatus, triggerEventType\n}", "\treturn out.RunID, out.Status, out.TriggerEventType\n}", 1)
		case "loadAgentDeliveryForEvent":
			before = strings.Replace(before, "var status, runStatus string\n\tif err := db.QueryRowContext(ctx, `\n\t\tSELECT\n\t\t\tCOALESCE(d.status, ''),\n\t\t\tCOALESCE(r.status, '')\n\t\tFROM event_deliveries d\n\t\tINNER JOIN runs r ON r.run_id = d.run_id\n\t\tWHERE d.event_id = $1::uuid\n\t\t  AND d.subscriber_type = 'agent'\n\t\t  AND d.subscriber_id = $2\n\t`, eventID, agentID).Scan(&status, &runStatus); err != nil {", "out, err := storetest.ReadStandaloneAgentDeliveryStorage(ctx, selected, eventID, agentID)\n\tif err != nil {", 1)
			before = strings.Replace(before, "\treturn status, runStatus\n}", "\treturn out.Status, out.RunStatus\n}", 1)
		case "executeStandaloneCompletionCandidate":
			before = strings.Replace(before, "var candidate runtimerunlifecycle.Candidate\n\tif err := db.QueryRowContext(ctx, `\n\t\tSELECT r.run_id::text, r.bundle_hash, r.completion_revision, r.completion_due_at\n\t\tFROM runs r\n\t\tJOIN events e ON e.run_id = r.run_id\n\t\tWHERE e.event_id = $1::uuid\n\t`, eventID).Scan(&candidate.RunID, &candidate.BundleHash, &candidate.Revision, &candidate.DueAt); err != nil {", "candidate, err := storetest.ReadStandaloneCompletionCandidate(ctx, pg, eventID)\n\tif err != nil {", 1)

		default:
			t.Fatal("unnamed standalone helper")
		}
		if before != row.After {
			t.Fatal("standalone read error, execution candidate or terminal eligibility assertion changed")
		}
		if selectedCausalObservationBody(t, row.File, row.Function) != row.After {
			t.Fatal("standalone helper diverged from finite migration")
		}
	}
	if matched != 3 {
		t.Fatalf("standalone helpers=%d, want3 plus two original caller updates", matched)
	}
}
func TestNativeBusStandaloneOwnersPreserveAllThreeJoinedPhysicalQueries(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	owners := map[string][2]string{
		"loadRunStateForEvent":                 {"internal/store/internal/backend/runlifecycle/standalone_fixture_storage.go", "ReadStandaloneRunStorageTx"},
		"loadAgentDeliveryForEvent":            {"internal/store/internal/backend/delivery/read_projections.go", "ReadStandaloneAgentDeliveryStorageTx"},
		"executeStandaloneCompletionCandidate": {"internal/store/internal/backend/runlifecycle/run_lifecycle_candidates.go", "ReadStandaloneCompletionCandidateTx"},
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-standalone-storage" {
			continue
		}
		matched++
		owner := owners[row.Function]
		before := eventDeliveryDiagnosticSQL(t, strings.ReplaceAll(strings.ReplaceAll(row.Before, "SELECT\n", "SELECT "), "r.run_id::text", "CAST(r.run_id AS TEXT)"))
		after := eventDeliveryDiagnosticSQL(t, selectedCausalObservationBody(t, owner[0], owner[1]))
		if len(before) != 1 || len(after) != 1 || strings.ReplaceAll(before[0], "$1::uuid", "$1") != after[0] {
			t.Fatalf("join, exact predicate, columns, null handling or candidate cut changed for %s: %v -> %v", row.Function, before, after)
		}
	}
	if matched != 3 {
		t.Fatalf("physical standalone witnesses=%d", matched)
	}
}
