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
