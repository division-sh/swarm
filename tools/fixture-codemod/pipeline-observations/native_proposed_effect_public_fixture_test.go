package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const proposedEffectOldOpening = "\tfor _, tc := range []struct {\n\t\tname string\n\t\topen func(*testing.T) (any, *sql.DB)\n\t}{\n\t\t{\n\t\t\tname: \"sqlite\",\n\t\t\topen: func(t *testing.T) (any, *sql.DB) {\n\t\t\t\tselected := storetest.StartSQLiteRuntimeStoreWithContext(t, context.Background())\n\t\t\t\treturn selected, storetest.DatabaseForTest(selected)\n\t\t\t},\n\t\t},\n\t\t{\n\t\t\tname: \"postgres\",\n\t\t\topen: func(t *testing.T) (any, *sql.DB) {\n\t\t\t\t_, db, cleanup := testutil.StartPostgres(t)\n\t\t\t\tt.Cleanup(cleanup)\n\t\t\t\treturn storetest.AdmitPostgresRuntimeStore(t, db), db\n\t\t\t},\n\t\t},\n\t} {\n"
const proposedEffectNativeOpening = "\tfor _, tc := range []struct {\n\t\tname string\n\t\topen func(*testing.T) any\n\t}{\n\t\t{\n\t\t\tname: \"sqlite\",\n\t\t\topen: func(t *testing.T) any {\n\t\t\t\treturn storetest.StartSQLiteRuntimeStoreWithContext(t, context.Background())\n\t\t\t},\n\t\t},\n\t\t{\n\t\t\tname: \"postgres\",\n\t\t\topen: func(t *testing.T) any {\n\t\t\t\treturn storetest.StartPostgresRuntimeStore(t)\n\t\t\t},\n\t\t},\n\t} {\n"
const proposedEffectOldCount = "\tquery := `SELECT\n\t\t(SELECT COUNT(*) FROM events WHERE run_id = ? AND event_name = 'platform.activity_requested'),\n\t\t(SELECT COUNT(*) FROM activity_attempts WHERE run_id = ? AND status = 'succeeded')`\n\targs := []any{runID, runID}\n\tif backend == \"postgres\" {\n\t\tquery = `SELECT\n\t\t\t(SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name = 'platform.activity_requested'),\n\t\t\t(SELECT COUNT(*) FROM activity_attempts WHERE run_id = $1::uuid AND status = 'succeeded')`\n\t\targs = []any{runID}\n\t}\n\tvar requests, attempts int\n\tif err := db.QueryRowContext(context.Background(), query, args...).Scan(&requests, &attempts); err != nil {\n\t\tt.Fatal(err)\n\t}\n"
const proposedEffectNativeCount = "\tcounts, err := storetest.ReadProposedEffectRunExecutionStorage(context.Background(), selected, runID)\n\tif err != nil {\n\t\tt.Fatal(err)\n\t}\n"

func nativeProposedEffectPublicSource(source string) string {
	source = strings.Replace(source, proposedEffectOldOpening, proposedEffectNativeOpening, 1)
	source = strings.Replace(source, "persistence, db := tc.open(t)", "persistence := tc.open(t)", 1)
	source = strings.Replace(source, "newProposedEffectMailboxHandler(t, persistence, db, source, fact)", "newProposedEffectMailboxHandler(t, persistence, source, fact)", 1)
	source = strings.Replace(source, "\t\t\tinsertProposedEffectAPIRun(t, fixtureCtx, db, tc.name, runID, fact)\n", "\t\t\tstoretest.RequireRun(t, fixtureCtx, persistence.(storetest.RunFixtureStore), storetest.RunFixture{\n\t\t\t\tOrigin: storetest.ScenarioSetupOrigin(), RunID: runID, BundleHash: fact.BundleHash(), Artifact: bundle.SourceArtifact,\n\t\t\t})\n", 1)
	source = strings.Replace(source, "assertProposedEffectAPIExecutionRows(t, db, tc.name, runID)", "assertProposedEffectAPIExecutionRows(t, persistence, runID)", 1)
	source = strings.Replace(source, "\tdb *sql.DB,\n", "", 1)
	source = strings.Replace(source, "db *sql.DB, backend, runID string", "selected any, runID string", 1)
	source = strings.Replace(source, proposedEffectOldCount, proposedEffectNativeCount, 1)
	source = strings.Replace(source, "if requests != 1 || attempts != 1", "if counts.Requests != 1 || counts.SuccessfulAttempts != 1", 1)
	return strings.Replace(source, "\", requests, attempts)", "\", counts.Requests, counts.SuccessfulAttempts)", 1)
}

func TestNativeProposedEffectPublicFixtureRetainsAdmissionProviderReplayAndCounts(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		root := row.Function == "TestMailboxDecideHTTPReleasesProposedEffectThroughProviderOnBothStores"
		if row.Family != "native-proposed-effect-public-fixture" && !root {
			continue
		}
		matched++
		before := nativeProposedEffectPublicSource(row.Before)
		if root {
			before = nativeMailboxCanonicalEventWorkload(t, row.Before)
		}
		want, err := canonicalFunction(before)
		got, afterErr := canonicalFunction(row.After)
		if root {
			got, afterErr = canonicalFunction(nativeMailboxCanonicalEventWorkload(t, row.After))
		}
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		value, sourceErr := canonicalFunction(actual)
		written, writtenErr := canonicalFunction(row.After)
		if err != nil || afterErr != nil || sourceErr != nil || writtenErr != nil || want != got || value != written {
			t.Fatalf("%s changed public effect workload beyond the exact native owner cuts", row.Function)
		}
		for _, raw := range []string{"*sql.DB", "storetest.Database", "testutil.StartPostgres", "AdmitPostgresRuntimeStore", "db.Query", "runlifecyclefixture.Require"} {
			if strings.Contains(actual, raw) {
				t.Fatalf("proposed-effect public fixture regained raw authority: %s", raw)
			}
		}
		for _, cut := range []string{"fact.BundleHash()", "bundle.SourceArtifact", "ReadProposedEffectRunExecutionStorage(context.Background(), selected, runID)", "counts.Requests != 1", "counts.SuccessfulAttempts != 1", "calls.Load()", "t.Fatal(", "t.Fatalf("} {
			mutant := strings.Replace(row.After, cut, "unreviewedEffectCut", 1)
			if mutant == row.After {
				continue
			}
			changed, mutantErr := canonicalFunction(mutant)
			if root && mutantErr == nil {
				changed, mutantErr = canonicalFunction(nativeMailboxCanonicalEventWorkload(t, mutant))
			}
			if mutantErr == nil && changed == want {
				t.Fatalf("effect source/identity/provider/assertion mutation accepted: %s", cut)
			}
		}
	}
	if matched != 3 {
		t.Fatalf("public effect recipes=%d,want3", matched)
	}
	source, err := os.ReadFile(filepath.Join("..", "..", "..", "internal/apiv1/operator_mailbox_proposed_effect_supported_surface_test.go"))
	if err != nil || strings.Contains(string(source), "func insertProposedEffectAPIRun(") {
		t.Fatalf("retired reconstructed run fixture survived: %v", err)
	}
}

func TestNativeProposedEffectExecutionOwnerKeepsExactPhysicalPredicates(t *testing.T) {
	var row recipe
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range rows {
		if candidate.Function == "assertProposedEffectAPIExecutionRows" {
			row = candidate
		}
	}
	if row.Family != "native-proposed-effect-public-fixture" {
		t.Fatal("exact proposed effect counter recipe is missing")
	}
	old := eventDeliveryDiagnosticSQL(t, strings.ReplaceAll(row.Before, "SELECT\n", "SELECT \n"))
	owner := selectedCausalObservationBody(t, "internal/store/internal/runtimepersistence/test_activity_result_readback.go", "ReadProposedEffectRunExecutionStorageForTest")
	fresh := eventDeliveryDiagnosticSQL(t, strings.ReplaceAll(owner, "SELECT\n", "SELECT \n"))
	normalize := func(query string) string {
		query = regexp.MustCompile(`\s+`).ReplaceAllString(query, "")
		query = strings.ReplaceAll(query, "run_id=$1::uuid", "CAST(run_idASTEXT)=$1")
		return strings.ReplaceAll(query, "run_id=?", "CAST(run_idASTEXT)=$1")
	}
	if len(old) != 2 || len(fresh) != 1 || normalize(old[0]) != normalize(fresh[0]) || normalize(old[1]) != normalize(fresh[0]) {
		t.Fatalf("physical run/status/event predicates changed: %v -> %v", old, fresh)
	}
	for _, cut := range []string{"validateChannelObservationOwner(selected)", "validateSelectedForkStorageIdentity(runID)", "readServedDeliveryObservation(ctx, selected,", "Scan(&observed.Requests, &observed.SuccessfulAttempts)", "return ProposedEffectRunExecutionStorage{}, err"} {
		if !strings.Contains(owner, cut) {
			t.Fatalf("native execution observation lost cut %q", cut)
		}
	}
}
