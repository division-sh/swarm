package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func mechanicalBusScalarConstruction(t *testing.T, row recipe) string {
	t.Helper()
	before := row.Before
	switch row.Function {
	case "newScalarTemplateInstanceParityStore":
		for _, pair := range [][2]string{
			{"(scalarTemplateInstanceParityStore, *sql.DB)", "scalarTemplateInstanceParityStore"},
			{"return &sqliteScalarTemplateInstanceStore{SQLiteRuntimeStore: selected}, storetest.DatabaseForTest(selected)", "return &sqliteScalarTemplateInstanceStore{SQLiteRuntimeStore: selected}"},
			{"_, db, cleanup := testutil.StartPostgres(t)\n\t\tt.Cleanup(cleanup)\n\t\treturn &postgresScalarTemplateInstanceStore{PostgresStore: storetest.AdmitPostgresRuntimeStore(t, db)}, db", "return &postgresScalarTemplateInstanceStore{PostgresStore: storetest.StartPostgresRuntimeStore(t)}"},
			{"return nil, nil", "return nil"},
		} {
			before = strings.Replace(before, pair[0], pair[1], 1)
		}
	case "TestScalarTemplateInstanceResolutionPersistsAndReplaysOnSQLiteAndPostgres":
		before = strings.Replace(before, "selected, db := newScalarTemplateInstanceParityStore(t, backend, ctx)", "selected := newScalarTemplateInstanceParityStore(t, backend, ctx)", 1)
		before = strings.Replace(before, "\t\t\trun := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID, Source: sourceFact, Artifact: bundle.SourceArtifact, StartedAt: time.Now().UTC().Add(-time.Minute)}\n\t\t\tif backend == \"postgres\" {\n\t\t\t\trunlifecyclefixture.RequirePostgres(t, ctx, db, run)\n\t\t\t} else {\n\t\t\t\trunlifecyclefixture.RequireSQLite(t, ctx, db, run)\n\t\t\t}", "\t\t\tstoretest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, BundleHash: sourceFact.BundleHash(), Artifact: bundle.SourceArtifact, StartedAt: time.Now().UTC().Add(-time.Minute)})", 1)
	default:
		t.Fatal("unknown scalar-template construction consumer")
	}
	return before
}

func TestNativeBusScalarConstructionPreservesDescriptorDriftAndDurableReplay(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-scalar-construction" {
			continue
		}
		matched++
		current := row
		row = historicalMechanicalRecipe(t, row)
		before := mechanicalBusScalarConstruction(t, row)
		mechanical, mechanicalErr := canonicalFunction(before)
		historical, historicalErr := canonicalFunction(row.After)
		if mechanicalErr != nil || historicalErr != nil || mechanical != historical {
			t.Fatal("scalar source, identity, clock, descriptor drift or replay assertion changed")
		}
		actual, err := canonicalFunction(selectedCausalObservationBody(t, row.File, row.Function))
		want, wantErr := canonicalFunction(current.After)
		if err != nil || wantErr != nil || actual != want {
			t.Fatal("scalar construction diverged from finite migration")
		}
	}
	if matched != 2 {
		t.Fatalf("scalar construction recipes=%d,want2", matched)
	}
}
