package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func normalizedNativePoolCleanup(source string) string {
	for _, exact := range []string{
		"\t\t\tvar workers sync.WaitGroup\n\t\t\tdefer func() {\n\t\t\t\tcancel()\n\t\t\t\tworkers.Wait()\n\t\t\t}()\n",
		"\t\t\tworkerContext, cancelWorkers := context.WithCancel(context.Background())\n\t\t\tvar workers sync.WaitGroup\n\t\t\tvar releaseOnce sync.Once\n\t\t\tdefer func() {\n\t\t\t\tcancelWorkers()\n\t\t\t\treleaseOnce.Do(func() { close(release) })\n\t\t\t\tworkers.Wait()\n\t\t\t}()\n",
		"\t\t\t\tworkers.Add(1)\n",
		"\t\t\t\t\tdefer workers.Done()\n",
	} {
		source = strings.ReplaceAll(source, exact, "")
	}
	for _, exact := range []string{"\t\t\t\t\tselect {\n\t\t\t\t\tcase <-start:\n\t\t\t\t\tcase <-ctx.Done():\n\t\t\t\t\t\treturn\n\t\t\t\t\t}", "\t\t\t\t\tselect {\n\t\t\t\t\tcase <-start:\n\t\t\t\t\tcase <-workerContext.Done():\n\t\t\t\t\t\treturn\n\t\t\t\t\t}"} {
		source = strings.ReplaceAll(source, exact, "\t\t\t\t\t<-start")
	}
	source = strings.ReplaceAll(source, "context.WithTimeout(workerContext, 15*time.Second)", "context.WithTimeout(context.Background(), 15*time.Second)")
	return strings.ReplaceAll(source, "releaseOnce.Do(func() { close(release) })", "close(release)")
}
func TestNativeBusPoolPreservesEverySaturationSurfaceAndTemporalCut(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-pool-saturation" {
			continue
		}
		matched++
		before := row.Before
		for _, replacement := range [][2]string{
			{"_, db, cleanup := testutil.StartPostgres(t)\n\t\t\tt.Cleanup(cleanup)\n\t\t\tpg := storetest.AdmitPostgresRuntimeStore(t, db)\n\t\t\tdb.SetMaxOpenConns(poolSize)\n\t\t\tdb.SetMaxIdleConns(poolSize)", "pg := storetest.StartPostgresRuntimeStore(t)\n\t\t\tif err := storetest.LimitPostgresPublicationFixturePool(context.Background(), pg); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}"},
			{"_, db, cleanup := testutil.StartPostgres(t)\n\t\t\tt.Cleanup(cleanup)\n\t\t\tseedStore := storetest.AdmitPostgresRuntimeStore(t, db)", "seedStore := storetest.StartPostgresRuntimeStore(t)"},
			{"runlifecyclefixture.RequirePostgres(t, context.Background(), db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runIDs[i], BundleHash: authorActivityTestBundleHash})", "storetest.RequireRun(t, context.Background(), seedStore, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runIDs[i], BundleHash: authorActivityTestBundleHash})"},
			{"db.SetMaxOpenConns(poolSize)\n\t\t\tdb.SetMaxIdleConns(poolSize)", "if err := storetest.LimitPostgresPublicationFixturePool(context.Background(), seedStore); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}"},
			{"\t\t\t\tselectedStore := storetest.AdmitPostgresRuntimeStore(t, db)\n", ""},
			{"PostgresStore: selectedStore,", "PostgresStore: seedStore,"},
			{"Store: selectedStore.PipelineObligations(),", "Store: seedStore.PipelineObligations(),"},
			{"\t\t\t\tvar count int\n\t\t\t\tif err := db.QueryRowContext(context.Background(), `\n\t\t\t\t\tSELECT COUNT(*)\n\t\t\t\t\tFROM event_receipts\n\t\t\t\t\tWHERE event_id = $1::uuid\n\t\t\t\t\t  AND subscriber_type = 'platform'\n\t\t\t\t\t  AND subscriber_id = 'pipeline'\n\t\t\t\t`, eventID).Scan(&count); err != nil {", "\t\t\t\tcount, err := storetest.CountPipelineEventReceiptStorage(context.Background(), pg, eventID)\n\t\t\t\tif err != nil {"},
			{"\t\t\t\tvar count int\n\t\t\t\tif err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, eventID).Scan(&count); err != nil {", "\t\t\t\tcount, err := storetest.CountPipelineEventReceiptStorage(context.Background(), seedStore, eventID)\n\t\t\t\tif err != nil {"},
			{"\t\t\t\t\tvar status string\n\t\t\t\t\tif err := db.QueryRowContext(context.Background(), `SELECT status FROM decision_card_route_obligations WHERE event_id = $1::uuid`, eventID).Scan(&status); err != nil || status != \"completed\" {", "\t\t\t\t\tstatus, err := storetest.ReadDecisionRouteStatusStorage(context.Background(), seedStore, eventID)\n\t\t\t\t\tif err != nil || status != \"completed\" {"},
		} {
			before = strings.Replace(before, replacement[0], replacement[1], 1)
		}
		if before != normalizedNativePoolCleanup(row.After) {
			t.Fatal("pool forms, claim alignment, real replay, exact receipt or timeout changed")
		}
		if selectedCausalObservationBody(t, row.File, row.Function) != row.After {
			t.Fatal("native pool caller diverged from finite migration")
		}
	}
	if matched != 2 {
		t.Fatalf("pool roots=%d, want2", matched)
	}
}

func TestNativeBusPoolOwnerKeepsFixedCapacityAndDecisionIdentity(t *testing.T) {
	actual := selectedCausalObservationBody(t, "internal/store/internal/backend/postgres/fixture_pool_saturation.go", "LimitPublicationFixturePool")
	want, err := canonicalFunction(`func (b *Backend) LimitPublicationFixturePool(ctx context.Context) error {
if err := b.Ping(ctx); err != nil { return err }
b.capacityMu.Lock()
defer b.capacityMu.Unlock()
if err := ctx.Err(); err != nil { return err }
if b.capacityReservations != 0 { return fmt.Errorf("publication pool fixture requires no retained claim sessions") }
b.db.SetMaxOpenConns(4)
b.db.SetMaxIdleConns(4)
b.baseOpenConnections = 4
return nil
}`)
	got, parseErr := canonicalFunction(actual)
	if err != nil || parseErr != nil || got != want {
		t.Fatal("pool fixture changed its exact four-slot cut or bypassed original reservation/refusal owner")
	}
	actual = selectedCausalObservationBody(t, "internal/store/internal/backend/pipelinepersistence/owner_operations.go", "FixtureDecisionRouteStatusTx")
	if !strings.Contains(actual, "`SELECT status FROM decision_card_route_obligations WHERE event_id=$1`") ||
		!strings.Contains(actual, "eventID).Scan(&status)") || strings.Contains(actual, "LIMIT") {
		t.Fatal("decision route witness lost its exact event-only row predicate")
	}
}
