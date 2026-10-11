package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// Only the reviewed construction and owner-forwarding cuts are normalized.
func normalizedBusRunConstruction(t *testing.T, source string) string {
	t.Helper()
	if strings.Contains(source, "func TestEventBusPublish_MixedEmptyAndTargetedNodeRoutesExecuteAndSettle(") {
		source = nativeExecutionConstructionSource(source)
	}
	source = normalizedBusGlobalChronology(source)
	for _, field := range [][2]string{
		{"stepBeginEventID", "step.begin"},
		{"microStartEventID", "child/micro.start"},
		{"microDoneEventID", "child/grandchild/micro.done"},
		{"microRelayedEventID", "child/micro.relayed"},
	} {
		before := "\tvar " + field[0] + " string\n\tif err := db.QueryRowContext(ctx, `SELECT event_id::text FROM events WHERE event_name = '" + field[1] + "' ORDER BY created_at DESC LIMIT 1`).Scan(&" + field[0] + "); err != nil {"
		after := "\t" + field[0] + ", err := storetest.ReadLatestNamedEventIdentityStorage(ctx, pg, \"" + field[1] + "\", \"\")\n\tif err != nil {"
		source = strings.Replace(source, before, after, 1)
	}
	source = strings.Replace(source, "\tctx = context.WithValue(ctx, publisherDispatchValueKey{}, \"publisher-value\")\n", "", 1)
	for _, pair := range [][2]string{
		{"assertNodeDeliveryStatus(t, db,", "assertNodeDeliveryStatus(t, pg,"},
		{"assertNodeDeliveryTarget(t, db,", "assertNodeDeliveryTarget(t, pg,"},
		{"countEventDeliveriesForEvent(t, ctx, db,", "countEventDeliveriesForEvent(t, ctx, pg,"},
		{"countPipelineReceiptsForEvent(t, ctx, db,", "countPipelineReceiptsForEvent(t, ctx, pg,"},
		{"newEventBusWorkflowCoordinator(eb, db, pg, module)", "newEventBusWorkflowCoordinator(eb, pg, module)"},
		{"_, db, cleanup := testutil.StartPostgres(t)\n\tt.Cleanup(cleanup)\n\tctx := eventBusTestRunContext(t, db)\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "pg := storetest.StartPostgresRuntimeStore(t)\n\tctx := eventBusTestRunContext(t, pg)"},
		{"_, db, _ := testutil.StartPostgres(t)\n\tctx := eventBusTestRunContext(t, db)\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "pg := storetest.StartPostgresRuntimeStore(t)\n\tctx := eventBusTestRunContext(t, pg)"},
		{"ctx := eventBusTestRunContext(t, db)\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "pg := storetest.AdmitPostgresRuntimeStore(t, db)\n\tctx := eventBusTestRunContext(t, pg)"},
		{"_, db, cleanup := testutil.StartPostgres(t)\n\tt.Cleanup(cleanup)\n\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "pg := storetest.StartPostgresRuntimeStore(t)"},
		{"eventBusTestRunContextForSource(t, db,", "eventBusTestRunContextForSource(t, pg,"},
		{"eventBusTestRunContext(t, db)", "eventBusTestRunContext(t, pg)"},
	} {
		source = strings.ReplaceAll(source, pair[0], pair[1])
	}
	value, err := canonicalFunction(source)
	if err != nil {
		return "parse-refused"
	}
	return value
}

const busSelectedRunContextShape = `func eventBusTestRunContextForSource(t *testing.T,selected eventBusRunFixtureStore,source semanticview.Source) context.Context {
t.Helper()
ctx := runtimecorrelation.WithRunID(testAuthorActivityContextForSource(context.Background(),source),eventBusTestRunID)
if err := ensureTestEventBusSourceArtifact(selected,source,testSourceArtifactFact(source)); err != nil { t.Fatalf("persist exact event bus test source artifact: %v",err) }
if err := storetest.EnsureEphemeralRun(ctx,selected,eventBusTestRunID,time.Date(2000,1,1,0,0,0,0,time.UTC)); err != nil { t.Fatalf("create event bus test run through lifecycle owner: %v",err) }
return ctx
}`

func busSelectedRunContextPreserved(t *testing.T, source string) bool {
	t.Helper()
	want, err := canonicalFunction(busSelectedRunContextShape)
	got, actualErr := canonicalFunction(source)
	return err == nil && actualErr == nil && want == got
}

func TestNativeBusRunConstructionPreservesOriginalOwnerAndCompleteCallers(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-selected-run-construction" {
			continue
		}
		matched++
		row = historicalMechanicalRecipe(t, row)
		switch row.Function {
		case "eventBusTestRunContextForSource":
			before := strings.Replace(row.Before, "db *sql.DB", "selected eventBusRunFixtureStore", 1)
			before = strings.Replace(before, "\tselected := storetest.AdmitPostgresRuntimeStore(t, db)\n", "", 1)
			if before != row.After || !busSelectedRunContextPreserved(t, row.After) {
				t.Fatal("source/run identity, timestamp, lifecycle owner or fail-closed setup changed")
			}
		case "eventBusTestRunContext":
			before := strings.Replace(row.Before, "db *sql.DB", "selected eventBusRunFixtureStore", 1)
			before = strings.Replace(before, "(t, db, nil)", "(t, selected, nil)", 1)
			if before != row.After {
				t.Fatal("default source context no longer forwards the exact selected owner")
			}
		default:
			if row.Function == "TestEventBusPublish_MixedEmptyAndTargetedNodeRoutesExecuteAndSettle" {
				assertNativeExecutionConstruction(t, row)
			}
			if normalizedNativePostCommitRoot(t, normalizedBusRunConstruction(t, row.Before)) != normalizedNativePostCommitRoot(t, normalizedBusRunConstruction(t, row.After)) {
				t.Fatalf("construction changed execution/temporal/assertion cuts: %s", row.Function)
			}
		}
	}
	if matched != 11 {
		t.Fatalf("new selected run construction recipes=%d, want11 plus two updated existing roots", matched)
	}
	actual := selectedCausalObservationBody(t, "internal/runtime/bus/eventbus_publish_test.go", "eventBusTestRunContextForSource")
	for _, pair := range [][2]string{
		{"EnsureEphemeralRun(\n\t\tctx, selected,", "EnsureEphemeralRun(\n\t\tctx, foreignStore,"},
		{"testSourceArtifactFact(source)", "testSourceArtifactFact(nil)"},
		{"eventBusTestRunID, time.Date", "otherRunID, time.Date"},
		{"time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)", "time.Now()"},
		{"; err != nil {", "; false {"},
	} {
		mutant := strings.Replace(actual, pair[0], pair[1], 1)
		if mutant == actual || busSelectedRunContextPreserved(t, mutant) {
			t.Fatalf("weakened native run setup admitted: %v", pair)
		}
	}
}
