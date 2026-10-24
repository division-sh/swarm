package main

import (
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
