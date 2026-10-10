package main

import (
	"reflect"
	"strings"
	"testing"
)

func normalizedBusGlobalChronology(source string) string {
	for _, pair := range [][2]string{
		{"\t\trows, _ := db.QueryContext(context.Background(), `SELECT event_name, COALESCE(entity_id::text,''), COALESCE(flow_instance,'') FROM events ORDER BY created_at ASC, event_id ASC`)\n\t\tdump := make([]string, 0)\n\t\tif rows != nil {\n\t\t\tdefer rows.Close()\n\t\t\tfor rows.Next() {\n\t\t\t\tvar name, entityID, flowInstance string\n\t\t\t\tif scanErr := rows.Scan(&name, &entityID, &flowInstance); scanErr == nil {\n\t\t\t\t\tdump = append(dump, name+\" entity=\"+entityID+\" flow=\"+flowInstance)\n\t\t\t\t}\n\t\t\t}\n\t\t}\n", "\t\trows, err := storetest.ReadGlobalEventChronology(context.Background(), pg)\n\t\tif err != nil {\n\t\t\tt.Fatalf(\"read root-state event diagnostic: %v\", err)\n\t\t}\n\t\tdump := make([]string, 0)\n\t\tfor _, row := range rows {\n\t\t\tdump = append(dump, row.Name+\" entity=\"+row.EntityID+\" flow=\"+row.FlowInstance)\n\t\t}\n"},
		{"\trows, err := db.QueryContext(context.Background(), `SELECT event_name FROM events ORDER BY created_at ASC, event_id ASC`)\n\tif err != nil {\n\t\tt.Fatalf(\"query events: %v\", err)\n\t}\n\tdefer rows.Close()\n\tfor rows.Next() {\n\t\tvar name string\n\t\tif err := rows.Scan(&name); err != nil {\n\t\t\tt.Fatalf(\"scan event: %v\", err)\n\t\t}\n\t\temitted = append(emitted, strings.TrimSpace(name))\n\t}\n\tif err := rows.Err(); err != nil {\n\t\tt.Fatalf(\"iterate events: %v\", err)\n\t}\n", "\trows, err := storetest.ReadGlobalEventChronology(context.Background(), pg)\n\tif err != nil {\n\t\tt.Fatalf(\"query events: %v\", err)\n\t}\n\tfor _, row := range rows {\n\t\temitted = append(emitted, strings.TrimSpace(row.Name))\n\t}\n"},
	} {
		source = strings.Replace(source, pair[0], pair[1], 1)
	}
	return source
}

func TestNativeBusGlobalChronologyRetainsScopeFieldsOrderAndCompleteJourney(t *testing.T) {
	root := selectedCausalObservationBody(t, "internal/runtime/bus/eventbus_publish_test.go", "TestEventBusPublish_NestedThreeLevelConnectChainExecutesEndToEnd")
	if strings.Contains(root, "db.") || strings.Contains(root, "testutil.StartPostgres") ||
		!strings.Contains(root, "pg := storetest.StartPostgresRuntimeStore(t)") ||
		strings.Count(root, "ReadGlobalEventChronology(context.Background(), pg)") != 2 {
		t.Fatal("nested chronology or construction retains raw/foreign authority")
	}
	owner := selectedCausalObservationBody(t, "internal/store/internal/backend/eventrecord/causal_observation.go", "ReadGlobalEventChronologyTx")
	queries := eventDeliveryDiagnosticSQL(t, owner)
	if len(queries) != 2 || !reflect.DeepEqual(queries[1:], eventDeliveryDiagnosticSQL(t, "func proof(){ _ = `SELECT event_name, COALESCE(entity_id::text,''), COALESCE(flow_instance,'') FROM events ORDER BY created_at ASC, event_id ASC` }")) ||
		!strings.Contains(owner, "rows.Scan(&row.Name, &row.EntityID, &row.FlowInstance)") ||
		!strings.Contains(owner, "if err := rows.Err(); err != nil") ||
		!strings.Contains(owner, "if err := rows.Close(); err != nil") {
		t.Fatal("global chronology lost exact native projection/order or failure checks")
	}
	for _, weakened := range []string{"WHERE run_id", "LIMIT ", "time.Now()", "since"} {
		if strings.Contains(owner, weakened) {
			t.Fatalf("global chronology acquired narrowing/synthetic behavior: %s", weakened)
		}
	}
}
