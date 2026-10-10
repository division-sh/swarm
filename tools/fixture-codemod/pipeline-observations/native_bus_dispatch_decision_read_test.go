package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeBusDispatchDecisionReadPreservesExactEventAndFailure(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-dispatch-decision-read" {
			continue
		}
		matched++
		before := strings.Replace(row.Before, "\tquery := `SELECT status FROM decision_card_route_obligations WHERE event_id = ?`\n\tif f.dialect == \"postgres\" {\n\t\tquery = `SELECT status FROM decision_card_route_obligations WHERE event_id = $1::uuid`\n\t}\n\tvar status string\n\tif err := f.db.QueryRowContext(f.ctx, query, eventID).Scan(&status); err != nil {", "\tstatus, err := storetest.ReadDecisionRouteStatusStorage(f.ctx, f.store, eventID)\n\tif err != nil {", 1)
		if before != row.After || selectedCausalObservationBody(t, row.File, row.Function) != row.After {
			t.Fatal("decision witness changed event identity, context, error or original owner")
		}
	}
	if matched != 1 {
		t.Fatalf("decision witness recipes=%d,want1", matched)
	}
}
