package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestNativeBusAgentMutationPreservesSettledOnlyExactRoleAndZeroAssertion(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-agent-dispatch-mutation" {
			continue
		}
		matched++
		owner := selectedCausalObservationBody(t, "internal/store/internal/backend/delivery/read_projections.go", "ReadAgentSettledAttemptCountTx")
		if !reflect.DeepEqual(eventDeliveryDiagnosticSQL(t, row.Before), eventDeliveryDiagnosticSQL(t, owner)) {
			t.Fatal("settled agent attempt witness changed physical event/role/subscriber predicates")
		}
		before := strings.Replace(row.Before, "\tvar outcomes int\n\tquery := `SELECT COUNT(*) FROM (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o JOIN event_deliveries d ON d.delivery_id = o.delivery_id WHERE d.event_id = ? AND d.subscriber_type = 'agent' AND d.subscriber_id = ?`\n\targs := []any{f.event.ID(), f.agentID}\n\tif f.dialect == \"postgres\" {\n\t\tquery = `SELECT COUNT(*) FROM (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o JOIN event_deliveries d ON d.delivery_id = o.delivery_id WHERE d.event_id = $1::uuid AND d.subscriber_type = 'agent' AND d.subscriber_id = $2`\n\t}\n\tif err := f.db.QueryRowContext(f.ctx, query, args...).Scan(&outcomes); err != nil {", "\toutcomes, err := storetest.ReadAgentSettledAttemptCount(f.ctx, f.store, f.event.ID(), f.agentID)\n\tif err != nil {", 1)
		if before != row.After || selectedCausalObservationBody(t, row.File, row.Function) != row.After {
			t.Fatal("dispatch mutation witness changed context, original owner or zero/error assertions")
		}
	}
	if matched != 1 {
		t.Fatalf("agent mutation recipes=%d,want1", matched)
	}
}
