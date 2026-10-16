package main

import (
	"strings"
	"testing"
)

func TestFamily145EventDeliveryOwnersKeepExactOriginalPredicatesAndDestinations(t *testing.T) {
	for _, tc := range []struct{ function, query, scan string }{
		{"CountGateEventNodeDeliveriesTx", `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node'`, "eventID).Scan(&count)"},
		{"ReadGateEventNodeDeliveryIdentityTx", `SELECT delivery_id FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node'`, "eventID).Scan(&id)"},
		{"CountGateEventDeliveredRowsTx", `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND status='delivered'`, "eventID).Scan(&count)"},
	} {
		physical := family145ConversationForkOwnerSource(t, "internal/backend/delivery/read_projections.go", tc.function)
		queries := family145A2ClaimLiteralQueries(t, physical)
		if len(queries) != 1 || queries[0] != tc.query || strings.Count(physical, tc.scan) != 1 {
			t.Fatalf("%s changed original physical scope, columns, casts or destination: %v", tc.function, queries)
		}
	}
}
