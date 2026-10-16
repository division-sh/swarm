package main

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145EventDeliveryReadCuts = map[string]string{
	"testA2BoundReplyJourney": "deliveries, err := storetest.CountGateEventNodeDeliveries(ctx, selected.events, replyID)",
	"TestA2AccumulatorUnkeyedDurableMultiplicityAndOrderOnBothStores": "deliveryID, err := storetest.ReadGateEventNodeDeliveryIdentity(proof.ctx, proof.selected.events, publication.event.ID())",
	"TestA2AccumulatorPublicationFailureRollsBackOnBothStores":        "delivered, err := storetest.CountGateEventDeliveredRows(proof.ctx, proof.selected.events, publication.event.ID())",
}

func family145EventDeliveryReadProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("event-level delivery observation changed original receipt, order, identity or rollback assertions: %s: %v", function, err)
	}
	if cut := family145EventDeliveryReadCuts[function]; strings.Count(after, cut) != 1 {
		return fmt.Errorf("event-level delivery observation changed exact selected owner/context/event/destination: %s", cut)
	}
	return nil
}

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
