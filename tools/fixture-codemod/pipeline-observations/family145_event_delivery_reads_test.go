package main

import (
	"encoding/json"
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

func TestFamily145EventDeliveryReadTailKeepsAllAssertionsAndOriginalScopes(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, row := range rows {
		cut := family145EventDeliveryReadCuts[row.Function]
		if cut == "" {
			continue
		}
		if found[row.Function] {
			t.Fatalf("ambiguous event-level delivery consumer: %s", row.Function)
		}
		found[row.Function] = true
		if err := family145EventDeliveryReadProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		if family145EventDeliveryReadProofError(row.Function, row.Before, strings.Replace(row.After, cut, "foreignDeliveryEvidence()", 1)) == nil {
			t.Fatalf("lost original scope/context/destination passed: %s", row.Function)
		}
		assertions, err := family145DescriptorBusinessAssertions(row.After)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if family145EventDeliveryReadProofError(row.Function, row.Before, strings.Replace(row.After, condition, "false", 1)) == nil {
				t.Fatalf("lost original delivery/receipt/multiplicity/rollback assertion passed: %s", condition)
			}
		}
	}
	if len(found) != len(family145EventDeliveryReadCuts) {
		t.Fatalf("event-level delivery consumer inventory=%d/%d", len(found), len(family145EventDeliveryReadCuts))
	}
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
