package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145CanonicalClaimSettlementCuts = map[string][]string{
	"requireA2CanonicalAccumulatorClaim": {
		"storage, err := storetest.ReadCanonicalAccumulatorClaimStorage(proof.ctx, proof.selected.events, publication.event.ID())",
		"deliveryID, status := storage.DeliveryID, storage.Status",
		"attempts, settled := storage.Attempts, storage.Settled",
		"return deliveryID",
	},
	"TestA2AccumulatorPersistedDuplicatedKeyEvidenceRefusesOnBothStores": {
		"before := a2CanonicalAccumulatorEvidence(t, proof)",
		"proof.execute(t, refused, failures.ClassSchemaInvalid)",
		"storage, err := storetest.ReadCanonicalAccumulatorRefusalStorage(proof.ctx, proof.selected.events, refused.event.ID())",
		"status, attempts, settled, open := storage.Status, storage.Attempts, storage.Settled, storage.Open",
		"requireRefusal()",
		"\n\t\t\twrite(original)\n\t\t\trestartA2CanonicalAccumulatorProof(t, proof)",
		"proof.execute(t, lawful, \"\")",
		"requireA2CanonicalAccumulatorClaim(t, proof, lawful)",
	},
	"TestReceiverCompositionActivationReuseAndConflictBothStores": {
		"pc.InterceptDeliveryRoute(ctx, delivery, prepared.DeliveryRoutes[0])",
		"after, found, err := pc.Load(ctx, identity)",
		"reloaded, found, err := selected.events.LoadPreparedPublishEvent(ctx, evt.ID())",
		"unrelated, found, err := pc.Load(ctx, unrelatedIdentity)",
		"storage, err := storetest.ReadReceiverAppearanceSettlementStorage(context.Background(), selected.events, evt.ID())",
		"status, outcomes := storage.Status, storage.Outcomes",
	},
}

func family145CanonicalClaimSettlementProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("claim/settlement conversion lost exact identity, refusal, restoration or receiver assertions: %s %v", function, err)
	}
	cursor := -1
	for _, cut := range family145CanonicalClaimSettlementCuts[function] {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor {
			return fmt.Errorf("claim/settlement conversion lost original observation cut or binding: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "event_delivery_attempts") || strings.Contains(after, "FROM event_deliveries") || strings.Contains(after, "QueryRow") {
		return fmt.Errorf("claim/settlement consumer retains its SQL interpreter: %s", function)
	}
	if function == "TestA2AccumulatorPersistedDuplicatedKeyEvidenceRefusesOnBothStores" && strings.Count(after, "requireRefusal()") != 2 {
		return fmt.Errorf("original refusal must be rechecked after lawful execution")
	}
	return nil
}

func TestFamily145CanonicalClaimSettlementChainRetainsAllAssertionsPredicatesAndCuts(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, row := range rows {
		if family145CanonicalClaimSettlementCuts[row.Function] == nil {
			continue
		}
		found[row.Function] = true
		if err := family145CanonicalClaimSettlementProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range family145CanonicalClaimSettlementCuts[row.Function] {
			if family145CanonicalClaimSettlementProofError(row.Function, row.Before, strings.Replace(row.After, cut, "foreignClaimCut()", 1)) == nil {
				t.Fatalf("lost original claim/settlement binding passed: %s", cut)
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.After)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if family145CanonicalClaimSettlementProofError(row.Function, row.Before, strings.Replace(row.After, condition, "false", 1)) == nil {
				t.Fatalf("lost original claim/settlement assertion passed: %s", condition)
			}
		}
	}
	if len(found) != len(family145CanonicalClaimSettlementCuts) {
		t.Fatalf("claim/settlement caller inventory=%d/%d", len(found), len(family145CanonicalClaimSettlementCuts))
	}
	for _, tc := range []struct{ function, caller, destination string }{
		{"ReadCanonicalAccumulatorClaimStorageTx", "requireA2CanonicalAccumulatorClaim", "eventID).Scan(&out.DeliveryID, &out.Status, &out.Attempts, &out.Settled)"},
		{"ReadCanonicalAccumulatorRefusalStorageTx", "TestA2AccumulatorPersistedDuplicatedKeyEvidenceRefusesOnBothStores", "eventID).Scan(&out.Status, &out.Attempts, &out.Settled, &out.Open)"},
		{"ReadReceiverAppearanceSettlementStorageTx", "TestReceiverCompositionActivationReuseAndConflictBothStores", "eventID).Scan(&out.Status, &out.Outcomes)"},
	} {
		owner := family145ConversationForkOwnerSource(t, "internal/backend/delivery/read_projections.go", tc.function)
		for _, row := range rows {
			if row.Function != tc.caller {
				continue
			}
			original := family145A2ClaimLiteralQueries(t, row.Before)
			queries := family145A2ClaimLiteralQueries(t, owner)
			var expected []string
			for _, query := range original {
				if strings.Contains(query, "FROM event_deliveries d") {
					expected = append(expected, query)
				}
			}
			if !reflect.DeepEqual(expected, queries) || len(queries) != 1 || strings.Count(owner, tc.destination) != 1 {
				t.Fatalf("%s changed original single SQL cut, predicate, strict scan or destination: %v/%v", tc.function, expected, queries)
			}
		}
	}
}
