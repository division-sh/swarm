package main

import (
	"strings"
	"testing"
)

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
