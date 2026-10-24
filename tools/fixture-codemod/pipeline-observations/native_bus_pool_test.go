package main

import (
	"strings"
	"testing"
)

func normalizedNativePoolCleanup(source string) string {
	for _, exact := range []string{
		"\t\t\tvar workers sync.WaitGroup\n\t\t\tdefer func() {\n\t\t\t\tcancel()\n\t\t\t\tworkers.Wait()\n\t\t\t}()\n",
		"\t\t\tworkerContext, cancelWorkers := context.WithCancel(context.Background())\n\t\t\tvar workers sync.WaitGroup\n\t\t\tvar releaseOnce sync.Once\n\t\t\tdefer func() {\n\t\t\t\tcancelWorkers()\n\t\t\t\treleaseOnce.Do(func() { close(release) })\n\t\t\t\tworkers.Wait()\n\t\t\t}()\n",
		"\t\t\t\tworkers.Add(1)\n",
		"\t\t\t\t\tdefer workers.Done()\n",
	} {
		source = strings.ReplaceAll(source, exact, "")
	}
	for _, exact := range []string{"\t\t\t\t\tselect {\n\t\t\t\t\tcase <-start:\n\t\t\t\t\tcase <-ctx.Done():\n\t\t\t\t\t\treturn\n\t\t\t\t\t}", "\t\t\t\t\tselect {\n\t\t\t\t\tcase <-start:\n\t\t\t\t\tcase <-workerContext.Done():\n\t\t\t\t\t\treturn\n\t\t\t\t\t}"} {
		source = strings.ReplaceAll(source, exact, "\t\t\t\t\t<-start")
	}
	source = strings.ReplaceAll(source, "context.WithTimeout(workerContext, 15*time.Second)", "context.WithTimeout(context.Background(), 15*time.Second)")
	return strings.ReplaceAll(source, "releaseOnce.Do(func() { close(release) })", "close(release)")
}

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
