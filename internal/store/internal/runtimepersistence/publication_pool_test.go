package runtimepersistence

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestPublicationPoolFixtureKeepsExactOriginalCapacityAndRefusesActiveReservations(t *testing.T) {
	selected, err := NewPostgresStore(testutil.StartPostgresDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = selected.Close() })
	if err := LimitPostgresPublicationFixturePoolForTest(context.Background(), selected); err != nil {
		t.Fatal(err)
	}
	assertCapacity := func(maximum, dedicated int) {
		t.Helper()
		got, held, err := selected.backend.ConnectionCapacity()
		if err != nil || got != maximum || held != dedicated {
			t.Fatalf("original capacity=%d/%d, err=%v; want %d/%d", got, held, err, maximum, dedicated)
		}
	}
	assertCapacity(4, 0)
	release := selected.backend.RetainConnectionCapacity()
	defer release()
	assertCapacity(5, 1)
	if err := LimitPostgresPublicationFixturePoolForTest(context.Background(), selected); err == nil {
		t.Fatal("pool fixture resized an active claim reservation")
	}
	assertCapacity(5, 1)
	release()
	assertCapacity(4, 0)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := LimitPostgresPublicationFixturePoolForTest(cancelled, selected); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled pool fault: %v", err)
	}
	assertCapacity(4, 0)
	if err := selected.Close(); err != nil {
		t.Fatal(err)
	}
	if err := LimitPostgresPublicationFixturePoolForTest(context.Background(), selected); err == nil {
		t.Fatal("closed pool accepted a fixture fault")
	}
	if err := LimitPostgresPublicationFixturePoolForTest(context.Background(), nil); err == nil {
		t.Fatal("missing original pool accepted a fixture fault")
	}
	if err := (&postgres.Backend{}).LimitPublicationFixturePool(context.Background()); err == nil {
		t.Fatal("uninitialized pool accepted a fixture fault")
	}
}
