package serveapp

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func channelAnchorStorageObservation(t *testing.T, h *channelOnboardingE2EHarness) storetest.ChannelAnchorObservation {
	t.Helper()
	if h.anchorObservation == nil {
		driver := "sqlite"
		if h.backend == servedparity.BackendExplicitPostgres {
			driver = "postgres"
		}
		reader, err := storetest.OpenChannelObservation(driver, h.storeDSN)
		if err != nil {
			t.Fatal(err)
		}
		h.anchorObservation = reader
	}
	return h.anchorObservation
}

func TestChannelAnchorStorageReaderSurvivesChildAndClosesWithHarnessBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			var reader storetest.ChannelAnchorObservation
			missing := uuid.NewString()
			t.Run("harness", func(t *testing.T) {
				h, _ := startChannelAnchorPublicJourney(t, backend, "storage-lifetime-token", false, 0, nil)
				t.Run("first-child", func(t *testing.T) {
					reader = channelAnchorStorageObservation(t, h)
					if raw, err := reader.ReadFirstSentSourceReceiptStorage(context.Background(), missing); raw != "" || err != sql.ErrNoRows {
						t.Fatalf("historical polling lost its exact absence sentinel: raw=%q err=%v", raw, err)
					}
				})
				t.Run("sibling", func(t *testing.T) {
					assertChannelAnchorSiblingObservation(t, h, reader, missing)
				})
			})
			if reader == nil {
				t.Fatal("reader was never reached")
			}
			if rows, err := reader.ReadChannelAnchorCardsStorage(context.Background(), missing, "human_task", ""); err == nil || len(rows) != 0 {
				t.Fatalf("parent cleanup left reader live: rows=%+v err=%v", rows, err)
			}
		})
	}
}

func assertChannelAnchorSiblingObservation(t *testing.T, h *channelOnboardingE2EHarness, reader storetest.ChannelAnchorObservation, missing string) {
	t.Helper()
	if next := channelAnchorStorageObservation(t, h); !reflect.DeepEqual(next, reader) {
		t.Fatal("child completion replaced the parent-owned reader")
	}
	if rows, err := reader.ReadChannelAnchorCardsStorage(context.Background(), missing, "human_task", ""); err != nil || len(rows) != 0 {
		t.Fatalf("sibling lost live reader: rows=%+v err=%v", rows, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if raw, err := reader.ReadFirstSentSourceReceiptStorage(ctx, missing); raw != "" || !errors.Is(err, context.Canceled) || err == sql.ErrNoRows {
		t.Fatalf("cancellation was reinterpreted as polling absence: raw=%q err=%v", raw, err)
	}
}

func openChannelAnchorStorageAtLocation(t testing.TB, backend, location string) storetest.ChannelObservation {
	t.Helper()
	reader, err := storetest.OpenChannelObservation(backend, location)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	})
	return reader
}
