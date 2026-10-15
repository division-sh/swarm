package serveapp

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func channelProcessStorageObservation(t *testing.T, h *channelOnboardingE2EHarness) storetest.ChannelProcessObservation {
	t.Helper()
	channelAnchorStorageObservation(t, h)
	reader, err := storetest.ObserveChannelProcessStorage(h.anchorObservation)
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

type foreignChannelProcessInspector struct{ storetest.ChannelObservation }

func TestChannelProcessObservationRefusesForeignOwnership(t *testing.T) {
	for _, source := range []storetest.ChannelObservation{nil, foreignChannelProcessInspector{}} {
		if reader, err := storetest.ObserveChannelProcessStorage(source); reader != nil || err == nil {
			t.Fatalf("foreign process inspector supplied storage facts: %v %v", reader, err)
		}
	}
}

func TestChannelProcessObservationPreservesAbsenceCancellationAndLifetimeBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			var reader storetest.ChannelProcessObservation
			t.Run("harness", func(t *testing.T) {
				h := newChannelOnboardingE2EHarness(t, backend, true)
				h.start(t)
				defer h.stop(t)
				reader = channelProcessStorageObservation(t, h)
				missing := uuid.NewString()
				assertChannelProcessObservationAbsenceAndCancellation(t, reader, missing)
			})
			if _, err := reader.ReadChannelNativeProcessAttemptStorage(context.Background()); err == nil || errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("borrowed process reader outlived its original inspector: %v", err)
			}
		})
	}
}

func assertChannelProcessObservationAbsenceAndCancellation(t *testing.T, reader storetest.ChannelProcessObservation, missing string) {
	t.Helper()
	for name, read := range map[string]func(context.Context) error{
		"setting": func(ctx context.Context) error {
			_, err := reader.ReadChannelNativeProcessSettingStorage(ctx, missing)
			return err
		},
		"native-attempt": func(ctx context.Context) error {
			_, err := reader.ReadChannelNativeProcessAttemptStorage(ctx)
			return err
		},
		"ack-attempt": func(ctx context.Context) error {
			_, err := reader.ReadChannelAcknowledgmentAttemptStorage(ctx)
			return err
		},
		"delivery-attempt": func(ctx context.Context) error {
			_, err := reader.ReadChannelDeliveryProcessAttemptStorage(ctx, missing)
			return err
		},
		"action-card": func(ctx context.Context) error {
			_, err := reader.ReadChannelProcessActionCardStorage(ctx, missing)
			return err
		},
		"receipt": func(ctx context.Context) error {
			_, err := reader.ReadChannelProcessRecoveredReceiptStorage(ctx, missing, missing)
			return err
		},
		"retained-receipt": func(ctx context.Context) error {
			_, err := reader.ReadChannelProcessRetainedReceiptStorage(ctx, missing, missing)
			return err
		},
		"standing": func(ctx context.Context) error {
			_, err := reader.ReadChannelCurrentStandingRunStorage(ctx)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := read(context.Background()); err != sql.ErrNoRows {
				t.Fatalf("missing physical row lost exact absence: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := read(ctx); !errors.Is(err, context.Canceled) || errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("cancellation was hidden as absence: %v", err)
			}
		})
	}
}
