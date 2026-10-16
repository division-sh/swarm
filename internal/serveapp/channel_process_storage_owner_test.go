package serveapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
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

func TestChannelProcessDiagnosticKeepsEveryNativeOperationCoordinateBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			h := newChannelOnboardingE2EHarness(t, backend, false)
			h.start(t)
			defer h.stop(t)
			result := startChannelOnboardingRPC(t, h, channelonboarding.VerbConnect, "diagnostic-private-token", nil)
			if result.Operation.Phase != channelonboarding.PhaseFailed || result.Operation.FailureCode != "public_ingress_unavailable" {
				t.Fatalf("expected a genuine terminal no-ingress operation: %+v", result)
			}
			op := result.Operation
			coordinate := op.Coordinate
			want := storetest.ChannelOnboardingOperationDiagnosticStorage{
				Phase: string(op.Phase), FailureCode: op.FailureCode, FailureMessage: op.FailureMessage,
				BundleHash: coordinate.BundleHash, BundleIdentity: coordinate.BundleIdentity,
				Inventory: coordinate.PackInventoryGeneration, RuntimeInstance: coordinate.RuntimeInstanceID,
				Publication: int64(coordinate.ContextPublicationGeneration), Plan: coordinate.PlanGeneration.Diagnostic(),
				Target: op.TargetSelector, TargetGeneration: int64(coordinate.TargetGeneration),
			}
			reader := channelProcessStorageObservation(t, h)
			got, err := reader.ReadChannelOnboardingOperationDiagnosticStorage(context.Background(), op.OperationID)
			if err != nil || got != want {
				t.Fatalf("diagnostic changed native identity/failure/coordinate: got=%+v want=%+v err=%v", got, want, err)
			}
			text := fmt.Sprintf("phase=%s failure=%s:%s bundle=%s identity=%s inventory=%s runtime_instance=%s publication=%d plan=%s target=%s target_generation=%d",
				want.Phase, want.FailureCode, want.FailureMessage, want.BundleHash, want.BundleIdentity, want.Inventory,
				want.RuntimeInstance, want.Publication, want.Plan, want.Target, want.TargetGeneration)
			if got := channelOnboardingOperationDiagnostic(t, backend, h.storeDSN, op.OperationID); got != text {
				t.Fatalf("diagnostic formatting or fresh-inspector read changed: got=%q want=%q", got, text)
			}
			if _, err := reader.ReadChannelOnboardingOperationDiagnosticStorage(context.Background(), uuid.NewString()); err != sql.ErrNoRows {
				t.Fatalf("diagnostic lost exact operation scope: %v", err)
			}
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := reader.ReadChannelOnboardingOperationDiagnosticStorage(cancelled, op.OperationID); !errors.Is(err, context.Canceled) {
				t.Fatalf("diagnostic supplied coordinates after cancellation: %v", err)
			}
		})
	}
}

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
