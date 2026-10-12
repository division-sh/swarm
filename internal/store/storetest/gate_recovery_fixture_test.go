package storetest

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestGateRecoveryPortsRefuseRawCancelledMissingAndClosedOwnersBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) RunFixtureStore
	}{
		{"sqlite", func(t *testing.T) RunFixtureStore { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) RunFixtureStore { return StartPostgresRuntimeStore(t) }},
	} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			id := uuid.NewString()
			reads := gateRecoveryOwnershipReads(id)
			for _, read := range reads {
				zero, err := read(context.Background(), selected)
				if !zero || (err != nil && !errors.Is(err, sql.ErrNoRows)) {
					t.Fatalf("empty physical observation changed: zero=%t err=%v", zero, err)
				}
			}
			actions := append(reads, gateRecoveryOwnershipFaults(id)...)
			assertGateRecoveryOwnershipFailures(t, selected, actions)
			// Missing obligations must refuse, never insert or report a live cut.
			for _, fault := range gateRecoveryOwnershipFaults(id)[1:] {
				if zero, err := fault(context.Background(), selected); !zero || err == nil {
					t.Fatalf("missing obligation supplied a successful fault: %t/%v", zero, err)
				}
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, action := range actions {
				if zero, err := action(context.Background(), selected); !zero || err == nil {
					t.Fatalf("closed owner supplied evidence/fault: %t/%v", zero, err)
				}
			}
		})
	}
}

func gateRecoveryOwnershipReads(id string) []func(context.Context, any) (bool, error) {
	return []func(context.Context, any) (bool, error){
		func(ctx context.Context, owner any) (bool, error) {
			n, err := CountGateRunProposedEffectContinuations(ctx, owner, id)
			return n == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			n, err := CountGateSourceNamedEvents(ctx, owner, "launch.approved", id)
			return n == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			n, err := CountGateEventSubscriberDeliveries(ctx, owner, id, "fixture-node")
			return n == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			n, err := ReadGateNodeDeliveryStatus(ctx, owner, id, "fixture-node")
			return n == "", err
		},
		func(ctx context.Context, owner any) (bool, error) {
			n, err := ReadGateLatestDeadLetterFailure(ctx, owner, id)
			return n == "", err
		},
		func(ctx context.Context, owner any) (bool, error) {
			n, err := ReadGateRunEventNames(ctx, owner, id)
			return n == nil, err
		},
	}
}

func gateRecoveryOwnershipFaults(id string) []func(context.Context, any) (bool, error) {
	at := time.Unix(1700000400, 0).UTC()
	return []func(context.Context, any) (bool, error){
		func(ctx context.Context, owner any) (bool, error) {
			err := InstallGateProposedEffectCreationFailure(ctx, owner)
			return true, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			err := SetGateDecisionRouteAttemptFault(ctx, owner, id, 2, at)
			return true, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			err := SetGateDecisionRouteDueFault(ctx, owner, id, at)
			return true, err
		},
	}
}

func assertGateRecoveryOwnershipFailures(t *testing.T, selected any, actions []func(context.Context, any) (bool, error)) {
	t.Helper()
	for _, action := range actions {
		for _, foreign := range []any{nil, &sql.DB{}, &sql.Tx{}, struct{}{}} {
			if zero, err := action(context.Background(), foreign); !zero || err == nil {
				t.Fatalf("raw/foreign %T supplied evidence/fault: %t/%v", foreign, zero, err)
			}
		}
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if zero, err := action(cancelled, selected); !zero || !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled owner supplied evidence/fault: %t/%v", zero, err)
		}
	}
}
