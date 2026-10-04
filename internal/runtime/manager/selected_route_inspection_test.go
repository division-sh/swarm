package manager

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/failures"
)

type selectedRouteInspectionReaderFunc func(context.Context) ([]SelectedContractRouteRecoveryRecord, error)

func (f selectedRouteInspectionReaderFunc) ListSelectedContractRouteRecoveryRecords(ctx context.Context) ([]SelectedContractRouteRecoveryRecord, error) {
	return f(ctx)
}

func TestSelectedRouteInspectionConsumesCanonicalEvidenceWithoutManager(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*SelectedContractRouteRecoveryRecord)
	}{
		{name: "valid"},
		{name: "wrong_owner", mutate: func(r *SelectedContractRouteRecoveryRecord) { r.Owner = "current_routes" }},
		{name: "wrong_fingerprint", mutate: func(r *SelectedContractRouteRecoveryRecord) { r.RouteTopologyFingerprint = "changed" }},
		{name: "missing_recipient_plan", mutate: func(r *SelectedContractRouteRecoveryRecord) {
			mutateSelectedContractRecoveryRecipient(t, r, "recipient_plan_events", func(e map[string]any) { delete(e, "agent_plan") })
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := selectedContractRouteRecoveryRecord(t, "00000000-0000-0000-0000-000000000609")
			if tc.mutate != nil {
				tc.mutate(&record)
			}
			calls := 0
			reader := selectedRouteInspectionReaderFunc(func(context.Context) ([]SelectedContractRouteRecoveryRecord, error) {
				calls++
				return []SelectedContractRouteRecoveryRecord{record}, nil
			})
			got, err := InspectSelectedContractRouteRecoveries(context.Background(), reader)
			if (err != nil) != (tc.mutate != nil) || calls != 1 {
				t.Fatalf("evidence=%+v err=%v calls=%d", got, err, calls)
			}
			if tc.mutate == nil {
				want, err := decodeSelectedContractRouteRecoveryTruth(record)
				if err != nil || len(got) != 1 || !reflect.DeepEqual(got[record.ForkRunID], want) {
					t.Fatalf("shared decoder evidence differs: got=%+v want=%+v err=%v", got, want, err)
				}
			} else if got != nil {
				t.Fatal("invalid evidence escaped as a partial accepted inventory")
			}
		})
	}
}

func TestSelectedRouteInspectionRequiredReadsAndCancellation(t *testing.T) {
	cause := errors.New("required selected route read failed")
	for _, reader := range []SelectedContractRouteRecoveryReader{nil, selectedRouteInspectionReaderFunc(func(context.Context) ([]SelectedContractRouteRecoveryRecord, error) {
		return nil, cause
	})} {
		got, err := InspectSelectedContractRouteRecoveries(context.Background(), reader)
		failure, ok := failures.As(err)
		if got != nil || !ok || failure.Failure.Class != failures.ClassDependencyUnavailable {
			t.Fatalf("missing/failed read became inspected empty inventory: got=%+v err=%v", got, err)
		}
		if reader != nil && !errors.Is(err, cause) {
			t.Fatalf("required read cause lost: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := InspectSelectedContractRouteRecoveries(ctx, selectedRouteInspectionReaderFunc(func(context.Context) ([]SelectedContractRouteRecoveryRecord, error) {
		t.Fatal("canceled inspection reached the ledger")
		return nil, nil
	})); !errors.Is(err, context.Canceled) {
		t.Fatalf("entry cancellation lost: %v", err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	if _, err := InspectSelectedContractRouteRecoveries(ctx, selectedRouteInspectionReaderFunc(func(context.Context) ([]SelectedContractRouteRecoveryRecord, error) {
		cancel()
		return nil, nil
	})); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation during a successful-looking read became empty success: %v", err)
	}
}
