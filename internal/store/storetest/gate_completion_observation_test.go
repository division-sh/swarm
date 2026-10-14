package storetest

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestGateCompletionReadPortsKeepDistinctCutsAndRefuseForeignEvidenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner := openReadbackControlInspection(t, backend)
			probe := CollectTransactions(t, owner, TransactionProbeOptions{})
			cuts := gateCompletionReadbackControls(uuid.NewString())
			assertReadbackEmptyAndForeignCuts(t, owner, cuts)
			if got := probe.Snapshot(); got.Active != 0 || got.Total.ReadCommits != uint64(len(cuts)) || got.Total.WriteCommits != 0 {
				t.Fatalf("gate completion conflated its separate read cuts: %+v", got)
			}
			assertConnectionReadbackLifetimeRefusal(t, owner, cuts)
		})
	}
}

func gateCompletionReadbackControls(id string) []func(context.Context, any) (bool, error) {
	return []func(context.Context, any) (bool, error){
		func(ctx context.Context, selected any) (bool, error) {
			value, err := ReadGateCompletionPrincipalStorage(ctx, selected)
			return value == "", err
		},
		func(ctx context.Context, selected any) (bool, error) {
			value, err := ReadGateCompletionCardStorage(ctx, selected, id)
			return value == (GateCompletionCardStorage{}), err
		},
		func(ctx context.Context, selected any) (bool, error) {
			value, err := ReadGateCompletionFlowStorage(ctx, selected, id)
			return value == (GateCompletionFlowStorage{}), err
		},
		func(ctx context.Context, selected any) (bool, error) {
			rows, err := ReadGateCompletionChangesStorage(ctx, selected, id)
			return len(rows) == 0, err
		},
		func(ctx context.Context, selected any) (bool, error) {
			rows, err := ReadGateCompletionReceiptsStorage(ctx, selected, id)
			return len(rows) == 0, err
		},
		func(ctx context.Context, selected any) (bool, error) {
			rows, err := ReadGateCompletionAPIStorage(ctx, selected, id, "mailbox.decide", "exact-key")
			return len(rows) == 0, err
		},
	}
}

func TestHumanTaskToolReadPortsRefuseForeignAndUnavailableOwnersBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner := openReadbackControlInspection(t, backend)
			id := uuid.NewString()
			cuts := []func(context.Context, any) (bool, error){
				func(ctx context.Context, selected any) (bool, error) {
					count, err := CountHumanTaskToolCardStorage(ctx, selected, id)
					return count == 0, err
				},
				func(ctx context.Context, selected any) (bool, error) {
					count, err := CountHumanTaskToolContinuationStorage(ctx, selected, id)
					return count == 0, err
				},
				func(ctx context.Context, selected any) (bool, error) {
					count, err := CountAllHumanTaskToolCardsStorage(ctx, selected)
					return count == 0, err
				},
			}
			assertReadbackEmptyAndForeignCuts(t, owner, cuts)
			assertConnectionReadbackLifetimeRefusal(t, owner, cuts)
		})
	}
}
