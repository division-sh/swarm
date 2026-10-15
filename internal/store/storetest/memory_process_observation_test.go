package storetest

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
)

func TestMemoryProcessObserversKeepIndependentReadOnlyCutsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			location := ""
			if backend == "sqlite" {
				location = StartSQLiteRuntimeStore(t).Path()
			} else {
				location = testutil.StartPostgresDSN(t)
				StartPostgresRuntimeStoreWithReopen(t, location)
			}
			owner, err := OpenReleaseProcessReadOnlyInspection(backend, location)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			})
			probe := CollectTransactions(t, owner, TransactionProbeOptions{})
			assertMemoryProcessEmptyObservers(t, owner)
			if got := probe.Snapshot(); got.Active != 0 || got.Total.ReadCommits != 6 || got.Total.WriteCommits != 0 {
				t.Fatalf("six independent native cuts changed read authority: %+v", got)
			}
			assertMemoryProcessObserverLifetimeRefusal(t, owner)
		})
	}
}

func assertMemoryProcessEmptyObservers(t *testing.T, owner ReleaseProcessReadOnlyInspection) {
	t.Helper()
	for _, read := range memoryProcessObserverControls() {
		if err := owner.InspectSnapshot(context.Background(), func(ctx context.Context) error {
			zero, err := read(ctx, owner)
			if !zero {
				t.Error("empty native store supplied invented memory evidence")
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		for _, foreign := range []any{nil, struct{}{}} {
			if zero, err := read(context.Background(), foreign); !zero || err == nil {
				t.Fatalf("foreign %T supplied native memory evidence: zero=%t err=%v", foreign, zero, err)
			}
		}
	}
}

func assertMemoryProcessObserverLifetimeRefusal(t *testing.T, owner ReleaseProcessReadOnlyInspection) {
	t.Helper()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, read := range memoryProcessObserverControls() {
		err := owner.InspectSnapshot(cancelled, func(ctx context.Context) error {
			_, err := read(ctx, owner)
			return err
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled memory read lost refusal: %v", err)
		}
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	for _, read := range memoryProcessObserverControls() {
		if zero, err := read(context.Background(), owner); !zero || err == nil {
			t.Fatalf("closed native reader supplied memory evidence: zero=%t err=%v", zero, err)
		}
	}
}

func memoryProcessObserverControls() []func(context.Context, any) (bool, error) {
	return []func(context.Context, any) (bool, error){
		func(ctx context.Context, owner any) (bool, error) {
			rows, err := ReadMemoryEnabledSessionsStorage(ctx, owner)
			return len(rows) == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			count, err := CountSucceededTelegramModeAttemptsStorage(ctx, owner, "mock")
			return count == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			count, err := CountTelegramToolAttemptsStorage(ctx, owner)
			return count == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			count, err := CountUnfinishedMemoryAgentDeliveriesStorage(ctx, owner)
			return count == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			count, err := CountTelegramRequestedReplyStorage(ctx, owner)
			return count == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			count, err := CountEventNameStorage(ctx, owner, "inbound.telegram")
			return count == 0, err
		},
	}
}
