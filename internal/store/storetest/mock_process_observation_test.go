package storetest

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
)

func TestMockProcessObserversKeepIndependentReadOnlyCutsBothStores(t *testing.T) {
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
			assertMockProcessEmptyObservers(t, owner)
			if got := probe.Snapshot(); got.Active != 0 || got.Total.ReadCommits != uint64(len(mockProcessObserverControls())) || got.Total.WriteCommits != 0 {
				t.Fatalf("independent mock cuts changed read authority: %+v", got)
			}
			assertMockProcessObserverLifetimeRefusal(t, owner)
		})
	}
}

func assertMockProcessEmptyObservers(t *testing.T, owner ReleaseProcessReadOnlyInspection) {
	t.Helper()
	for _, read := range mockProcessObserverControls() {
		if err := owner.InspectSnapshot(context.Background(), func(ctx context.Context) error {
			zero, err := read(ctx, owner)
			if !zero {
				t.Error("empty native store supplied invented mock evidence")
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		for _, foreign := range []any{nil, struct{}{}} {
			if zero, err := read(context.Background(), foreign); !zero || err == nil {
				t.Fatalf("foreign %T supplied native mock evidence: zero=%t err=%v", foreign, zero, err)
			}
		}
	}
}

func assertMockProcessObserverLifetimeRefusal(t *testing.T, owner ReleaseProcessReadOnlyInspection) {
	t.Helper()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, read := range mockProcessObserverControls() {
		err := owner.InspectSnapshot(cancelled, func(ctx context.Context) error {
			_, err := read(ctx, owner)
			return err
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled mock read lost refusal: %v", err)
		}
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	for _, read := range mockProcessObserverControls() {
		if zero, err := read(context.Background(), owner); !zero || err == nil {
			t.Fatalf("closed native reader supplied mock evidence: zero=%t err=%v", zero, err)
		}
	}
}

func mockProcessObserverControls() []func(context.Context, any) (bool, error) {
	var reads []func(context.Context, any) (bool, error)
	for _, count := range []func(context.Context, any) (int, error){
		CountMockConversationTurnsStorage, CountMockPhraseTurnsStorage,
		CountUnfinishedMockAgentDeliveriesStorage, CountMockTelegramDecisionCardsStorage,
		CountMockPythonSettledAttemptsStorage, CountNonMockExternalAttemptsStorage,
		CountLiveTelegramAttemptsStorage, CountMockAuthorActivityStorage,
		CountMockTelegramReplyStorage,
		func(ctx context.Context, owner any) (int, error) {
			return CountExecutionModeSpendStorage(ctx, owner, "mock")
		},
		func(ctx context.Context, owner any) (int, error) {
			return CountExecutionModeSpendStorage(ctx, owner, "live")
		},
	} {
		reads = append(reads, func(ctx context.Context, owner any) (bool, error) {
			got, err := count(ctx, owner)
			return got == 0, err
		})
	}
	reads = append(reads, mockProcessRowObserverControls()...)
	return append(reads, servedControlObserverControls()...)
}

func emptyServedControlRead[T any](read func(context.Context, any) (T, error)) func(context.Context, any) (bool, error) {
	return func(ctx context.Context, owner any) (bool, error) {
		var zero T
		got, err := read(ctx, owner)
		if errors.Is(err, sql.ErrNoRows) {
			err = nil
		}
		return reflect.DeepEqual(got, zero), err
	}
}

func servedControlObserverControls() []func(context.Context, any) (bool, error) {
	const key = "absent-native-directive-key"
	const run = "00000000-0000-4000-8000-000000000145"
	const entity = "00000000-0000-4000-8000-000000000146"
	return []func(context.Context, any) (bool, error){
		emptyServedControlRead(func(ctx context.Context, owner any) (DirectiveIdentityStorage, error) {
			return ReadDirectiveIdentityStorage(ctx, owner, key)
		}),
		emptyServedControlRead(func(ctx context.Context, owner any) (int, error) {
			return CountKeyedDirectiveOperationsStorage(ctx, owner, key)
		}),
		emptyServedControlRead(func(ctx context.Context, owner any) (DirectiveFailureStorage, error) {
			return ReadDirectiveFailureStorage(ctx, owner, key)
		}),
		emptyServedControlRead(func(ctx context.Context, owner any) (DirectiveReplayStorage, error) {
			return ReadDirectiveReplayStorage(ctx, owner, key)
		}),
		emptyServedControlRead(func(ctx context.Context, owner any) ([]byte, error) {
			return ReadDirectivePipelineFailureStorage(ctx, owner, key)
		}),
		emptyServedControlRead(func(ctx context.Context, owner any) (int, error) {
			return CountSuccessfulDirectivePipelineReceiptsStorage(ctx, owner, run)
		}),
		emptyServedControlRead(func(ctx context.Context, owner any) (string, error) {
			return ReadDirectiveProjectionResourceStorage(ctx, owner, key)
		}),
		emptyServedControlRead(func(ctx context.Context, owner any) (string, error) {
			return ReadDirectivePayloadOperationIDStorage(ctx, owner, run)
		}),
		emptyServedControlRead(func(ctx context.Context, owner any) (ServedSetupRunStorage, error) {
			return ReadServedSetupRunStorage(ctx, owner, run)
		}),
		emptyServedControlRead(func(ctx context.Context, owner any) (ServedSetupEntityStorage, error) {
			return ReadServedSetupEntityStorage(ctx, owner, run, entity)
		}),
		emptyServedControlRead(func(ctx context.Context, owner any) (int, error) {
			return CountAllRunEntityMutationsStorage(ctx, owner, run, entity)
		}),
		emptyServedControlRead(func(ctx context.Context, owner any) ([]ServedSetupMutationAuthorityStorage, error) {
			return ReadServedSetupMutationAuthoritiesStorage(ctx, owner, run, entity)
		}),
		emptyServedControlRead(func(ctx context.Context, owner any) ([]ServedSetupConstructorMutationStorage, error) {
			return ReadServedSetupConstructorMutationsStorage(ctx, owner, run, entity)
		}),
	}
}

func mockProcessRowObserverControls() []func(context.Context, any) (bool, error) {
	return []func(context.Context, any) (bool, error){
		func(ctx context.Context, owner any) (bool, error) {
			rows, err := ReadMockConversationTurnDiagnosticsStorage(ctx, owner)
			return len(rows.Rows) == 0 && rows.ScanError == nil, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			rows, err := ReadMockAgentDeliveryDiagnosticsStorage(ctx, owner)
			return len(rows.Rows) == 0 && rows.ScanError == nil, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			rows, err := ReadMockDecisionCardsStorage(ctx, owner)
			return len(rows) == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			rows, err := ReadMockConnectorEventDiagnosticsStorage(ctx, owner)
			return len(rows) == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			rows, err := ReadMockConnectorAttemptDiagnosticsStorage(ctx, owner)
			return len(rows) == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			rows, err := ReadMockConnectorDeliveryDiagnosticsStorage(ctx, owner)
			return len(rows) == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			value, err := ReadLatestMockPhraseRequestStorage(ctx, owner)
			if errors.Is(err, sql.ErrNoRows) {
				err = nil
			}
			return value == "", err
		},
		func(ctx context.Context, owner any) (bool, error) {
			value, err := ReadMockEntityIdentityStorage(ctx, owner, "00000000-0000-4000-8000-000000000145")
			if errors.Is(err, sql.ErrNoRows) {
				err = nil
			}
			return value == "", err
		},
	}
}
