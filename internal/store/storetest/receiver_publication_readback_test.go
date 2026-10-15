package storetest

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestConnectionReadbackPortsKeepDistinctOwnedCutsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner := openReadbackControlInspection(t, backend)
			probe := CollectTransactions(t, owner, TransactionProbeOptions{})
			cuts := connectionReadbackControls(uuid.NewString())
			assertReadbackEmptyAndForeignCuts(t, owner, cuts)
			if got := probe.Snapshot(); got.Active != 0 || got.Total.ReadCommits != uint64(len(cuts)) || got.Total.WriteCommits != 0 {
				t.Fatalf("connection readback changed four independent read cuts: %+v", got)
			}
			assertConnectionReadbackLifetimeRefusal(t, owner, cuts)
		})
	}
}

func assertConnectionReadbackLifetimeRefusal(t *testing.T, owner ReleaseProcessReadOnlyInspection, cuts []func(context.Context, any) (bool, error)) {
	t.Helper()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, cut := range cuts {
		err := owner.InspectSnapshot(cancelled, func(ctx context.Context) error {
			_, err := cut(ctx, owner)
			return err
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled connection read lost refusal: %v", err)
		}
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	for _, cut := range cuts {
		if zero, err := cut(context.Background(), owner); !zero || err == nil {
			t.Fatalf("closed owner supplied connection evidence: zero=%t err=%v", zero, err)
		}
	}
}

func connectionReadbackControls(id string) []func(context.Context, any) (bool, error) {
	return []func(context.Context, any) (bool, error){
		func(ctx context.Context, owner any) (bool, error) {
			rows, err := ReadConnectionWorkerInstancePathsStorage(ctx, owner, id)
			return len(rows) == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			rows, err := ReadOrderedConnectionReadyEventIDsStorage(ctx, owner, id)
			return len(rows) == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			rows, err := ReadNestedRunNamedEventIDs(ctx, owner, id, "work.ready")
			return len(rows) == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			raw, err := ReadDeliveryPayloadProjectionTextStorage(ctx, owner, id)
			return raw == "", err
		},
	}
}

func TestPublicationAndNumericReadbackPortsRefuseForeignEvidenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner := openReadbackControlInspection(t, backend)
			probe := CollectTransactions(t, owner, TransactionProbeOptions{})
			id := uuid.NewString()
			cuts := append(publicationReadbackControls(id), receiverMutationReadbackControls(id)...)
			cuts = append(cuts, receiverConstructionReadbackControls(id)...)
			assertReadbackEmptyAndForeignCuts(t, owner, cuts)
			if got := probe.Snapshot(); got.Active != 0 || got.Total.ReadCommits != uint64(len(cuts)) || got.Total.WriteCommits != 0 {
				t.Fatalf("publication read cuts changed: %+v", got)
			}
			assertConnectionReadbackLifetimeRefusal(t, owner, cuts)
		})
	}
}

func openReadbackControlInspection(t *testing.T, backend string) ReleaseProcessReadOnlyInspection {
	t.Helper()
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
	return owner
}

func assertReadbackEmptyAndForeignCuts(t *testing.T, owner ReleaseProcessReadOnlyInspection, cuts []func(context.Context, any) (bool, error)) {
	t.Helper()
	for _, cut := range cuts {
		if err := owner.InspectSnapshot(context.Background(), func(ctx context.Context) error {
			zero, err := cut(ctx, owner)
			if !zero {
				t.Error("empty store supplied readback evidence")
			}
			if err == sql.ErrNoRows {
				return nil
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		for _, foreign := range []any{nil, struct{}{}} {
			if zero, err := cut(context.Background(), foreign); !zero || err == nil {
				t.Fatalf("foreign %T supplied readback evidence: zero=%t err=%v", foreign, zero, err)
			}
		}
	}
}

func receiverConstructionReadbackControls(id string) []func(context.Context, any) (bool, error) {
	return []func(context.Context, any) (bool, error){
		func(ctx context.Context, owner any) (bool, error) {
			row, err := ReadActivityPublicationSourceStorage(ctx, owner, id, id)
			return row == (ActivityPublicationSourceStorage{}), err
		},
		func(ctx context.Context, owner any) (bool, error) {
			count, err := CountActivityPublicationResultArmsStorage(ctx, owner, id)
			return count == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			path, err := ReadActivityPublicationTemplatePathStorage(ctx, owner, id, "template")
			return path == "", err
		},
		func(ctx context.Context, owner any) (bool, error) {
			entity, err := ReadActivityPublicationTemplateEntityStorage(ctx, owner, id, "instance", "template")
			return entity == "", err
		},
		func(ctx context.Context, owner any) (bool, error) {
			row, err := ReadLoopObservedRevisionMutationStorage(ctx, owner, id, id, id)
			return row == (ReceiverFieldMutationStorage{}), err
		},
		func(ctx context.Context, owner any) (bool, error) {
			count, err := CountLoopObservedRevisionMutationsStorage(ctx, owner, id, id, id)
			return count == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			row, err := ReadLoopReceiverSettlementStorage(ctx, owner, id, id)
			return row == (LoopReceiverSettlementStorage{}), err
		},
		func(ctx context.Context, owner any) (bool, error) {
			count, err := CountReceiverFirstClaimAgentDeliveriesStorage(ctx, owner, id)
			return count == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			row, err := ReadReceiverAgentIdentityStorage(ctx, owner, id, "instance")
			return row == (ReceiverAgentIdentityStorage{}), err
		},
		func(ctx context.Context, owner any) (bool, error) {
			count, err := CountReceiverLiveParsedTurnsStorage(ctx, owner, id)
			return count == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			count, err := CountReceiverRunTurnsStorage(ctx, owner, id)
			return count == 0, err
		},
	}
}

func publicationReadbackControls(id string) []func(context.Context, any) (bool, error) {
	return []func(context.Context, any) (bool, error){
		func(ctx context.Context, owner any) (bool, error) {
			rows, err := ReadPublicationResultSourcesStorage(ctx, owner, id)
			return len(rows) == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			count, err := CountOpenPublicationFanOutIntentsStorage(ctx, owner, id)
			return count == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			count, err := CountNoncommittedPublicationFanOutOutcomesStorage(ctx, owner, id)
			return count == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			raw, err := ReadPublicationFanOutFailureStorage(ctx, owner, id)
			return raw == "", err
		},
		func(ctx context.Context, owner any) (bool, error) {
			row, err := ReadPublicationTemplateIdentityStorage(ctx, owner, id, id)
			return row == (PublicationTemplateIdentityStorage{}), err
		},
		func(ctx context.Context, owner any) (bool, error) {
			row, err := ReadPublicationRuleSelectionStorage(ctx, owner, id, id, "missing")
			return row == (PublicationRuleSelectionStorage{}), err
		},
		func(ctx context.Context, owner any) (bool, error) {
			raw, err := ReadNumericCompletionPayloadStorage(ctx, owner, id)
			return raw == "", err
		},
		func(ctx context.Context, owner any) (bool, error) {
			count, err := CountNumericRunNamedEventStorage(ctx, owner, id, "numeric.completed")
			return count == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			row, err := ReadNumericRunEntityStorage(ctx, owner, id)
			return row == (NumericRunEntityStorage{}), err
		},
		func(ctx context.Context, owner any) (bool, error) {
			raw, err := ReadNumericRunEntityIdentityStorage(ctx, owner, id)
			return raw == "", err
		},
		func(ctx context.Context, owner any) (bool, error) {
			raw, err := ReadNumericMockActivityResultStorage(ctx, owner, id)
			return raw == "", err
		},
	}
}

func receiverMutationReadbackControls(id string) []func(context.Context, any) (bool, error) {
	return []func(context.Context, any) (bool, error){
		func(ctx context.Context, owner any) (bool, error) {
			row, err := ReadForkReceiverSettlementStorage(ctx, owner, id, id)
			return row == (ForkReceiverSettlementStorage{}), err
		},
		func(ctx context.Context, owner any) (bool, error) {
			row, err := ReadForkReceiverBusinessSettlementStorage(ctx, owner, id, id)
			return row == (ForkReceiverSettlementStorage{}), err
		},
		func(ctx context.Context, owner any) (bool, error) {
			count, err := CountForkReceiverSettledAttemptsStorage(ctx, owner, id)
			return count == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			row, err := ReadForkReceiverRuleSelectionStorage(ctx, owner, id)
			return row == (PublicationRuleSelectionStorage{}), err
		},
		func(ctx context.Context, owner any) (bool, error) {
			row, err := ReadReceiverBusinessTokenMutationStorage(ctx, owner, id, id, id)
			return row == (ReceiverFieldMutationStorage{}), err
		},
		func(ctx context.Context, owner any) (bool, error) {
			row, err := ReadReceiverProcessedCountMutationStorage(ctx, owner, id, id, id)
			return row == (ReceiverFieldMutationStorage{}), err
		},
		func(ctx context.Context, owner any) (bool, error) {
			rows, err := ReadReceiverMarkerMutationStorage(ctx, owner, id, id, id)
			return len(rows) == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			count, err := CountReceiverBusinessTokenMutationsStorage(ctx, owner, id, id)
			return count == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			count, err := CountReceiverMarkerMutationsStorage(ctx, owner, id, id)
			return count == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			count, err := CountReceiverProcessedCountMutationsStorage(ctx, owner, id, id, id)
			return count == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			count, err := CountReceiverEntityCausedMutationsStorage(ctx, owner, id, id, id)
			return count == 0, err
		},
		func(ctx context.Context, owner any) (bool, error) {
			raw, err := ReadReceiverParentEntityIdentityStorage(ctx, owner, id, "missing")
			return raw == "", err
		},
		func(ctx context.Context, owner any) (bool, error) {
			row, err := ReadReceiverFinishedEffectStorage(ctx, owner, id, "missing", id)
			return row == (ReceiverFinishedEffectStorage{}), err
		},
	}
}
