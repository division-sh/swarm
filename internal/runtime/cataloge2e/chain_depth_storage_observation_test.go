package cataloge2e

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestCatalogChainDepthStoragePreservesExactThreeWayWitnessBothStores(t *testing.T) {
	fixture := catalogRuntimeFixture(t, "catalog.runtime.event_loop", "test-chain-depth-limit")
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			transcript := buildCatalogExecutionTranscript(t, fixture)
			h, _ := executeCatalogTranscript(t, fixture, backend, transcript)
			reader, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			ctx := testAuthorActivityContext(context.Background())
			entityID := h.expectedTriggerEntityID(transcript.expected)
			if entityID == "" {
				t.Fatal("real chain journey did not publish an entity")
			}
			h.db = nil
			assertChainDepthExceeded(t, reader, " "+entityID+" ", true)
			assertChainDepthExceeded(t, reader, uuid.NewString(), false)
			var selected any = h.sqlite
			if h.pg != nil {
				selected = h.pg
			}
			probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
			facts, err := storetest.ReadChainDepthDeadLetterStorage(ctx, reader, catalogRuntimeRunID, entityID)
			if err != nil || facts.Count != 1 || facts.Depth != 6 {
				t.Fatalf("chain relation=%+v err=%v", facts, err)
			}
			node := identitytest.RootNode(t, "node-6").Key()
			if count, err := storetest.CountChainDepthDiagnosticStorage(ctx, reader, catalogRuntimeRunID, entityID, node); err != nil || count != 1 {
				t.Fatalf("chain diagnostic=%d err=%v", count, err)
			}
			if count, err := storetest.CountRecordedDeadLetterStorage(ctx, reader, catalogRuntimeRunID); err != nil || count != 1 {
				t.Fatalf("chain activity=%d err=%v", count, err)
			}
			if proof := probe.Snapshot(); proof.Total.ReadCommits != 3 || proof.Total.WriteCommits != 0 || proof.Active != 0 {
				t.Fatalf("chain witnesses escaped exact coordinator: %+v", proof)
			}
			foreignRun := uuid.NewString()
			if facts, err := storetest.ReadChainDepthDeadLetterStorage(ctx, reader, foreignRun, entityID); err != nil || facts != (storetest.ChainDepthDeadLetterStorage{}) {
				t.Fatalf("foreign run relation=%+v err=%v", facts, err)
			}
			if count, err := storetest.CountChainDepthDiagnosticStorage(ctx, reader, catalogRuntimeRunID, entityID, "other-node"); err != nil || count != 0 {
				t.Fatalf("foreign handler diagnostic=%d err=%v", count, err)
			}
			if count, err := storetest.CountRecordedDeadLetterStorage(ctx, reader, foreignRun); err != nil || count != 0 {
				t.Fatalf("foreign run activity=%d err=%v", count, err)
			}
			requireChainDepthStorageRefusals(t, ctx, reader, entityID, node)
			if h.pg != nil {
				err = h.pg.Close()
			} else {
				err = h.sqlite.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			if facts, err := storetest.ReadChainDepthDeadLetterStorage(ctx, reader, catalogRuntimeRunID, entityID); err == nil || facts != (storetest.ChainDepthDeadLetterStorage{}) {
				t.Fatalf("closed relation=%+v err=%v", facts, err)
			}
			if count, err := storetest.CountRecordedDeadLetterStorage(ctx, reader, catalogRuntimeRunID); err == nil || count != 0 {
				t.Fatalf("closed activity=%d err=%v", count, err)
			}
		})
	}
}

func requireChainDepthStorageRefusals(t *testing.T, ctx context.Context, reader catalogOperatorEventLister, entityID, node string) {
	t.Helper()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if facts, err := storetest.ReadChainDepthDeadLetterStorage(canceled, reader, catalogRuntimeRunID, entityID); !errors.Is(err, context.Canceled) || facts != (storetest.ChainDepthDeadLetterStorage{}) {
		t.Fatalf("canceled relation=%+v err=%v", facts, err)
	}
	if count, err := storetest.CountChainDepthDiagnosticStorage(canceled, reader, catalogRuntimeRunID, entityID, node); !errors.Is(err, context.Canceled) || count != 0 {
		t.Fatalf("canceled diagnostic=%d err=%v", count, err)
	}
	if count, err := storetest.CountRecordedDeadLetterStorage(canceled, reader, catalogRuntimeRunID); !errors.Is(err, context.Canceled) || count != 0 {
		t.Fatalf("canceled activity=%d err=%v", count, err)
	}
	if _, err := storetest.ReadChainDepthDeadLetterStorage(ctx, struct{}{}, catalogRuntimeRunID, entityID); err == nil {
		t.Fatal("foreign relation owner accepted")
	}
	if _, err := storetest.CountChainDepthDiagnosticStorage(ctx, struct{}{}, catalogRuntimeRunID, entityID, node); err == nil {
		t.Fatal("foreign diagnostic owner accepted")
	}
	if _, err := storetest.CountRecordedDeadLetterStorage(ctx, struct{}{}, catalogRuntimeRunID); err == nil {
		t.Fatal("foreign activity owner accepted")
	}
}
