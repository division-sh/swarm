package cataloge2e

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestCatalogReplyStorageObservationRequiresOriginalNativeOwnerBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			h := &runtimeHarness{t: t, ctx: ctx, backend: backend}
			var selected any
			var closeOwner func() error
			if backend == catalogBackendPostgres {
				h.pg = storetest.StartPostgresRuntimeStore(t)
				selected, closeOwner = h.pg, h.pg.Close
			} else {
				h.sqlite = storetest.StartSQLiteRuntimeStore(t)
				selected, closeOwner = h.sqlite, h.sqlite.Close
			}
			reader, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
			counts, err := storetest.ReadReplyContextStorageCounts(ctx, reader, catalogRuntimeRunID)
			if err != nil || counts != (storetest.ReplyContextStorageCounts{}) {
				t.Fatalf("empty reply witness=%+v err=%v", counts, err)
			}
			if proof := probe.Snapshot(); proof.Total.ReadCommits != 1 || proof.Total.WriteCommits != 0 || proof.Active != 0 {
				t.Fatalf("reply read escaped original coordinator: %+v", proof)
			}
			if row, err := storetest.ReadFirstReplyContextStorage(ctx, reader, catalogRuntimeRunID); err == nil || row != (storetest.FirstReplyContextStorage{}) {
				t.Fatalf("missing required reply row received evidence=%+v err=%v", row, err)
			}
			if ids, err := storetest.ReadReplyContextRequestIDs(ctx, reader, catalogRuntimeRunID); err != nil || len(ids) != 0 {
				t.Fatalf("empty reply ID witness=%v err=%v", ids, err)
			}
			if rows, err := storetest.ReadEarliestEventPipelineReceiptRows(ctx, reader); err != nil || len(rows) != 0 {
				t.Fatalf("empty reply diagnostic=%+v err=%v", rows, err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if counts, err := storetest.ReadReplyContextStorageCounts(canceled, reader, catalogRuntimeRunID); !errors.Is(err, context.Canceled) || counts != (storetest.ReplyContextStorageCounts{}) {
				t.Fatalf("canceled reply count=%+v err=%v", counts, err)
			}
			if ids, err := storetest.ReadReplyContextRequestIDs(canceled, reader, catalogRuntimeRunID); !errors.Is(err, context.Canceled) || ids != nil {
				t.Fatalf("canceled reply IDs=%v err=%v", ids, err)
			}
			if err := closeOwner(); err != nil {
				t.Fatal(err)
			}
			if counts, err := storetest.ReadReplyContextStorageCounts(ctx, reader, catalogRuntimeRunID); err == nil || counts != (storetest.ReplyContextStorageCounts{}) {
				t.Fatalf("closed reply count=%+v err=%v", counts, err)
			}
			if rows, err := storetest.ReadEarliestEventPipelineReceiptRows(ctx, reader); err == nil || rows != nil {
				t.Fatalf("closed reply diagnostic=%+v err=%v", rows, err)
			}
		})
	}
	if counts, err := storetest.ReadReplyContextStorageCounts(context.Background(), struct{}{}, catalogRuntimeRunID); err == nil || counts != (storetest.ReplyContextStorageCounts{}) {
		t.Fatalf("foreign reply owner returned counts=%+v err=%v", counts, err)
	}
}
