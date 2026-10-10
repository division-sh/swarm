package cataloge2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestCatalogCreationStoragePreservesTimePathAndPhysicalScopeBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			foreignRun := uuid.NewString()
			h := newRuntimeHarnessForBackend(t, canonicalrouting.CopyReceiverOptionalChild(t, false), backend, true, foreignRun)
			boundary := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			h.startedAt = boundary
			h.seedInitialState(catalogRuntimeRunID)
			ctx := worklifetime.WithOccurrence(catalogRunContext(h, foreignRun), h.rt.WorkOccurrence())
			ctx = runtimeeffects.WithExecutionMode(ctx, executionmode.Live)
			if err := h.rt.Manager.ActivateFlowInstance(ctx, runtimepipeline.FlowInstanceActivationRequest{
				ContractBundle: semanticview.Wrap(h.bundle),
				Instance:       flowidentity.Stored(semanticview.Wrap(h.bundle), ".", foreignRun, foreignRun, foreignRun, ""),
				OccurredAt:     boundary.Add(time.Second),
			}); err != nil {
				t.Fatal(err)
			}
			h.shutdown()
			ctx = testAuthorActivityContext(context.Background())
			reader, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			var selected any = h.pg
			closeOwner := h.pg.Close
			if h.sqlite != nil {
				selected, closeOwner = h.sqlite, h.sqlite.Close
			}
			probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
			if count, err := storetest.CountWorkflowHeadersCreatedSince(ctx, reader, boundary); err != nil || count != 4 {
				t.Fatalf("physical all-run/inclusive count=%d want=4 err=%v", count, err)
			}
			if proof := probe.Snapshot(); proof.Total.ReadCommits != 1 || proof.Total.WriteCommits != 0 || proof.Active != 0 {
				t.Fatalf("creation read escaped native coordinator: %+v", proof)
			}
			if count, err := storetest.CountWorkflowHeadersCreatedSince(ctx, reader, boundary.Add(time.Microsecond)); err != nil || count != 2 {
				t.Fatalf("later cut count=%d want=2 err=%v", count, err)
			}
			for _, test := range []struct {
				path  string
				since time.Time
				count int
			}{
				{catalogRuntimeRunID, boundary, 1}, {catalogRuntimeRunID, boundary.Add(time.Microsecond), 0},
				{"sink", boundary, 2}, {"missing", boundary, 0},
			} {
				if count, err := storetest.CountWorkflowHeadersForPathCreatedSince(ctx, reader, test.path, test.since); err != nil || count != test.count {
					t.Fatalf("path=%s count=%d want=%d err=%v", test.path, count, test.count, err)
				}
			}
			fields, found, err := storetest.ReadLatestWorkflowFieldsForPath(ctx, reader, catalogRuntimeRunID)
			if err != nil || !found || string(fields) != "{}" {
				t.Fatalf("constructed root field projection=%s found=%t err=%v", fields, found, err)
			}
			if fields, found, err := storetest.ReadLatestWorkflowFieldsForPath(ctx, reader, "missing"); err != nil || found || fields != nil {
				t.Fatalf("missing path received fields=%s found=%t err=%v", fields, found, err)
			}
			assertFlowInstanceCreated(t, reader, boundary, map[string]any{})
			assertFlowInstanceCount(t, reader, boundary, 4)
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if count, err := storetest.CountWorkflowHeadersCreatedSince(canceled, reader, boundary); !errors.Is(err, context.Canceled) || count != 0 {
				t.Fatalf("canceled creation count=%d err=%v", count, err)
			}
			if fields, found, err := storetest.ReadLatestWorkflowFieldsForPath(canceled, reader, catalogRuntimeRunID); !errors.Is(err, context.Canceled) || found || fields != nil {
				t.Fatalf("canceled creation fields=%s found=%t err=%v", fields, found, err)
			}
			if err := closeOwner(); err != nil {
				t.Fatal(err)
			}
			if count, err := storetest.CountWorkflowHeadersCreatedSince(ctx, reader, boundary); err == nil || count != 0 {
				t.Fatalf("closed creation count=%d err=%v", count, err)
			}
		})
	}
	if fields, found, err := storetest.ReadLatestWorkflowFieldsForPath(context.Background(), struct{}{}, "root"); err == nil || found || fields != nil {
		t.Fatalf("foreign creation reader received fields=%s found=%t err=%v", fields, found, err)
	}
}
