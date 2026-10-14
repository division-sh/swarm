package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func TestOrdinaryInstancePlanningOwnsOneFiniteReadCutBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, root := newR7KeylessIndexProof(t, backend)
			source := semanticview.Wrap(f.bundle)
			fact, _ := correlation.SourceArtifactFactFromContext(f.ctx)
			scope, err := pipeline.NewFlowInstanceLookupScope(source, fact, root.Readiness.RunID, []string{"detail", "sibling"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			request, err := pipeline.NewExactFlowInstanceLookup(source, fact, flowidentity.RunScopedFlowInstance{RunID: root.Readiness.RunID, Route: root.Identity.Route()})
			if err != nil {
				t.Fatal(err)
			}
			// Serve injects this canonical wrapper, not the selected facade itself.
			index := pipeline.FlowInstanceIndexReader(pipeline.NewWorkflowPersistence(f.store.(pipeline.WorkflowPersistenceOwner)))
			collector, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(restore)
			ctx := f.ctx
			readSix := func(ctx context.Context, reader pipeline.FlowInstanceIndexReader) error {
				for read := 0; read < 3; read++ {
					observed, found, err := reader.LookupFlowInstance(ctx, request)
					if err != nil || !found || observed.Identity() != root.Identity {
						t.Fatalf("root construction changed: found=%t err=%v", found, err)
					}
					members, err := reader.ListFlowInstances(ctx, scope)
					if err != nil || len(members) != 2 {
						t.Fatalf("constructed inventory changed: count=%d err=%v", len(members), err)
					}
				}
				return nil
			}
			for pass := 0; pass < 40; pass++ {
				if err := readSix(ctx, index); err != nil {
					t.Fatal(err)
				}
			}
			// The existing collector separately records retained process-authority
			// reads; these index owners use ordinary selected read transactions.
			if counts := collector.Snapshot(); counts.Total.ReadCommits != 240+counts.Retained.ReadCommits || counts.Total.WriteCommits != 0 || counts.Total.Failed != 0 || counts.Active != 0 {
				t.Fatalf("uncomposed forty six-read cuts lost their 240-read control: %+v", counts)
			}
			t.Logf("uncomposed: total_reads=%d retained_authority_reads=%d index_reads=240", collector.Snapshot().Total.ReadCommits, collector.Snapshot().Retained.ReadCommits)
			planning, ok := index.(pipeline.FlowInstancePlanningSnapshotOwner)
			if !ok {
				t.Fatal("ordinary planning has no native per-pass instance snapshot")
			}
			collector, err = collector.Replace(transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(collector.Stop)
			var retained pipeline.FlowInstanceIndexReader
			for pass := 0; pass < 40; pass++ {
				if err := planning.WithFlowInstancePlanningSnapshot(ctx, scope, func(ctx context.Context, reader pipeline.FlowInstanceIndexReader) error {
					retained = reader
					return readSix(ctx, reader)
				}); err != nil {
					t.Fatal(err)
				}
				if observed, found, err := retained.LookupFlowInstance(ctx, request); err == nil || found || observed.Valid() {
					t.Fatal("ordinary snapshot reader survived its planning pass")
				}
				if members, err := retained.ListFlowInstances(ctx, scope); err == nil || len(members) != 0 {
					t.Fatal("ordinary inventory reader survived its planning pass")
				}
			}
			counts := collector.Snapshot()
			if counts.Total.ReadCommits != 40+counts.Retained.ReadCommits || counts.Total.WriteCommits != 0 || counts.Total.Failed != 0 || counts.Active != 0 {
				t.Fatalf("forty six-read planning cuts did not own exactly forty read transactions: %+v", counts)
			}
			t.Logf("composed: total_reads=%d retained_authority_reads=%d index_reads=40", counts.Total.ReadCommits, counts.Retained.ReadCommits)
			// Leave only one connection available beyond any retained authority.
			capacity := f.db.Stats().MaxOpenConnections
			f.db.SetMaxOpenConns(f.db.Stats().InUse + 1)
			t.Cleanup(func() { f.db.SetMaxOpenConns(capacity) })
			bounded, release := context.WithTimeout(ctx, 5*time.Second)
			defer release()
			if err := planning.WithFlowInstancePlanningSnapshot(bounded, scope, readSix); err != nil {
				t.Fatalf("nested index calls require a second free connection: %v", err)
			}
			owner := flowidentity.RunScopedFlowInstance{RunID: request.RunID(), Route: root.Identity.Route()}
			restoreHeader, err := FaultFlowConstructorHeaderForTest(ctx, f.store, owner, pipelinepersistence.FlowConstructorHeaderFaultField("revision"), int64(11))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := restoreHeader(ctx); err != nil {
					t.Error(err)
				}
			})
			if err := planning.WithFlowInstancePlanningSnapshot(bounded, scope, func(ctx context.Context, reader pipeline.FlowInstanceIndexReader) error {
				observed, found, err := reader.LookupFlowInstance(ctx, request)
				if err != nil || !found || observed.HeaderRevision() != 11 {
					t.Fatalf("later planning pass reused the earlier header: revision=%d found=%t err=%v", observed.HeaderRevision(), found, err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, found, err := index.LookupFlowInstance(bounded, request); err != nil || !found {
				t.Fatalf("completed planning retained the only free connection: found=%t err=%v", found, err)
			}
			f.db.SetMaxOpenConns(capacity)
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			called := false
			if err := planning.WithFlowInstancePlanningSnapshot(cancelled, scope, func(context.Context, pipeline.FlowInstanceIndexReader) error {
				called = true
				return nil
			}); !errors.Is(err, context.Canceled) || called {
				t.Fatalf("cancelled planning admitted a reader: called=%t err=%v", called, err)
			}
		})
	}
}
