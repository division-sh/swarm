package runtimepersistence

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture"
)

func TestR7DirectoryReadsOwnSnapshotAndFreshCommittedHeadersBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, root := newR7KeylessIndexProof(t, backend)
			source := semanticview.Wrap(f.bundle)
			fact, _ := correlation.SourceArtifactFactFromContext(f.ctx)
			owner := flowidentity.RunScopedFlowInstance{RunID: root.Readiness.RunID, Route: root.Identity.Route()}
			request, err := pipeline.NewExactFlowInstanceLookup(source, fact, owner)
			if err != nil {
				t.Fatal(err)
			}
			index := f.store.(pipeline.FlowInstanceIndexReader)
			first, found, err := index.LookupFlowInstance(f.ctx, request)
			if err != nil || !found || first.HeaderRevision() != 1 {
				t.Fatalf("initial native observation: %+v found=%t err=%v", first, found, err)
			}
			tx, err := f.db.BeginTx(f.ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			if _, err := tx.ExecContext(f.ctx, `UPDATE flow_instances SET revision=revision+10 WHERE run_id=$1 AND instance_path=$2`, owner.RunID, owner.Route.InstancePath); err != nil {
				t.Fatal(err)
			}
			borrowed := runtimepipelinefixture.WithSQLTx(f.ctx, tx)
			if observed, found, err := index.LookupFlowInstance(borrowed, request); err != nil || !found || observed.HeaderRevision() != first.HeaderRevision() {
				t.Fatalf("index borrowed an ambient uncommitted transaction: %+v found=%t err=%v", observed, found, err)
			}
			cancelled, cancel := context.WithCancel(borrowed)
			cancel()
			if observed, found, err := index.LookupFlowInstance(cancelled, request); !errors.Is(err, context.Canceled) || found || observed.Valid() {
				t.Fatalf("ambient transaction bypassed cancellation: %+v found=%t err=%v", observed, found, err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if observed, found, err := index.LookupFlowInstance(f.ctx, request); err != nil || !found || observed.HeaderRevision() != 11 || first.HeaderRevision() != 1 {
				t.Fatalf("index reused a stale snapshot or mutated the old observation: %+v found=%t err=%v", observed, found, err)
			}
		})
	}
}
