package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/google/uuid"
)

func TestHistoricalPlanningContextKeepsLifetimeAndDetachedEvidenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newOrdinaryForkPredecessorFixture(t, backend)
			source := semanticview.Wrap(f.bundle)
			scope, err := pipeline.NewFlowInstanceLookupScope(source, mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash()), f.owner.RunID, []string{".", "review"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			observed, err := f.store.(pipeline.FlowInstanceIndexReader).ListFlowInstances(f.ctx, scope)
			if err != nil {
				t.Fatal(err)
			}
			headers := make([]pipeline.WorkflowInstance, len(observed))
			for i, item := range observed {
				headers[i], err = item.WorkflowInstance()
				if err != nil {
					t.Fatal(err)
				}
			}
			run, err := ReadRuntimeLogRunSnapshotForTest(f.ctx, f.store, f.owner.RunID)
			if err != nil {
				t.Fatal(err)
			}
			var reader pipelinepersistence.HistoricalFlowInstanceReader
			switch native := f.store.(type) {
			case *SQLiteRuntimeStore:
				reader = native.runForkSQLiteOwner
			case *PostgresStore:
				reader = native.runForkPostgresOwner
			}
			before := forkReceiverFaultSnapshot(t, f.ctx, f.store)
			if err := readServedDeliveryObservation(f.ctx, f.store, func(ctx context.Context, tx *sql.Tx) error {
				original, err := reader.ReadHistoricalFlowInstanceConstructionBatchTx(ctx, tx, source, run, headers)
				if err != nil {
					return err
				}
				var escaped context.Context
				err = reader.WithHistoricalFlowInstancePlanningTx(ctx, tx, source, run, func(bounded context.Context) error {
					escaped = bounded
					got, err := reader.ReadHistoricalFlowInstanceConstructionBatchTx(bounded, tx, source, run, headers)
					if err != nil || !reflect.DeepEqual(got, original) {
						t.Fatalf("planning changed canonical evidence: %+v/%v", got, err)
					}
					got[0].Construction.InstanceKey = "caller mutation"
					again, err := reader.ReadHistoricalFlowInstanceConstructionBatchTx(bounded, tx, source, run, headers)
					if err != nil || !reflect.DeepEqual(again, original) {
						t.Fatalf("returned evidence corrupted reconstruction: %+v/%v", again, err)
					}
					wrong := run
					wrong.RunID = uuid.NewString()
					if got, err := reader.ReadHistoricalFlowInstanceConstructionBatchTx(bounded, tx, source, wrong, headers); err == nil || got != nil {
						t.Fatal("planning crossed its admitted run")
					}
					if got, err := reader.ReadHistoricalFlowInstanceConstructionBatchTx(bounded, &sql.Tx{}, source, run, headers); err == nil || got != nil {
						t.Fatal("planning crossed its admitted transaction")
					}
					var visited []string
					other := cancellingHistoricalDeclarationSource{Source: source, cancel: func() {}, visited: &visited}
					if got, err := reader.ReadHistoricalFlowInstanceConstructionBatchTx(bounded, tx, other, run, headers); err == nil || got != nil || len(visited) != 0 {
						t.Fatal("planning crossed its admitted source")
					}
					cancelled, cancel := context.WithCancel(bounded)
					cancel()
					if got, err := reader.ReadHistoricalFlowInstanceConstructionBatchTx(cancelled, tx, source, run, headers); !errors.Is(err, context.Canceled) || got != nil {
						t.Fatal("successful memo hid later cancellation")
					}
					return nil
				})
				if err != nil {
					return err
				}
				if got, err := reader.ReadHistoricalFlowInstanceConstructionBatchTx(escaped, tx, source, run, headers); err == nil || got != nil || !strings.Contains(err.Error(), "lifetime has ended") {
					t.Fatalf("read escaped planning callback: %+v/%v", got, err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if after := forkReceiverFaultSnapshot(t, f.ctx, f.store); !reflect.DeepEqual(before, after) {
				t.Fatal("planning controls changed durable facts")
			}
		})
	}
}
