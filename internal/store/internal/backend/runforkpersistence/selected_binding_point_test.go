package runforkpersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	artifactstore "github.com/division-sh/swarm/internal/store/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestSelectedBindingPersistsDisjointForkPointsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := selectedBindingRootDatabase(t, backend)
			ctx := context.Background()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			source, _ := sourceartifactfixture.Rooted(t, "point-target", sourceartifactfixture.Artifact())
			if backend == "postgres" {
				_, err = artifactstore.EnsurePostgresRootedSourceTx(ctx, tx, source, time.Now())
			} else {
				_, err = artifactstore.EnsureSQLiteRootedSourceTx(ctx, tx, source, time.Now())
			}
			if err != nil {
				t.Fatal(err)
			}
			eventID := uuid.NewString()
			for _, point := range []runfork.RunForkPoint{
				{Kind: runfork.RunForkPointEvent, Revision: 4, Input: eventID, EventID: eventID, EventName: "worker.ready", Timestamp: time.Now().UTC()},
				{Kind: runfork.RunForkPointDeploymentRevision, Revision: 7},
			} {
				req := runfork.RunForkSelectedContractBindingRequest{
					ForkRunID: uuid.NewString(), SourceRunID: uuid.NewString(), ForkPoint: point,
					ContractSelection:   runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
					TargetRootAdmission: source.Admission(),
				}
				insertSelectedBindingChildFixture(t, ctx, tx, req.ForkRunID, source)
				inserted, err := insertRunForkSelectedContractBinding(ctx, tx, req, time.Now().UTC())
				if err != nil {
					t.Fatal(err)
				}
				loaded, err := loadRunForkSelectedContractBinding(ctx, tx, inserted.ForkRunID)
				if err != nil {
					t.Fatal(err)
				}
				if inserted.ForkPoint != loaded.ForkPoint {
					t.Fatalf("inserted binding invents non-durable point fields: inserted=%+v loaded=%+v", inserted.ForkPoint, loaded.ForkPoint)
				}
				if loaded.ForkPoint.Kind != point.Kind || loaded.ForkPoint.Revision != point.Revision ||
					loaded.ForkPoint.EventID != point.EventID || loaded.ForkEventID != point.EventID {
					t.Fatalf("point changed across selected binding: %+v want %+v", loaded, point)
				}
				if _, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_bindings SET fork_point_kind='deployment_revision' WHERE fork_run_id=$1`, inserted.ForkRunID); err != nil {
					t.Fatal(err)
				}
				if point.Kind == runfork.RunForkPointEvent {
					if _, err := loadRunForkSelectedContractBinding(ctx, tx, inserted.ForkRunID); err == nil {
						t.Fatal("mixed persisted fork point was accepted")
					}
				}
			}
		})
	}
}
