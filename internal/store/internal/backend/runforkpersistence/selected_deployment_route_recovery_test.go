package runforkpersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestSelectedDeploymentRouteRecoveryRoundTripBothStores(t *testing.T) {
	evidence := activationEqualitySourceRecipients(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := activationEqualityDatabase(t, backend)
			event := activationEqualityRecord(t, evidence)
			topology, planning, err := decodeRunForkSelectedContractRouteRecoveryModels(event)
			if err != nil {
				t.Fatal(err)
			}
			point := runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 7}
			request := runfork.RunForkSelectedContractRouteRecoveryRequest{
				ForkRunID: event.ForkRunID, SourceRunID: event.SourceRunID,
				ForkPoint: point, ContractSelection: event.ContractSelection,
				RouteTopology: topology, RecipientPlanning: planning,
			}
			record, err := normalizeRunForkSelectedContractRouteRecovery(request, time.Now().UTC())
			if err != nil || record.ForkEventID != "" {
				t.Fatalf("normalize eventless point: record=%+v err=%v", record, err)
			}
			ctx := context.Background()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if err := insertRunForkSelectedContractRouteRecovery(ctx, tx, record); err != nil {
				t.Fatal(err)
			}
			loaded, err := loadRunForkSelectedContractRouteRecovery(ctx, tx, `WHERE fork_run_id = $1`, record.ForkRunID)
			if err != nil || loaded.ForkPoint != point || loaded.ForkEventID != "" {
				t.Fatalf("eventless route readback: point=%+v event=%q err=%v", loaded.ForkPoint, loaded.ForkEventID, err)
			}
			if err := validateRunForkSelectedContractRouteRecoveryAtActivation(ctx, tx, record); err != nil {
				t.Fatalf("exact deployment route activation: %v", err)
			}
			wrong := record
			wrong.ForkPoint.Revision++
			if err := validateRunForkSelectedContractRouteRecoveryAtActivation(ctx, tx, wrong); err == nil {
				t.Fatal("different deployment revision used persisted route evidence")
			}
			request.ForkEventID = uuid.NewString()
			if _, err := normalizeRunForkSelectedContractRouteRecovery(request, time.Now().UTC()); err == nil {
				t.Fatal("deployment revision accepted a fabricated event")
			}
		})
	}
}
