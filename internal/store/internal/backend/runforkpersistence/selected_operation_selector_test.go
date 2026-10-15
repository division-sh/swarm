package runforkpersistence

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestSelectedRecoveryUsesResolvedEventNotOriginalLatestSelectorBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			ctx := context.Background()
			point := runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, EventID: uuid.NewString(), Revision: 4}
			request := runfork.ForkOperationRequest{
				OperationID: uuid.NewString(), Actor: "operator", IdempotencyKey: "original-latest", TransportHash: "original-request",
				SourceRunID: uuid.NewString(), ResolvedPoint: &point, TargetBundleHash: "sha256:bundle",
				ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
			}
			binding := runfork.RunForkSelectedContractBinding{
				Owner: runfork.RunForkSelectedContractBindingOwner, BindingID: uuid.NewString(), ForkRunID: uuid.NewString(), SourceRunID: request.SourceRunID,
				ForkPoint: point, ForkEventID: point.EventID, ContractSelection: request.ContractSelection, CreatedAt: time.Now().UTC(),
			}
			var original runfork.ForkOperationRecord
			withForkOperationTx(t, db, func(tx *sql.Tx) {
				var err error
				original, _, err = bindForkOperationTx(ctx, tx, request, binding.ForkRunID, binding.BindingID, backend == "postgres")
				if err != nil {
					t.Fatal(err)
				}
			})
			withForkOperationTx(t, db, func(tx *sql.Tx) {
				record, err := loadSelectedForkRecoveryOperationTx(ctx, tx, binding, request.TargetBundleHash, backend == "postgres")
				if err != nil || record == nil || !reflect.DeepEqual(*record, original) || record.Request.ForkEventID != "" {
					t.Fatalf("recovery replaced original latest or lost resolved cut: record=%+v err=%v", record, err)
				}
				for _, change := range []string{"foreign_event", "foreign_revision", "foreign_binding", "foreign_source", "foreign_bundle"} {
					hostile, hash := binding, request.TargetBundleHash
					switch change {
					case "foreign_event":
						hostile.ForkEventID = uuid.NewString()
					case "foreign_revision":
						hostile.ForkPoint.Revision++
					case "foreign_binding":
						hostile.BindingID = uuid.NewString()
					case "foreign_source":
						hostile.SourceRunID = uuid.NewString()
					case "foreign_bundle":
						hash = "sha256:foreign"
					}
					if record, err := loadSelectedForkRecoveryOperationTx(ctx, tx, hostile, hash, backend == "postgres"); err == nil || record != nil {
						t.Fatalf("crossed %s became recovery evidence: record=%+v err=%v", change, record, err)
					}
				}
				after, found, err := loadForkOperationByIDTx(ctx, tx, request.OperationID, backend == "postgres")
				if err != nil || !found || !reflect.DeepEqual(after, original) {
					t.Fatalf("read-only recovery changed permanent request: %+v found=%t err=%v", after, found, err)
				}
			})
		})
	}
}
