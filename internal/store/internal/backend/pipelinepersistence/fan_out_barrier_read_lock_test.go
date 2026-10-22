package pipelinepersistence

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func TestTransferredBarrierReadPreservesMutationLockBoundary(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, mutable := range []bool{false, true} {
			name := map[bool]string{false: "sqlite", true: "postgres"}[postgres] + map[bool]string{false: "/observation", true: "/mutation"}[mutable]
			t.Run(name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				key := fanoutobligation.IntentKey{RunID: uuid.NewString(), TriggeringDeliveryID: uuid.NewString(),
					ElementRef: contracts.FanOutElementRef{FlowPath: "producer", Family: "fan_out", SemanticPath: `nodes["producer"].handlers["work.requested"].fan_out[0]`}}
				mock.ExpectBegin()
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				query := `FROM fan_out_obligation_barriers WHERE run_id=.* ORDER BY triggering_delivery_id, flow_path, declaration_family, semantic_path`
				if postgres && mutable {
					query += " FOR UPDATE"
				}
				mock.ExpectQuery(query+"$").WithArgs(key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath).
					WillReturnRows(sqlmock.NewRows([]string{"triggering_delivery_id"}))
				if mutable {
					_, err = LoadFanOutBarrierContinuationTx(context.Background(), tx, postgres, key)
				} else {
					_, err = ReadFanOutBarrierContinuationTx(context.Background(), tx, postgres, key)
				}
				if err == nil || !strings.Contains(err.Error(), "lacks its exact unfinished owner") {
					t.Fatalf("read did not use its exact lock policy and shared decoder: %v", err)
				}
				mock.ExpectRollback()
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
