package pipelinepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func TestIssue642InheritedCanceledFanOutFoldBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, origin := range []string{"handler", "deployment"} {
				t.Run(origin, func(t *testing.T) {
					ctx := context.Background()
					db := fanOutReadbackTestDB(t, backend)
					request := deploymentStoreRequest(1)
					key := request.Key
					at := time.Now().UTC().Truncate(time.Microsecond)
					if origin == "handler" {
						key = seedFanOutReadbackClaim(t, db).Claim.Key
					}
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					if origin == "deployment" {
						if err := insertDeploymentFanOutIntentRowTx(ctx, tx, request, at); err != nil {
							t.Fatal(err)
						}
					}
					if _, err := tx.ExecContext(ctx, `UPDATE fan_out_intents SET status='closed',cardinality=1,cursor=1,claim_owner=NULL,lease_expires_at=NULL WHERE run_id=$1`, key.RunID); err != nil {
						t.Fatal(err)
					}
					if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_outcomes (run_id,deployment_feed_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,source_event_id,inherited_disposition,created_at)
				VALUES ($1,NULLIF($2,''),NULLIF($3,''),NULLIF($4,''),NULLIF($5,''),NULLIF($6,''),0,'committed',$7,'canceled',$8)`, key.RunID, key.DeploymentFeedID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath, uuid.NewString(), at); err != nil {
						t.Fatal(err)
					}
					fold, err := foldFanOutIntentTerminalDispositions(ctx, tx, backend == "postgres", key)
					if err != nil || !fold.Terminal() || fold.Summary.Total != 1 || fold.Summary.Canceled != 1 || fold.Summary.Succeeded != 0 || fold.Summary.DeadLettered != 0 {
						t.Fatalf("inherited cancellation did not remain cancellation: %+v %v", fold, err)
					}
					outcome := fanoutobligation.Outcome{Ordinal: 0, Kind: fanoutobligation.OutcomeCommitted, SourceEventID: uuid.NewString(), InheritedDisposition: "canceled", CreatedAt: at}
					if err := outcome.Validate(); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}
