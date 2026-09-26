package pipelinepersistence

import (
	"context"
	"testing"
	"time"

	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestDeploymentFanOutSummaryUsesFeedIdentityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			db := fanOutReadbackTestDB(t, backend)
			if _, err := db.ExecContext(ctx, `CREATE TABLE fan_out_obligation_barriers (run_id TEXT, status TEXT)`); err != nil {
				t.Fatal(err)
			}
			request := deploymentStoreRequest(1)
			now := time.Now().UTC().Truncate(time.Microsecond)
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := insertDeploymentFanOutIntentRowTx(ctx, tx, request, now); err != nil {
				_ = tx.Rollback()
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			failure := runtimeengine.NormalizeFailure(&runtimeengine.EmitPayloadContractError{
				Event: "items.ready", Kind: runtimeengine.EmitPayloadSchemaMismatch,
				Path: "$.name", Constraint: "type", Expected: "string", Actual: "integer",
				Detail: "invalid deployment row",
			}, "test", "commit")
			failureJSON, err := runtimefailures.MarshalEnvelope(failure.Failure)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET status='blocked',blocked_reason=$1 WHERE run_id=$2 AND deployment_feed_id=$3`,
				string(failureJSON), request.Key.RunID, request.Key.DeploymentFeedID); err != nil {
				t.Fatal(err)
			}
			summary, err := fanOutRunSummary(ctx, db, backend == "postgres", request.Key.RunID, now.Add(time.Second))
			if err != nil || summary.Blocked != 1 || summary.Owed != 1 || len(summary.BlockedIntents) != 1 ||
				summary.BlockedIntents[0].DeploymentFeedID != request.Key.DeploymentFeedID ||
				summary.BlockedIntents[0].TriggeringDeliveryID != "" {
				t.Fatalf("blocked deployment summary: %+v err=%v", summary, err)
			}
			if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET status='closed',cursor=1,blocked_reason=NULL WHERE run_id=$1 AND deployment_feed_id=$2`,
				request.Key.RunID, request.Key.DeploymentFeedID); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO fan_out_outcomes (run_id,deployment_feed_id,ordinal,outcome_kind,failure,created_at)
				VALUES ($1,$2,0,'semantic_rejected',$3,$4)`, request.Key.RunID, request.Key.DeploymentFeedID, string(failureJSON), now); err != nil {
				t.Fatal(err)
			}
			summary, err = fanOutRunSummary(ctx, db, backend == "postgres", request.Key.RunID, now.Add(time.Second))
			if err != nil || summary.SemanticRejected != 1 || summary.Owed != 0 || summary.SemanticRejectionSample == nil ||
				summary.SemanticRejectionSample.DeploymentFeedID != request.Key.DeploymentFeedID ||
				summary.SemanticRejectionSample.TriggeringDeliveryID != "" {
				t.Fatalf("rejected deployment summary: %+v err=%v", summary, err)
			}
		})
	}
}
