package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func TestDeploymentFanOutSummaryUsesFeedIdentityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			db := fanOutReadbackTestDB(t, backend)
			request := deploymentStoreRequest(1)
			feed := durabledata.DeploymentFeed{RunID: request.Key.RunID, Declaration: request.Deployment.Declaration, SourceInvocationID: uuid.NewString()}
			firstID := deploymentFeedID(feed)
			feed.SourceInvocationID = uuid.NewString()
			if firstID == deploymentFeedID(feed) {
				t.Fatal("independent imports into the same run/declaration share a feed identity")
			}
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
			if _, err := db.ExecContext(ctx, `UPDATE fan_out_outcomes
				SET outcome_kind='continued',event_id=$3,source_event_id=$4,inherited_disposition='dead_lettered',failure=NULL
				WHERE run_id=$1 AND deployment_feed_id=$2`, request.Key.RunID, request.Key.DeploymentFeedID, uuid.NewString(), uuid.NewString()); err != nil {
				t.Fatal(err)
			}
			summary, err = fanOutRunSummary(ctx, db, backend == "postgres", request.Key.RunID, now.Add(time.Second))
			if summary.Committed != 1 || summary.SemanticRejected != 0 || summary.SemanticRejectionSample != nil || summary.Owed != 0 ||
				err == nil || !strings.Contains(err.Error(), "load fan-out settlement batch") {
				t.Fatalf("continued ordinal counts once, but missing publication must refuse complete readback: %+v err=%v", summary, err)
			}
			if _, err := db.ExecContext(ctx, `UPDATE fan_out_outcomes
				SET outcome_kind='semantic_rejected',event_id=NULL,source_event_id=NULL,inherited_disposition=NULL,failure=$3
				WHERE run_id=$1 AND deployment_feed_id=$2`, request.Key.RunID, request.Key.DeploymentFeedID, string(failureJSON)); err != nil {
				t.Fatal(err)
			}
			query := fanoutobligation.DeploymentOutcomeQuery{RunID: request.Key.RunID, DeploymentFeedID: request.Key.DeploymentFeedID, Limit: 1}
			page, err := readDeploymentFanOutOutcomes(ctx, db, query)
			if err != nil || len(page.Outcomes) != 1 || page.Outcomes[0].Kind != fanoutobligation.OutcomeSemanticRejected || page.NextOrdinal != 1 || *page.More {
				t.Fatalf("semantic row readback: %+v err=%v", page, err)
			}
			eventID := uuid.NewString()
			if _, err := db.ExecContext(ctx, `INSERT INTO fan_out_outcomes (run_id,deployment_feed_id,ordinal,outcome_kind,event_id,created_at)
				VALUES ($1,$2,1,'committed',$3,$4)`, query.RunID, query.DeploymentFeedID, eventID, now); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET cardinality=2,cursor=2 WHERE run_id=$1 AND deployment_feed_id=$2`, query.RunID, query.DeploymentFeedID); err != nil {
				t.Fatal(err)
			}
			page, err = readDeploymentFanOutOutcomes(ctx, db, query)
			if err != nil || !*page.More || page.NextOrdinal != 1 {
				t.Fatalf("bounded first page: %+v err=%v", page, err)
			}
			query.FromOrdinal = page.NextOrdinal
			page, err = readDeploymentFanOutOutcomes(ctx, db, query)
			if err != nil || len(page.Outcomes) != 1 || page.Outcomes[0].EventID != eventID || *page.More || page.NextOrdinal != 2 {
				t.Fatalf("committed second page: %+v err=%v", page, err)
			}
			query.RunID = uuid.NewString()
			if _, err := readDeploymentFanOutOutcomes(ctx, db, query); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("foreign run consumed feed outcomes: %v", err)
			}
		})
	}
}
