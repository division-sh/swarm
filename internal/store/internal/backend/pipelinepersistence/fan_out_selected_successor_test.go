package pipelinepersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func selectedSuccessorStoreDB(t *testing.T, backend string) *sql.DB {
	t.Helper()
	if backend == "sqlite" {
		db := deploymentStoreDB(t)
		if _, err := db.Exec(`CREATE TABLE fan_out_outcomes (
			run_id TEXT NOT NULL, deployment_feed_id TEXT NOT NULL,
			ordinal BIGINT NOT NULL, outcome_kind TEXT NOT NULL, event_id TEXT NOT NULL,
			PRIMARY KEY (run_id, deployment_feed_id, ordinal), UNIQUE (event_id))`); err != nil {
			t.Fatal(err)
		}
		return db
	}
	_, db, _ := testutil.StartEmptyPostgres(t)
	for _, ddl := range []string{
		`CREATE TABLE fan_out_intents (
			run_id TEXT NOT NULL, origin_kind TEXT NOT NULL, triggering_delivery_id TEXT,
			deployment_feed_id TEXT, flow_path TEXT, declaration_family TEXT, semantic_path TEXT,
			bundle_hash TEXT NOT NULL, semantic_digest TEXT, source_kind TEXT NOT NULL,
			source_event_id TEXT, source_run_id TEXT, source_entity_id TEXT, source_field TEXT, source_mutation_id TEXT,
			source_resource_flow_path TEXT, source_resource_event_name TEXT, source_resource_version_id TEXT,
			deployment_schema_digest TEXT, cardinality INTEGER NOT NULL, cursor INTEGER NOT NULL,
			status TEXT NOT NULL, next_chunk_size INTEGER NOT NULL, last_served_at TIMESTAMP,
			created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL, claim_owner TEXT,
			claim_generation INTEGER NOT NULL DEFAULT 0, lease_expires_at TIMESTAMP, blocked_reason TEXT,
			capsule TEXT, retry_ready_at TIMESTAMP, retry_failure TEXT,
			UNIQUE (run_id, deployment_feed_id))`,
		`CREATE TABLE fan_out_outcomes (
			run_id TEXT NOT NULL, deployment_feed_id TEXT NOT NULL,
			ordinal BIGINT NOT NULL, outcome_kind TEXT NOT NULL, event_id TEXT NOT NULL,
			PRIMARY KEY (run_id, deployment_feed_id, ordinal), UNIQUE (event_id))`,
		`CREATE TABLE runs (run_id TEXT PRIMARY KEY, bundle_hash TEXT NOT NULL, status TEXT NOT NULL)`,
		`CREATE TABLE run_control_state (run_id TEXT PRIMARY KEY, control_status TEXT)`,
		`CREATE TABLE run_fork_selected_contract_bindings (binding_id TEXT PRIMARY KEY, fork_run_id TEXT NOT NULL)`,
		`CREATE TABLE run_fork_selected_contract_runtime_executions (
			execution_id TEXT PRIMARY KEY, binding_id TEXT NOT NULL, fork_run_id TEXT NOT NULL,
			generation INTEGER NOT NULL, fence_generation INTEGER NOT NULL, execution_owner TEXT NOT NULL,
			state TEXT NOT NULL, lease_expires_at TIMESTAMP NOT NULL)`,
		`CREATE TABLE runtime_generation_grants (
			grant_id TEXT, state_version INTEGER, state TEXT, bundle_hash TEXT,
			selected_binding_id TEXT, selected_fork_run_id TEXT, selected_execution_id TEXT,
			runtime_generation INTEGER, source_set_revision TEXT)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func TestSelectedDeploymentSuccessorResumesOnlyCommittedCursor(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := selectedSuccessorStoreDB(t, backend)
			ctx := context.Background()
			request := deploymentStoreRequest(3)
			bindingID, oldExecutionID, nextExecutionID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			now := time.Now().UTC()
			grantFor := func(executionID string, generation uint64) startupownership.GrantEvidence {
				binding := startupownership.SelectedForkGrantBinding{
					BindingID: bindingID, ForkRunID: request.Key.RunID, ExecutionID: executionID,
					ExecutionGeneration: generation, FenceGeneration: 1, ExecutionOwner: "owner",
				}
				for _, field := range []*string{&binding.AdmissionFingerprint, &binding.ContainerPlanFingerprint, &binding.ActorCensusFingerprint, &binding.EffectiveConfigFingerprint, &binding.DeclarationPlanFingerprint, &binding.PreparationFingerprint} {
					*field = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				}
				return startupownership.GrantEvidence{
					GrantID: uuid.NewString(), ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "owner", ProcessBootID: uuid.NewString(),
					BundleHash: request.Deployment.BundleHash, RuntimeInstanceID: uuid.NewString(), RuntimeGeneration: generation,
					StateVersion: 1, State: startupownership.GrantAdmitted, SelectedFork: &binding,
				}
			}
			oldGrant, nextGrant := grantFor(oldExecutionID, 1), grantFor(nextExecutionID, 2)
			setup, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := insertDeploymentFanOutIntentRowTx(ctx, setup, request, now); err != nil {
				t.Fatal(err)
			}
			for _, stmt := range []struct {
				query string
				args  []any
			}{
				{`INSERT INTO runs VALUES ($1,$2,'running')`, []any{request.Key.RunID, request.Deployment.BundleHash}},
				{`INSERT INTO run_fork_selected_contract_bindings VALUES ($1,$2)`, []any{bindingID, request.Key.RunID}},
				{`INSERT INTO run_fork_selected_contract_runtime_executions VALUES ($1,$2,$3,1,1,'owner','running',$4)`, []any{oldExecutionID, bindingID, request.Key.RunID, now.Add(time.Minute)}},
				{`INSERT INTO runtime_generation_grants VALUES ($1,1,'admitted',$2,$3,$4,$5,1,NULL)`, []any{oldGrant.GrantID, oldGrant.BundleHash, bindingID, request.Key.RunID, oldExecutionID}},
				{`INSERT INTO fan_out_outcomes VALUES ($1,$2,0,'committed',$3)`, []any{request.Key.RunID, request.Key.DeploymentFeedID, uuid.NewString()}},
				{`UPDATE fan_out_intents SET cursor=1 WHERE run_id=$1 AND deployment_feed_id=$2`, []any{request.Key.RunID, request.Key.DeploymentFeedID}},
			} {
				if _, err := setup.ExecContext(ctx, stmt.query, stmt.args...); err != nil {
					t.Fatal(err)
				}
			}
			if err := setup.Commit(); err != nil {
				t.Fatal(err)
			}

			interrupted, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := interrupted.ExecContext(ctx, `UPDATE fan_out_intents SET cursor=2 WHERE run_id=$1 AND deployment_feed_id=$2`, request.Key.RunID, request.Key.DeploymentFeedID); err != nil {
				t.Fatal(err)
			}
			if _, err := interrupted.ExecContext(ctx, `INSERT INTO fan_out_outcomes VALUES ($1,$2,1,'committed',$3)`, request.Key.RunID, request.Key.DeploymentFeedID, uuid.NewString()); err != nil {
				t.Fatal(err)
			}
			if err := interrupted.Rollback(); err != nil {
				t.Fatal(err)
			}

			transition, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, stmt := range []struct {
				query string
				args  []any
			}{
				{`UPDATE run_fork_selected_contract_runtime_executions SET state='failed',lease_expires_at=$1 WHERE execution_id=$2`, []any{now.Add(-time.Second), oldExecutionID}},
				{`INSERT INTO run_fork_selected_contract_runtime_executions VALUES ($1,$2,$3,2,1,'owner','running',$4)`, []any{nextExecutionID, bindingID, request.Key.RunID, now.Add(time.Minute)}},
				{`INSERT INTO runtime_generation_grants VALUES ($1,1,'admitted',$2,$3,$4,$5,2,NULL)`, []any{nextGrant.GrantID, nextGrant.BundleHash, bindingID, request.Key.RunID, nextExecutionID}},
			} {
				if _, err := transition.ExecContext(ctx, stmt.query, stmt.args...); err != nil {
					t.Fatal(err)
				}
			}
			if err := transition.Commit(); err != nil {
				t.Fatal(err)
			}

			recovered, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Rollback()
			if _, found, err := observeSelectedDeploymentCandidateTx(ctx, recovered, now, oldGrant, request); err != nil || found {
				t.Fatalf("fenced predecessor observed feed: found=%t err=%v", found, err)
			}
			if _, found, err := observeSelectedDeploymentCandidateTx(ctx, recovered, now, nextGrant, request); err != nil || !found {
				t.Fatalf("exact successor missed feed: found=%t err=%v", found, err)
			}
			intent, err := loadDeploymentFanOutIntentTx(ctx, recovered, request.Key)
			if err != nil || intent.Cursor != 1 || intent.Request.Cardinality != 3 {
				t.Fatalf("successor changed or skipped committed cursor: intent=%+v err=%v", intent, err)
			}
			var count int
			if err := recovered.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND deployment_feed_id=$2`, request.Key.RunID, request.Key.DeploymentFeedID).Scan(&count); err != nil || count != 1 {
				t.Fatalf("successor duplicated feed intent: count=%d err=%v", count, err)
			}
			if err := recovered.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND deployment_feed_id=$2`, request.Key.RunID, request.Key.DeploymentFeedID).Scan(&count); err != nil || count != 1 {
				t.Fatalf("interrupted ordinal became a durable outcome: count=%d err=%v", count, err)
			}
		})
	}
}
