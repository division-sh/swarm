package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func deploymentStoreRequest(cardinality int) fanoutobligation.IntentRequest {
	declaration := durabledata.DeclarationRef{FlowPath: ".", EventName: "items.ready"}
	version := durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("1", 64))
	return fanoutobligation.IntentRequest{
		Key: fanoutobligation.IntentKey{RunID: uuid.NewString(), DeploymentFeedID: uuid.NewString()},
		Deployment: &fanoutobligation.DeploymentOrigin{
			BundleHash:  "bundle-v2:sha256:" + strings.Repeat("2", 64),
			Declaration: declaration, VersionID: version,
			SchemaDigest: durabledata.SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("3", 64)),
		},
		Source:      fanoutobligation.SourceRef{Kind: fanoutobligation.SourceResourceVersion, Declaration: declaration, VersionID: version},
		Cardinality: cardinality,
	}
}

func deploymentStoreDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "deployment.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
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

func TestDeploymentIntentRowRoundTripAndHostileMixedFacts(t *testing.T) {
	for _, cardinality := range []int{0, 3} {
		t.Run(strings.Repeat("x", cardinality+1), func(t *testing.T) {
			db := deploymentStoreDB(t)
			ctx := context.Background()
			request := deploymentStoreRequest(cardinality)
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			now := time.Now().UTC()
			if err := insertDeploymentFanOutIntentRowTx(ctx, tx, request, now); err != nil {
				t.Fatal(err)
			}
			got, err := loadDeploymentFanOutIntentTx(ctx, tx, request.Key)
			if err != nil {
				t.Fatal(err)
			}
			if got.Request.Key != request.Key || got.Source != request.Source || got.Request.OriginKind() != fanoutobligation.OriginDeployment || got.Request.Cardinality != cardinality {
				t.Fatalf("deployment round trip differs: %#v", got)
			}
			if cardinality == 0 && (got.Status != fanoutobligation.StatusClosed || got.Cursor != 0) {
				t.Fatalf("zero-row outcome: %#v", got)
			}
			if cardinality != 0 && got.Status != fanoutobligation.StatusOpen {
				t.Fatalf("nonempty outcome: %#v", got)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE fan_out_intents SET triggering_delivery_id=$1 WHERE run_id=$2`, uuid.NewString(), request.Key.RunID); err != nil {
				t.Fatal(err)
			}
			if _, err := loadDeploymentFanOutIntentTx(ctx, tx, request.Key); err == nil {
				t.Fatal("mixed delivery and deployment origin decoded")
			}
			if _, err := tx.ExecContext(ctx, `UPDATE fan_out_intents SET triggering_delivery_id=NULL,origin_kind='unknown' WHERE run_id=$1`, request.Key.RunID); err != nil {
				t.Fatal(err)
			}
			if _, err := loadDeploymentFanOutIntentTx(ctx, tx, request.Key); err == nil {
				t.Fatal("unknown origin decoded")
			}
		})
	}
}

func TestDeploymentClaimUsesExactOriginKeyOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			db := fanOutReadbackTestDB(t, backend)
			request := deploymentStoreRequest(3)
			now := time.Now().UTC().Truncate(time.Microsecond)
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if err := insertDeploymentFanOutIntentRowTx(ctx, tx, request, now); err != nil {
				t.Fatal(err)
			}
			intent, err := scanFanOutIntent(tx.QueryRowContext(ctx, `SELECT `+fanOutIntentColumns+` FROM fan_out_intents WHERE run_id=$1 AND deployment_feed_id=$2`, request.Key.RunID, request.Key.DeploymentFeedID))
			if err != nil || intent.Request.Key != request.Key {
				t.Fatalf("union decoder: intent=%#v err=%v", intent, err)
			}
			claimRequest := runtimepipeline.FanOutClaimRequest{Candidate: &request.Key, BundleHash: request.Deployment.BundleHash, Owner: "deployment-worker", Now: now, Lease: time.Minute}
			var claim fanoutobligation.Claim
			if err := claimFanOutIntentRow(ctx, tx, claimRequest, now, &intent, &claim); err != nil {
				t.Fatal(err)
			}
			if got, err := loadOwnedFanOutIntentTx(ctx, tx, backend == "postgres", claim, false); err != nil || got.Request.Key != request.Key {
				t.Fatalf("exact deployment claim: intent=%#v err=%v", got, err)
			}
			foreign := claim
			foreign.Key.DeploymentFeedID = uuid.NewString()
			if _, err := loadOwnedFanOutIntentTx(ctx, tx, backend == "postgres", foreign, false); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
				t.Fatalf("foreign feed claim error = %v", err)
			}
			handler := claim
			handler.Key.DeploymentFeedID = ""
			handler.Key.TriggeringDeliveryID = uuid.NewString()
			handler.Key.ElementRef = runtimecontracts.FanOutElementRef{FlowPath: ".", Family: "fan_out", SemanticPath: "nodes.scatter"}
			if _, err := loadOwnedFanOutIntentTx(ctx, tx, backend == "postgres", handler, false); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
				t.Fatalf("handler key borrowed deployment row: %v", err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE fan_out_intents SET triggering_delivery_id=$1 WHERE run_id=$2 AND deployment_feed_id=$3`, uuid.NewString(), request.Key.RunID, request.Key.DeploymentFeedID); err != nil {
				t.Fatal(err)
			}
			if _, err := scanFanOutIntent(tx.QueryRowContext(ctx, `SELECT `+fanOutIntentColumns+` FROM fan_out_intents WHERE run_id=$1 AND deployment_feed_id=$2`, request.Key.RunID, request.Key.DeploymentFeedID)); err == nil {
				t.Fatal("general intent decoder accepted mixed deployment and handler origin")
			}
		})
	}
}

func TestOrdinaryCandidateSelectorIncludesDeploymentFeedsOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			db := fanOutReadbackTestDB(t, backend)
			for _, ddl := range []string{
				`CREATE TABLE runs (run_id TEXT PRIMARY KEY, bundle_hash TEXT NOT NULL, status TEXT NOT NULL)`,
				`CREATE TABLE run_control_state (run_id TEXT PRIMARY KEY, control_status TEXT)`,
				`CREATE TABLE run_fork_selected_contract_bindings (binding_id TEXT, fork_run_id TEXT)`,
				`CREATE TABLE run_fork_selected_contract_runtime_executions (binding_id TEXT, fork_run_id TEXT, execution_id TEXT, generation INTEGER, state TEXT, lease_expires_at TIMESTAMP)`,
				`CREATE TABLE runtime_generation_grants (grant_id TEXT, state_version INTEGER, state TEXT, bundle_hash TEXT, selected_binding_id TEXT, selected_fork_run_id TEXT, selected_execution_id TEXT, runtime_generation INTEGER)`,
			} {
				if _, err := db.ExecContext(ctx, ddl); err != nil {
					t.Fatal(err)
				}
			}
			first := deploymentStoreRequest(2)
			second := first
			second.Key.DeploymentFeedID = uuid.NewString()
			second.Deployment = &fanoutobligation.DeploymentOrigin{
				BundleHash:   first.Deployment.BundleHash,
				Declaration:  durabledata.DeclarationRef{FlowPath: ".", EventName: "other.ready"},
				VersionID:    durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("4", 64)),
				SchemaDigest: first.Deployment.SchemaDigest,
			}
			second.Source.Declaration = second.Deployment.Declaration
			second.Source.VersionID = second.Deployment.VersionID
			grant := startupownership.GrantEvidence{GrantID: uuid.NewString(), BundleHash: first.Deployment.BundleHash}
			if _, err := db.ExecContext(ctx, `INSERT INTO runs VALUES ($1,$2,'running')`, first.Key.RunID, first.Deployment.BundleHash); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO runtime_generation_grants (grant_id,state_version,state,bundle_hash,selected_binding_id) VALUES ($1,1,'admitted',$2,NULL)`, grant.GrantID, grant.BundleHash); err != nil {
				t.Fatal(err)
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			at := time.Now().UTC().Add(-time.Minute)
			if err := insertDeploymentFanOutIntentRowTx(ctx, tx, first, at); err != nil {
				t.Fatal(err)
			}
			if err := insertDeploymentFanOutIntentRowTx(ctx, tx, second, at.Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			candidate, found, err := observeFanOutCandidateTx(ctx, tx, backend == "postgres", time.Now, []startupownership.GrantEvidence{grant}, nil)
			if err != nil || !found || candidate.Key != first.Key || candidate.GrantID != grant.GrantID {
				t.Fatalf("first deployment candidate: %#v found=%v err=%v", candidate, found, err)
			}
			candidate, found, err = observeFanOutCandidateTx(ctx, tx, backend == "postgres", time.Now, []startupownership.GrantEvidence{grant}, []fanoutobligation.IntentKey{first.Key})
			if err != nil || !found || candidate.Key != second.Key {
				t.Fatalf("excluded feed hid sibling: %#v found=%v err=%v", candidate, found, err)
			}
		})
	}
}

func TestHandlerWriterRejectsDeploymentRequest(t *testing.T) {
	request := deploymentStoreRequest(1)
	if err := commitFanOutIntentTx(context.Background(), nil, false, nil, request, request.Key.RunID, nil, "", time.Now().UTC()); err == nil {
		t.Fatal("handler writer accepted deployment origin")
	}
}

func TestSelectedDeploymentCandidateRequiresExactBindingAndLiveExecution(t *testing.T) {
	db := deploymentStoreDB(t)
	ctx := context.Background()
	request := deploymentStoreRequest(2)
	binding := startupownership.SelectedForkGrantBinding{
		BindingID: uuid.NewString(), ForkRunID: request.Key.RunID, ExecutionID: uuid.NewString(),
		ExecutionGeneration: 1, FenceGeneration: 1, ExecutionOwner: "owner",
	}
	for _, pointer := range []*string{&binding.AdmissionFingerprint, &binding.ContainerPlanFingerprint, &binding.ActorCensusFingerprint, &binding.EffectiveConfigFingerprint, &binding.DeclarationPlanFingerprint, &binding.PreparationFingerprint} {
		*pointer = "sha256:" + strings.Repeat("a", 64)
	}
	grant := startupownership.GrantEvidence{
		GrantID: uuid.NewString(), ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "owner", ProcessBootID: uuid.NewString(),
		BundleHash: request.Deployment.BundleHash, RuntimeInstanceID: uuid.NewString(), RuntimeGeneration: 1,
		StateVersion: 1, State: startupownership.GrantAdmitted, SelectedFork: &binding,
	}
	now := time.Now().UTC()
	claim := runtimepipeline.FanOutClaimRequest{Owner: "worker", BundleHash: grant.BundleHash, Candidate: &request.Key, Now: now, Lease: time.Minute}
	if err := validateFanOutCandidate(claim, grant); err != nil {
		t.Fatalf("selected deployment candidate was refused: %v", err)
	}
	handler := request.Key
	handler.DeploymentFeedID = ""
	handler.TriggeringDeliveryID = uuid.NewString()
	handler.ElementRef = runtimecontracts.FanOutElementRef{FlowPath: ".", Family: "fan_out", SemanticPath: "nodes.scatter"}
	claim.Candidate = &handler
	if err := validateFanOutCandidate(claim, grant); err == nil {
		t.Fatal("selected grant admitted handler origin")
	}
	foreign := request.Key
	foreign.RunID = uuid.NewString()
	claim.Candidate = &foreign
	if err := validateFanOutCandidate(claim, grant); err == nil {
		t.Fatal("selected grant admitted foreign run")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := insertDeploymentFanOutIntentRowTx(ctx, tx, request, now); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO runs VALUES ($1,$2,'running')`, []any{request.Key.RunID, request.Deployment.BundleHash}},
		{`INSERT INTO run_fork_selected_contract_bindings VALUES ($1,$2)`, []any{binding.BindingID, request.Key.RunID}},
		{`INSERT INTO run_fork_selected_contract_runtime_executions VALUES ($1,$2,$3,1,1,'owner','running',$4)`, []any{binding.ExecutionID, binding.BindingID, request.Key.RunID, now.Add(time.Minute)}},
		{`INSERT INTO runtime_generation_grants VALUES ($1,1,'admitted',$2,$3,$4,$5,1,NULL)`, []any{grant.GrantID, grant.BundleHash, binding.BindingID, request.Key.RunID, binding.ExecutionID}},
	} {
		if _, err := tx.ExecContext(ctx, stmt.query, stmt.args...); err != nil {
			t.Fatal(err)
		}
	}
	got, found, err := observeSelectedDeploymentCandidateTx(ctx, tx, now, grant, request)
	if err != nil || !found || got != request.Key {
		t.Fatalf("exact selected candidate: %#v %v %v", got, found, err)
	}
	listed, err := listSelectedDeploymentFeedsTx(ctx, tx, false, selectedFeedAdmissionProbe{runID: request.Key.RunID}, grant)
	if err != nil || len(listed) != 1 || listed[0].Request.Key != request.Key || listed[0].Source != request.Source || listed[0].Status != fanoutobligation.StatusOpen {
		t.Fatalf("selected feed list lost exact persisted source: %#v err=%v", listed, err)
	}
	if _, err := listSelectedDeploymentFeedsTx(ctx, tx, false, selectedFeedAdmissionProbe{runID: uuid.NewString()}, grant); err == nil {
		t.Fatal("foreign selected run listed deployment feeds")
	}
	selected, found, err := observeFanOutCandidateTx(ctx, tx, false, func() time.Time { return now }, []startupownership.GrantEvidence{grant}, nil)
	if err != nil || !found || selected.Key != request.Key || selected.GrantID != grant.GrantID {
		t.Fatalf("shared selector missed exact selected grant: %#v %v %v", selected, found, err)
	}
	ordinary := startupownership.GrantEvidence{GrantID: uuid.NewString(), BundleHash: grant.BundleHash}
	if _, err := tx.ExecContext(ctx, `INSERT INTO runtime_generation_grants (grant_id,state_version,state,bundle_hash) VALUES ($1,1,'admitted',$2)`, ordinary.GrantID, ordinary.BundleHash); err != nil {
		t.Fatal(err)
	}
	_, found, err = observeFanOutCandidateTx(ctx, tx, false, func() time.Time { return now }, []startupownership.GrantEvidence{ordinary}, nil)
	if err != nil || found {
		t.Fatalf("ordinary grant borrowed selected feed: found=%v err=%v", found, err)
	}
	if err := insertDeploymentFanOutIntentRowTx(ctx, tx, request, now); err == nil {
		t.Fatal("duplicate deployment feed inserted")
	}
	retired := grant
	retired.State = startupownership.GrantRetired
	if _, _, err := observeSelectedDeploymentCandidateTx(ctx, tx, now, retired, request); err == nil {
		t.Fatal("retired selected grant observed work")
	}
	wrongGeneration := grant
	wrongBinding := *grant.SelectedFork
	wrongBinding.ExecutionGeneration++
	wrongGeneration.RuntimeGeneration++
	wrongGeneration.SelectedFork = &wrongBinding
	_, found, err = observeSelectedDeploymentCandidateTx(ctx, tx, now, wrongGeneration, request)
	if err != nil || found {
		t.Fatalf("wrong selected generation observed work: %v %v", found, err)
	}
	wrongFeed := request
	wrongFeed.Key.DeploymentFeedID = uuid.NewString()
	_, found, err = observeSelectedDeploymentCandidateTx(ctx, tx, now, grant, wrongFeed)
	if err != nil || found {
		t.Fatalf("foreign feed selected: %v %v", found, err)
	}
	wrongVersion := request
	version := durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("4", 64))
	wrongVersion.Deployment = &fanoutobligation.DeploymentOrigin{
		BundleHash: request.Deployment.BundleHash, Declaration: request.Deployment.Declaration,
		VersionID: version, SchemaDigest: request.Deployment.SchemaDigest,
	}
	wrongVersion.Source.VersionID = version
	_, found, err = observeSelectedDeploymentCandidateTx(ctx, tx, now, grant, wrongVersion)
	if err != nil || found {
		t.Fatalf("foreign version selected: %v %v", found, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET lease_expires_at=$1`, now.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	_, found, err = observeSelectedDeploymentCandidateTx(ctx, tx, now, grant, request)
	if err != nil || found {
		t.Fatalf("expired selected execution selected: %v %v", found, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET lease_expires_at=$1`, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runtime_generation_grants SET selected_binding_id=$1`, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	_, found, err = observeSelectedDeploymentCandidateTx(ctx, tx, now, grant, request)
	if err != nil || found {
		t.Fatalf("foreign selected binding selected: %v %v", found, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runtime_generation_grants SET selected_binding_id=$1,source_set_revision='borrowed'`, binding.BindingID); err != nil {
		t.Fatal(err)
	}
	_, found, err = observeSelectedDeploymentCandidateTx(ctx, tx, now, grant, request)
	if err != nil || found {
		t.Fatalf("source-set impersonation observed selected feed: %v %v", found, err)
	}
}

type selectedFeedAdmissionProbe struct{ runID string }

func (selectedFeedAdmissionProbe) ObserveFanOutGrantsTx(context.Context, *sql.Tx, []startupownership.GrantEvidence) error {
	return nil
}

func (selectedFeedAdmissionProbe) ObserveFanOutRunTx(context.Context, *sql.Tx, startupownership.GrantEvidence, string) (bool, error) {
	return false, nil
}

func (a selectedFeedAdmissionProbe) AdmitFanOutRunTx(_ context.Context, _ *sql.Tx, grant startupownership.GrantEvidence, runID string) (bool, error) {
	return grant.SelectedFork != nil && runID == a.runID && runID == grant.SelectedFork.ForkRunID, nil
}

func (selectedFeedAdmissionProbe) AdmitFanOutCleanupTx(context.Context, *sql.Tx, startupownership.GrantEvidence) error {
	return nil
}
