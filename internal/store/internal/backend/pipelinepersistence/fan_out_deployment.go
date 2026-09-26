package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
	"github.com/google/uuid"
)

// These projections target the same intent table as handler fan-out after its
// origin-disjoint schema cutover. The deployment arm has no triggering delivery,
// handler declaration, or execution capsule.
const deploymentFanOutIntentColumns = `
	i.run_id, i.origin_kind, i.deployment_feed_id, i.bundle_hash,
	i.source_kind, i.source_resource_flow_path, i.source_resource_event_name,
	i.source_resource_version_id, i.deployment_schema_digest,
	i.cardinality, i.cursor, i.status, i.next_chunk_size, i.last_served_at,
	i.created_at, i.updated_at, i.claim_owner, i.claim_generation,
	i.lease_expires_at, i.blocked_reason, i.retry_ready_at, i.retry_failure,
	CASE WHEN i.triggering_delivery_id IS NULL AND i.flow_path IS NULL
		AND i.declaration_family IS NULL AND i.semantic_path IS NULL
		AND i.semantic_digest IS NULL AND i.capsule IS NULL
		AND i.source_event_id IS NULL AND i.source_run_id IS NULL
		AND i.source_entity_id IS NULL AND i.source_field IS NULL
		AND i.source_mutation_id IS NULL THEN 1 ELSE 0 END`

func deploymentFeedID(feed durabledata.DeploymentFeed) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.deployment-feed.v1\x00"+feed.RunID+"\x00"+feed.Declaration.Key())).String()
}

func (s *PipelinePostgresOwner) CreateDeploymentFeedTx(ctx context.Context, tx *sql.Tx, feed durabledata.DeploymentFeed) error {
	if s == nil || s.resourceData == nil {
		return errors.New("deployment feed requires selected durable-data owner")
	}
	return createDeploymentFeedTx(ctx, tx, s.resourceData, feed)
}

func (s *PipelineSQLiteOwner) CreateDeploymentFeedTx(ctx context.Context, tx *sql.Tx, feed durabledata.DeploymentFeed) error {
	if s == nil || s.resourceData == nil {
		return errors.New("deployment feed requires selected durable-data owner")
	}
	return createDeploymentFeedTx(ctx, tx, s.resourceData, feed)
}

func createDeploymentFeedTx(ctx context.Context, tx *sql.Tx, data *storedurabledata.Owner, feed durabledata.DeploymentFeed) error {
	if err := feed.Validate(); err != nil {
		return err
	}
	if tx == nil || data == nil {
		return errors.New("deployment feed requires its selected transaction and durable-data owner")
	}
	source, err := storedurabledata.RequirePinnedSourceTx(ctx, data, tx, feed.RunID, feed.BundleHash, feed.Declaration)
	if err != nil {
		return err
	}
	if source.Declaration != feed.Declaration || source.VersionID != feed.VersionID || source.SchemaDigest != feed.SchemaDigest || source.RowCount != int(feed.RowCount) {
		return errors.New("deployment feed disagrees with exact pinned version metadata")
	}
	request := fanoutobligation.IntentRequest{
		Key: fanoutobligation.IntentKey{RunID: feed.RunID, DeploymentFeedID: deploymentFeedID(feed)},
		Deployment: &fanoutobligation.DeploymentOrigin{
			BundleHash: feed.BundleHash, Declaration: feed.Declaration,
			VersionID: feed.VersionID, SchemaDigest: feed.SchemaDigest,
		},
		Source: fanoutobligation.SourceRef{
			Kind: fanoutobligation.SourceResourceVersion, Declaration: feed.Declaration, VersionID: feed.VersionID,
		},
		Cardinality: int(feed.RowCount),
	}
	return insertDeploymentFanOutIntentRowTx(ctx, tx, request, time.Now().UTC())
}

// insertDeploymentFanOutIntentRowTx is the row writer used after the selected
// transaction has proved the exact pinned source and recorded its revision
// fact. It deliberately cannot create a handler-shaped origin.
func insertDeploymentFanOutIntentRowTx(ctx context.Context, tx *sql.Tx, request fanoutobligation.IntentRequest, at time.Time) error {
	if tx == nil || at.IsZero() || request.Deployment == nil {
		return errors.New("deployment intent row requires transaction, admission time and deployment origin")
	}
	if err := request.Validate(); err != nil {
		return err
	}
	status := fanoutobligation.StatusOpen
	if request.Cardinality == 0 {
		status = fanoutobligation.StatusClosed
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO fan_out_intents (
		run_id, origin_kind, deployment_feed_id, bundle_hash,
		source_kind, source_resource_flow_path, source_resource_event_name,
		source_resource_version_id, deployment_schema_digest, cardinality, cursor,
		status, next_chunk_size, created_at, updated_at
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,0,$11,$12,$13,$13)`,
		request.Key.RunID, string(fanoutobligation.OriginDeployment), request.Key.DeploymentFeedID,
		request.Deployment.BundleHash, string(request.Source.Kind), request.Source.Declaration.FlowPath,
		request.Source.Declaration.EventName, string(request.Source.VersionID),
		string(request.Deployment.SchemaDigest), request.Cardinality, string(status),
		fanoutobligation.InitialChunkSize, at.UTC())
	if err != nil {
		return fmt.Errorf("insert deployment fan-out intent: %w", err)
	}
	return nil
}

func loadDeploymentFanOutIntentTx(ctx context.Context, tx *sql.Tx, key fanoutobligation.IntentKey) (fanoutobligation.Intent, error) {
	if tx == nil || key.DeploymentFeedID == "" {
		return fanoutobligation.Intent{}, errors.New("deployment intent lookup requires exact feed key and transaction")
	}
	if err := key.Validate(); err != nil {
		return fanoutobligation.Intent{}, err
	}
	return scanDeploymentFanOutIntent(tx.QueryRowContext(ctx,
		`SELECT `+deploymentFanOutIntentColumns+` FROM fan_out_intents i WHERE i.run_id=$1 AND i.deployment_feed_id=$2`,
		key.RunID, key.DeploymentFeedID))
}

func scanDeploymentFanOutIntent(row rowScanner) (fanoutobligation.Intent, error) {
	var runID, origin, feedID, bundleHash, sourceKind, flowPath, eventName, versionID, schemaDigest, status string
	var cardinality, cursor, nextChunk, disjoint int
	var claimOwner, blockedReason sql.NullString
	var claimGeneration uint64
	var lastServedRaw, createdRaw, updatedRaw, leaseRaw, retryAtRaw any
	var retryFailureRaw []byte
	if err := row.Scan(&runID, &origin, &feedID, &bundleHash, &sourceKind, &flowPath, &eventName,
		&versionID, &schemaDigest, &cardinality, &cursor, &status, &nextChunk, &lastServedRaw,
		&createdRaw, &updatedRaw, &claimOwner, &claimGeneration, &leaseRaw, &blockedReason,
		&retryAtRaw, &retryFailureRaw, &disjoint); err != nil {
		return fanoutobligation.Intent{}, err
	}
	if origin != string(fanoutobligation.OriginDeployment) || disjoint != 1 {
		return fanoutobligation.Intent{}, errors.New("deployment intent contains handler or unknown-origin facts")
	}
	lastServed, _, err := sqliteTimeValue(lastServedRaw)
	if err != nil {
		return fanoutobligation.Intent{}, fmt.Errorf("decode deployment last-served time: %w", err)
	}
	createdAt, created, err := sqliteTimeValue(createdRaw)
	if err != nil || !created {
		return fanoutobligation.Intent{}, errors.New("deployment intent requires valid creation time")
	}
	updatedAt, updated, err := sqliteTimeValue(updatedRaw)
	if err != nil || !updated {
		return fanoutobligation.Intent{}, errors.New("deployment intent requires valid update time")
	}
	lease, _, err := sqliteTimeValue(leaseRaw)
	if err != nil {
		return fanoutobligation.Intent{}, fmt.Errorf("decode deployment lease: %w", err)
	}
	declaration := durabledata.DeclarationRef{FlowPath: flowPath, EventName: eventName}
	source := fanoutobligation.SourceRef{Kind: fanoutobligation.SourceKind(sourceKind), Declaration: declaration, VersionID: durabledata.VersionID(versionID)}
	intent := fanoutobligation.Intent{
		Request: fanoutobligation.IntentRequest{
			Key: fanoutobligation.IntentKey{RunID: runID, DeploymentFeedID: feedID},
			Deployment: &fanoutobligation.DeploymentOrigin{
				BundleHash: bundleHash, Declaration: declaration,
				VersionID: source.VersionID, SchemaDigest: durabledata.SchemaDigest(schemaDigest),
			},
			Source: source, Cardinality: cardinality,
		},
		Source: source, Cursor: cursor, Status: fanoutobligation.Status(status), NextChunkSize: nextChunk,
		LastServedAt: lastServed, CreatedAt: createdAt.UTC(), UpdatedAt: updatedAt.UTC(),
		ClaimOwner: claimOwner.String, ClaimGeneration: claimGeneration, LeaseExpiresAt: lease,
		BlockedReason: blockedReason.String,
	}
	if retryAtRaw != nil || retryFailureRaw != nil {
		at, present, err := sqliteTimeValue(retryAtRaw)
		if err != nil || !present {
			return fanoutobligation.Intent{}, errors.New("deployment retry requires valid due time")
		}
		failure, err := runtimefailures.UnmarshalEnvelope(retryFailureRaw)
		if err != nil {
			return fanoutobligation.Intent{}, fmt.Errorf("deployment retry failure: %w", err)
		}
		intent.Retry = &fanoutobligation.RetryWait{ReadyAt: at, Failure: failure}
	}
	if err := intent.Validate(); err != nil {
		return fanoutobligation.Intent{}, err
	}
	return intent, nil
}

// observeSelectedDeploymentCandidateTx is only a candidate prefilter. The
// mutation transaction must independently prove the full selected grant and
// exact feed authority before claiming or advancing an ordinal.
func observeSelectedDeploymentCandidateTx(ctx context.Context, tx *sql.Tx, at time.Time, grant startupownership.GrantEvidence, expected fanoutobligation.IntentRequest) (fanoutobligation.IntentKey, bool, error) {
	if tx == nil || at.IsZero() || grant.SelectedFork == nil || grant.State != startupownership.GrantAdmitted || expected.Deployment == nil {
		return fanoutobligation.IntentKey{}, false, errors.New("selected deployment observation requires admitted selected authority")
	}
	if err := grant.Validate(); err != nil {
		return fanoutobligation.IntentKey{}, false, err
	}
	if err := expected.Validate(); err != nil {
		return fanoutobligation.IntentKey{}, false, err
	}
	binding := grant.SelectedFork
	if expected.Key.RunID != binding.ForkRunID || expected.Deployment.BundleHash != grant.BundleHash {
		return fanoutobligation.IntentKey{}, false, errors.New("selected deployment request differs from the exact grant")
	}
	row := tx.QueryRowContext(ctx, `SELECT `+deploymentFanOutIntentColumns+`
		FROM fan_out_intents i
		JOIN runs r ON r.run_id=i.run_id
		LEFT JOIN run_control_state c ON c.run_id=i.run_id
		JOIN run_fork_selected_contract_bindings b ON b.fork_run_id=i.run_id
		JOIN run_fork_selected_contract_runtime_executions e ON e.binding_id=b.binding_id AND e.fork_run_id=i.run_id
		JOIN runtime_generation_grants g ON g.grant_id=$1 AND g.selected_binding_id=b.binding_id
			AND g.selected_fork_run_id=i.run_id AND g.selected_execution_id=e.execution_id
		WHERE i.origin_kind='deployment' AND i.run_id=$2 AND i.bundle_hash=$3
			AND i.deployment_feed_id=$10 AND i.source_resource_flow_path=$11
			AND i.source_resource_event_name=$12 AND i.source_resource_version_id=$13
			AND i.deployment_schema_digest=$14
			AND i.status='open' AND (i.claim_owner IS NULL OR i.lease_expires_at<=$4)
			AND (i.retry_ready_at IS NULL OR i.retry_ready_at<=$4)
			AND r.status='running' AND r.bundle_hash=i.bundle_hash
			AND COALESCE(c.control_status,'') NOT IN ('paused','stopped')
			AND b.binding_id=$5 AND e.execution_id=$6 AND e.generation=$7
			AND e.fence_generation=$8 AND e.execution_owner=$9 AND e.state='running'
			AND e.lease_expires_at>$4 AND g.state='admitted'
			AND g.source_set_revision IS NULL
			AND g.bundle_hash=i.bundle_hash AND g.runtime_generation=e.generation
			AND g.state_version=(SELECT MAX(head.state_version) FROM runtime_generation_grants head WHERE head.grant_id=g.grant_id)
		ORDER BY COALESCE(i.last_served_at,i.created_at),i.created_at,i.deployment_feed_id LIMIT 1`,
		grant.GrantID, binding.ForkRunID, grant.BundleHash, at, binding.BindingID,
		binding.ExecutionID, binding.ExecutionGeneration, binding.FenceGeneration, binding.ExecutionOwner,
		expected.Key.DeploymentFeedID, expected.Source.Declaration.FlowPath, expected.Source.Declaration.EventName,
		string(expected.Source.VersionID), string(expected.Deployment.SchemaDigest))
	intent, err := scanDeploymentFanOutIntent(row)
	if errors.Is(err, sql.ErrNoRows) {
		return fanoutobligation.IntentKey{}, false, nil
	}
	if err != nil {
		return fanoutobligation.IntentKey{}, false, err
	}
	if intent.Request.Key != expected.Key || intent.Request.Source != expected.Source || *intent.Request.Deployment != *expected.Deployment || intent.Request.Cardinality != expected.Cardinality {
		return fanoutobligation.IntentKey{}, false, errors.New("selected deployment candidate differs from exact feed request")
	}
	state, err := intent.ServingAt(at)
	if err != nil || state != fanoutobligation.ServingEligible {
		return fanoutobligation.IntentKey{}, false, errors.New("selected deployment candidate disagrees with canonical serving state")
	}
	return intent.Request.Key, true, nil
}
