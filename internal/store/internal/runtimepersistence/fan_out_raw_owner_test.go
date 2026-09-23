package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/google/uuid"
)

// Raw persistence tests isolate claim/cursor/source semantics from process
// startup. These adapters are deliberately absent from the production facade.
type rawFanOutTestAdmission struct{}

func (rawFanOutTestAdmission) AdmitFanOutCleanupTx(context.Context, *sql.Tx, startupownership.GrantEvidence) error {
	return nil
}

func (rawFanOutTestAdmission) ObserveFanOutGrantsTx(context.Context, *sql.Tx, []startupownership.GrantEvidence) error {
	return nil
}

func (rawFanOutTestAdmission) ObserveFanOutRunTx(context.Context, *sql.Tx, startupownership.GrantEvidence, string) (bool, error) {
	return true, nil
}

func (rawFanOutTestAdmission) AdmitFanOutRunTx(context.Context, *sql.Tx, startupownership.GrantEvidence, string) (bool, error) {
	return true, nil
}

type rawFanOutTestFactory interface {
	NewFanOutServingStore(pipelinepersistence.FanOutAdmission) (startupownership.FanOutServingStore, error)
}

func rawFanOutTestOwner(factory rawFanOutTestFactory, bundle string) (pipeline.FanOutObligationOwner, error) {
	store, err := factory.NewFanOutServingStore(rawFanOutTestAdmission{})
	if err != nil {
		return nil, err
	}
	return store.BindFanOutGrant(startupownership.GrantEvidence{
		GrantID: uuid.NewString(), ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "raw-store-test",
		ProcessBootID: uuid.NewString(), BundleHash: bundle, RuntimeInstanceID: uuid.NewString(),
		RuntimeGeneration: 1, SourceSetRevision: "raw-store-test", StateVersion: 3, State: startupownership.GrantAdmitted,
	})
}

func rawFanOutTestClaimOwner(ctx context.Context, db *sql.DB, factory rawFanOutTestFactory, claim fanoutobligation.Claim) (pipeline.FanOutObligationOwner, error) {
	var bundle string
	err := db.QueryRowContext(ctx, `SELECT bundle_hash FROM fan_out_intents WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`,
		claim.Key.RunID, claim.Key.TriggeringDeliveryID, claim.Key.ElementRef.FlowPath, claim.Key.ElementRef.Family, claim.Key.ElementRef.SemanticPath).Scan(&bundle)
	if err != nil {
		return nil, err
	}
	return rawFanOutTestOwner(factory, bundle)
}

func rawFanOutTestClaim(ctx context.Context, db *sql.DB, factory rawFanOutTestFactory, request pipeline.FanOutClaimRequest, now func() time.Time) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
	if err := request.Validate(); err != nil {
		return fanoutobligation.Intent{}, fanoutobligation.Claim{}, false, err
	}
	if request.Candidate == nil {
		var key fanoutobligation.IntentKey
		err := db.QueryRowContext(ctx, `SELECT run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path FROM fan_out_intents
			WHERE bundle_hash=$1 AND status='open' AND (claim_owner IS NULL OR lease_expires_at <= $2) AND (retry_ready_at IS NULL OR retry_ready_at <= $2)
			ORDER BY COALESCE(last_served_at,created_at),created_at,run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path LIMIT 1`, request.BundleHash, now().UTC()).Scan(
			&key.RunID, &key.TriggeringDeliveryID, &key.ElementRef.FlowPath, &key.ElementRef.Family, &key.ElementRef.SemanticPath)
		if errors.Is(err, sql.ErrNoRows) {
			return fanoutobligation.Intent{}, fanoutobligation.Claim{}, false, nil
		}
		if err != nil {
			return fanoutobligation.Intent{}, fanoutobligation.Claim{}, false, err
		}
		request.Candidate = &key
	}
	owner, err := rawFanOutTestOwner(factory, request.BundleHash)
	if err != nil {
		return fanoutobligation.Intent{}, fanoutobligation.Claim{}, false, err
	}
	return owner.ClaimFanOutIntent(ctx, request)
}

func (s *PostgresStore) ClaimFanOutIntent(ctx context.Context, request pipeline.FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
	return rawFanOutTestClaim(ctx, s.backend.ConstructionHandle(), s.pipelinePostgresOwner, request, time.Now)
}

func (s *SQLiteRuntimeStore) ClaimFanOutIntent(ctx context.Context, request pipeline.FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
	return rawFanOutTestClaim(ctx, s.backend.ConstructionHandle(), s.pipelineSQLiteOwner, request, s.now)
}

func (s *PostgresStore) LoadFanOutEvaluation(ctx context.Context, claim fanoutobligation.Claim) (pipeline.FanOutEvaluationInput, error) {
	owner, err := rawFanOutTestClaimOwner(ctx, s.backend.ConstructionHandle(), s.pipelinePostgresOwner, claim)
	if err != nil {
		return pipeline.FanOutEvaluationInput{}, err
	}
	return owner.LoadFanOutEvaluation(ctx, claim)
}

func (s *SQLiteRuntimeStore) LoadFanOutEvaluation(ctx context.Context, claim fanoutobligation.Claim) (pipeline.FanOutEvaluationInput, error) {
	owner, err := rawFanOutTestClaimOwner(ctx, s.backend.ConstructionHandle(), s.pipelineSQLiteOwner, claim)
	if err != nil {
		return pipeline.FanOutEvaluationInput{}, err
	}
	return owner.LoadFanOutEvaluation(ctx, claim)
}

func (s *PostgresStore) CommitFanOutChunk(ctx context.Context, command pipeline.FanOutChunkCommand) (pipeline.CommittedFanOutChunk, error) {
	owner, err := rawFanOutTestClaimOwner(ctx, s.backend.ConstructionHandle(), s.pipelinePostgresOwner, command.Claim)
	if err != nil {
		return pipeline.CommittedFanOutChunk{}, err
	}
	return owner.CommitFanOutChunk(ctx, command)
}

func (s *SQLiteRuntimeStore) CommitFanOutChunk(ctx context.Context, command pipeline.FanOutChunkCommand) (pipeline.CommittedFanOutChunk, error) {
	owner, err := rawFanOutTestClaimOwner(ctx, s.backend.ConstructionHandle(), s.pipelineSQLiteOwner, command.Claim)
	if err != nil {
		return pipeline.CommittedFanOutChunk{}, err
	}
	return owner.CommitFanOutChunk(ctx, command)
}

func (s *PostgresStore) BlockFanOutClaim(ctx context.Context, request pipeline.FanOutBlockRequest) (pipeline.FanOutClaimSettlement, error) {
	owner, err := rawFanOutTestClaimOwner(ctx, s.backend.ConstructionHandle(), s.pipelinePostgresOwner, request.Claim)
	if err != nil {
		return pipeline.FanOutClaimSettlement{}, err
	}
	return owner.BlockFanOutClaim(ctx, request)
}

func (s *SQLiteRuntimeStore) BlockFanOutClaim(ctx context.Context, request pipeline.FanOutBlockRequest) (pipeline.FanOutClaimSettlement, error) {
	owner, err := rawFanOutTestClaimOwner(ctx, s.backend.ConstructionHandle(), s.pipelineSQLiteOwner, request.Claim)
	if err != nil {
		return pipeline.FanOutClaimSettlement{}, err
	}
	return owner.BlockFanOutClaim(ctx, request)
}

func (s *PostgresStore) ReleaseFanOutClaim(ctx context.Context, claim fanoutobligation.Claim) (pipeline.FanOutClaimSettlement, error) {
	owner, err := rawFanOutTestClaimOwner(ctx, s.backend.ConstructionHandle(), s.pipelinePostgresOwner, claim)
	if err != nil {
		return pipeline.FanOutClaimSettlement{}, err
	}
	return owner.ReleaseFanOutClaim(ctx, claim)
}

func (s *SQLiteRuntimeStore) ReleaseFanOutClaim(ctx context.Context, claim fanoutobligation.Claim) (pipeline.FanOutClaimSettlement, error) {
	owner, err := rawFanOutTestClaimOwner(ctx, s.backend.ConstructionHandle(), s.pipelineSQLiteOwner, claim)
	if err != nil {
		return pipeline.FanOutClaimSettlement{}, err
	}
	return owner.ReleaseFanOutClaim(ctx, claim)
}

func (s *PostgresStore) ReleaseFanOutRetryable(ctx context.Context, request pipeline.FanOutRetryableRelease) (pipeline.FanOutClaimSettlement, error) {
	owner, err := rawFanOutTestClaimOwner(ctx, s.backend.ConstructionHandle(), s.pipelinePostgresOwner, request.Claim)
	if err != nil {
		return pipeline.FanOutClaimSettlement{}, err
	}
	return owner.ReleaseFanOutRetryable(ctx, request)
}

func (s *SQLiteRuntimeStore) ReleaseFanOutRetryable(ctx context.Context, request pipeline.FanOutRetryableRelease) (pipeline.FanOutClaimSettlement, error) {
	owner, err := rawFanOutTestClaimOwner(ctx, s.backend.ConstructionHandle(), s.pipelineSQLiteOwner, request.Claim)
	if err != nil {
		return pipeline.FanOutClaimSettlement{}, err
	}
	return owner.ReleaseFanOutRetryable(ctx, request)
}
