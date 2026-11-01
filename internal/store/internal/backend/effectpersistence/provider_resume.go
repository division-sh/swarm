package effectpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	storemanagedcapability "github.com/division-sh/swarm/internal/store/internal/backend/managedcapability"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

// Legacy or incomplete provenance retains terminal recovery. Only the exact
// journal acknowledged before launch may spend the existing delivery retry.
func providerResumeJournalTx(ctx context.Context, tx *sql.Tx, postgres bool, recovered completionRecoveryAttempt, attempt runtimeeffects.Attempt, now time.Time) (*runtimeeffects.ProviderCallJournal, error) {
	journal, err := validatedProviderCallJournalTx(ctx, tx, postgres, recovered, attempt)
	if err != nil || journal == nil {
		return nil, err
	}
	current, err := providerResumeTurnCurrentTx(ctx, tx, postgres, attempt, journal.Call, now)
	if err != nil || !current {
		return nil, err
	}
	return journal, nil
}

func validatedProviderCallJournalTx(ctx context.Context, tx *sql.Tx, postgres bool, recovered completionRecoveryAttempt, attempt runtimeeffects.Attempt) (*runtimeeffects.ProviderCallJournal, error) {
	if attempt.Origin.Kind != runtimeeffects.CompletionOriginDelivery {
		return nil, nil
	}
	var evidence struct {
		Journal *runtimeeffects.ProviderCallJournal `json:"provider_call_journal"`
	}
	if json.Unmarshal([]byte(recovered.AuthorityEvidence), &evidence) != nil || evidence.Journal == nil {
		return nil, nil
	}
	journal := evidence.Journal
	surface, err := storemanagedcapability.DecodeStored([]byte(recovered.CapabilitySurface), recovered.CapabilityProjection)
	if err != nil || !journal.ReadOnly(surface) {
		return nil, nil
	}
	frame, err := agentframe.DecodeDurable(recovered.AgentFrame)
	if err != nil {
		return nil, nil
	}
	query := `SELECT request_fingerprint,bundle_hash FROM runtime_external_effect_operations WHERE operation_id=$1::uuid`
	if !postgres {
		query = `SELECT request_fingerprint,bundle_hash FROM runtime_external_effect_operations WHERE operation_id=?`
	}
	var fingerprint, bundleHash string
	if err := tx.QueryRowContext(ctx, query, recovered.OperationID).Scan(&fingerprint, &bundleHash); err != nil {
		return nil, err
	}
	req := runtimeeffects.AuthorizeRequest{OperationID: recovered.OperationID, AttemptID: recovered.AttemptID,
		Kind: runtimeeffects.KindProviderTurn, Adapter: recovered.Adapter, Transport: recovered.Transport,
		RequestFingerprint: fingerprint, AgentFrame: &frame, CapabilitySurface: &surface, TurnTimeout: journal.Call.TurnTimeout}
	if journal.SourceHash != bundleHash || journal.Validate(attempt.Authority, req) != nil {
		return nil, nil
	}
	return journal, nil
}

func providerResumeTurnCurrentTx(ctx context.Context, tx *sql.Tx, postgres bool, attempt runtimeeffects.Attempt, call runtimeeffects.ProviderCall, now time.Time) (bool, error) {
	turnID, _, err := businessTurnIdentity(attempt.Origin)
	if err != nil {
		return false, err
	}
	query := `SELECT run_id::text,agent_id,bound_ns,bound_emit,first_launched_at,cancel_reason,settled_at
		FROM runtime_agent_turn_lifetimes WHERE turn_id=$1::uuid FOR UPDATE`
	if !postgres {
		query = `SELECT run_id,agent_id,bound_ns,bound_emit,first_launched_at,cancel_reason,settled_at
			FROM runtime_agent_turn_lifetimes WHERE turn_id=?`
	}
	var runID, agentID string
	var bound sql.NullInt64
	var emit, canceled sql.NullString
	var launched conversationForkTimeValue
	var settled any
	err = tx.QueryRowContext(ctx, query, turnID).Scan(&runID, &agentID, &bound, &emit, &launched, &canceled, &settled)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if runID != attempt.Authority.Target.RunID || agentID != attempt.Authority.Target.AgentID ||
		!launched.Valid || canceled.Valid || settled != nil || bound.Valid != emit.Valid || bound.Valid != (call.TurnTimeout != nil) {
		return false, nil
	}
	if call.TurnTimeout != nil && (bound.Int64 != int64(call.TurnTimeout.After) || emit.String != call.TurnTimeout.Emit ||
		!now.Before(launched.Time.Add(call.TurnTimeout.After))) {
		return false, nil
	}
	return true, nil
}

func providerRecoveryDisposition(ctx context.Context, tx *sql.Tx, postgres bool, recovered completionRecoveryAttempt, attempt runtimeeffects.Attempt, expired bool, now time.Time) (runtimedelivery.FailureDisposition, error) {
	if expired || (recovered.State != string(runtimeeffects.StateLaunched) && recovered.State != string(runtimeeffects.StateResponseObserved)) {
		return runtimedelivery.FailureDeadLetter, nil
	}
	journal, err := providerResumeJournalTx(ctx, tx, postgres, recovered, attempt, now)
	if err != nil {
		return "", err
	}
	if journal != nil {
		return runtimedelivery.FailureRetry, nil
	}
	return runtimedelivery.FailureDeadLetter, nil
}

type providerCallRecoveryResult struct {
	recovery runtimeeffects.ProviderCallRecovery
	found    bool
}

func providerResumeRetryBaseTx(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt, now time.Time) (time.Duration, error) {
	target := attempt.Authority.Target
	if !target.Memory.Enabled {
		return 0, nil
	}
	var expiry conversationForkTimeValue
	err := tx.QueryRowContext(ctx, `SELECT lease_expires_at FROM agent_sessions
		WHERE session_id=$1 AND run_id=$2 AND lease_grant_id=$3 AND lease_holder=$4 AND status='active'`,
		target.SessionID, target.RunID, attempt.SessionGrantID, attempt.SessionLockOwner).Scan(&expiry)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return max(0, expiry.Time.Sub(now)), nil
}

func (s *EffectPostgresOwner) LoadProviderCallResume(ctx context.Context, req runtimeeffects.CompletionContinuationRequest) (runtimeeffects.ProviderCallRecovery, bool, error) {
	if err := s.requireCurrent(); err != nil {
		return runtimeeffects.ProviderCallRecovery{}, false, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, mutation *mutationprotocol.Attempt) (providerCallRecoveryResult, error) {
		var value providerCallRecoveryResult
		err := mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			if err := requireExternalEffectAuthorityPostgres(ctx, tx, req.Authority, false); err != nil {
				return err
			}
			if s.delivery == nil {
				return errors.New("provider resume delivery owner is not bound")
			}
			if err := s.delivery.ValidateProviderOriginTx(ctx, tx, req.Origin.Delivery); err != nil {
				return err
			}
			var err error
			value, err = loadProviderCallResumeTx(ctx, tx, true, req)
			return err
		})
		return value, err
	})
	value, acknowledged := result.Value()
	return value.recovery, acknowledged && value.found, result.Err()
}

func (s *EffectSQLiteOwner) LoadProviderCallResume(ctx context.Context, req runtimeeffects.CompletionContinuationRequest) (runtimeeffects.ProviderCallRecovery, bool, error) {
	if err := s.requireCurrent(); err != nil {
		return runtimeeffects.ProviderCallRecovery{}, false, err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite read provider resume", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, mutation *mutationprotocol.Attempt) (providerCallRecoveryResult, error) {
		var value providerCallRecoveryResult
		err := mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			if err := requireExternalEffectAuthoritySQLite(ctx, tx, req.Authority, false); err != nil {
				return err
			}
			if s.delivery == nil {
				return errors.New("provider resume delivery owner is not bound")
			}
			if err := s.delivery.ValidateProviderOriginTx(ctx, tx, req.Origin.Delivery); err != nil {
				return err
			}
			var err error
			value, err = loadProviderCallResumeTx(ctx, tx, false, req)
			return err
		})
		return value, err
	})
	value, acknowledged := result.Value()
	return value.recovery, acknowledged && value.found, result.Err()
}

func loadProviderCallResumeTx(ctx context.Context, tx *sql.Tx, postgres bool, req runtimeeffects.CompletionContinuationRequest) (providerCallRecoveryResult, error) {
	var result providerCallRecoveryResult
	admission, admitted := managedexecution.FromContext(ctx)
	if !req.Authority.Valid() || !admitted || admission.ExecutionAuthorityID != req.ExecutionAuthorityID ||
		req.Origin.Kind != runtimeeffects.CompletionOriginDelivery || req.Origin.Validate() != nil || req.SessionID == "" {
		return result, fmt.Errorf("provider resume requires fresh execution, claim and session admission")
	}
	candidates, err := loadProviderResumePredecessorsTx(ctx, tx, postgres, req.Origin.Delivery)
	if err != nil || len(candidates) == 0 {
		return result, err
	}
	if len(candidates) != 1 {
		return result, fmt.Errorf("provider resume has ambiguous predecessor attempts")
	}
	recovered := candidates[0]
	now := time.Now().UTC()
	attempt, _, err := completionRecoverySettlement(recovered, runtimeeffects.StateOutcomeUncertain, nil, now)
	if err != nil {
		return result, err
	}
	journal, err := providerResumeJournalTx(ctx, tx, postgres, recovered, attempt, now)
	if err != nil {
		return result, err
	}
	if journal == nil {
		return result, fmt.Errorf("provider resume predecessor journal is missing or invalid")
	}
	frame, err := agentframe.DecodeDurable(recovered.AgentFrame)
	if err != nil {
		return result, err
	}
	bundleHash, err := requiredExternalEffectBundleHash(ctx, req.Authority)
	if err != nil {
		return result, err
	}
	identity := req.Authority.Normal.Identity
	if req.Authority.Kind == runtimeeffects.AuthoritySelectedContractFork {
		identity = frame.Session.AgentIdentity
		if req.Authority.SelectedFork.ForkRunID != identity.RunID ||
			!admission.AuthorizesSelected(req.Authority.SelectedFork.ExecutionID, identity.RunID, req.Authority.SelectedFork.Generation) {
			return result, fmt.Errorf("provider resume selected run differs from its predecessor")
		}
	} else if req.Authority.Kind != runtimeeffects.AuthorityNormalAgent || !admission.AuthorizesNormal() {
		return result, fmt.Errorf("provider resume requires normal or selected agent authority")
	}
	same, err := agentidentity.Equal(identity, frame.Session.AgentIdentity)
	if err != nil || !same || journal.SourceHash != bundleHash || req.Memory != journal.Call.Memory ||
		(req.Memory.Enabled && req.SessionID != journal.Call.SessionID) {
		return result, fmt.Errorf("provider resume current source, actor or session contradicts its predecessor")
	}
	if completed, err := providerResumeDecisionRecordedTx(ctx, tx, !postgres, recovered, *journal, now); err != nil || completed {
		return result, err
	}
	parent, err := providerResumeParentTx(ctx, tx, postgres, recovered, frame, now)
	if err != nil {
		return result, err
	}
	return providerCallRecoveryResult{recovery: runtimeeffects.ProviderCallRecovery{Journal: *journal, Frame: frame, Parent: parent}, found: true}, nil
}

func loadProviderResumePredecessorsTx(ctx context.Context, tx *sql.Tx, postgres bool, claim runtimedelivery.Claim) ([]completionRecoveryAttempt, error) {
	query := sqliteCompletionRecoverySelect
	if postgres {
		query = postgresCompletionRecoverySelect
	}
	query += `JOIN event_delivery_attempts prior ON prior.delivery_id=a.origin_delivery_id
		AND prior.claim_version=a.origin_claim_version AND prior.claim_token=a.origin_claim_token
		WHERE o.effect_kind='provider_turn' AND a.state='outcome_uncertain' AND o.state='outcome_uncertain'
		AND a.origin_kind='delivery' AND CAST(a.origin_delivery_id AS TEXT)=$1 AND CAST(a.origin_run_id AS TEXT)=$2
		AND a.origin_route_identity=$3 AND a.origin_subscriber_type=$4 AND a.origin_subscriber_id=$5
		AND prior.closure_kind='settled' AND prior.open_marker=FALSE AND prior.completed_at IS NOT NULL
		AND prior.outcome='retry_scheduled' AND prior.reason_code='effect_recovery_outcome_unconfirmed'
		AND a.origin_claim_version=$6-1 LIMIT 2`
	if postgres {
		query += ` FOR UPDATE OF o,a`
	}
	rows, err := tx.QueryContext(ctx, query, claim.DeliveryID(), claim.RunID(), claim.RouteIdentity(), string(claim.SubscriberClass()), claim.SubscriberID(), claim.Version())
	if err != nil {
		return nil, err
	}
	return scanCompletionRecoveryAttempts(rows)
}

func providerResumeParentTx(ctx context.Context, tx *sql.Tx, postgres bool, recovered completionRecoveryAttempt, frame agentframe.Frame, now time.Time) (*runtimeeffects.CompletionContinuationSnapshot, error) {
	if frame.Turn.Kind != agentframe.TurnToolContinuation {
		return nil, nil
	}
	parentID, err := uuid.Parse(strings.TrimPrefix(frame.Turn.ParentFrameID, "agent-frame:v1:"))
	if err != nil || parentID == uuid.Nil {
		return nil, fmt.Errorf("provider resume parent frame identity is invalid")
	}
	query := sqliteCompletionRecoverySelect
	if postgres {
		query = postgresCompletionRecoverySelect
	}
	rows, err := tx.QueryContext(ctx, query+`WHERE a.usage_target_id=$1 AND a.origin_delivery_id=$2 AND a.origin_run_id=$3
		AND a.origin_route_identity=$4 AND a.origin_claim_version=$5 AND a.origin_claim_token=$6
		AND a.origin_subscriber_id=$7 AND a.origin_subscriber_type='agent' AND o.effect_kind='provider_turn'
		AND a.state='settled' AND o.state='settled' LIMIT 2`, parentID.String(), recovered.OriginDeliveryID, recovered.OriginRunID,
		recovered.OriginRouteIdentity, recovered.OriginClaimVersion, recovered.OriginClaimToken, recovered.OriginSubscriber)
	if err != nil {
		return nil, err
	}
	parents, err := scanCompletionRecoveryAttempts(rows)
	if err != nil {
		return nil, err
	}
	if len(parents) != 1 {
		return nil, fmt.Errorf("provider resume requires one exact settled parent")
	}
	parent := parents[0]
	attempt, _, err := completionRecoverySettlement(parent, runtimeeffects.StateSettled, nil, now)
	if err != nil {
		return nil, err
	}
	journal, err := validatedProviderCallJournalTx(ctx, tx, postgres, parent, attempt)
	if err != nil {
		return nil, err
	}
	if journal == nil || journal.FrameID != frame.Turn.ParentFrameID || parent.Adapter != recovered.Adapter {
		return nil, fmt.Errorf("provider resume parent journal is missing or contradictory")
	}
	var raw string
	if err := tx.QueryRowContext(ctx, `SELECT CAST(evidence AS TEXT) FROM runtime_external_effect_attempts WHERE attempt_id=$1`, parent.AttemptID).Scan(&raw); err != nil {
		return nil, err
	}
	payload, err := runtimeeffects.ReadCompletionContinuationPayload(json.RawMessage(raw), runtimeeffects.Fingerprint(journal.Call.Request.Body))
	if err != nil {
		return nil, err
	}
	continuation, err := agentframe.NewToolContinuation(frame.Turn.ParentFrameID, frame.Turn.ToolResult)
	if err != nil {
		return nil, err
	}
	if err := runtimellm.ValidateCompletionToolContinuation(payload, recovered.Adapter, continuation); err != nil {
		return nil, err
	}
	surface, err := storemanagedcapability.DecodeStored([]byte(parent.CapabilitySurface), parent.CapabilityProjection)
	if err != nil {
		return nil, err
	}
	return &runtimeeffects.CompletionContinuationSnapshot{Request: append([]byte(nil), journal.Call.Request.Body...), Payload: payload,
		Surface: surface, Phase: runtimeeffects.CompletionProjectionResponseConsumed}, nil
}

func requireProviderResumeParentTx(ctx context.Context, tx *sql.Tx, postgres bool, authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest) error {
	if req.Kind != runtimeeffects.KindProviderTurn || req.Origin.Kind != runtimeeffects.CompletionOriginDelivery {
		return nil
	}
	hasParent := req.ProviderCall != nil && req.ProviderCall.PredecessorAttemptID != ""
	if !hasParent && req.Origin.Delivery.Version() <= 1 {
		return nil
	}
	admission, ok := managedexecution.FromContext(ctx)
	if !ok {
		return fmt.Errorf("provider resume parent requires current execution admission")
	}
	value, err := loadProviderCallResumeTx(ctx, tx, postgres, runtimeeffects.CompletionContinuationRequest{
		Authority: authority, Origin: req.Origin, ExecutionAuthorityID: admission.ExecutionAuthorityID,
		SessionID: authority.Target.SessionID, Memory: authority.Target.Memory,
	})
	if err != nil {
		return err
	}
	if !value.found && !hasParent {
		return nil
	}
	if !hasParent {
		return fmt.Errorf("provider recovery retry requires its exact interrupted call")
	}
	prior := value.recovery.Journal
	if !value.found || prior.AttemptID != req.ProviderCall.PredecessorAttemptID || req.OperationID == prior.OperationID ||
		req.AttemptID == prior.AttemptID || req.AgentFrame == nil || req.CapabilitySurface == nil {
		return fmt.Errorf("provider resume parent does not identify a fresh physical attempt")
	}
	fingerprint, err := prior.Call.Request.Fingerprint()
	if err != nil || fingerprint != req.RequestFingerprint || req.ProviderCall.TurnCount != prior.Call.TurnCount ||
		!bytes.Equal(req.ProviderCall.Messages, prior.Call.Messages) || !bytes.Equal(req.ProviderCall.ProviderContract, prior.Call.ProviderContract) {
		return fmt.Errorf("provider resume changed its interrupted request or input")
	}
	old := value.recovery.Frame
	oldCandidates, _ := json.Marshal(old.Turn.Capability.Candidates)
	newCandidates, _ := json.Marshal(req.AgentFrame.Turn.Capability.Candidates)
	old.Turn.Capability = req.AgentFrame.Turn.Capability
	oldInput, _ := json.Marshal(struct {
		Session agentframe.SessionContract
		Turn    agentframe.TurnContext
	}{old.Session, old.Turn})
	newInput, _ := json.Marshal(struct {
		Session agentframe.SessionContract
		Turn    agentframe.TurnContext
	}{req.AgentFrame.Session, req.AgentFrame.Turn})
	if !bytes.Equal(oldCandidates, newCandidates) || !bytes.Equal(oldInput, newInput) {
		return fmt.Errorf("provider resume changed its frozen frame or callable surface")
	}
	return nil
}
