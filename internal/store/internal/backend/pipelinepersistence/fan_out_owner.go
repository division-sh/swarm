package pipelinepersistence

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/fanoutorigin"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

const fanOutIntentColumns = `
	run_id, triggering_delivery_id, flow_path, declaration_family, semantic_path, bundle_hash, semantic_digest,
	source_kind, source_event_id, source_run_id, source_entity_id, source_field, source_mutation_id,
	source_resource_flow_path, source_resource_event_name, source_resource_version_id,
	cardinality, cursor, status, next_chunk_size, last_served_at, created_at, updated_at,
	claim_owner, claim_generation, lease_expires_at, blocked_reason, capsule, retry_ready_at, retry_failure`

type rowScanner interface{ Scan(...any) error }

func scanFanOutIntent(row rowScanner) (fanoutobligation.Intent, error) {
	var (
		runID, deliveryID, flowPath, family, semanticPath, bundleHash, digest                 string
		sourceKind, sourceEventID, sourceRunID, sourceEntityID, sourceField, sourceMutationID sql.NullString
		resourceFlowPath, resourceEvent, resourceVersion                                      sql.NullString
		cardinality, cursor, nextChunk                                                        int
		status                                                                                string
		lastServedRaw, createdAtRaw, updatedAtRaw, leaseRaw                                   any
		claimOwner, blockedReason                                                             sql.NullString
		claimGeneration                                                                       uint64
		capsuleRaw                                                                            []byte
		retryAtRaw                                                                            any
		retryFailureRaw                                                                       []byte
	)
	if err := row.Scan(
		&runID, &deliveryID, &flowPath, &family, &semanticPath, &bundleHash, &digest,
		&sourceKind, &sourceEventID, &sourceRunID, &sourceEntityID, &sourceField, &sourceMutationID,
		&resourceFlowPath, &resourceEvent, &resourceVersion,
		&cardinality, &cursor, &status, &nextChunk, &lastServedRaw, &createdAtRaw, &updatedAtRaw,
		&claimOwner, &claimGeneration, &leaseRaw, &blockedReason, &capsuleRaw, &retryAtRaw, &retryFailureRaw,
	); err != nil {
		return fanoutobligation.Intent{}, err
	}
	lastServed, _, err := sqliteTimeValue(lastServedRaw)
	if err != nil {
		return fanoutobligation.Intent{}, fmt.Errorf("decode fan-out last-served time: %w", err)
	}
	createdAt, created, err := sqliteTimeValue(createdAtRaw)
	if err != nil || !created {
		return fanoutobligation.Intent{}, fmt.Errorf("decode fan-out created time: %w", err)
	}
	updatedAt, updated, err := sqliteTimeValue(updatedAtRaw)
	if err != nil || !updated {
		return fanoutobligation.Intent{}, fmt.Errorf("decode fan-out updated time: %w", err)
	}
	lease, _, err := sqliteTimeValue(leaseRaw)
	if err != nil {
		return fanoutobligation.Intent{}, fmt.Errorf("decode fan-out lease time: %w", err)
	}
	var capsule fanoutobligation.Capsule
	if err := canonicaljson.DecodePreservingNumberLexemes(capsuleRaw, &capsule); err != nil {
		return fanoutobligation.Intent{}, fmt.Errorf("decode fan-out capsule: %w", err)
	}
	source := fanoutobligation.SourceRef{
		Kind: fanoutobligation.SourceKind(sourceKind.String), EventID: sourceEventID.String,
		RunID: sourceRunID.String, EntityID: sourceEntityID.String, Field: sourceField.String, MutationID: sourceMutationID.String,
		Declaration: durableDeclarationRef(resourceFlowPath.String, resourceEvent.String),
		VersionID:   durableVersionID(resourceVersion.String),
	}
	requestSource := source
	if requestSource.Kind == fanoutobligation.SourceEntityField {
		requestSource.MutationID = ""
	}
	intent := fanoutobligation.Intent{
		Request: fanoutobligation.IntentRequest{
			Key:     fanoutobligation.IntentKey{RunID: runID, TriggeringDeliveryID: deliveryID, ElementRef: runtimeFanOutElementRef(flowPath, family, semanticPath)},
			PlanRef: runtimeFanOutPlanRef(bundleHash, flowPath, family, semanticPath, digest), Source: requestSource, Cardinality: cardinality, Capsule: capsule,
		},
		Source: source, Cursor: cursor, Status: fanoutobligation.Status(status), NextChunkSize: nextChunk,
		LastServedAt: lastServed, CreatedAt: createdAt.UTC(), UpdatedAt: updatedAt.UTC(),
		ClaimOwner: claimOwner.String, ClaimGeneration: claimGeneration, LeaseExpiresAt: lease, BlockedReason: blockedReason.String,
	}
	if retryAtRaw != nil || retryFailureRaw != nil {
		at, present, err := sqliteTimeValue(retryAtRaw)
		if err != nil {
			return fanoutobligation.Intent{}, fmt.Errorf("fan-out retry requires valid due time: %w", err)
		}
		if !present {
			return fanoutobligation.Intent{}, errors.New("fan-out retry requires due time")
		}
		failure, err := runtimefailures.UnmarshalEnvelope(retryFailureRaw)
		if err != nil {
			return fanoutobligation.Intent{}, fmt.Errorf("fan-out retry failure: %w", err)
		}
		intent.Retry = &fanoutobligation.RetryWait{ReadyAt: at, Failure: failure}
	}
	if err := intent.Validate(); err != nil {
		return fanoutobligation.Intent{}, err
	}
	return intent, nil
}

func durableDeclarationRef(flowPath, eventName string) durabledata.DeclarationRef {
	return durabledata.DeclarationRef{FlowPath: strings.TrimSpace(flowPath), EventName: strings.TrimSpace(eventName)}
}

func durableVersionID(raw string) durabledata.VersionID {
	return durabledata.VersionID(strings.TrimSpace(raw))
}

func runtimeFanOutElementRef(flowPath, family, semanticPath string) runtimecontracts.FanOutElementRef {
	return runtimecontracts.FanOutElementRef{FlowPath: strings.TrimSpace(flowPath), Family: strings.TrimSpace(family), SemanticPath: strings.TrimSpace(semanticPath)}
}

func runtimeFanOutPlanRef(bundleHash, flowPath, family, semanticPath, digest string) runtimecontracts.FanOutPlanRef {
	return runtimecontracts.FanOutPlanRef{BundleHash: strings.TrimSpace(bundleHash), ElementRef: runtimeFanOutElementRef(flowPath, family, semanticPath), SemanticDigest: strings.TrimSpace(digest)}
}

func (s *fanOutPostgresOwner) ClaimFanOutIntent(ctx context.Context, request runtimepipeline.FanOutClaimRequest) (intent fanoutobligation.Intent, claim fanoutobligation.Claim, found bool, err error) {
	if s == nil || s.backend == nil {
		return intent, claim, false, fmt.Errorf("postgres fan-out owner is required")
	}
	if err := validateFanOutCandidate(request, s.grant); err != nil {
		return intent, claim, false, err
	}
	committed, err := s.backend.RunTransactionOutcome(ctx, func(txctx context.Context, tx *sql.Tx) error {
		transactiontest.Mark(txctx, transactiontest.FanOutClaim)
		ready, err := admitFanOutRun(txctx, tx, s.admission, s.grant, request.Candidate.RunID, true)
		if err != nil || !ready {
			return err
		}
		row := tx.QueryRowContext(txctx, `SELECT `+fanOutIntentColumns+` FROM fan_out_intents
			WHERE status='open' AND bundle_hash=$1 AND (claim_owner IS NULL OR lease_expires_at <= clock_timestamp())
			AND (retry_ready_at IS NULL OR retry_ready_at <= clock_timestamp())
			AND run_id=$2 AND triggering_delivery_id=$3 AND flow_path=$4 AND declaration_family=$5 AND semantic_path=$6
			FOR UPDATE SKIP LOCKED`, request.BundleHash, request.Candidate.RunID, request.Candidate.TriggeringDeliveryID,
			request.Candidate.ElementRef.FlowPath, request.Candidate.ElementRef.Family, request.Candidate.ElementRef.SemanticPath)
		var scanErr error
		intent, scanErr = scanFanOutIntent(row)
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil
		}
		if scanErr != nil {
			return scanErr
		}
		admittedAt, clockErr := fanOutAdmissionTime(txctx, tx, true, nil)
		if clockErr != nil {
			return clockErr
		}
		found = true
		return claimFanOutIntentRow(txctx, tx, request, admittedAt, &intent, &claim)
	})
	if !committed {
		return fanoutobligation.Intent{}, fanoutobligation.Claim{}, false, err
	}
	return intent, claim, found, err
}

func (s *fanOutSQLiteOwner) ClaimFanOutIntent(ctx context.Context, request runtimepipeline.FanOutClaimRequest) (intent fanoutobligation.Intent, claim fanoutobligation.Claim, found bool, err error) {
	if s == nil || s.backend == nil {
		return intent, claim, false, fmt.Errorf("sqlite fan-out owner is required")
	}
	if err := validateFanOutCandidate(request, s.grant); err != nil {
		return intent, claim, false, err
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	committed, err := s.backend.RunTransactionOutcome(ctx, "claim fan-out intent", func(txctx context.Context, tx *sql.Tx) error {
		transactiontest.Mark(txctx, transactiontest.FanOutClaim)
		ready, err := admitFanOutRun(txctx, tx, s.admission, s.grant, request.Candidate.RunID, true)
		if err != nil || !ready {
			return err
		}
		admittedAt, clockErr := fanOutAdmissionTime(txctx, tx, false, s.now)
		if clockErr != nil {
			return clockErr
		}
		row := tx.QueryRowContext(txctx, `SELECT `+fanOutIntentColumns+` FROM fan_out_intents
			WHERE status='open' AND bundle_hash=? AND (claim_owner IS NULL OR lease_expires_at <= ?)
			AND (retry_ready_at IS NULL OR retry_ready_at <= ?)
			AND run_id=? AND triggering_delivery_id=? AND flow_path=? AND declaration_family=? AND semantic_path=?`,
			request.BundleHash, admittedAt, admittedAt, request.Candidate.RunID, request.Candidate.TriggeringDeliveryID,
			request.Candidate.ElementRef.FlowPath, request.Candidate.ElementRef.Family, request.Candidate.ElementRef.SemanticPath)
		var scanErr error
		intent, scanErr = scanFanOutIntent(row)
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil
		}
		if scanErr != nil {
			return scanErr
		}
		found = true
		return claimFanOutIntentRow(txctx, tx, request, admittedAt, &intent, &claim)
	})
	if !committed {
		return fanoutobligation.Intent{}, fanoutobligation.Claim{}, false, err
	}
	return intent, claim, found, err
}

func claimFanOutIntentRow(ctx context.Context, tx *sql.Tx, request runtimepipeline.FanOutClaimRequest, admittedAt time.Time, intent *fanoutobligation.Intent, claim *fanoutobligation.Claim) error {
	state, err := intent.ServingAt(admittedAt)
	if err != nil {
		return err
	}
	if state != fanoutobligation.ServingEligible {
		return fanoutobligation.ErrStaleClaim
	}
	lease := admittedAt.Add(request.Lease).UTC()
	nextGeneration := intent.ClaimGeneration + 1
	result, err := tx.ExecContext(ctx, `UPDATE fan_out_intents SET claim_owner=$1,claim_generation=$2,lease_expires_at=$3,updated_at=$4,retry_ready_at=NULL,retry_failure=NULL
		WHERE run_id=$5 AND triggering_delivery_id=$6 AND flow_path=$7 AND declaration_family=$8 AND semantic_path=$9 AND status='open' AND claim_generation=$10`,
		request.Owner, nextGeneration, lease, admittedAt, intent.Request.Key.RunID, intent.Request.Key.TriggeringDeliveryID,
		intent.Request.Key.ElementRef.FlowPath, intent.Request.Key.ElementRef.Family, intent.Request.Key.ElementRef.SemanticPath, intent.ClaimGeneration)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		if err != nil {
			return err
		}
		return fanoutobligation.ErrStaleClaim
	}
	intent.ClaimOwner = request.Owner
	intent.ClaimGeneration = nextGeneration
	intent.LeaseExpiresAt = lease
	intent.UpdatedAt = admittedAt
	intent.Retry = nil
	*claim = fanoutobligation.Claim{Key: intent.Request.Key, Owner: request.Owner, Generation: nextGeneration, LeaseUntil: lease}
	return nil
}

func (s *fanOutPostgresOwner) LoadFanOutEvaluation(ctx context.Context, claim fanoutobligation.Claim) (runtimepipeline.FanOutEvaluationInput, error) {
	var intent fanoutobligation.Intent
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		transactiontest.Mark(ctx, transactiontest.FanOutLoad)
		var err error
		intent, err = observeFanOutClaim(ctx, tx, s.admission, s.grant, true, claim, time.Now)
		return err
	})
	if err != nil {
		return runtimepipeline.FanOutEvaluationInput{}, err
	}
	return loadFanOutEvaluation(ctx, s.backend.ConstructionHandle(), true, intent)
}

func (s *fanOutSQLiteOwner) LoadFanOutEvaluation(ctx context.Context, claim fanoutobligation.Claim) (runtimepipeline.FanOutEvaluationInput, error) {
	var intent fanoutobligation.Intent
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		transactiontest.Mark(ctx, transactiontest.FanOutLoad)
		var err error
		intent, err = observeFanOutClaim(ctx, tx, s.admission, s.grant, false, claim, s.now)
		return err
	})
	if err != nil {
		return runtimepipeline.FanOutEvaluationInput{}, err
	}
	return loadFanOutEvaluation(ctx, s.backend.ConstructionHandle(), false, intent)
}

// Hydrate the immutable source after admission, outside the SQLite writer lock.
func loadFanOutEvaluation(ctx context.Context, db *sql.DB, postgres bool, intent fanoutobligation.Intent) (runtimepipeline.FanOutEvaluationInput, error) {
	var input runtimepipeline.FanOutEvaluationInput
	var err error
	var triggers []events.PersistedReplayEvent
	if postgres {
		triggers, err = hydratePostgresPersistedReplayEvents(ctx, db, []string{intent.Request.Capsule.Lineage.ParentEventID})
	} else {
		triggers, err = hydrateSQLitePersistedReplayEvents(ctx, db, []string{intent.Request.Capsule.Lineage.ParentEventID})
	}
	if err != nil || len(triggers) != 1 || triggers[0].ReplayFailure != nil {
		if err == nil {
			err = fmt.Errorf("fan-out triggering event hydration produced %d canonical records", len(triggers))
		}
		return input, err
	}
	input.Trigger = triggers[0].Event
	triggerInLineage, err := fanoutorigin.SourceRunInLineage(ctx, db, postgres, intent.Request.Key.RunID, input.Trigger.RunID())
	if err != nil {
		return input, err
	}
	if !triggerInLineage {
		return input, fmt.Errorf("fan-out triggering event run %s is outside intent run %s fork lineage", input.Trigger.RunID(), intent.Request.Key.RunID)
	}
	input.StartOrdinal = intent.Cursor
	endOrdinal := intent.ChunkEndOrdinal()
	var raw []byte
	switch intent.Source.Kind {
	case fanoutobligation.SourceEventPayloadField:
		if input.Trigger.ID() != intent.Source.EventID {
			return input, fmt.Errorf("fan-out payload source disagrees with triggering event")
		}
		input.Items, err = collectionFieldRangeFromJSON(input.Trigger.Payload(), intent.Source.Field, intent.Request.Cardinality, intent.Cursor, endOrdinal)
	case fanoutobligation.SourceEntityField:
		inLineage, lineageErr := fanoutorigin.SourceRunInLineage(ctx, db, postgres, intent.Request.Key.RunID, intent.Source.RunID)
		if lineageErr != nil {
			return input, lineageErr
		}
		if !inLineage {
			return input, fmt.Errorf("fan-out entity source run %s is outside intent run %s fork lineage", intent.Source.RunID, intent.Request.Key.RunID)
		}
		if err := db.QueryRowContext(ctx, `SELECT new_value FROM entity_mutations WHERE mutation_id=$1 AND run_id=$2 AND entity_id=$3 AND domain='authored_field' AND path=$4`, intent.Source.MutationID, intent.Source.RunID, intent.Source.EntityID, intent.Source.Field).Scan(&raw); err != nil {
			return input, err
		}
		input.Items, err = collectionRangeFromJSON(raw, intent.Request.Cardinality, intent.Cursor, endOrdinal)
	case fanoutobligation.SourceResourceVersion:
		if err := db.QueryRowContext(ctx, `SELECT v.canonical_jsonl FROM resource_versions v JOIN resource_version_pins p ON p.version_id=v.version_id AND p.run_id=$1 AND p.flow_path=$2 AND p.event_name=$3 WHERE v.version_id=$4 AND v.pruned_at IS NULL`, intent.Request.Key.RunID, intent.Source.Declaration.FlowPath, intent.Source.Declaration.EventName, intent.Source.VersionID).Scan(&raw); err != nil {
			return input, err
		}
		input.Items, err = collectionRangeFromJSONL(raw, intent.Request.Cardinality, intent.Cursor, endOrdinal)
	default:
		return input, fmt.Errorf("unsupported fan-out source kind %q", intent.Source.Kind)
	}
	if err != nil {
		return input, err
	}
	if err := input.Validate(intent); err != nil {
		return input, err
	}
	return input, nil
}

func collectionFieldRangeFromJSON(raw []byte, field string, want, start, end int) ([]any, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, err
	}
	value, ok := object[strings.TrimSpace(field)]
	if !ok {
		return nil, fmt.Errorf("fan-out source field %s is absent", strings.TrimSpace(field))
	}
	return collectionRangeFromJSON(value, want, start, end)
}

func collectionRangeFromJSON(raw []byte, want, start, end int) ([]any, error) {
	if start < 0 || end < start || end > want {
		return nil, fmt.Errorf("fan-out source range [%d,%d) is invalid for cardinality %d", start, end, want)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil || token != json.Delim('[') {
		if err == nil {
			err = fmt.Errorf("source value is not a collection")
		}
		return nil, fmt.Errorf("decode fan-out source collection: %w", err)
	}
	items := make([]any, 0, end-start)
	count := 0
	for decoder.More() {
		var item any
		if err := decoder.Decode(&item); err != nil {
			return nil, fmt.Errorf("decode fan-out source item %d: %w", count, err)
		}
		if count >= start && count < end {
			items = append(items, item)
		}
		count++
	}
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("close fan-out source collection: %w", err)
	}
	if count != want {
		return nil, fmt.Errorf("fan-out immutable source cardinality = %d, want %d", count, want)
	}
	return items, nil
}

func collectionRangeFromJSONL(raw []byte, want, start, end int) ([]any, error) {
	if start < 0 || end < start || end > want {
		return nil, fmt.Errorf("fan-out resource range [%d,%d) is invalid for cardinality %d", start, end, want)
	}
	items := make([]any, 0, end-start)
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 64*1024), durabledata.MaxCanonicalRowBytes+1)
	count := 0
	for scanner.Scan() {
		admitted, err := canonicaljson.Decode(scanner.Bytes())
		if err != nil {
			return nil, err
		}
		item, err := workflowexpr.ProjectSemanticValue(admitted)
		if err != nil {
			return nil, err
		}
		if count >= start && count < end {
			items = append(items, item)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if count != want {
		return nil, fmt.Errorf("fan-out immutable resource cardinality = %d, want %d", count, want)
	}
	return items, nil
}

func (s *fanOutPostgresOwner) ReleaseFanOutClaim(ctx context.Context, claim fanoutobligation.Claim) error {
	return releaseFanOutClaim(ctx, s.backend, "", true, time.Now, claim, s.grant.BundleHash, func(ctx context.Context, tx *sql.Tx) error {
		return s.admission.AdmitFanOutCleanupTx(ctx, tx, s.grant)
	})
}

func (s *fanOutSQLiteOwner) ReleaseFanOutClaim(ctx context.Context, claim fanoutobligation.Claim) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	return releaseFanOutClaim(ctx, s.backend, "release fan-out claim", false, s.now, claim, s.grant.BundleHash, func(ctx context.Context, tx *sql.Tx) error {
		return s.admission.AdmitFanOutCleanupTx(ctx, tx, s.grant)
	})
}

func (s *fanOutPostgresOwner) ReleaseFanOutRetryable(ctx context.Context, request runtimepipeline.FanOutRetryableRelease) error {
	return releaseFanOutRetryable(ctx, s.backend, "", true, time.Now, request, func(ctx context.Context, tx *sql.Tx) error {
		return admitFanOutClaim(ctx, tx, s.admission, s.grant, true, request.Claim)
	})
}

func (s *fanOutSQLiteOwner) ReleaseFanOutRetryable(ctx context.Context, request runtimepipeline.FanOutRetryableRelease) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	return releaseFanOutRetryable(ctx, s.backend, "release retryable fan-out claim", false, s.now, request, func(ctx context.Context, tx *sql.Tx) error {
		return admitFanOutClaim(ctx, tx, s.admission, s.grant, false, request.Claim)
	})
}

func releaseFanOutRetryable(ctx context.Context, backend any, label string, postgres bool, observeNow func() time.Time, request runtimepipeline.FanOutRetryableRelease, admit func(context.Context, *sql.Tx) error) error {
	if err := request.Validate(); err != nil {
		return err
	}
	operation := func(txctx context.Context, tx *sql.Tx) error {
		transactiontest.Mark(txctx, transactiontest.FanOutRetry)
		if err := admit(txctx, tx); err != nil {
			return err
		}
		if _, err := lockClaimedFanOutIntent(txctx, tx, postgres, request.Claim, observeNow); err != nil {
			return err
		}
		now, err := fanOutAdmissionTime(txctx, tx, postgres, observeNow)
		if err != nil {
			return err
		}
		failure, err := runtimefailures.MarshalEnvelope(request.Failure)
		if err != nil {
			return err
		}
		query := `UPDATE fan_out_intents
			SET claim_owner=NULL,lease_expires_at=NULL,last_served_at=$1,updated_at=$1,
				next_chunk_size=CASE WHEN next_chunk_size <= 1 THEN 1 ELSE (next_chunk_size + 1) / 2 END,
				retry_ready_at=$9,retry_failure=$10
			WHERE run_id=$2 AND triggering_delivery_id=$3 AND flow_path=$4 AND declaration_family=$5 AND semantic_path=$6 AND claim_owner=$7 AND claim_generation=$8`
		if postgres {
			query = strings.ReplaceAll(query, "retry_failure=$10", "retry_failure=$10::jsonb")
		}
		result, err := tx.ExecContext(txctx, query,
			now, request.Claim.Key.RunID,
			request.Claim.Key.TriggeringDeliveryID, request.Claim.Key.ElementRef.FlowPath, request.Claim.Key.ElementRef.Family, request.Claim.Key.ElementRef.SemanticPath,
			request.Claim.Owner, request.Claim.Generation, now.Add(time.Second), string(failure))
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil || rows != 1 {
			if err != nil {
				return err
			}
			return fanoutobligation.ErrStaleClaim
		}
		return nil
	}
	switch typed := backend.(type) {
	case interface {
		RunTransaction(context.Context, func(context.Context, *sql.Tx) error) error
	}:
		return typed.RunTransaction(ctx, operation)
	case interface {
		RunTransaction(context.Context, string, func(context.Context, *sql.Tx) error) error
	}:
		return typed.RunTransaction(ctx, label, operation)
	default:
		return fmt.Errorf("fan-out transaction owner is unavailable")
	}
}

func blockFanOutClaim(ctx context.Context, tx *sql.Tx, postgres bool, observeNow func() time.Time, effects *revisionEffects, request runtimepipeline.FanOutBlockRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if _, err := lockClaimedFanOutIntent(ctx, tx, postgres, request.Claim, observeNow); err != nil {
		return err
	}
	now, err := fanOutAdmissionTime(ctx, tx, postgres, observeNow)
	if err != nil {
		return err
	}
	failure, err := runtimefailures.MarshalEnvelope(request.Failure)
	if err != nil {
		return err
	}
	query := `UPDATE fan_out_intents SET status='blocked',blocked_reason=$1,claim_owner=NULL,lease_expires_at=NULL,last_served_at=$2,updated_at=$2
		WHERE run_id=$3 AND triggering_delivery_id=$4 AND flow_path=$5 AND declaration_family=$6 AND semantic_path=$7 AND status='open' AND claim_owner=$8 AND claim_generation=$9`
	if !postgres {
		query = postgresPlaceholdersToSQLite(query, 9)
	}
	result, err := tx.ExecContext(ctx, query, string(failure), now, request.Claim.Key.RunID,
		request.Claim.Key.TriggeringDeliveryID, request.Claim.Key.ElementRef.FlowPath, request.Claim.Key.ElementRef.Family, request.Claim.Key.ElementRef.SemanticPath,
		request.Claim.Owner, request.Claim.Generation)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		if err != nil {
			return err
		}
		return fanoutobligation.ErrStaleClaim
	}
	ref, err := privaterunforkrevision.FanOutIntentFact(request.Claim.Key)
	if err != nil {
		return err
	}
	return effects.AddFacts(request.Claim.Key.RunID, ref)
}

func (s *fanOutPostgresOwner) BlockFanOutClaim(ctx context.Context, request runtimepipeline.FanOutBlockRequest) error {
	effects := newRevisionEffects()
	return s.runPrivateAuthorActivityMutation(ctx, effects, func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
		transactiontest.Mark(txctx, transactiontest.FanOutBlock)
		if err := admitFanOutClaim(txctx, tx, s.admission, s.grant, true, request.Claim); err != nil {
			return err
		}
		return blockFanOutClaim(txctx, tx, true, time.Now, effects, request)
	})
}

func (s *fanOutSQLiteOwner) BlockFanOutClaim(ctx context.Context, request runtimepipeline.FanOutBlockRequest) error {
	effects := newRevisionEffects()
	return s.runPrivateAuthorActivityMutation(ctx, "block fan-out claim", effects, func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
		transactiontest.Mark(txctx, transactiontest.FanOutBlock)
		if err := admitFanOutClaim(txctx, tx, s.admission, s.grant, false, request.Claim); err != nil {
			return err
		}
		return blockFanOutClaim(txctx, tx, false, s.now, effects, request)
	})
}

type transactionRunner interface {
	RunTransaction(context.Context, func(context.Context, *sql.Tx) error) error
}

func releaseFanOutClaim(ctx context.Context, backend any, label string, postgres bool, observeNow func() time.Time, claim fanoutobligation.Claim, bundleHash string, admit func(context.Context, *sql.Tx) error) error {
	if err := claim.Validate(); err != nil {
		return err
	}
	operation := func(txctx context.Context, tx *sql.Tx) error {
		transactiontest.Mark(txctx, transactiontest.FanOutRelease)
		if err := admit(txctx, tx); err != nil {
			return err
		}
		if err := requireFanOutClaimBundle(txctx, tx, postgres, claim, bundleHash); err != nil {
			return err
		}
		now, err := fanOutAdmissionTime(txctx, tx, postgres, observeNow)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(txctx, `UPDATE fan_out_intents SET claim_owner=NULL,lease_expires_at=NULL,last_served_at=$1,updated_at=$1 WHERE run_id=$2 AND triggering_delivery_id=$3 AND flow_path=$4 AND declaration_family=$5 AND semantic_path=$6 AND claim_owner=$7 AND claim_generation=$8`, now, claim.Key.RunID, claim.Key.TriggeringDeliveryID, claim.Key.ElementRef.FlowPath, claim.Key.ElementRef.Family, claim.Key.ElementRef.SemanticPath, claim.Owner, claim.Generation)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil || rows != 1 {
			return fanoutobligation.ErrStaleClaim
		}
		return nil
	}
	switch typed := backend.(type) {
	case interface {
		RunTransaction(context.Context, func(context.Context, *sql.Tx) error) error
	}:
		return typed.RunTransaction(ctx, operation)
	case interface {
		RunTransaction(context.Context, string, func(context.Context, *sql.Tx) error) error
	}:
		return typed.RunTransaction(ctx, label, operation)
	default:
		return fmt.Errorf("fan-out transaction owner is unavailable")
	}
}

func commitFanOutChunk(
	ctx context.Context,
	store eventCommitTxStore,
	postgres bool,
	run func(context.Context, *revisionEffects, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) (bool, error),
	observeNow func() time.Time,
	prepare func(*runLifecycleCandidateHandoffReservation, runtimerunlifecycle.CandidateRequestResult) error,
	requestCandidate func(context.Context, *sql.Tx, string) (runtimerunlifecycle.CandidateRequestResult, error),
	readback pipelineQueryer,
	command runtimepipeline.FanOutChunkCommand,
) (runtimepipeline.CommittedFanOutChunk, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedFanOutChunk{}, err
	}
	handoff, err := reserveRunLifecycleCandidateHandoff(ctx)
	if err != nil {
		return runtimepipeline.CommittedFanOutChunk{}, err
	}
	defer handoff.Rollback()
	effects := newRevisionEffects()
	result := runtimepipeline.CommittedFanOutChunk{Publications: make([]runtimeengine.CommittedDurablePublication, 0, len(command.Outcomes))}
	operationComplete := false
	committed, err := run(ctx, effects, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) (resultErr error) {
		if err := handoff.ResetAttempt(); err != nil {
			return err
		}
		result = runtimepipeline.CommittedFanOutChunk{Publications: make([]runtimeengine.CommittedDurablePublication, 0, len(command.Outcomes))}
		operationComplete = false
		// Each retry owns a fresh handle; no outcome or admission facts survive it.
		var outcomeInsert *sql.Stmt
		defer func() {
			if outcomeInsert != nil {
				if err := outcomeInsert.Close(); err != nil && resultErr == nil {
					resultErr = err
					operationComplete = false
				}
			}
		}()
		intent, err := lockClaimedFanOutIntent(txctx, tx, postgres, command.Claim, observeNow)
		if err != nil {
			return err
		}
		if len(command.Outcomes) > intent.NextChunkSize || intent.Cursor+len(command.Outcomes) > intent.Request.Cardinality {
			return fmt.Errorf("fan-out chunk exceeds claimed range")
		}
		var trigger events.Event
		for _, outcome := range command.Outcomes {
			if outcome.Publication == nil {
				continue
			}
			var records []events.PersistedReplayEvent
			if postgres {
				records, err = hydratePostgresPersistedReplayEvents(txctx, tx, []string{intent.Request.Capsule.Lineage.ParentEventID})
			} else {
				records, err = hydrateSQLitePersistedReplayEvents(txctx, tx, []string{intent.Request.Capsule.Lineage.ParentEventID})
			}
			if err != nil {
				return err
			}
			if len(records) != 1 || records[0].ReplayFailure != nil {
				return fmt.Errorf("fan-out chunk requires the exact immutable trigger")
			}
			trigger = records[0].Event
			inLineage, err := fanoutorigin.SourceRunInLineage(txctx, tx, postgres, intent.Request.Key.RunID, trigger.RunID())
			if err != nil {
				return err
			}
			if !inLineage {
				return fmt.Errorf("fan-out chunk trigger is outside the destination fork lineage")
			}
			break
		}
		for index, outcome := range command.Outcomes {
			wantOrdinal := intent.Cursor + index
			if outcome.Ordinal != wantOrdinal {
				return fmt.Errorf("fan-out chunk ordinal %d = %d, want contiguous %d", index, outcome.Ordinal, wantOrdinal)
			}
			kind := fanoutobligation.OutcomeSemanticRejected
			var eventID string
			failure := any(nil)
			if outcome.Publication != nil {
				plan, ok := outcome.Publication.(runtimebus.EnginePublicationPlan)
				if !ok {
					return fmt.Errorf("fan-out publication %d has unexpected type %T", index, outcome.Publication)
				}
				projection, err := fanoutobligation.PrepareOrdinalEmission(intent, trigger, outcome.Ordinal)
				if err != nil {
					return err
				}
				committed, err := store.commitFanOutPublicationTx(txctx, tx, story, effects, plan.PublicationCommand(), projection, handoff)
				if err != nil {
					return fmt.Errorf("commit fan-out publication ordinal %d: %w", outcome.Ordinal, err)
				}
				evidence, err := runtimebus.NewCommittedEnginePublication(plan, committed)
				if err != nil {
					return err
				}
				result.Publications = append(result.Publications, evidence)
				kind = fanoutobligation.OutcomeCommitted
				eventID = evidence.CommittedDurablePublicationEventID()
			} else {
				failure = string(outcome.Failure)
			}
			if outcomeInsert == nil {
				query := `INSERT INTO fan_out_outcomes (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,event_id,source_event_id,inherited_disposition,failure,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,NULLIF($8,''),NULL,NULL,$9,$10)`
				if postgres {
					query = strings.ReplaceAll(query, "NULLIF($8,'')", "NULLIF($8,'')::uuid")
					query = strings.ReplaceAll(query, "$9", "$9::jsonb")
				} else {
					query = postgresPlaceholdersToSQLite(query, 10)
				}
				outcomeInsert, err = tx.PrepareContext(txctx, query)
				if err != nil {
					return fmt.Errorf("insert fan-out outcome ordinal %d: %w", outcome.Ordinal, err)
				}
			}
			if _, err := outcomeInsert.ExecContext(txctx, command.Claim.Key.RunID, command.Claim.Key.TriggeringDeliveryID, command.Claim.Key.ElementRef.FlowPath, command.Claim.Key.ElementRef.Family, command.Claim.Key.ElementRef.SemanticPath, outcome.Ordinal, string(kind), nullableText(eventID), failure, command.Now.UTC()); err != nil {
				return fmt.Errorf("insert fan-out outcome ordinal %d: %w", outcome.Ordinal, err)
			}
			ref, err := privaterunforkrevision.FanOutOutcomeFact(command.Claim.Key, outcome.Ordinal)
			if err != nil {
				return err
			}
			if err := effects.AddFacts(command.Claim.Key.RunID, ref); err != nil {
				return err
			}
		}
		nextCursor := intent.Cursor + len(command.Outcomes)
		status := fanoutobligation.StatusOpen
		if nextCursor == intent.Request.Cardinality {
			status = fanoutobligation.StatusClosed
		}
		servedAt, err := fanOutAdmissionTime(txctx, tx, postgres, observeNow)
		if err != nil {
			return err
		}
		update, err := tx.ExecContext(txctx, `UPDATE fan_out_intents SET cursor=$1,status=$2,updated_at=$3,last_served_at=$3,next_chunk_size=$11,claim_owner=NULL,lease_expires_at=NULL WHERE run_id=$4 AND triggering_delivery_id=$5 AND flow_path=$6 AND declaration_family=$7 AND semantic_path=$8 AND claim_owner=$9 AND claim_generation=$10 AND status='open'`, nextCursor, string(status), servedAt, command.Claim.Key.RunID, command.Claim.Key.TriggeringDeliveryID, command.Claim.Key.ElementRef.FlowPath, command.Claim.Key.ElementRef.Family, command.Claim.Key.ElementRef.SemanticPath, command.Claim.Owner, command.Claim.Generation, fanoutobligation.MaxChunkSize)
		if err != nil {
			return err
		}
		rows, err := update.RowsAffected()
		if err != nil || rows != 1 {
			return fanoutobligation.ErrStaleClaim
		}
		ref, err := privaterunforkrevision.FanOutIntentFact(command.Claim.Key)
		if err != nil {
			return err
		}
		if err := effects.AddFacts(command.Claim.Key.RunID, ref); err != nil {
			return err
		}
		intent.Cursor, intent.Status, intent.UpdatedAt = nextCursor, status, servedAt
		intent.LastServedAt, intent.NextChunkSize = servedAt, fanoutobligation.MaxChunkSize
		intent.ClaimOwner, intent.LeaseExpiresAt = "", time.Time{}
		result.Intent = intent
		if status == fanoutobligation.StatusClosed {
			candidate, err := requestCandidate(txctx, tx, command.Claim.Key.RunID)
			if err != nil {
				return err
			}
			if err := prepare(handoff, candidate); err != nil {
				return err
			}
		}
		operationComplete = true
		return nil
	})
	if !committed {
		if err == nil || !operationComplete {
			return runtimepipeline.CommittedFanOutChunk{}, err
		}
		readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		committed, readErr := reconcileFanOutChunk(readCtx, readback, postgres, command)
		if readErr != nil {
			return runtimepipeline.CommittedFanOutChunk{}, runtimefailures.Wrap(
				runtimefailures.ClassOutcomeUncertain,
				"fan_out_chunk_commit_unconfirmed",
				"runtime.fan_out",
				"reconcile_chunk_commit",
				map[string]any{"intent_id": command.Claim.Key.String(), "claim_generation": command.Claim.Generation},
				errors.Join(err, readErr),
			)
		}
		if !committed {
			return runtimepipeline.CommittedFanOutChunk{}, err
		}
	}
	// Keep post-commit failures out of the runtime's mutation retry path.
	result.PostCommitFailure = err
	if err := handoff.Commit(); err != nil {
		result.PostCommitFailure = errors.Join(result.PostCommitFailure, err)
	}
	return result, nil
}

func reconcileFanOutChunk(ctx context.Context, db pipelineQueryer, postgres bool, command runtimepipeline.FanOutChunkCommand) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("fan-out chunk readback owner is required")
	}
	intent, err := scanFanOutIntent(db.QueryRowContext(ctx, `SELECT `+fanOutIntentColumns+` FROM fan_out_intents WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`, command.Claim.Key.RunID, command.Claim.Key.TriggeringDeliveryID, command.Claim.Key.ElementRef.FlowPath, command.Claim.Key.ElementRef.Family, command.Claim.Key.ElementRef.SemanticPath))
	if err != nil {
		return false, fmt.Errorf("read fan-out intent after unconfirmed commit: %w", err)
	}
	start := command.Outcomes[0].Ordinal
	end := start + len(command.Outcomes)
	query := `SELECT ordinal,outcome_kind,COALESCE(event_id::text,''),COALESCE(source_event_id::text,''),COALESCE(inherited_disposition,''),failure FROM fan_out_outcomes WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5 AND ordinal>=$6 AND ordinal<$7 ORDER BY ordinal`
	if !postgres {
		query = strings.ReplaceAll(query, "event_id::text", "event_id")
		query = strings.ReplaceAll(query, "source_event_id::text", "source_event_id")
	}
	rows, err := db.QueryContext(ctx, query, command.Claim.Key.RunID, command.Claim.Key.TriggeringDeliveryID, command.Claim.Key.ElementRef.FlowPath, command.Claim.Key.ElementRef.Family, command.Claim.Key.ElementRef.SemanticPath, start, end)
	if err != nil {
		return false, fmt.Errorf("read fan-out outcomes after unconfirmed commit: %w", err)
	}
	defer rows.Close()
	type persistedOutcome struct {
		ordinal                                       int
		kind, eventID, sourceID, inheritedDisposition string
		failure                                       any
	}
	persisted := make([]persistedOutcome, 0, len(command.Outcomes))
	for rows.Next() {
		var outcome persistedOutcome
		if err := rows.Scan(&outcome.ordinal, &outcome.kind, &outcome.eventID, &outcome.sourceID, &outcome.inheritedDisposition, &outcome.failure); err != nil {
			return false, err
		}
		persisted = append(persisted, outcome)
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if len(persisted) == 0 && intent.Cursor == start && intent.Status == fanoutobligation.StatusOpen && intent.ClaimOwner == command.Claim.Owner && intent.ClaimGeneration == command.Claim.Generation {
		return false, nil
	}
	// A successor may have advanced or canceled the mutable header after our
	// atomic release. Only the exact immutable outcome range proves our commit.
	if intent.Cursor < end || len(persisted) != len(command.Outcomes) {
		return false, fmt.Errorf("fan-out commit readback is neither exact commit nor exact no-commit")
	}
	for index, actual := range persisted {
		want := command.Outcomes[index]
		if actual.ordinal != want.Ordinal || actual.sourceID != "" || actual.inheritedDisposition != "" {
			return false, fmt.Errorf("fan-out commit readback disagrees at ordinal %d", want.Ordinal)
		}
		if want.Publication != nil {
			if actual.kind != string(fanoutobligation.OutcomeCommitted) || actual.eventID != want.Publication.DurablePublicationEventID() || actual.failure != nil {
				return false, fmt.Errorf("fan-out committed publication readback disagrees at ordinal %d", want.Ordinal)
			}
			continue
		}
		if actual.kind != string(fanoutobligation.OutcomeSemanticRejected) || actual.eventID != "" || !semanticJSONEqual(actual.failure, want.Failure) {
			return false, fmt.Errorf("fan-out semantic rejection readback disagrees at ordinal %d", want.Ordinal)
		}
	}
	return true, nil
}

func semanticJSONEqual(actual any, expected json.RawMessage) bool {
	var raw []byte
	switch value := actual.(type) {
	case nil:
		return false
	case []byte:
		raw = value
	case string:
		raw = []byte(value)
	default:
		var err error
		raw, err = json.Marshal(value)
		if err != nil {
			return false
		}
	}
	var left, right any
	leftDecoder := json.NewDecoder(bytes.NewReader(raw))
	leftDecoder.UseNumber()
	rightDecoder := json.NewDecoder(bytes.NewReader(expected))
	rightDecoder.UseNumber()
	return leftDecoder.Decode(&left) == nil && rightDecoder.Decode(&right) == nil && reflect.DeepEqual(left, right)
}

func nullableText(raw string) any {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	return strings.TrimSpace(raw)
}

func lockOwnedFanOutIntent(ctx context.Context, tx *sql.Tx, postgres bool, claim fanoutobligation.Claim) (fanoutobligation.Intent, error) {
	return loadOwnedFanOutIntentTx(ctx, tx, postgres, claim, true)
}

func loadOwnedFanOutIntentTx(ctx context.Context, tx *sql.Tx, postgres bool, claim fanoutobligation.Claim, lock bool) (fanoutobligation.Intent, error) {
	query := `SELECT ` + fanOutIntentColumns + ` FROM fan_out_intents WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`
	if postgres && lock {
		query += ` FOR UPDATE`
	}
	intent, err := scanFanOutIntent(tx.QueryRowContext(ctx, query, claim.Key.RunID, claim.Key.TriggeringDeliveryID, claim.Key.ElementRef.FlowPath, claim.Key.ElementRef.Family, claim.Key.ElementRef.SemanticPath))
	if errors.Is(err, sql.ErrNoRows) {
		return fanoutobligation.Intent{}, fanoutobligation.ErrStaleClaim
	}
	if err != nil {
		return fanoutobligation.Intent{}, err
	}
	if intent.Status != fanoutobligation.StatusOpen || intent.ClaimOwner != claim.Owner || intent.ClaimGeneration != claim.Generation {
		return fanoutobligation.Intent{}, fanoutobligation.ErrStaleClaim
	}
	return intent, nil
}

func lockClaimedFanOutIntent(ctx context.Context, tx *sql.Tx, postgres bool, claim fanoutobligation.Claim, observeNow func() time.Time) (fanoutobligation.Intent, error) {
	intent, err := lockOwnedFanOutIntent(ctx, tx, postgres, claim)
	if err != nil {
		return fanoutobligation.Intent{}, err
	}
	// Read the authorization clock after acquiring the row/writer lock. SQL's
	// transaction-start timestamp would allow an expired waiter to mutate.
	now, err := fanOutAdmissionTime(ctx, tx, postgres, observeNow)
	if err != nil {
		return fanoutobligation.Intent{}, err
	}
	if err := intent.AdmitClaim(claim, now); err != nil {
		return fanoutobligation.Intent{}, err
	}
	return intent, nil
}

func fanOutAdmissionTime(ctx context.Context, db pipelineQueryer, postgres bool, observeNow func() time.Time) (time.Time, error) {
	if postgres {
		var now time.Time
		if err := db.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return time.Time{}, err
		}
		return now.UTC(), nil
	}
	if observeNow == nil {
		return time.Time{}, fmt.Errorf("fan-out selected-store clock is required")
	}
	return observeNow().UTC(), nil
}

func (s *fanOutPostgresOwner) CommitFanOutChunk(ctx context.Context, command runtimepipeline.FanOutChunkCommand) (result runtimepipeline.CommittedFanOutChunk, resultErr error) {
	group := s.publicationGroups.get(command.Claim)
	if err := requireFanOutPublicationGroup(s.admission, group, command); err != nil {
		return result, err
	}
	if group != nil {
		unlock, err := group.lockAttempt(command)
		if err != nil {
			return result, err
		}
		defer unlock()
		defer func() {
			if resultErr == nil {
				group.committed = true
			}
		}()
	}
	return commitFanOutChunk(ctx, s, true,
		func(ctx context.Context, effects *revisionEffects, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) (bool, error) {
			run := s.runPrivateAuthorActivityMutationOutcome
			if group != nil {
				run = group.runPostgresPublication
			}
			var operationErr error
			committed, err := run(ctx, effects, func(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
				if group != nil {
					group.publicationTx.Store(tx)
					defer group.publicationTx.Store(nil)
				}
				transactiontest.Mark(ctx, transactiontest.FanOutChunk)
				if err := admitFanOutClaim(ctx, tx, s.admission, s.grant, true, command.Claim); err != nil {
					return err
				}
				operationErr = fn(ctx, tx, story)
				return operationErr
			})
			if group != nil && !committed && operationErr != nil && err == operationErr {
				_, group.restrictAfterRollback = runtimepipeline.FanOutSafeAggregateFailure(operationErr)
			}
			return committed, err
		},
		time.Now,
		func(reservation *runLifecycleCandidateHandoffReservation, result runtimerunlifecycle.CandidateRequestResult) error {
			return reservation.Prepare(s.runLifecycleCandidates, result)
		},
		func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.CandidateRequestResult, error) {
			return requestPostgresCompletionCandidateTx(ctx, tx, runID, nil, false)
		}, s.backend, command)
}

func (s *fanOutSQLiteOwner) CommitFanOutChunk(ctx context.Context, command runtimepipeline.FanOutChunkCommand) (result runtimepipeline.CommittedFanOutChunk, resultErr error) {
	group := s.publicationGroups.get(command.Claim)
	if err := requireFanOutPublicationGroup(s.admission, group, command); err != nil {
		return result, err
	}
	if group != nil {
		unlock, err := group.lockAttempt(command)
		if err != nil {
			return result, err
		}
		defer unlock()
		defer func() {
			if resultErr == nil {
				group.committed = true
			}
		}()
	}
	return commitFanOutChunk(ctx, s, false,
		func(ctx context.Context, effects *revisionEffects, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) (bool, error) {
			var operationErr error
			committed, err := s.runPrivateAuthorActivityMutationOutcome(ctx, "commit fan-out chunk", effects, func(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
				if group != nil {
					group.publicationTx.Store(tx)
					defer group.publicationTx.Store(nil)
				}
				transactiontest.Mark(ctx, transactiontest.FanOutChunk)
				if err := admitFanOutClaim(ctx, tx, s.admission, s.grant, false, command.Claim); err != nil {
					return err
				}
				operationErr = fn(ctx, tx, story)
				return operationErr
			})
			if group != nil && !committed && operationErr != nil && err == operationErr {
				_, group.restrictAfterRollback = runtimepipeline.FanOutSafeAggregateFailure(operationErr)
			}
			return committed, err
		},
		s.now,
		func(reservation *runLifecycleCandidateHandoffReservation, result runtimerunlifecycle.CandidateRequestResult) error {
			return reservation.Prepare(s.runLifecycleCandidates, result)
		},
		func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.CandidateRequestResult, error) {
			return requestSQLiteCompletionCandidateTx(ctx, tx, runID, nil, s.now(), false)
		}, s.backend, command)
}

func cancelRunFanOut(ctx context.Context, postgres bool, effects *revisionEffects, tx *sql.Tx, runID, reason string, at time.Time) error {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(reason) == "" || at.IsZero() {
		return fanOutCancellationFailure("validate_request", fmt.Errorf("fan-out cancellation requires run, reason, and time"))
	}
	query := `SELECT ` + fanOutIntentColumns + ` FROM fan_out_intents WHERE run_id=$1 AND status IN ('open','blocked')`
	if postgres {
		query += ` FOR UPDATE`
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return fanOutCancellationFailure("select_intents", err)
	}
	defer rows.Close()
	intents := make([]fanoutobligation.Intent, 0)
	for rows.Next() {
		intent, err := scanFanOutIntent(rows)
		if err != nil {
			return fanOutCancellationFailure("decode_intent", err)
		}
		intents = append(intents, intent)
	}
	if err := rows.Err(); err != nil {
		return fanOutCancellationFailure("select_intents", err)
	}
	for _, intent := range intents {
		update := `UPDATE fan_out_intents SET status='canceled',blocked_reason=$1,claim_owner=NULL,lease_expires_at=NULL,retry_ready_at=NULL,retry_failure=NULL,updated_at=$2 WHERE run_id=$3 AND triggering_delivery_id=$4 AND flow_path=$5 AND declaration_family=$6 AND semantic_path=$7 AND status IN ('open','blocked')`
		if !postgres {
			update = postgresPlaceholdersToSQLite(update, 7)
		}
		result, err := tx.ExecContext(ctx, update, reason, at.UTC(), runID, intent.Request.Key.TriggeringDeliveryID, intent.Request.Key.ElementRef.FlowPath, intent.Request.Key.ElementRef.Family, intent.Request.Key.ElementRef.SemanticPath)
		if err != nil {
			return fanOutCancellationFailure("cancel_intents", err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return fanOutCancellationFailure("cancel_intents", err)
		}
		if changed > 0 {
			ref, err := privaterunforkrevision.FanOutIntentFact(intent.Request.Key)
			if err != nil {
				return fanOutCancellationFailure("revision_effects", err)
			}
			if err := effects.AddFacts(runID, ref); err != nil {
				return fanOutCancellationFailure("revision_effects", err)
			}
		}
	}
	if err := suppressRunTerminalFanOutBarriersTx(ctx, tx, postgres, effects, runID, at); err != nil {
		return fanOutCancellationFailure("cancel_barriers", err)
	}
	return nil
}

func (s *PipelinePostgresOwner) CancelRunFanOut(ctx context.Context, runID, reason string, at time.Time) error {
	effects := newRevisionEffects()
	return s.runPrivateAuthorActivityMutation(ctx, effects, func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
		return cancelRunFanOut(txctx, true, effects, tx, runID, reason, at)
	})
}

func (s *PipelineSQLiteOwner) CancelRunFanOut(ctx context.Context, runID, reason string, at time.Time) error {
	effects := newRevisionEffects()
	return s.runPrivateAuthorActivityMutation(ctx, "cancel run fan-out", effects, func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
		return cancelRunFanOut(txctx, false, effects, tx, runID, reason, at)
	})
}

func fanOutRunSummary(ctx context.Context, db pipelineQueryer, postgres bool, runID string, now time.Time) (fanoutobligation.RunSummary, error) {
	summary := fanoutobligation.RunSummary{RunID: strings.TrimSpace(runID), BlockedIntents: make([]fanoutobligation.BlockedIntentDiagnosis, 0)}
	if summary.RunID == "" || now.IsZero() {
		return summary, fmt.Errorf("fan-out summary requires run and observation time")
	}
	var oldestRaw any
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN status='open' THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN status='blocked' THEN 1 ELSE 0 END),0),COALESCE(SUM(cardinality),0),COALESCE(SUM(cursor),0),COALESCE(SUM(CASE WHEN status IN ('open','blocked') THEN cardinality-cursor ELSE 0 END),0),COALESCE(MIN(next_chunk_size),0),COALESCE(MAX(next_chunk_size),0),MIN(CASE WHEN status IN ('open','blocked') THEN created_at END) FROM fan_out_intents WHERE run_id=$1`, summary.RunID).Scan(&summary.Intents, &summary.Open, &summary.Blocked, &summary.Cardinality, &summary.Cursor, &summary.Owed, &summary.MinNextChunk, &summary.MaxNextChunk, &oldestRaw); err != nil {
		return summary, err
	}
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(SUM(CASE WHEN outcome_kind='committed' THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN outcome_kind='semantic_rejected' THEN 1 ELSE 0 END),0) FROM fan_out_outcomes WHERE run_id=$1`, summary.RunID).Scan(&summary.Committed, &summary.SemanticRejected); err != nil {
		return summary, err
	}
	if summary.SemanticRejected > 0 {
		var sample fanoutobligation.FanOutSemanticRejectionSample
		var failureRaw any
		if err := db.QueryRowContext(ctx, `SELECT triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,failure FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='semantic_rejected' ORDER BY triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal LIMIT 1`, summary.RunID).Scan(
			&sample.TriggeringDeliveryID, &sample.FlowPath, &sample.Family, &sample.SemanticPath, &sample.Ordinal, &failureRaw,
		); err != nil {
			return summary, err
		}
		failure, err := runtimefailures.UnmarshalEnvelope(jsonRawMessageValue(failureRaw))
		if err != nil {
			return summary, fmt.Errorf("decode fan-out semantic rejection sample: %w", err)
		}
		sample.Failure = failure
		summary.SemanticRejectionSample = &sample
	}
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(SUM(CASE WHEN status='canceled' THEN cardinality-cursor ELSE 0 END),0) FROM fan_out_intents WHERE run_id=$1`, summary.RunID).Scan(&summary.Canceled); err != nil {
		return summary, err
	}
	if err := foldFanOutPublicationSettlement(ctx, db, postgres, summary.RunID, &summary); err != nil {
		return summary, err
	}
	barriers, err := summarizeFanOutDeliveryBarriersRun(ctx, db, summary.RunID)
	if err != nil {
		return summary, err
	}
	summary.BarrierArmed = barriers.Armed
	summary.BarrierPending = barriers.ClosedPending
	summary.BarrierTerminal = barriers.Terminal
	blocked, err := db.QueryContext(ctx, `SELECT triggering_delivery_id,flow_path,declaration_family,semantic_path,cursor,cardinality-cursor,blocked_reason FROM fan_out_intents WHERE run_id=$1 AND status='blocked' ORDER BY triggering_delivery_id,flow_path,declaration_family,semantic_path`, summary.RunID)
	if err != nil {
		return summary, err
	}
	defer blocked.Close()
	for blocked.Next() {
		var diagnosis fanoutobligation.BlockedIntentDiagnosis
		var raw any
		if err := blocked.Scan(&diagnosis.TriggeringDeliveryID, &diagnosis.FlowPath, &diagnosis.Family, &diagnosis.SemanticPath, &diagnosis.Cursor, &diagnosis.Owed, &raw); err != nil {
			return summary, err
		}
		failureRaw := []byte(fmt.Sprint(raw))
		if bytesValue, ok := raw.([]byte); ok {
			failureRaw = bytesValue
		}
		diagnosis.Failure, err = runtimefailures.UnmarshalEnvelope(failureRaw)
		if err != nil {
			return summary, fmt.Errorf("decode blocked fan-out diagnosis: %w", err)
		}
		summary.BlockedIntents = append(summary.BlockedIntents, diagnosis)
	}
	if err := blocked.Err(); err != nil {
		return summary, err
	}
	oldest, present, err := sqliteTimeValue(oldestRaw)
	if err != nil {
		return summary, fmt.Errorf("decode oldest fan-out time: %w", err)
	}
	if present && now.After(oldest) {
		summary.OldestAgeMS = now.Sub(oldest).Milliseconds()
	}
	return summary, summary.Validate()
}

func foldFanOutPublicationSettlement(ctx context.Context, db pipelineQueryer, postgres bool, runID string, summary *fanoutobligation.RunSummary) error {
	rows, err := db.QueryContext(ctx, `
		SELECT triggering_delivery_id, flow_path, declaration_family, semantic_path
		FROM fan_out_intents
		WHERE run_id=$1
		ORDER BY triggering_delivery_id, flow_path, declaration_family, semantic_path
	`, runID)
	if err != nil {
		return err
	}
	defer rows.Close()
	keys := make([]fanoutobligation.IntentKey, 0)
	for rows.Next() {
		var key fanoutobligation.IntentKey
		key.RunID = runID
		if err := rows.Scan(&key.TriggeringDeliveryID, &key.ElementRef.FlowPath, &key.ElementRef.Family, &key.ElementRef.SemanticPath); err != nil {
			return err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// PostgreSQL permits only one active result stream per transaction
	// connection. Finish the canonical outcome read before consulting each
	// event and delivery owner.
	if err := rows.Close(); err != nil {
		return err
	}
	for _, key := range keys {
		fold, err := foldFanOutIntentTerminalDispositions(ctx, db, postgres, key)
		if err != nil {
			return err
		}
		summary.Settled += fold.Summary.Succeeded + fold.Summary.DeadLettered + fold.Summary.NoRoute
		summary.Unsettled += fold.PendingCommitted
	}
	return nil
}

func (s *PipelinePostgresOwner) FanOutRunSummary(ctx context.Context, runID string, now time.Time) (fanoutobligation.RunSummary, error) {
	var summary fanoutobligation.RunSummary
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		summary, err = fanOutRunSummary(txctx, tx, true, runID, now)
		return err
	})
	return summary, err
}

func (s *PipelineSQLiteOwner) FanOutRunSummary(ctx context.Context, runID string, now time.Time) (fanoutobligation.RunSummary, error) {
	var summary fanoutobligation.RunSummary
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		summary, err = fanOutRunSummary(txctx, tx, false, runID, now)
		return err
	})
	return summary, err
}

func (s *PipelinePostgresOwner) SummarizeFanOutRunTx(ctx context.Context, tx *sql.Tx, runID string, now time.Time) (fanoutobligation.RunSummary, error) {
	return fanOutRunSummary(ctx, tx, true, runID, now)
}

func (s *PipelineSQLiteOwner) SummarizeFanOutRunTx(ctx context.Context, tx *sql.Tx, runID string, now time.Time) (fanoutobligation.RunSummary, error) {
	return fanOutRunSummary(ctx, tx, false, runID, now)
}

var _ runtimepipeline.FanOutObligationOwner = (*fanOutPostgresOwner)(nil)
var _ runtimepipeline.FanOutObligationOwner = (*fanOutSQLiteOwner)(nil)
