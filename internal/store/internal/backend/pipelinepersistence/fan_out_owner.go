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
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

const fanOutIntentColumns = `
	run_id, triggering_delivery_id, flow_path, declaration_family, semantic_path, bundle_hash, semantic_digest,
	source_kind, source_event_id, source_run_id, source_entity_id, source_field, source_mutation_id,
	source_resource_flow_path, source_resource_event_name, source_resource_version_id,
	cardinality, cursor, status, next_chunk_size, last_chunk_ms, last_served_at, created_at, updated_at,
	claim_owner, claim_generation, lease_expires_at, blocked_reason, capsule`

type rowScanner interface{ Scan(...any) error }

func scanFanOutIntent(row rowScanner) (fanoutobligation.Intent, error) {
	var (
		runID, deliveryID, flowPath, family, semanticPath, bundleHash, digest                 string
		sourceKind, sourceEventID, sourceRunID, sourceEntityID, sourceField, sourceMutationID sql.NullString
		resourceFlowPath, resourceEvent, resourceVersion                                      sql.NullString
		cardinality, cursor, nextChunk                                                        int
		lastChunkMS                                                                           int64
		status                                                                                string
		lastServedRaw, createdAtRaw, updatedAtRaw, leaseRaw                                   any
		claimOwner, blockedReason                                                             sql.NullString
		claimGeneration                                                                       uint64
		capsuleRaw                                                                            []byte
	)
	if err := row.Scan(
		&runID, &deliveryID, &flowPath, &family, &semanticPath, &bundleHash, &digest,
		&sourceKind, &sourceEventID, &sourceRunID, &sourceEntityID, &sourceField, &sourceMutationID,
		&resourceFlowPath, &resourceEvent, &resourceVersion,
		&cardinality, &cursor, &status, &nextChunk, &lastChunkMS, &lastServedRaw, &createdAtRaw, &updatedAtRaw,
		&claimOwner, &claimGeneration, &leaseRaw, &blockedReason, &capsuleRaw,
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
	if err := json.Unmarshal(capsuleRaw, &capsule); err != nil {
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
		Source: source, Cursor: cursor, Status: fanoutobligation.Status(status), NextChunkSize: nextChunk, LastChunkMS: lastChunkMS,
		LastServedAt: lastServed, CreatedAt: createdAt.UTC(), UpdatedAt: updatedAt.UTC(),
		ClaimOwner: claimOwner.String, ClaimGeneration: claimGeneration, LeaseExpiresAt: lease, BlockedReason: blockedReason.String,
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

func (s *PipelinePostgresOwner) ClaimFanOutIntent(ctx context.Context, request runtimepipeline.FanOutClaimRequest) (intent fanoutobligation.Intent, claim fanoutobligation.Claim, found bool, err error) {
	if s == nil || s.backend == nil {
		return intent, claim, false, fmt.Errorf("postgres fan-out owner is required")
	}
	if err := request.Validate(); err != nil {
		return intent, claim, false, err
	}
	err = s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		row := tx.QueryRowContext(txctx, `SELECT `+fanOutIntentColumns+` FROM fan_out_intents
			WHERE status='open' AND bundle_hash=$1 AND (claim_owner IS NULL OR lease_expires_at <= $2)
			ORDER BY last_served_at ASC NULLS FIRST, created_at ASC, run_id ASC, triggering_delivery_id ASC, flow_path ASC, declaration_family ASC, semantic_path ASC
			FOR UPDATE SKIP LOCKED LIMIT 1`, request.BundleHash, request.Now.UTC())
		var scanErr error
		intent, scanErr = scanFanOutIntent(row)
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil
		}
		if scanErr != nil {
			return scanErr
		}
		found = true
		return claimFanOutIntentRow(txctx, tx, request, &intent, &claim)
	})
	return intent, claim, found, err
}

func (s *PipelineSQLiteOwner) ClaimFanOutIntent(ctx context.Context, request runtimepipeline.FanOutClaimRequest) (intent fanoutobligation.Intent, claim fanoutobligation.Claim, found bool, err error) {
	if s == nil || s.backend == nil {
		return intent, claim, false, fmt.Errorf("sqlite fan-out owner is required")
	}
	if err := request.Validate(); err != nil {
		return intent, claim, false, err
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	err = s.backend.RunTransaction(ctx, "claim fan-out intent", func(txctx context.Context, tx *sql.Tx) error {
		row := tx.QueryRowContext(txctx, `SELECT `+fanOutIntentColumns+` FROM fan_out_intents
			WHERE status='open' AND bundle_hash=? AND (claim_owner IS NULL OR lease_expires_at <= ?)
			ORDER BY CASE WHEN last_served_at IS NULL THEN 0 ELSE 1 END, last_served_at ASC, created_at ASC, run_id ASC, triggering_delivery_id ASC, flow_path ASC, declaration_family ASC, semantic_path ASC
			LIMIT 1`, request.BundleHash, request.Now.UTC())
		var scanErr error
		intent, scanErr = scanFanOutIntent(row)
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil
		}
		if scanErr != nil {
			return scanErr
		}
		found = true
		return claimFanOutIntentRow(txctx, tx, request, &intent, &claim)
	})
	return intent, claim, found, err
}

func claimFanOutIntentRow(ctx context.Context, tx *sql.Tx, request runtimepipeline.FanOutClaimRequest, intent *fanoutobligation.Intent, claim *fanoutobligation.Claim) error {
	lease := request.Now.Add(request.Lease).UTC()
	nextGeneration := intent.ClaimGeneration + 1
	result, err := tx.ExecContext(ctx, `UPDATE fan_out_intents SET claim_owner=$1,claim_generation=$2,lease_expires_at=$3,updated_at=$4
		WHERE run_id=$5 AND triggering_delivery_id=$6 AND flow_path=$7 AND declaration_family=$8 AND semantic_path=$9 AND status='open' AND claim_generation=$10`,
		request.Owner, nextGeneration, lease, request.Now.UTC(), intent.Request.Key.RunID, intent.Request.Key.TriggeringDeliveryID,
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
	intent.UpdatedAt = request.Now.UTC()
	*claim = fanoutobligation.Claim{Key: intent.Request.Key, Owner: request.Owner, Generation: nextGeneration, LeaseUntil: lease}
	return nil
}

func (s *PipelinePostgresOwner) LoadFanOutEvaluation(ctx context.Context, claim fanoutobligation.Claim) (runtimepipeline.FanOutEvaluationInput, error) {
	return loadFanOutEvaluation(ctx, s.backend.ConstructionHandle(), true, claim, time.Now().UTC())
}

func (s *PipelineSQLiteOwner) LoadFanOutEvaluation(ctx context.Context, claim fanoutobligation.Claim) (runtimepipeline.FanOutEvaluationInput, error) {
	return loadFanOutEvaluation(ctx, s.backend.ConstructionHandle(), false, claim, s.now())
}

func loadFanOutEvaluation(ctx context.Context, db *sql.DB, postgres bool, claim fanoutobligation.Claim, now time.Time) (runtimepipeline.FanOutEvaluationInput, error) {
	var input runtimepipeline.FanOutEvaluationInput
	if err := claim.Validate(); err != nil {
		return input, err
	}
	row := db.QueryRowContext(ctx, `SELECT `+fanOutIntentColumns+` FROM fan_out_intents
		WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5
		AND status='open' AND claim_owner=$6 AND claim_generation=$7 AND lease_expires_at>$8`,
		claim.Key.RunID, claim.Key.TriggeringDeliveryID, claim.Key.ElementRef.FlowPath, claim.Key.ElementRef.Family, claim.Key.ElementRef.SemanticPath, claim.Owner, claim.Generation, now.UTC())
	intent, err := scanFanOutIntent(row)
	if errors.Is(err, sql.ErrNoRows) {
		return input, fanoutobligation.ErrStaleClaim
	}
	if err != nil {
		return input, err
	}
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
	triggerInLineage, err := fanOutSourceRunInLineage(ctx, db, postgres, intent.Request.Key.RunID, input.Trigger.RunID())
	if err != nil {
		return input, err
	}
	if !triggerInLineage {
		return input, fmt.Errorf("fan-out triggering event run %s is outside intent run %s fork lineage", input.Trigger.RunID(), intent.Request.Key.RunID)
	}
	input.StartOrdinal = intent.Cursor
	endOrdinal := intent.Cursor + intent.NextChunkSize
	if endOrdinal > intent.Request.Cardinality {
		endOrdinal = intent.Request.Cardinality
	}
	var raw []byte
	switch intent.Source.Kind {
	case fanoutobligation.SourceEventPayloadField:
		if input.Trigger.ID() != intent.Source.EventID {
			return input, fmt.Errorf("fan-out payload source disagrees with triggering event")
		}
		input.Items, err = collectionFieldRangeFromJSON(input.Trigger.Payload(), intent.Source.Field, intent.Request.Cardinality, intent.Cursor, endOrdinal)
	case fanoutobligation.SourceEntityField:
		inLineage, lineageErr := fanOutSourceRunInLineage(ctx, db, postgres, intent.Request.Key.RunID, intent.Source.RunID)
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

func fanOutSourceRunInLineage(ctx context.Context, db *sql.DB, postgres bool, intentRunID, sourceRunID string) (bool, error) {
	query := `
		WITH RECURSIVE lineage(run_id) AS (
			SELECT ?
			UNION
			SELECT runs.forked_from_run_id
			FROM runs
			JOIN lineage ON runs.run_id=lineage.run_id
			WHERE runs.forked_from_run_id IS NOT NULL
		)
		SELECT EXISTS(SELECT 1 FROM lineage WHERE run_id=?)`
	if postgres {
		query = `
			WITH RECURSIVE lineage(run_id) AS (
				SELECT $1::uuid
				UNION
				SELECT runs.forked_from_run_id
				FROM runs
				JOIN lineage ON runs.run_id=lineage.run_id
				WHERE runs.forked_from_run_id IS NOT NULL
			)
			SELECT EXISTS(SELECT 1 FROM lineage WHERE run_id=$2::uuid)`
	}
	var found bool
	if err := db.QueryRowContext(ctx, query, intentRunID, sourceRunID).Scan(&found); err != nil {
		return false, fmt.Errorf("validate fan-out entity source run lineage: %w", err)
	}
	return found, nil
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
		var item any
		decoder := json.NewDecoder(strings.NewReader(scanner.Text()))
		decoder.UseNumber()
		if err := decoder.Decode(&item); err != nil {
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

func (s *PipelinePostgresOwner) ReleaseFanOutClaim(ctx context.Context, claim fanoutobligation.Claim) error {
	return releaseFanOutClaim(ctx, s.backend, "", claim)
}

func (s *PipelineSQLiteOwner) ReleaseFanOutClaim(ctx context.Context, claim fanoutobligation.Claim) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	return releaseFanOutClaim(ctx, s.backend, "release fan-out claim", claim)
}

func retryableFanOutChunk(current int) int {
	next := (current + 1) / 2
	if next < fanoutobligation.MinChunkSize {
		return fanoutobligation.MinChunkSize
	}
	return next
}

func observedFanOutMilliseconds(duration time.Duration) int64 {
	if duration <= 0 {
		return 0
	}
	milliseconds := duration.Milliseconds()
	if milliseconds == 0 {
		return 1
	}
	return milliseconds
}

func (s *PipelinePostgresOwner) ReleaseFanOutRetryable(ctx context.Context, request runtimepipeline.FanOutRetryableRelease) error {
	return releaseFanOutRetryable(ctx, s.backend, "", request)
}

func (s *PipelineSQLiteOwner) ReleaseFanOutRetryable(ctx context.Context, request runtimepipeline.FanOutRetryableRelease) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	return releaseFanOutRetryable(ctx, s.backend, "release retryable fan-out claim", request)
}

func releaseFanOutRetryable(ctx context.Context, backend any, label string, request runtimepipeline.FanOutRetryableRelease) error {
	if err := request.Validate(); err != nil {
		return err
	}
	operation := func(txctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(txctx, `UPDATE fan_out_intents
			SET claim_owner=NULL,lease_expires_at=NULL,last_served_at=$1,updated_at=$1,
				next_chunk_size=CASE WHEN next_chunk_size <= 1 THEN 1 ELSE (next_chunk_size + 1) / 2 END,
				last_chunk_ms=$2
			WHERE run_id=$3 AND triggering_delivery_id=$4 AND flow_path=$5 AND declaration_family=$6 AND semantic_path=$7 AND claim_owner=$8 AND claim_generation=$9`,
			request.Now.UTC(), observedFanOutMilliseconds(request.ObservedDuration), request.Claim.Key.RunID,
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

func blockFanOutClaim(ctx context.Context, tx *sql.Tx, postgres bool, effects *revisionEffects, request runtimepipeline.FanOutBlockRequest) error {
	if err := request.Validate(); err != nil {
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
	result, err := tx.ExecContext(ctx, query, string(failure), request.Now.UTC(), request.Claim.Key.RunID,
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
	return effects.Add(request.Claim.Key.RunID, privaterunforkrevision.FamilyFanOutObligations)
}

func (s *PipelinePostgresOwner) BlockFanOutClaim(ctx context.Context, request runtimepipeline.FanOutBlockRequest) error {
	effects := newRevisionEffects()
	return s.runPrivateAuthorActivityMutation(ctx, effects, func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
		return blockFanOutClaim(txctx, tx, true, effects, request)
	})
}

func (s *PipelineSQLiteOwner) BlockFanOutClaim(ctx context.Context, request runtimepipeline.FanOutBlockRequest) error {
	effects := newRevisionEffects()
	return s.runPrivateAuthorActivityMutation(ctx, "block fan-out claim", effects, func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
		return blockFanOutClaim(txctx, tx, false, effects, request)
	})
}

type transactionRunner interface {
	RunTransaction(context.Context, func(context.Context, *sql.Tx) error) error
}

func releaseFanOutClaim(ctx context.Context, backend any, label string, claim fanoutobligation.Claim) error {
	if err := claim.Validate(); err != nil {
		return err
	}
	operation := func(txctx context.Context, tx *sql.Tx) error {
		now := time.Now().UTC()
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

func adaptiveFanOutChunk(current int, duration time.Duration) int {
	next := current
	if duration <= 250*time.Millisecond {
		next++
	} else if duration > time.Second {
		next = retryableFanOutChunk(current)
	}
	if next < fanoutobligation.MinChunkSize {
		return fanoutobligation.MinChunkSize
	}
	if next > fanoutobligation.MaxChunkSize {
		return fanoutobligation.MaxChunkSize
	}
	return next
}

func commitFanOutChunk(
	ctx context.Context,
	store eventCommitTxStore,
	postgres bool,
	run func(context.Context, *revisionEffects, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error,
	finishTurn func(context.Context, fanoutobligation.Claim, fanoutobligation.Status, int, int64, time.Time) error,
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
	selectedStoreCallStarted := time.Now()
	operationComplete := false
	err = run(ctx, effects, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		intent, err := lockClaimedFanOutIntent(txctx, tx, postgres, command.Claim, command.Now)
		if err != nil {
			return err
		}
		if len(command.Outcomes) > intent.NextChunkSize || intent.Cursor+len(command.Outcomes) > intent.Request.Cardinality {
			return fmt.Errorf("fan-out chunk exceeds claimed range")
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
				committed, err := store.commitPublicationTx(txctx, tx, story, effects, plan.PublicationCommand(), handoff)
				if err != nil {
					return fanOutPublicationSemanticError(outcome.Ordinal, err)
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
			query := `INSERT INTO fan_out_outcomes (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,event_id,source_event_id,inherited_disposition,failure,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,NULLIF($8,''),NULL,NULL,$9,$10)`
			if postgres {
				query = strings.ReplaceAll(query, "NULLIF($8,'')", "NULLIF($8,'')::uuid")
				query = strings.ReplaceAll(query, "$9", "$9::jsonb")
			} else {
				query = postgresPlaceholdersToSQLite(query, 10)
			}
			if _, err := tx.ExecContext(txctx, query, command.Claim.Key.RunID, command.Claim.Key.TriggeringDeliveryID, command.Claim.Key.ElementRef.FlowPath, command.Claim.Key.ElementRef.Family, command.Claim.Key.ElementRef.SemanticPath, outcome.Ordinal, string(kind), nullableText(eventID), failure, command.Now.UTC()); err != nil {
				return fmt.Errorf("insert fan-out outcome ordinal %d: %w", outcome.Ordinal, err)
			}
		}
		nextCursor := intent.Cursor + len(command.Outcomes)
		status := fanoutobligation.StatusOpen
		if nextCursor == intent.Request.Cardinality {
			status = fanoutobligation.StatusClosed
		}
		update, err := tx.ExecContext(txctx, `UPDATE fan_out_intents SET cursor=$1,status=$2,updated_at=$3,claim_owner=CASE WHEN $2='closed' THEN NULL ELSE claim_owner END,lease_expires_at=CASE WHEN $2='closed' THEN NULL ELSE lease_expires_at END WHERE run_id=$4 AND triggering_delivery_id=$5 AND flow_path=$6 AND declaration_family=$7 AND semantic_path=$8 AND claim_owner=$9 AND claim_generation=$10 AND status='open'`, nextCursor, string(status), command.Now.UTC(), command.Claim.Key.RunID, command.Claim.Key.TriggeringDeliveryID, command.Claim.Key.ElementRef.FlowPath, command.Claim.Key.ElementRef.Family, command.Claim.Key.ElementRef.SemanticPath, command.Claim.Owner, command.Claim.Generation)
		if err != nil {
			return err
		}
		rows, err := update.RowsAffected()
		if err != nil || rows != 1 {
			return fanoutobligation.ErrStaleClaim
		}
		if err := effects.Add(command.Claim.Key.RunID, privaterunforkrevision.FamilyFanOutObligations); err != nil {
			return err
		}
		intent.Cursor, intent.Status, intent.UpdatedAt = nextCursor, status, command.Now.UTC()
		if status == fanoutobligation.StatusClosed {
			intent.ClaimOwner, intent.LeaseExpiresAt = "", time.Time{}
		}
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
	if err != nil {
		if !operationComplete {
			return runtimepipeline.CommittedFanOutChunk{}, err
		}
		readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		committed, readErr := reconcileFanOutChunk(readCtx, readback, postgres, command, result)
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
	observedDuration := time.Since(selectedStoreCallStarted)
	nextChunk := adaptiveFanOutChunk(result.Intent.NextChunkSize, observedDuration)
	lastChunkMS := observedFanOutMilliseconds(observedDuration)
	observedAt := time.Now().UTC()
	if observeNow != nil {
		observedAt = observeNow().UTC()
	}
	if observedAt.Before(command.Now.UTC()) {
		observedAt = command.Now.UTC()
	}
	if finishTurn == nil {
		result.PostCommitFailure = fmt.Errorf("fan-out successful turn release owner is required")
	} else if finishErr := finishTurn(ctx, command.Claim, result.Intent.Status, nextChunk, lastChunkMS, observedAt); finishErr != nil {
		result.PostCommitFailure = fmt.Errorf("finish successful fan-out turn: %w", finishErr)
	} else {
		result.Intent.NextChunkSize = nextChunk
		result.Intent.LastChunkMS = lastChunkMS
		result.Intent.LastServedAt = observedAt
		result.Intent.UpdatedAt = observedAt
		result.Intent.ClaimOwner = ""
		result.Intent.LeaseExpiresAt = time.Time{}
	}
	if err := handoff.Commit(); err != nil {
		result.PostCommitFailure = errors.Join(result.PostCommitFailure, err)
	}
	return result, nil
}

func finishFanOutSuccessfulTurn(
	ctx context.Context,
	backend any,
	label string,
	claim fanoutobligation.Claim,
	status fanoutobligation.Status,
	nextChunk int,
	lastChunkMS int64,
	observedAt time.Time,
) error {
	if err := claim.Validate(); err != nil {
		return err
	}
	if status != fanoutobligation.StatusOpen && status != fanoutobligation.StatusClosed {
		return fmt.Errorf("fan-out successful turn status %q is invalid", status)
	}
	if nextChunk < fanoutobligation.MinChunkSize || nextChunk > fanoutobligation.MaxChunkSize || lastChunkMS < 0 || observedAt.IsZero() {
		return fmt.Errorf("fan-out successful turn observation is invalid")
	}
	operation := func(txctx context.Context, tx *sql.Tx) error {
		query := `UPDATE fan_out_intents SET next_chunk_size=$1,last_chunk_ms=$2,last_served_at=$3,updated_at=$3,claim_owner=NULL,lease_expires_at=NULL
			WHERE run_id=$4 AND triggering_delivery_id=$5 AND flow_path=$6 AND declaration_family=$7 AND semantic_path=$8 AND status=$9 AND claim_generation=$10
			AND ((status='open' AND claim_owner=$11) OR (status='closed' AND claim_owner IS NULL))`
		result, err := tx.ExecContext(txctx, query, nextChunk, lastChunkMS, observedAt.UTC(), claim.Key.RunID,
			claim.Key.TriggeringDeliveryID, claim.Key.ElementRef.FlowPath, claim.Key.ElementRef.Family, claim.Key.ElementRef.SemanticPath,
			string(status), claim.Generation, claim.Owner)
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

func reconcileFanOutChunk(ctx context.Context, db pipelineQueryer, postgres bool, command runtimepipeline.FanOutChunkCommand, expected runtimepipeline.CommittedFanOutChunk) (bool, error) {
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
	if intent.Cursor != expected.Intent.Cursor || intent.Status != expected.Intent.Status || intent.ClaimOwner != expected.Intent.ClaimOwner || intent.ClaimGeneration != expected.Intent.ClaimGeneration || len(persisted) != len(command.Outcomes) {
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

func fanOutPublicationSemanticError(ordinal int, err error) error {
	failure, ok := runtimefailures.EnvelopeFromError(err)
	if !ok || failure.Retryable || !failure.Deterministic || failure.Class == runtimefailures.ClassInternalFailure || failure.Class == runtimefailures.ClassOutcomeUncertain {
		return fmt.Errorf("commit fan-out publication ordinal %d: %w", ordinal, err)
	}
	return runtimepipeline.NewFanOutItemSemanticError(ordinal, failure, err)
}

func nullableText(raw string) any {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	return strings.TrimSpace(raw)
}

func lockClaimedFanOutIntent(ctx context.Context, tx *sql.Tx, postgres bool, claim fanoutobligation.Claim, now time.Time) (fanoutobligation.Intent, error) {
	query := `SELECT ` + fanOutIntentColumns + ` FROM fan_out_intents WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5 AND status='open' AND claim_owner=$6 AND claim_generation=$7 AND lease_expires_at>$8`
	if postgres {
		query += ` FOR UPDATE`
	}
	intent, err := scanFanOutIntent(tx.QueryRowContext(ctx, query, claim.Key.RunID, claim.Key.TriggeringDeliveryID, claim.Key.ElementRef.FlowPath, claim.Key.ElementRef.Family, claim.Key.ElementRef.SemanticPath, claim.Owner, claim.Generation, now.UTC()))
	if errors.Is(err, sql.ErrNoRows) {
		return fanoutobligation.Intent{}, fanoutobligation.ErrStaleClaim
	}
	return intent, err
}

func (s *PipelinePostgresOwner) CommitFanOutChunk(ctx context.Context, command runtimepipeline.FanOutChunkCommand) (runtimepipeline.CommittedFanOutChunk, error) {
	return commitFanOutChunk(ctx, s, true,
		func(ctx context.Context, effects *revisionEffects, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
			return s.runPrivateAuthorActivityMutation(ctx, effects, fn)
		},
		func(ctx context.Context, claim fanoutobligation.Claim, status fanoutobligation.Status, nextChunk int, lastChunkMS int64, observedAt time.Time) error {
			return finishFanOutSuccessfulTurn(ctx, s.backend, "", claim, status, nextChunk, lastChunkMS, observedAt)
		}, time.Now,
		func(reservation *runLifecycleCandidateHandoffReservation, result runtimerunlifecycle.CandidateRequestResult) error {
			return reservation.Prepare(s.runLifecycleCandidates, result)
		},
		func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.CandidateRequestResult, error) {
			return requestPostgresCompletionCandidateTx(ctx, tx, runID, nil, false)
		}, s.backend, command)
}

func (s *PipelineSQLiteOwner) CommitFanOutChunk(ctx context.Context, command runtimepipeline.FanOutChunkCommand) (runtimepipeline.CommittedFanOutChunk, error) {
	return commitFanOutChunk(ctx, s, false,
		func(ctx context.Context, effects *revisionEffects, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
			return s.runPrivateAuthorActivityMutation(ctx, "commit fan-out chunk", effects, fn)
		},
		func(ctx context.Context, claim fanoutobligation.Claim, status fanoutobligation.Status, nextChunk int, lastChunkMS int64, observedAt time.Time) error {
			return finishFanOutSuccessfulTurn(ctx, s.backend, "finish successful fan-out turn", claim, status, nextChunk, lastChunkMS, observedAt)
		}, s.now,
		func(reservation *runLifecycleCandidateHandoffReservation, result runtimerunlifecycle.CandidateRequestResult) error {
			return reservation.Prepare(s.runLifecycleCandidates, result)
		},
		func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.CandidateRequestResult, error) {
			return requestSQLiteCompletionCandidateTx(ctx, tx, runID, nil, s.now(), false)
		}, s.backend, command)
}

func cancelRunFanOut(ctx context.Context, postgres bool, effects *revisionEffects, tx *sql.Tx, runID, reason string, at time.Time) error {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(reason) == "" || at.IsZero() {
		return fmt.Errorf("fan-out cancellation requires run, reason, and time")
	}
	query := `SELECT ` + fanOutIntentColumns + ` FROM fan_out_intents WHERE run_id=$1 AND status IN ('open','blocked')`
	if postgres {
		query += ` FOR UPDATE`
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return err
	}
	defer rows.Close()
	intents := make([]fanoutobligation.Intent, 0)
	for rows.Next() {
		intent, err := scanFanOutIntent(rows)
		if err != nil {
			return err
		}
		intents = append(intents, intent)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, intent := range intents {
		update := `UPDATE fan_out_intents SET status='canceled',blocked_reason=$1,claim_owner=NULL,lease_expires_at=NULL,updated_at=$2 WHERE run_id=$3 AND triggering_delivery_id=$4 AND flow_path=$5 AND declaration_family=$6 AND semantic_path=$7 AND status IN ('open','blocked')`
		if !postgres {
			update = postgresPlaceholdersToSQLite(update, 7)
		}
		if _, err := tx.ExecContext(ctx, update, reason, at.UTC(), runID, intent.Request.Key.TriggeringDeliveryID, intent.Request.Key.ElementRef.FlowPath, intent.Request.Key.ElementRef.Family, intent.Request.Key.ElementRef.SemanticPath); err != nil {
			return err
		}
	}
	if err := suppressRunTerminalFanOutBarriersTx(ctx, tx, postgres, effects, runID, at); err != nil {
		return err
	}
	if len(intents) > 0 {
		return effects.Add(runID, privaterunforkrevision.FamilyFanOutObligations)
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
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN status='open' THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN status='blocked' THEN 1 ELSE 0 END),0),COALESCE(SUM(cardinality),0),COALESCE(SUM(cursor),0),COALESCE(SUM(CASE WHEN status IN ('open','blocked') THEN cardinality-cursor ELSE 0 END),0),COALESCE(MIN(next_chunk_size),0),COALESCE(MAX(next_chunk_size),0),COALESCE(MAX(last_chunk_ms),0),MIN(CASE WHEN status IN ('open','blocked') THEN created_at END) FROM fan_out_intents WHERE run_id=$1`, summary.RunID).Scan(&summary.Intents, &summary.Open, &summary.Blocked, &summary.Cardinality, &summary.Cursor, &summary.Owed, &summary.MinNextChunk, &summary.MaxNextChunk, &summary.LastChunkMaxMS, &oldestRaw); err != nil {
		return summary, err
	}
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(SUM(CASE WHEN outcome_kind='committed' THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN outcome_kind='semantic_rejected' THEN 1 ELSE 0 END),0) FROM fan_out_outcomes WHERE run_id=$1`, summary.RunID).Scan(&summary.Committed, &summary.Rejected); err != nil {
		return summary, err
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

var _ runtimepipeline.FanOutObligationOwner = (*PipelinePostgresOwner)(nil)
var _ runtimepipeline.FanOutObligationOwner = (*PipelineSQLiteOwner)(nil)
