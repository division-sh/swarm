package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
	"github.com/google/uuid"
)

type runForkFanOutBarrierOwner interface {
	MaterializeRunForkFanOutBarrierTx(context.Context, *mutationprotocol.Attempt, string, fanoutbarrier.Barrier, *genericschedule.Activation, *genericschedule.JoinContinuation, runtimecontracts.FanOutPlanRef, *loopruntime.ForkChildReference, time.Time) error
	CreateDeploymentFeedTx(context.Context, *sql.Tx, runtimedata.DeploymentFeed) error
}

type forkDeploymentCarriage struct {
	inherit           bool
	cursor            int
	status            fanoutobligation.Status
	blockedReason     string
	outcomes          []fanoutobligation.Outcome
	pendingDeployment []runfork.RunForkDeploymentPendingEvent
	sourceRunID       string
}

func projectForkDeploymentCarriage(obligation runfork.RunForkFanOutObligation, target runtimedata.PinnedSource) (forkDeploymentCarriage, error) {
	intent := obligation.Intent
	if err := intent.Validate(); err != nil {
		return forkDeploymentCarriage{}, err
	}
	if intent.Request.Deployment == nil || intent.Source.Declaration != target.Declaration || target.RowCount < 0 {
		return forkDeploymentCarriage{}, fmt.Errorf("fork deployment source does not match exact child pin")
	}
	if target.VersionID != intent.Source.VersionID {
		return freshForkDeploymentCarriage(target)
	}
	if target.SchemaDigest != intent.Request.Deployment.SchemaDigest || target.RowCount != intent.Request.Cardinality {
		return forkDeploymentCarriage{}, fmt.Errorf("fork deployment unchanged version contradicts source schema or cardinality")
	}
	return forkDeploymentCarriage{
		inherit: true, cursor: intent.Cursor, status: intent.Status, blockedReason: intent.BlockedReason,
		outcomes: obligation.Outcomes, pendingDeployment: obligation.PendingDeployment,
		sourceRunID: intent.Request.Key.RunID,
	}, nil
}

func freshForkDeploymentCarriage(target runtimedata.PinnedSource) (forkDeploymentCarriage, error) {
	if target.RowCount < 0 {
		return forkDeploymentCarriage{}, fmt.Errorf("fork deployment child pin has negative cardinality")
	}
	status := fanoutobligation.StatusOpen
	if target.RowCount == 0 {
		status = fanoutobligation.StatusClosed
	}
	return forkDeploymentCarriage{status: status}, nil
}

func requireForkResourceSourcePinAgreement(plan runfork.RunForkPlan, pins []runtimedata.Pin) error {
	byDeclaration := make(map[runtimedata.DeclarationRef]runtimedata.VersionID, len(pins))
	for _, pin := range pins {
		if _, duplicate := byDeclaration[pin.Declaration]; duplicate {
			return fmt.Errorf("fork has duplicate resource pin for %s", pin.Declaration.Key())
		}
		byDeclaration[pin.Declaration] = pin.VersionID
	}
	for _, obligation := range plan.FanOutObligations {
		source := obligation.Intent.Source
		if source.Kind != fanoutobligation.SourceResourceVersion {
			continue
		}
		if obligation.Intent.Request.Deployment == nil {
			return fmt.Errorf("fork resource source %s has no deployment origin", source.Declaration.Key())
		}
		if _, pinned := byDeclaration[source.Declaration]; !pinned {
			return fmt.Errorf("fork deployment feed requires child pin for %s", source.Declaration.Key())
		}
	}
	return nil
}

func requireExactMaterializedRunForkFanOut(ctx context.Context, tx *sql.Tx, postgres, lockPostgres bool, forkRunID string, plan runfork.RunForkPlan, planRefs map[fanoutobligation.IntentKey]runtimecontracts.FanOutPlanRef, original semanticview.OriginalLoopCarriage, targetBundleHash string, data *storedurabledata.Owner, pins []runtimedata.Pin) error {
	var intentCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='handler'`, forkRunID).Scan(&intentCount); err != nil {
		return fmt.Errorf("count materialized fork fan-out intents: %w", err)
	}
	wantHandlerCount := len(plan.FanOutObligations) - countRunForkSourceDeploymentFeeds(plan)
	if intentCount != wantHandlerCount {
		return fmt.Errorf("fork materialization %s has %d handler fan-out intents, want %d", forkRunID, intentCount, wantHandlerCount)
	}
	for _, obligation := range plan.FanOutObligations {
		if obligation.Intent.Request.Deployment != nil {
			continue
		}
		sourceIntent := obligation.Intent
		if err := sourceIntent.Request.ValidateSourceProjection(); err != nil {
			return fmt.Errorf("fixed fork fan-out source/plan agreement: %w", err)
		}
		projectedCapsule, _, err := projectRunForkFanOutCapsule(ctx, tx, lockPostgres, forkRunID, plan, obligation, original)
		if err != nil {
			return err
		}
		planRef, ok := planRefs[sourceIntent.Request.Key]
		if !ok {
			return fmt.Errorf("fork fan-out plan proof missing for %s", sourceIntent.Request.Key.String())
		}
		var (
			bundleHash, semanticDigest, sourceKind, sourceField, status, blockedReason string
			sourceEvent, sourceRun, sourceEntity, sourceMutation                       sql.NullString
			resourceFlowPath, resourceEvent, resourceVersion                           sql.NullString
			cardinality, cursor, nextChunk                                             int
			capsuleRaw                                                                 []byte
			claimOwner                                                                 sql.NullString
			claimGeneration                                                            uint64
			leaseExpires, lastServed                                                   any
			retryReadyAt, retryFailure                                                 any
		)
		intentQuery := `
			SELECT bundle_hash, semantic_digest, source_kind,
				source_event_id, source_run_id, source_entity_id,
				COALESCE(source_field, ''), source_mutation_id,
				source_resource_flow_path, source_resource_event_name, source_resource_version_id,
				cardinality, cursor, status, next_chunk_size, capsule,
				claim_owner, claim_generation, lease_expires_at, last_served_at, COALESCE(blocked_reason, ''), retry_ready_at, retry_failure
			FROM fan_out_intents
			WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`
		if postgres {
			intentQuery = strings.NewReplacer(
				"source_event_id, source_run_id, source_entity_id", "source_event_id::text, source_run_id::text, source_entity_id::text",
				"source_mutation_id,", "source_mutation_id::text,",
			).Replace(intentQuery)
		}
		if err := tx.QueryRowContext(ctx, intentQuery, forkRunID, sourceIntent.Request.Key.TriggeringDeliveryID, sourceIntent.Request.Key.ElementRef.FlowPath, sourceIntent.Request.Key.ElementRef.Family, sourceIntent.Request.Key.ElementRef.SemanticPath).Scan(
			&bundleHash, &semanticDigest, &sourceKind,
			&sourceEvent, &sourceRun, &sourceEntity, &sourceField, &sourceMutation,
			&resourceFlowPath, &resourceEvent, &resourceVersion,
			&cardinality, &cursor, &status, &nextChunk, &capsuleRaw,
			&claimOwner, &claimGeneration, &leaseExpires, &lastServed, &blockedReason, &retryReadyAt, &retryFailure,
		); err != nil {
			return fmt.Errorf("load materialized fork fan-out %s: %w", sourceIntent.Request.Key.String(), err)
		}
		for _, field := range []struct {
			name string
			raw  any
		}{{"lease_expires_at", leaseExpires}, {"last_served_at", lastServed}} {
			if _, _, err := sqliteTimeValue(field.raw); err != nil {
				return fmt.Errorf("decode materialized fork fan-out %s: %w", field.name, err)
			}
		}
		var capsule fanoutobligation.Capsule
		if err := canonicaljson.DecodePreservingNumberLexemes(capsuleRaw, &capsule); err != nil {
			return fmt.Errorf("decode materialized fork fan-out capsule: %w", err)
		}
		materializedRequest := sourceIntent.Request
		materializedRequest.PlanRef, materializedRequest.Capsule = planRef, capsule
		if err := materializedRequest.ValidateSourceProjection(); err != nil {
			return fmt.Errorf("materialized fork fan-out source/plan agreement: %w", err)
		}
		source := sourceIntent.Source
		// Only SQL NULL proves untouched claim/service state, not an empty or zero decoded time.
		if bundleHash != planRef.BundleHash || semanticDigest != planRef.SemanticDigest || sourceKind != string(source.Kind) ||
			strings.TrimSpace(sourceEvent.String) != strings.TrimSpace(source.EventID) || strings.TrimSpace(sourceRun.String) != strings.TrimSpace(source.RunID) ||
			strings.TrimSpace(sourceEntity.String) != strings.TrimSpace(source.EntityID) || sourceField != strings.TrimSpace(source.Field) ||
			strings.TrimSpace(sourceMutation.String) != strings.TrimSpace(source.MutationID) || strings.TrimSpace(resourceFlowPath.String) != strings.TrimSpace(source.Declaration.FlowPath) ||
			strings.TrimSpace(resourceEvent.String) != strings.TrimSpace(source.Declaration.EventName) || strings.TrimSpace(resourceVersion.String) != strings.TrimSpace(string(source.VersionID)) ||
			cardinality != sourceIntent.Request.Cardinality || cursor != sourceIntent.Cursor || status != string(sourceIntent.Status) || nextChunk != fanoutobligation.InitialChunkSize ||
			!capsule.Equal(projectedCapsule) || claimOwner.Valid || claimGeneration != 0 || leaseExpires != nil || lastServed != nil || retryReadyAt != nil || retryFailure != nil || blockedReason != strings.TrimSpace(sourceIntent.BlockedReason) {
			return fmt.Errorf("fork materialization %s fan-out intent conflicts with fixed plan", forkRunID)
		}
		outcomeQuery := `
			SELECT ordinal, outcome_kind, event_id, source_event_id, inherited_disposition, failure, created_at
			FROM fan_out_outcomes
			WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5
			ORDER BY ordinal`
		if postgres {
			outcomeQuery = strings.Replace(outcomeQuery, "event_id, source_event_id", "event_id::text, source_event_id::text", 1)
		}
		rows, err := tx.QueryContext(ctx, outcomeQuery, forkRunID, sourceIntent.Request.Key.TriggeringDeliveryID, sourceIntent.Request.Key.ElementRef.FlowPath, sourceIntent.Request.Key.ElementRef.Family, sourceIntent.Request.Key.ElementRef.SemanticPath)
		if err != nil {
			return fmt.Errorf("load materialized fork fan-out outcomes: %w", err)
		}
		terminal := make(map[int]fanoutobligation.Outcome, len(obligation.Outcomes))
		pending := make(map[int]runfork.RunForkFanOutPendingReplay, len(obligation.PendingReplays))
		bound := make(map[int]runForkContinuedOutcomeRow, len(obligation.PendingReplays))
		for _, outcome := range obligation.Outcomes {
			terminal[outcome.Ordinal] = outcome
		}
		for _, replay := range obligation.PendingReplays {
			pending[replay.Ordinal] = replay
		}
		seen := make(map[int]bool)
		for rows.Next() {
			var ordinal int
			var kind string
			var eventID, sourceEventID, inheritedDisposition sql.NullString
			var failure []byte
			var createdRaw any
			if err := rows.Scan(&ordinal, &kind, &eventID, &sourceEventID, &inheritedDisposition, &failure, &createdRaw); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan materialized fork fan-out outcome: %w", err)
			}
			createdAt, present, err := sqliteTimeValue(createdRaw)
			if err != nil {
				_ = rows.Close()
				return fmt.Errorf("decode materialized fork fan-out outcome created_at: %w", err)
			}
			if seen[ordinal] || !present || createdAt.IsZero() {
				_ = rows.Close()
				return fmt.Errorf("fork materialization %s has duplicate or undated fan-out outcome %d", forkRunID, ordinal)
			}
			seen[ordinal] = true
			if _, continuing := pending[ordinal]; continuing {
				bound[ordinal] = runForkContinuedOutcomeRow{kind: kind, eventID: eventID, sourceEventID: sourceEventID,
					disposition: inheritedDisposition, failurePresent: failure != nil, createdAt: createdAt}
				continue
			}
			want, expected := terminal[ordinal]
			if !expected {
				_ = rows.Close()
				return fmt.Errorf("fork materialization %s has excess fan-out outcome %d", forkRunID, ordinal)
			}
			wantSourceEventID := strings.TrimSpace(want.SourceEventID)
			if strings.TrimSpace(want.EventID) != "" {
				wantSourceEventID = strings.TrimSpace(want.EventID)
			}
			if ordinal != want.Ordinal || kind != string(want.Kind) || eventID.Valid || strings.TrimSpace(sourceEventID.String) != wantSourceEventID ||
				strings.TrimSpace(inheritedDisposition.String) != string(want.InheritedDisposition) || !equalOptionalJSON(failure, want.Failure) || !present || createdAt.IsZero() {
				_ = rows.Close()
				return fmt.Errorf("fork materialization %s fan-out outcome %d conflicts with fixed plan", forkRunID, ordinal)
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("read materialized fork fan-out outcomes: %w", err)
		}
		_ = rows.Close()
		for ordinal := range terminal {
			if !seen[ordinal] {
				return fmt.Errorf("fork materialization %s is missing fan-out outcome %d", forkRunID, ordinal)
			}
		}
		for _, replay := range obligation.PendingReplays {
			row, present := bound[replay.Ordinal]
			if err := requireRunForkContinuedPublicationReadback(ctx, tx, postgres, forkRunID, plan, replay, row, present); err != nil {
				return err
			}
		}
	}
	return requireExactMaterializedRunForkDeploymentFeeds(ctx, tx, postgres, forkRunID, targetBundleHash, data, plan, pins)
}

func equalOptionalJSON(left, right []byte) bool {
	if len(left) == 0 || string(left) == "null" {
		return len(right) == 0 || string(right) == "null"
	}
	if len(right) == 0 || string(right) == "null" {
		return false
	}
	var leftValue, rightValue any
	return json.Unmarshal(left, &leftValue) == nil && json.Unmarshal(right, &rightValue) == nil && reflect.DeepEqual(leftValue, rightValue)
}

func resolveRunForkFanOutPlanRefs(plan runfork.RunForkPlan, targetBundleHash string, proofs semanticview.FanOutPlanCarriage) (map[fanoutobligation.IntentKey]runtimecontracts.FanOutPlanRef, error) {
	requirements, err := runfork.FanOutContinuationRequirements(plan)
	if err != nil {
		return nil, err
	}
	targetBundleHash = strings.TrimSpace(targetBundleHash)
	if !proofs.IsZero() {
		if err := proofs.RequireSource(targetBundleHash); err != nil {
			return nil, err
		}
	}
	resolved := make(map[fanoutobligation.IntentKey]runtimecontracts.FanOutPlanRef, len(plan.FanOutObligations))
	for _, obligation := range plan.FanOutObligations {
		if err := obligation.Intent.Request.ValidateSourceProjection(); err != nil {
			return nil, fmt.Errorf("pending fan-out source/plan agreement: %w", err)
		}
		if obligation.Intent.Request.Deployment != nil {
			if obligation.Intent.Request.PlanRef != (runtimecontracts.FanOutPlanRef{}) {
				return nil, fmt.Errorf("deployment feed cannot carry a handler plan proof")
			}
			continue
		}
		source := obligation.Intent.Request.PlanRef
		if !requirements[obligation.Intent.Request.Key] {
			resolved[obligation.Intent.Request.Key] = source
			continue
		}
		if proofs.IsZero() {
			if source.BundleHash != targetBundleHash {
				return nil, fmt.Errorf("selected bundle %s has no proof for pending fan_out declaration %s", targetBundleHash, fanOutElementLabel(source.ElementRef))
			}
			resolved[obligation.Intent.Request.Key] = source
			continue
		}
		proof, semantics, err := proofs.Resolve(source.ElementRef)
		if err != nil {
			return nil, err
		}
		if err := obligation.Intent.Request.AdmitSelectedPlan(proof, semantics); err != nil {
			return nil, err
		}
		resolved[obligation.Intent.Request.Key] = proof
	}
	return resolved, nil
}

func materializeRunForkFanOutObligations(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	attempt *mutationprotocol.Attempt,
	barriers runForkFanOutBarrierOwner,
	forkRunID string,
	plan runfork.RunForkPlan,
	planRefs map[fanoutobligation.IntentKey]runtimecontracts.FanOutPlanRef,
	original semanticview.OriginalLoopCarriage,
	targetBundleHash string,
	data *storedurabledata.Owner,
	pins []runtimedata.Pin,
	now time.Time,
) (int, error) {
	if err := runfork.ValidateFanOutPendingReplayAdmission(plan); err != nil {
		return 0, err
	}
	snapshot, barrierSchedules, err := loadRunForkBarrierMaterializationEvidence(ctx, tx, plan)
	if err != nil {
		return 0, err
	}
	for _, obligation := range plan.FanOutObligations {
		if obligation.Intent.Request.Deployment != nil {
			continue
		}
		intent := obligation.Intent
		capsuleProjection, generation, err := projectRunForkFanOutCapsule(ctx, tx, postgres, forkRunID, plan, obligation, original)
		if err != nil {
			return 0, err
		}
		intent.Request.Capsule = capsuleProjection
		planRef, ok := planRefs[intent.Request.Key]
		if !ok {
			return 0, fmt.Errorf("fork fan-out plan proof missing for %s", intent.Request.Key.String())
		}
		intent.Request.Key.RunID = forkRunID
		intent.Request.PlanRef = planRef
		intent.Cursor = obligation.Intent.Cursor
		intent.NextChunkSize = fanoutobligation.InitialChunkSize
		intent.LastServedAt = time.Time{}
		intent.Retry = nil
		intent.ClaimOwner = ""
		intent.ClaimGeneration = 0
		intent.LeaseExpiresAt = time.Time{}
		intent.CreatedAt = now
		intent.UpdatedAt = now
		if err := intent.Validate(); err != nil {
			return 0, fmt.Errorf("validate materialized fork fan-out %s: %w", intent.Request.Key.String(), err)
		}
		capsule, err := fanoutobligation.MarshalCapsule(intent.Request.Capsule)
		if err != nil {
			return 0, fmt.Errorf("encode materialized fork fan-out capsule: %w", err)
		}
		intentInsert := `
			INSERT INTO fan_out_intents (
				run_id, triggering_delivery_id, flow_path, declaration_family, semantic_path,
				bundle_hash, semantic_digest, source_kind, source_event_id,
				source_run_id, source_entity_id, source_field, source_mutation_id,
				source_resource_flow_path, source_resource_event_name, source_resource_version_id,
				cardinality, cursor, status, next_chunk_size, capsule,
				created_at, updated_at, claim_owner, claim_generation, lease_expires_at,
				last_served_at, blocked_reason
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7,
				$8, $9, $10, $11, $12, $13, $14, $15, $16,
				$17, $18, $19, $20, $21,
				$22, $22, NULL, 0, NULL, NULL, $23
			)
		`
		if postgres {
			intentInsert = strings.Replace(intentInsert, "$21,", "$21::jsonb,", 1)
		}
		if _, err := tx.ExecContext(ctx, intentInsert, forkRunID, intent.Request.Key.TriggeringDeliveryID, intent.Request.Key.ElementRef.FlowPath, intent.Request.Key.ElementRef.Family, intent.Request.Key.ElementRef.SemanticPath,
			planRef.BundleHash, planRef.SemanticDigest, string(intent.Source.Kind), nullableRunForkString(intent.Source.EventID),
			nullableRunForkString(intent.Source.RunID), nullableRunForkString(intent.Source.EntityID), nullableRunForkString(intent.Source.Field), nullableRunForkString(intent.Source.MutationID),
			nullableRunForkString(intent.Source.Declaration.FlowPath), nullableRunForkString(intent.Source.Declaration.EventName), nullableRunForkString(string(intent.Source.VersionID)),
			intent.Request.Cardinality, intent.Cursor, string(intent.Status), intent.NextChunkSize, capsule, now, nullableRunForkString(intent.BlockedReason)); err != nil {
			return 0, fmt.Errorf("insert materialized fork fan-out %s: %w", intent.Request.Key.String(), err)
		}
		intentRef, err := runforkrevision.FanOutIntentFact(intent.Request.Key)
		if err != nil {
			return 0, err
		}
		if err := attempt.AddFacts(forkRunID, intentRef); err != nil {
			return 0, err
		}
		for _, sourceOutcome := range obligation.Outcomes {
			outcome := sourceOutcome
			outcome.CreatedAt = now
			if err := outcome.Validate(); err != nil {
				return 0, fmt.Errorf("validate inherited fork fan-out outcome %d: %w", outcome.Ordinal, err)
			}
			var failure any
			if len(outcome.Failure) != 0 {
				failure = string(outcome.Failure)
			}
			outcomeInsert := `
				INSERT INTO fan_out_outcomes (
					run_id, triggering_delivery_id, flow_path, declaration_family, semantic_path,
					ordinal, outcome_kind, event_id, source_event_id, inherited_disposition, failure, created_at
				) VALUES (
					$1, $2, $3, $4, $5, $6, $7,
					$8, $9, $10, $11, $12
				)
			`
			if postgres {
				outcomeInsert = strings.Replace(outcomeInsert, "$11,", "$11::jsonb,", 1)
			}
			if _, err := tx.ExecContext(ctx, outcomeInsert, forkRunID, intent.Request.Key.TriggeringDeliveryID, intent.Request.Key.ElementRef.FlowPath, intent.Request.Key.ElementRef.Family, intent.Request.Key.ElementRef.SemanticPath,
				outcome.Ordinal, string(outcome.Kind), nullableRunForkString(outcome.EventID), nullableRunForkString(outcome.SourceEventID), nullableRunForkString(string(outcome.InheritedDisposition)), failure, now); err != nil {
				return 0, fmt.Errorf("insert inherited fork fan-out outcome %d: %w", outcome.Ordinal, err)
			}
			outcomeRef, err := runforkrevision.FanOutOutcomeFact(intent.Request.Key, outcome.Ordinal)
			if err != nil {
				return 0, err
			}
			if err := attempt.AddFacts(forkRunID, outcomeRef); err != nil {
				return 0, err
			}
		}
		if err := materializeRunForkObligationBarrier(ctx, attempt, barriers, forkRunID, snapshot, barrierSchedules, obligation.Barrier, planRef, generation, now); err != nil {
			return 0, err
		}
	}
	count, err := materializeRunForkDeploymentFeeds(ctx, tx, postgres, attempt, barriers, data, forkRunID, targetBundleHash, plan, pins, now)
	if err != nil {
		return 0, err
	}
	return len(plan.FanOutObligations) - countRunForkSourceDeploymentFeeds(plan) + count, nil
}

func loadRunForkBarrierMaterializationEvidence(ctx context.Context, tx *sql.Tx, plan runfork.RunForkPlan) (*runForkRevisionSnapshot, map[string]genericschedule.Activation, error) {
	for _, obligation := range plan.FanOutObligations {
		if obligation.Barrier == nil || obligation.Barrier.Status != fanoutbarrier.StatusClosedPending {
			continue
		}
		snapshot, err := loadRunForkPointSnapshot(ctx, tx, plan.SourceRunID, plan.ForkPoint)
		if err != nil {
			return nil, nil, err
		}
		schedules, err := loadRunForkPendingBarrierSchedules(snapshot, plan)
		return snapshot, schedules, err
	}
	return nil, nil, nil
}

func materializeRunForkObligationBarrier(ctx context.Context, attempt *mutationprotocol.Attempt, owner runForkFanOutBarrierOwner, childRun string, snapshot *runForkRevisionSnapshot, schedules map[string]genericschedule.Activation, barrier *fanoutbarrier.Barrier, selected runtimecontracts.FanOutPlanRef, generation *loopruntime.ForkChildReference, at time.Time) error {
	if barrier == nil {
		return nil
	}
	if owner == nil {
		return fmt.Errorf("fork fan-out barrier requires selected-store pipeline owner")
	}
	var schedule *genericschedule.Activation
	if captured, found := schedules[barrier.ScheduleActivationID]; found {
		schedule = &captured
	}
	var published *genericschedule.JoinContinuation
	if barrier.Status == fanoutbarrier.StatusClosedPending && (barrier.TransferredOccurrence != nil || (schedule != nil && schedule.Status == genericschedule.StatusFired)) {
		var continuation genericschedule.JoinContinuation
		err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			var err error
			continuation, _, err = prepareRunForkBarrierTransfer(ctx, tx, snapshot, *barrier, childRun, selected, generation, at)
			return err
		})
		if err != nil {
			return err
		}
		published = &continuation
	}
	return owner.MaterializeRunForkFanOutBarrierTx(ctx, attempt, childRun, *barrier, schedule, published, selected, generation, at)
}

func countRunForkSourceDeploymentFeeds(plan runfork.RunForkPlan) int {
	count := 0
	for _, obligation := range plan.FanOutObligations {
		if obligation.Intent.Request.Deployment != nil {
			count++
		}
	}
	return count
}

func materializeRunForkDeploymentFeeds(ctx context.Context, tx *sql.Tx, postgres bool, attempt *mutationprotocol.Attempt, writer runForkFanOutBarrierOwner, data *storedurabledata.Owner, forkRunID, targetBundleHash string, plan runfork.RunForkPlan, pins []runtimedata.Pin, now time.Time) (int, error) {
	if len(pins) != 0 && (writer == nil || data == nil) {
		return 0, fmt.Errorf("fork deployment feed requires selected pipeline and durable-data owners")
	}
	sourceByDeclaration := make(map[runtimedata.DeclarationRef]runfork.RunForkFanOutObligation)
	for _, obligation := range plan.FanOutObligations {
		if obligation.Intent.Request.Deployment == nil {
			continue
		}
		ref := obligation.Intent.Source.Declaration
		if _, duplicate := sourceByDeclaration[ref]; duplicate {
			return 0, fmt.Errorf("fork source has duplicate deployment feed for %s", ref.Key())
		}
		sourceByDeclaration[ref] = obligation
	}
	for _, pin := range pins {
		if pin.RunID != forkRunID {
			return 0, fmt.Errorf("fork deployment pin belongs to another run")
		}
		target, err := storedurabledata.RequirePinnedSourceTx(ctx, data, tx, forkRunID, targetBundleHash, pin.Declaration)
		if err != nil {
			return 0, err
		}
		if target.VersionID != pin.VersionID || target.SchemaDigest != pin.SchemaDigest {
			return 0, fmt.Errorf("fork deployment target contradicts exact child pin for %s", pin.Declaration.Key())
		}
		feed := runtimedata.DeploymentFeed{
			RunID: forkRunID, BundleHash: targetBundleHash, Declaration: target.Declaration,
			VersionID: target.VersionID, SchemaDigest: target.SchemaDigest, RowCount: uint64(target.RowCount),
		}
		if err := writer.CreateDeploymentFeedTx(ctx, tx, feed); err != nil {
			return 0, err
		}
		key, err := loadRunForkChildDeploymentFeedKeyTx(ctx, tx, forkRunID, target.Declaration)
		if err != nil {
			return 0, err
		}
		if source, present := sourceByDeclaration[target.Declaration]; present {
			carriage, err := projectForkDeploymentCarriage(source, target)
			if err != nil {
				return 0, err
			}
			if carriage.inherit {
				if err := carryRunForkDeploymentFeedPrefixTx(ctx, tx, key, carriage); err != nil {
					return 0, err
				}
				for _, outcome := range carriage.outcomes {
					if outcome.EventID != "" {
						return 0, fmt.Errorf("fork deployment terminal prefix cannot own a source-run event")
					}
					var failure any
					if len(outcome.Failure) != 0 {
						failure = string(outcome.Failure)
					}
					query := `INSERT INTO fan_out_outcomes (run_id,deployment_feed_id,ordinal,outcome_kind,event_id,source_event_id,inherited_disposition,failure,created_at)
					VALUES ($1,$2,$3,$4,NULL,$5,$6,$7,$8)`
					if postgres {
						query = strings.Replace(query, "$7,", "$7::jsonb,", 1)
					}
					if _, err := tx.ExecContext(ctx, query, forkRunID, key.DeploymentFeedID, outcome.Ordinal, string(outcome.Kind),
						nullableRunForkString(outcome.SourceEventID), nullableRunForkString(string(outcome.InheritedDisposition)), failure, now); err != nil {
						return 0, fmt.Errorf("carry fork deployment ordinal %d: %w", outcome.Ordinal, err)
					}
					ref, err := runforkrevision.FanOutOutcomeFact(key, outcome.Ordinal)
					if err != nil {
						return 0, err
					}
					if err := attempt.AddFacts(forkRunID, ref); err != nil {
						return 0, err
					}
				}
			}
		}
		ref, err := runforkrevision.FanOutIntentFact(key)
		if err != nil {
			return 0, err
		}
		if err := attempt.AddFacts(forkRunID, ref); err != nil {
			return 0, err
		}
	}
	return len(pins), nil
}

func carryRunForkDeploymentFeedPrefixTx(ctx context.Context, tx *sql.Tx, key fanoutobligation.IntentKey, carriage forkDeploymentCarriage) error {
	// The feed was inserted in this transaction. Its insertion timestamp is
	// authoritative; an earlier materialization timestamp must not move it back.
	result, err := tx.ExecContext(ctx, `UPDATE fan_out_intents SET cursor=$3,status=$4,blocked_reason=$5,updated_at=created_at WHERE run_id=$1 AND deployment_feed_id=$2`,
		key.RunID, key.DeploymentFeedID, carriage.cursor, string(carriage.status), nullableRunForkString(carriage.blockedReason))
	if err != nil {
		return fmt.Errorf("carry fork deployment feed prefix: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count carried fork deployment feed rows: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("carry fork deployment feed prefix changed %d rows, want 1", rows)
	}
	return nil
}

func loadRunForkChildDeploymentFeedKeyTx(ctx context.Context, tx *sql.Tx, runID string, declaration runtimedata.DeclarationRef) (fanoutobligation.IntentKey, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(deployment_feed_id AS TEXT) FROM fan_out_intents
		WHERE run_id=$1 AND origin_kind='deployment' AND source_resource_flow_path=$2 AND source_resource_event_name=$3`,
		runID, declaration.FlowPath, declaration.EventName)
	if err != nil {
		return fanoutobligation.IntentKey{}, err
	}
	defer rows.Close()
	var key fanoutobligation.IntentKey
	key.RunID = runID
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return key, err
		}
		return key, fmt.Errorf("fork child has no deployment feed for %s", declaration.Key())
	}
	if err := rows.Scan(&key.DeploymentFeedID); err != nil {
		return key, err
	}
	if rows.Next() {
		return key, fmt.Errorf("fork child has duplicate deployment feeds for %s", declaration.Key())
	}
	if err := rows.Err(); err != nil {
		return key, err
	}
	return key, key.Validate()
}

func bindRunForkFanOutPendingReplays(
	ctx context.Context,
	tx *sql.Tx,
	attempt *mutationprotocol.Attempt,
	forkRunID string,
	plan runfork.RunForkPlan,
	now time.Time,
) error {
	if err := runfork.ValidateFanOutPendingReplayAdmission(plan); err != nil {
		return err
	}
	for _, obligation := range plan.FanOutObligations {
		if obligation.Intent.Request.Deployment != nil {
			continue
		}
		for _, replay := range obligation.PendingReplays {
			forkEventID, err := requireRunForkFanOutReplayMembershipTx(ctx, tx, plan.SourceRunID, forkRunID, replay)
			if err != nil {
				return err
			}
			key := obligation.Intent.Request.Key
			key.RunID = forkRunID
			if err := bindRunForkContinuedOutcomeTx(ctx, tx, attempt, key, replay, forkEventID, now); err != nil {
				return err
			}
		}
	}
	return nil
}

func requireRunForkFanOutReplayMembershipTx(ctx context.Context, tx *sql.Tx, sourceRunID, forkRunID string, replay runfork.RunForkFanOutPendingReplay) (string, error) {
	if tx == nil || sourceRunID == forkRunID || len(replay.SourceDeliveryIDs) == 0 {
		return "", fmt.Errorf("fork fan-out pending ordinal %d requires exact replay membership", replay.Ordinal)
	}
	wantSources, err := expectedRunForkFanOutReplaySources(sourceRunID, forkRunID, replay)
	if err != nil {
		return "", err
	}
	forkEventID := deterministicRunForkReplayEventID(forkRunID, replay.SourceEventID)
	wantChildren, err := readRunForkFanOutReplayLineageTx(ctx, tx, sourceRunID, forkRunID, forkEventID, replay, wantSources)
	if err != nil {
		return "", err
	}
	if err := requireRunForkFanOutChildMembershipTx(ctx, tx, forkRunID, forkEventID, replay.Ordinal, wantChildren); err != nil {
		return "", err
	}
	return forkEventID, nil
}

func expectedRunForkFanOutReplaySources(sourceRunID, forkRunID string, replay runfork.RunForkFanOutPendingReplay) (map[string]bool, error) {
	for _, id := range append([]string{sourceRunID, forkRunID, replay.SourceEventID}, replay.SourceDeliveryIDs...) {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return nil, fmt.Errorf("fork fan-out pending ordinal %d has noncanonical replay identity", replay.Ordinal)
		}
	}
	wantSources := make(map[string]bool, len(replay.SourceDeliveryIDs))
	for _, id := range replay.SourceDeliveryIDs {
		if _, duplicate := wantSources[id]; duplicate {
			return nil, fmt.Errorf("fork fan-out pending ordinal %d repeats source delivery %s", replay.Ordinal, id)
		}
		wantSources[id] = false
	}
	return wantSources, nil
}

func readRunForkFanOutReplayLineageTx(ctx context.Context, tx *sql.Tx, sourceRunID, forkRunID, forkEventID string, replay runfork.RunForkFanOutPendingReplay, wantSources map[string]bool) (map[string]bool, error) {
	// Do not filter away a contradictory source run or a second event for the
	// source publication. Every lineage row for either publication must agree.
	rows, err := tx.QueryContext(ctx, `SELECT CAST(r.fork_run_id AS TEXT),CAST(r.source_run_id AS TEXT),
		CAST(r.source_event_id AS TEXT),CAST(r.source_delivery_id AS TEXT),
		CAST(r.fork_event_id AS TEXT),CAST(r.fork_delivery_id AS TEXT),
		CAST(d.run_id AS TEXT),CAST(d.event_id AS TEXT)
		FROM run_fork_delivery_event_replays r
		LEFT JOIN event_deliveries d ON d.delivery_id=r.fork_delivery_id
		WHERE (r.fork_run_id=$1 AND r.source_event_id=$2) OR r.fork_event_id=$3`,
		forkRunID, replay.SourceEventID, forkEventID)
	if err != nil {
		return nil, fmt.Errorf("read fork fan-out replay membership: %w", err)
	}
	defer rows.Close()
	wantChildren := make(map[string]bool, len(wantSources))
	for rows.Next() {
		var childRun, sourceRun, sourceEvent, sourceDelivery, childEvent, childDelivery string
		var deliveryRun, deliveryEvent sql.NullString
		if err := rows.Scan(&childRun, &sourceRun, &sourceEvent, &sourceDelivery, &childEvent, &childDelivery, &deliveryRun, &deliveryEvent); err != nil {
			return nil, fmt.Errorf("scan fork fan-out replay membership: %w", err)
		}
		seen, wanted := wantSources[sourceDelivery]
		if !wanted || seen || childRun != forkRunID || sourceRun != sourceRunID || sourceEvent != replay.SourceEventID ||
			childEvent != forkEventID || childDelivery == "" || !deliveryRun.Valid || !deliveryEvent.Valid ||
			deliveryRun.String != forkRunID || deliveryEvent.String != forkEventID {
			return nil, fmt.Errorf("fork fan-out pending ordinal %d has contradictory replay lineage for source delivery %s", replay.Ordinal, sourceDelivery)
		}
		if _, duplicate := wantChildren[childDelivery]; duplicate {
			return nil, fmt.Errorf("fork fan-out pending ordinal %d repeats child delivery %s", replay.Ordinal, childDelivery)
		}
		wantSources[sourceDelivery] = true
		wantChildren[childDelivery] = false
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read fork fan-out replay membership: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for id, seen := range wantSources {
		if !seen {
			return nil, fmt.Errorf("fork fan-out pending ordinal %d omits source delivery %s", replay.Ordinal, id)
		}
	}
	return wantChildren, nil
}

func requireRunForkFanOutChildMembershipTx(ctx context.Context, tx *sql.Tx, forkRunID, forkEventID string, ordinal int, wantChildren map[string]bool) error {
	children, err := tx.QueryContext(ctx, `SELECT CAST(delivery_id AS TEXT),CAST(run_id AS TEXT),CAST(event_id AS TEXT)
		FROM event_deliveries WHERE event_id=$1`, forkEventID)
	if err != nil {
		return fmt.Errorf("read fork fan-out child deliveries: %w", err)
	}
	defer children.Close()
	for children.Next() {
		var id, runID, eventID string
		if err := children.Scan(&id, &runID, &eventID); err != nil {
			return err
		}
		seen, wanted := wantChildren[id]
		if !wanted || seen || runID != forkRunID || eventID != forkEventID {
			return fmt.Errorf("fork fan-out pending ordinal %d has contradictory child delivery %s", ordinal, id)
		}
		wantChildren[id] = true
	}
	if err := children.Err(); err != nil {
		return err
	}
	for id, seen := range wantChildren {
		if !seen {
			return fmt.Errorf("fork fan-out pending ordinal %d omits child delivery %s", ordinal, id)
		}
	}
	return nil
}

// Callers prove publication and recipient authority before binding. This writer
// records only the continued ordinal fact; it never creates delivery history.
func bindRunForkContinuedOutcomeTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, childKey fanoutobligation.IntentKey, replay runfork.RunForkFanOutPendingReplay, forkEventID string, now time.Time) error {
	if tx == nil || attempt == nil {
		return fmt.Errorf("fork fan-out continuation requires its mutation transaction")
	}
	if now.IsZero() {
		return fmt.Errorf("fork fan-out continuation requires its publication time")
	}
	if err := childKey.Validate(); err != nil {
		return err
	}
	outcome := fanoutobligation.Outcome{
		Ordinal: replay.Ordinal, Kind: fanoutobligation.OutcomeContinued,
		EventID: forkEventID, SourceEventID: replay.SourceEventID,
		InheritedDisposition: replay.InheritedDisposition, CreatedAt: now.UTC(),
	}
	if err := outcome.Validate(); err != nil {
		return err
	}
	for _, id := range []string{outcome.EventID, outcome.SourceEventID} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return fmt.Errorf("fork fan-out continuation requires exact canonical event identities")
		}
	}
	args := []any{childKey.RunID, childKey.TriggeringDeliveryID, childKey.ElementRef.FlowPath, childKey.ElementRef.Family, childKey.ElementRef.SemanticPath, outcome.Ordinal}
	insert := `INSERT INTO fan_out_outcomes (
		run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,
		outcome_kind,event_id,source_event_id,inherited_disposition,failure,created_at)
		VALUES ($1,$2,$3,$4,$5,$6,'continued',$7,$8,$9,NULL,$10)
		ON CONFLICT (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal) DO NOTHING`
	where := `run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5 AND ordinal=$6`
	if childKey.DeploymentFeedID != "" {
		args = []any{childKey.RunID, childKey.DeploymentFeedID, outcome.Ordinal}
		insert = `INSERT INTO fan_out_outcomes (
			run_id,deployment_feed_id,ordinal,outcome_kind,event_id,source_event_id,inherited_disposition,failure,created_at)
			VALUES ($1,$2,$3,'continued',$4,$5,$6,NULL,$7)
			ON CONFLICT (run_id,deployment_feed_id,ordinal) DO NOTHING`
		where = `run_id=$1 AND deployment_feed_id=$2 AND ordinal=$3`
	}
	result, err := tx.ExecContext(ctx, insert, append(args, outcome.EventID, outcome.SourceEventID,
		nullableRunForkString(string(outcome.InheritedDisposition)), outcome.CreatedAt.UTC())...)
	if err != nil {
		return fmt.Errorf("bind fork fan-out continued ordinal %d: %w", outcome.Ordinal, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed < 0 || changed > 1 {
		return fmt.Errorf("fork fan-out continued ordinal %d changed %d rows", outcome.Ordinal, changed)
	}
	var kind string
	var eventID, sourceEventID, disposition sql.NullString
	var failure, createdRaw any
	if err := tx.QueryRowContext(ctx, `SELECT outcome_kind,CAST(event_id AS TEXT),CAST(source_event_id AS TEXT),inherited_disposition,failure,created_at
		FROM fan_out_outcomes WHERE `+where, args...).Scan(&kind, &eventID, &sourceEventID, &disposition, &failure, &createdRaw); err != nil {
		return err
	}
	createdAt, present, err := sqliteTimeValue(createdRaw)
	if err != nil || !present || createdAt.IsZero() {
		return fmt.Errorf("fork fan-out continued ordinal %d has invalid creation time: %v", outcome.Ordinal, err)
	}
	if !matchesRunForkFanOutContinuedOutcome(outcome, kind, eventID, sourceEventID, disposition, failure != nil) {
		return fmt.Errorf("fork fan-out continued ordinal %d conflicts with exact continuation", outcome.Ordinal)
	}
	if changed == 1 {
		ref, err := runforkrevision.FanOutOutcomeFact(childKey, outcome.Ordinal)
		if err != nil {
			return err
		}
		if err := attempt.AddFacts(childKey.RunID, ref); err != nil {
			return err
		}
	}
	return nil
}

func matchesRunForkFanOutContinuedOutcome(want fanoutobligation.Outcome, kind string, eventID, sourceEventID, disposition sql.NullString, failurePresent bool) bool {
	return kind == string(fanoutobligation.OutcomeContinued) && eventID.Valid && eventID.String == want.EventID &&
		sourceEventID.Valid && sourceEventID.String == want.SourceEventID && !failurePresent &&
		disposition.Valid == (want.InheritedDisposition != "") && disposition.String == string(want.InheritedDisposition)
}

func fanOutElementLabel(ref runtimecontracts.FanOutElementRef) string {
	identity, err := ref.DeclarationIdentity()
	if err != nil {
		return "<invalid>"
	}
	return identity.Key()
}
