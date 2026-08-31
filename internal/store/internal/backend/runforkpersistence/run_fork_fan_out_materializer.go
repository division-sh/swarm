package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

type runForkFanOutBarrierOwner interface {
	MaterializeRunForkFanOutBarrierTx(context.Context, *sql.Tx, *runforkrevision.Effects, string, fanoutbarrier.Barrier, runtimecontracts.FanOutPlanRef, time.Time) error
}

func requireExactMaterializedRunForkFanOut(ctx context.Context, tx *sql.Tx, postgres bool, forkRunID string, plan runfork.RunForkPlan, planRefs map[runtimecontracts.FanOutElementRef]runtimecontracts.FanOutPlanRef) error {
	var intentCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1`, forkRunID).Scan(&intentCount); err != nil {
		return fmt.Errorf("count materialized fork fan-out intents: %w", err)
	}
	if intentCount != len(plan.FanOutObligations) {
		return fmt.Errorf("fork materialization %s has %d fan-out intents, want %d", forkRunID, intentCount, len(plan.FanOutObligations))
	}
	for _, obligation := range plan.FanOutObligations {
		sourceIntent := obligation.Intent
		planRef := planRefs[sourceIntent.Request.PlanRef.ElementRef]
		var (
			bundleHash, semanticDigest, sourceKind, sourceField, status, blockedReason string
			sourceEvent, sourceRun, sourceEntity, sourceMutation                       sql.NullString
			resourcePackage, resourceEvent, resourceVersion                            sql.NullString
			cardinality, cursor, nextChunk                                             int
			capsuleRaw                                                                 []byte
			claimOwner                                                                 sql.NullString
			claimGeneration                                                            uint64
			leaseExpires, lastServed                                                   sql.NullTime
		)
		intentQuery := `
			SELECT bundle_hash, semantic_digest, source_kind,
				source_event_id, source_run_id, source_entity_id,
				COALESCE(source_field, ''), source_mutation_id,
				source_resource_package_key, source_resource_event_name, source_resource_version_id,
				cardinality, cursor, status, next_chunk_size, capsule,
				claim_owner, claim_generation, lease_expires_at, last_served_at, COALESCE(blocked_reason, '')
			FROM fan_out_intents
			WHERE run_id=$1 AND triggering_delivery_id=$2 AND package_key=$3 AND element_id=$4`
		if postgres {
			intentQuery = strings.NewReplacer(
				"source_event_id, source_run_id, source_entity_id", "source_event_id::text, source_run_id::text, source_entity_id::text",
				"source_mutation_id,", "source_mutation_id::text,",
			).Replace(intentQuery)
		}
		if err := tx.QueryRowContext(ctx, intentQuery, forkRunID, sourceIntent.Request.Key.TriggeringDeliveryID, sourceIntent.Request.Key.ElementRef.PackageKey, sourceIntent.Request.Key.ElementRef.ElementID).Scan(
			&bundleHash, &semanticDigest, &sourceKind,
			&sourceEvent, &sourceRun, &sourceEntity, &sourceField, &sourceMutation,
			&resourcePackage, &resourceEvent, &resourceVersion,
			&cardinality, &cursor, &status, &nextChunk, &capsuleRaw,
			&claimOwner, &claimGeneration, &leaseExpires, &lastServed, &blockedReason,
		); err != nil {
			return fmt.Errorf("load materialized fork fan-out %s: %w", sourceIntent.Request.Key.String(), err)
		}
		var capsule fanoutobligation.Capsule
		if err := canonicaljson.DecodePreservingNumberLexemes(capsuleRaw, &capsule); err != nil {
			return fmt.Errorf("decode materialized fork fan-out capsule: %w", err)
		}
		source := sourceIntent.Source
		if bundleHash != planRef.BundleHash || semanticDigest != planRef.SemanticDigest || sourceKind != string(source.Kind) ||
			strings.TrimSpace(sourceEvent.String) != strings.TrimSpace(source.EventID) || strings.TrimSpace(sourceRun.String) != strings.TrimSpace(source.RunID) ||
			strings.TrimSpace(sourceEntity.String) != strings.TrimSpace(source.EntityID) || sourceField != strings.TrimSpace(source.Field) ||
			strings.TrimSpace(sourceMutation.String) != strings.TrimSpace(source.MutationID) || strings.TrimSpace(resourcePackage.String) != strings.TrimSpace(source.Declaration.PackageKey) ||
			strings.TrimSpace(resourceEvent.String) != strings.TrimSpace(source.Declaration.EventName) || strings.TrimSpace(resourceVersion.String) != strings.TrimSpace(string(source.VersionID)) ||
			cardinality != sourceIntent.Request.Cardinality || cursor != sourceIntent.Cursor || status != string(sourceIntent.Status) || nextChunk != fanoutobligation.InitialChunkSize ||
			!reflect.DeepEqual(capsule, sourceIntent.Request.Capsule) || claimOwner.Valid || claimGeneration != 0 || leaseExpires.Valid || lastServed.Valid || blockedReason != strings.TrimSpace(sourceIntent.BlockedReason) {
			return fmt.Errorf("fork materialization %s fan-out intent conflicts with fixed plan", forkRunID)
		}
		outcomeQuery := `
			SELECT ordinal, outcome_kind, event_id, source_event_id, inherited_disposition, failure, created_at
			FROM fan_out_outcomes
			WHERE run_id=$1 AND triggering_delivery_id=$2 AND package_key=$3 AND element_id=$4
			ORDER BY ordinal`
		if postgres {
			outcomeQuery = strings.Replace(outcomeQuery, "event_id, source_event_id", "event_id::text, source_event_id::text", 1)
		}
		rows, err := tx.QueryContext(ctx, outcomeQuery, forkRunID, sourceIntent.Request.Key.TriggeringDeliveryID, sourceIntent.Request.Key.ElementRef.PackageKey, sourceIntent.Request.Key.ElementRef.ElementID)
		if err != nil {
			return fmt.Errorf("load materialized fork fan-out outcomes: %w", err)
		}
		index := 0
		for rows.Next() {
			var ordinal int
			var kind string
			var eventID, sourceEventID, inheritedDisposition sql.NullString
			var failure []byte
			var createdAt time.Time
			if err := rows.Scan(&ordinal, &kind, &eventID, &sourceEventID, &inheritedDisposition, &failure, &createdAt); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan materialized fork fan-out outcome: %w", err)
			}
			if index >= len(obligation.Outcomes) {
				_ = rows.Close()
				return fmt.Errorf("fork materialization %s has excess fan-out outcomes", forkRunID)
			}
			want := obligation.Outcomes[index]
			wantSourceEventID := strings.TrimSpace(want.SourceEventID)
			if strings.TrimSpace(want.EventID) != "" {
				wantSourceEventID = strings.TrimSpace(want.EventID)
			}
			if ordinal != want.Ordinal || kind != string(want.Kind) || eventID.Valid || strings.TrimSpace(sourceEventID.String) != wantSourceEventID ||
				strings.TrimSpace(inheritedDisposition.String) != string(want.InheritedDisposition) || !equalOptionalJSON(failure, want.Failure) || createdAt.IsZero() {
				_ = rows.Close()
				return fmt.Errorf("fork materialization %s fan-out outcome %d conflicts with fixed plan", forkRunID, ordinal)
			}
			index++
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("read materialized fork fan-out outcomes: %w", err)
		}
		_ = rows.Close()
		if index != len(obligation.Outcomes) {
			return fmt.Errorf("fork materialization %s has %d fan-out outcomes, want %d", forkRunID, index, len(obligation.Outcomes))
		}
	}
	return nil
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

func resolveRunForkFanOutPlanRefs(plan runfork.RunForkPlan, targetBundleHash string, proofs []runtimecontracts.FanOutPlanRef) (map[runtimecontracts.FanOutElementRef]runtimecontracts.FanOutPlanRef, error) {
	targetBundleHash = strings.TrimSpace(targetBundleHash)
	proofByElement := make(map[runtimecontracts.FanOutElementRef]runtimecontracts.FanOutPlanRef, len(proofs))
	for _, proof := range proofs {
		if _, err := proof.ElementRef.ContractElementRef(); err != nil {
			return nil, fmt.Errorf("selected fan-out plan proof: %w", err)
		}
		if strings.TrimSpace(proof.BundleHash) != targetBundleHash || strings.TrimSpace(proof.SemanticDigest) == "" {
			return nil, fmt.Errorf("selected fan-out plan proof must belong to target bundle %s", targetBundleHash)
		}
		if prior, duplicate := proofByElement[proof.ElementRef]; duplicate && prior != proof {
			return nil, fmt.Errorf("selected fan-out plan proof is contradictory for %s/%s", proof.ElementRef.PackageKey, proof.ElementRef.ElementID)
		}
		proofByElement[proof.ElementRef] = proof
	}
	resolved := make(map[runtimecontracts.FanOutElementRef]runtimecontracts.FanOutPlanRef, len(plan.FanOutObligations))
	for _, obligation := range plan.FanOutObligations {
		source := obligation.Intent.Request.PlanRef
		if source.BundleHash == targetBundleHash {
			if proof, present := proofByElement[source.ElementRef]; present {
				if proof.SemanticDigest != source.SemanticDigest {
					return nil, fmt.Errorf("selected bundle changed pending fan_out element %s/%s semantic digest", source.ElementRef.PackageKey, source.ElementRef.ElementID)
				}
				resolved[source.ElementRef] = proof
			} else {
				resolved[source.ElementRef] = source
			}
			continue
		}
		proof, ok := proofByElement[source.ElementRef]
		if !ok {
			return nil, fmt.Errorf("selected bundle %s has no proof for pending fan_out element %s/%s", targetBundleHash, source.ElementRef.PackageKey, source.ElementRef.ElementID)
		}
		if proof.SemanticDigest != source.SemanticDigest {
			return nil, fmt.Errorf("selected bundle changed pending fan_out element %s/%s semantic digest", source.ElementRef.PackageKey, source.ElementRef.ElementID)
		}
		resolved[source.ElementRef] = proof
	}
	if len(proofByElement) != 0 && len(proofByElement) != len(resolved) {
		return nil, fmt.Errorf("selected fan-out plan proof contains elements outside the fixed fork plan")
	}
	return resolved, nil
}

func materializeRunForkFanOutObligations(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	effects *runforkrevision.Effects,
	barriers runForkFanOutBarrierOwner,
	forkRunID string,
	plan runfork.RunForkPlan,
	planRefs map[runtimecontracts.FanOutElementRef]runtimecontracts.FanOutPlanRef,
	now time.Time,
) (int, error) {
	if err := runfork.ValidateFanOutPendingReplayAdmission(plan); err != nil {
		return 0, err
	}
	for _, obligation := range plan.FanOutObligations {
		intent := obligation.Intent
		planRef, ok := planRefs[intent.Request.PlanRef.ElementRef]
		if !ok {
			return 0, fmt.Errorf("fork fan-out plan proof missing for %s", intent.Request.Key.String())
		}
		intent.Request.Key.RunID = forkRunID
		intent.Request.PlanRef = planRef
		intent.Cursor = obligation.Intent.Cursor
		intent.NextChunkSize = fanoutobligation.InitialChunkSize
		intent.LastServedAt = time.Time{}
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
				run_id, triggering_delivery_id, package_key, element_id,
				bundle_hash, semantic_digest, source_kind, source_event_id,
				source_run_id, source_entity_id, source_field, source_mutation_id,
				source_resource_package_key, source_resource_event_name, source_resource_version_id,
				cardinality, cursor, status, next_chunk_size, capsule,
				created_at, updated_at, claim_owner, claim_generation, lease_expires_at,
				last_served_at, blocked_reason
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7,
				$8, $9, $10, $11, $12, $13, $14, $15,
				$16, $17, $18, $19, $20,
				$21, $21, NULL, 0, NULL, NULL, $22
			)
		`
		if postgres {
			intentInsert = strings.Replace(intentInsert, "$20,", "$20::jsonb,", 1)
		}
		if _, err := tx.ExecContext(ctx, intentInsert, forkRunID, intent.Request.Key.TriggeringDeliveryID, intent.Request.Key.ElementRef.PackageKey, intent.Request.Key.ElementRef.ElementID,
			planRef.BundleHash, planRef.SemanticDigest, string(intent.Source.Kind), nullableRunForkString(intent.Source.EventID),
			nullableRunForkString(intent.Source.RunID), nullableRunForkString(intent.Source.EntityID), nullableRunForkString(intent.Source.Field), nullableRunForkString(intent.Source.MutationID),
			nullableRunForkString(intent.Source.Declaration.PackageKey), nullableRunForkString(intent.Source.Declaration.EventName), nullableRunForkString(string(intent.Source.VersionID)),
			intent.Request.Cardinality, intent.Cursor, string(intent.Status), intent.NextChunkSize, capsule, now, nullableRunForkString(intent.BlockedReason)); err != nil {
			return 0, fmt.Errorf("insert materialized fork fan-out %s: %w", intent.Request.Key.String(), err)
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
					run_id, triggering_delivery_id, package_key, element_id,
					ordinal, outcome_kind, event_id, source_event_id, inherited_disposition, failure, created_at
				) VALUES (
					$1, $2, $3, $4, $5, $6,
					$7, $8, $9, $10, $11
				)
			`
			if postgres {
				outcomeInsert = strings.Replace(outcomeInsert, "$10,", "$10::jsonb,", 1)
			}
			if _, err := tx.ExecContext(ctx, outcomeInsert, forkRunID, intent.Request.Key.TriggeringDeliveryID, intent.Request.Key.ElementRef.PackageKey, intent.Request.Key.ElementRef.ElementID,
				outcome.Ordinal, string(outcome.Kind), nullableRunForkString(outcome.EventID), nullableRunForkString(outcome.SourceEventID), nullableRunForkString(string(outcome.InheritedDisposition)), failure, now); err != nil {
				return 0, fmt.Errorf("insert inherited fork fan-out outcome %d: %w", outcome.Ordinal, err)
			}
		}
		if obligation.Barrier != nil {
			if barriers == nil {
				return 0, fmt.Errorf("fork fan-out barrier requires selected-store pipeline owner")
			}
			if err := barriers.MaterializeRunForkFanOutBarrierTx(ctx, tx, effects, forkRunID, *obligation.Barrier, planRef, now); err != nil {
				return 0, err
			}
		}
	}
	if len(plan.FanOutObligations) > 0 {
		if err := effects.Add(forkRunID, runforkrevision.FamilyFanOutObligations); err != nil {
			return 0, err
		}
	}
	return len(plan.FanOutObligations), nil
}

func bindRunForkFanOutPendingReplays(
	ctx context.Context,
	tx *sql.Tx,
	effects *runforkrevision.Effects,
	forkRunID string,
	plan runfork.RunForkPlan,
	now time.Time,
) error {
	if err := runfork.ValidateFanOutPendingReplayAdmission(plan); err != nil {
		return err
	}
	changed := false
	for _, obligation := range plan.FanOutObligations {
		for _, replay := range obligation.PendingReplays {
			forkEventID := deterministicRunForkReplayEventID(forkRunID, replay.SourceEventID)
			var replayCount int
			if err := tx.QueryRowContext(ctx, `
				SELECT COUNT(*)
				FROM run_fork_delivery_event_replays
				WHERE fork_run_id=$1 AND source_event_id=$2 AND fork_event_id=$3
			`, forkRunID, replay.SourceEventID, forkEventID).Scan(&replayCount); err != nil {
				return err
			}
			if replayCount == 0 {
				return fmt.Errorf("fork fan-out pending ordinal %d has no child-local replay for source event %s", replay.Ordinal, replay.SourceEventID)
			}
			result, err := tx.ExecContext(ctx, `
				INSERT INTO fan_out_outcomes (
					run_id, triggering_delivery_id, package_key, element_id,
					ordinal, outcome_kind, event_id, source_event_id, inherited_disposition, failure, created_at
				) VALUES ($1,$2,$3,$4,$5,'committed',$6,NULL,NULL,NULL,$7)
				ON CONFLICT (run_id, triggering_delivery_id, package_key, element_id, ordinal) DO NOTHING
			`, forkRunID, obligation.Intent.Request.Key.TriggeringDeliveryID,
				obligation.Intent.Request.Key.ElementRef.PackageKey, obligation.Intent.Request.Key.ElementRef.ElementID,
				replay.Ordinal, forkEventID, now.UTC())
			if err != nil {
				return fmt.Errorf("bind fork fan-out pending ordinal %d: %w", replay.Ordinal, err)
			}
			inserted, err := rowsAffected(result)
			if err != nil {
				return err
			}
			changed = changed || inserted
			var ownedEvent, sourceEvent, inherited sql.NullString
			if err := tx.QueryRowContext(ctx, `
				SELECT event_id, source_event_id, inherited_disposition
				FROM fan_out_outcomes
				WHERE run_id=$1 AND triggering_delivery_id=$2 AND package_key=$3 AND element_id=$4 AND ordinal=$5
			`, forkRunID, obligation.Intent.Request.Key.TriggeringDeliveryID,
				obligation.Intent.Request.Key.ElementRef.PackageKey, obligation.Intent.Request.Key.ElementRef.ElementID,
				replay.Ordinal).Scan(&ownedEvent, &sourceEvent, &inherited); err != nil {
				return err
			}
			if strings.TrimSpace(ownedEvent.String) != forkEventID || sourceEvent.Valid || inherited.Valid {
				return fmt.Errorf("fork fan-out pending ordinal %d conflicts with child-local replay", replay.Ordinal)
			}
		}
	}
	if changed {
		return effects.Add(forkRunID, runforkrevision.FamilyFanOutObligations)
	}
	return nil
}
