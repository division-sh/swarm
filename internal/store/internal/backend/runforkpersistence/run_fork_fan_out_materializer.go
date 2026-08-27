package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func requireExactMaterializedRunForkFanOut(ctx context.Context, tx *sql.Tx, forkRunID string, plan runfork.RunForkPlan, planRefs map[runtimecontracts.FanOutElementRef]runtimecontracts.FanOutPlanRef) error {
	var intentCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1::uuid`, forkRunID).Scan(&intentCount); err != nil {
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
		if err := tx.QueryRowContext(ctx, `
			SELECT bundle_hash, semantic_digest, source_kind,
				source_event_id::text, source_run_id::text, source_entity_id::text,
				COALESCE(source_field, ''), source_mutation_id::text,
				source_resource_package_key, source_resource_event_name, source_resource_version_id,
				cardinality, cursor, status, next_chunk_size, capsule,
				claim_owner, claim_generation, lease_expires_at, last_served_at, COALESCE(blocked_reason, '')
			FROM fan_out_intents
			WHERE run_id=$1::uuid AND triggering_delivery_id=$2::uuid AND package_key=$3 AND element_id=$4
		`, forkRunID, sourceIntent.Request.Key.TriggeringDeliveryID, sourceIntent.Request.Key.ElementRef.PackageKey, sourceIntent.Request.Key.ElementRef.ElementID).Scan(
			&bundleHash, &semanticDigest, &sourceKind,
			&sourceEvent, &sourceRun, &sourceEntity, &sourceField, &sourceMutation,
			&resourcePackage, &resourceEvent, &resourceVersion,
			&cardinality, &cursor, &status, &nextChunk, &capsuleRaw,
			&claimOwner, &claimGeneration, &leaseExpires, &lastServed, &blockedReason,
		); err != nil {
			return fmt.Errorf("load materialized fork fan-out %s: %w", sourceIntent.Request.Key.String(), err)
		}
		var capsule fanoutobligation.Capsule
		if err := json.Unmarshal(capsuleRaw, &capsule); err != nil {
			return fmt.Errorf("decode materialized fork fan-out capsule: %w", err)
		}
		source := sourceIntent.Source
		if bundleHash != planRef.BundleHash || semanticDigest != planRef.SemanticDigest || sourceKind != string(source.Kind) ||
			strings.TrimSpace(sourceEvent.String) != strings.TrimSpace(source.EventID) || strings.TrimSpace(sourceRun.String) != strings.TrimSpace(source.RunID) ||
			strings.TrimSpace(sourceEntity.String) != strings.TrimSpace(source.EntityID) || sourceField != strings.TrimSpace(source.Field) ||
			strings.TrimSpace(sourceMutation.String) != strings.TrimSpace(source.MutationID) || strings.TrimSpace(resourcePackage.String) != strings.TrimSpace(source.Declaration.PackageKey) ||
			strings.TrimSpace(resourceEvent.String) != strings.TrimSpace(source.Declaration.EventName) || strings.TrimSpace(resourceVersion.String) != strings.TrimSpace(string(source.VersionID)) ||
			cardinality != sourceIntent.Request.Cardinality || cursor != len(obligation.Outcomes) || status != string(sourceIntent.Status) || nextChunk != fanoutobligation.InitialChunkSize ||
			!reflect.DeepEqual(capsule, sourceIntent.Request.Capsule) || claimOwner.Valid || claimGeneration != 0 || leaseExpires.Valid || lastServed.Valid || blockedReason != strings.TrimSpace(sourceIntent.BlockedReason) {
			return fmt.Errorf("fork materialization %s fan-out intent conflicts with fixed plan", forkRunID)
		}
		rows, err := tx.QueryContext(ctx, `
			SELECT ordinal, outcome_kind, event_id::text, source_event_id::text, failure, created_at
			FROM fan_out_outcomes
			WHERE run_id=$1::uuid AND triggering_delivery_id=$2::uuid AND package_key=$3 AND element_id=$4
			ORDER BY ordinal
		`, forkRunID, sourceIntent.Request.Key.TriggeringDeliveryID, sourceIntent.Request.Key.ElementRef.PackageKey, sourceIntent.Request.Key.ElementRef.ElementID)
		if err != nil {
			return fmt.Errorf("load materialized fork fan-out outcomes: %w", err)
		}
		index := 0
		for rows.Next() {
			var ordinal int
			var kind string
			var eventID, sourceEventID sql.NullString
			var failure []byte
			var createdAt time.Time
			if err := rows.Scan(&ordinal, &kind, &eventID, &sourceEventID, &failure, &createdAt); err != nil {
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
			if ordinal != want.Ordinal || kind != string(want.Kind) || eventID.Valid || strings.TrimSpace(sourceEventID.String) != wantSourceEventID || !equalOptionalJSON(failure, want.Failure) || createdAt.IsZero() {
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
	effects *runforkrevision.Effects,
	forkRunID string,
	plan runfork.RunForkPlan,
	planRefs map[runtimecontracts.FanOutElementRef]runtimecontracts.FanOutPlanRef,
	now time.Time,
) (int, error) {
	for _, obligation := range plan.FanOutObligations {
		intent := obligation.Intent
		planRef, ok := planRefs[intent.Request.PlanRef.ElementRef]
		if !ok {
			return 0, fmt.Errorf("fork fan-out plan proof missing for %s", intent.Request.Key.String())
		}
		intent.Request.Key.RunID = forkRunID
		intent.Request.PlanRef = planRef
		intent.Cursor = len(obligation.Outcomes)
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
		capsule, err := json.Marshal(intent.Request.Capsule)
		if err != nil {
			return 0, fmt.Errorf("encode materialized fork fan-out capsule: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO fan_out_intents (
				run_id, triggering_delivery_id, package_key, element_id,
				bundle_hash, semantic_digest, source_kind, source_event_id,
				source_run_id, source_entity_id, source_field, source_mutation_id,
				source_resource_package_key, source_resource_event_name, source_resource_version_id,
				cardinality, cursor, status, next_chunk_size, capsule,
				created_at, updated_at, claim_owner, claim_generation, lease_expires_at,
				last_served_at, blocked_reason
			) VALUES (
				$1::uuid, $2::uuid, $3, $4, $5, $6, $7,
				NULLIF($8, '')::uuid, NULLIF($9, '')::uuid, NULLIF($10, '')::uuid, NULLIF($11, ''), NULLIF($12, '')::uuid,
				NULLIF($13, ''), NULLIF($14, ''), NULLIF($15, ''),
				$16, $17, $18, $19, $20::jsonb,
				$21, $21, NULL, 0, NULL, NULL, NULLIF($22, '')
			)
		`, forkRunID, intent.Request.Key.TriggeringDeliveryID, intent.Request.Key.ElementRef.PackageKey, intent.Request.Key.ElementRef.ElementID,
			planRef.BundleHash, planRef.SemanticDigest, string(intent.Source.Kind), intent.Source.EventID,
			intent.Source.RunID, intent.Source.EntityID, intent.Source.Field, intent.Source.MutationID,
			intent.Source.Declaration.PackageKey, intent.Source.Declaration.EventName, string(intent.Source.VersionID),
			intent.Request.Cardinality, intent.Cursor, string(intent.Status), intent.NextChunkSize, capsule, now, intent.BlockedReason); err != nil {
			return 0, fmt.Errorf("insert materialized fork fan-out %s: %w", intent.Request.Key.String(), err)
		}
		for _, sourceOutcome := range obligation.Outcomes {
			outcome := sourceOutcome
			if outcome.Kind == fanoutobligation.OutcomeCommitted {
				if strings.TrimSpace(outcome.EventID) != "" {
					outcome.SourceEventID = outcome.EventID
				}
				outcome.EventID = ""
			}
			outcome.CreatedAt = now
			if err := outcome.Validate(); err != nil {
				return 0, fmt.Errorf("validate inherited fork fan-out outcome %d: %w", outcome.Ordinal, err)
			}
			var failure any
			if len(outcome.Failure) != 0 {
				failure = string(outcome.Failure)
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO fan_out_outcomes (
					run_id, triggering_delivery_id, package_key, element_id,
					ordinal, outcome_kind, event_id, source_event_id, failure, created_at
				) VALUES (
					$1::uuid, $2::uuid, $3, $4, $5, $6,
					NULLIF($7, '')::uuid, NULLIF($8, '')::uuid, $9::jsonb, $10
				)
			`, forkRunID, intent.Request.Key.TriggeringDeliveryID, intent.Request.Key.ElementRef.PackageKey, intent.Request.Key.ElementRef.ElementID,
				outcome.Ordinal, string(outcome.Kind), outcome.EventID, outcome.SourceEventID, failure, now); err != nil {
				return 0, fmt.Errorf("insert inherited fork fan-out outcome %d: %w", outcome.Ordinal, err)
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
