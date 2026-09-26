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
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
)

type runForkFanOutBarrierOwner interface {
	MaterializeRunForkFanOutBarrierTx(context.Context, *mutationprotocol.Attempt, string, fanoutbarrier.Barrier, runtimecontracts.FanOutPlanRef, *loopruntime.ForkChildReference, time.Time) error
	CreateDeploymentFeedTx(context.Context, *sql.Tx, runtimedata.DeploymentFeed) error
}

type forkDeploymentCarriage struct {
	inherit       bool
	cursor        int
	status        fanoutobligation.Status
	blockedReason string
	outcomes      []fanoutobligation.Outcome
	pending       []runfork.RunForkFanOutPendingReplay
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
		status := fanoutobligation.StatusOpen
		if target.RowCount == 0 {
			status = fanoutobligation.StatusClosed
		}
		return forkDeploymentCarriage{status: status}, nil
	}
	if target.SchemaDigest != intent.Request.Deployment.SchemaDigest || target.RowCount != intent.Request.Cardinality {
		return forkDeploymentCarriage{}, fmt.Errorf("fork deployment unchanged version contradicts source schema or cardinality")
	}
	return forkDeploymentCarriage{
		inherit: true, cursor: intent.Cursor, status: intent.Status, blockedReason: intent.BlockedReason,
		outcomes: obligation.Outcomes, pending: obligation.PendingReplays,
	}, nil
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

func requireExactMaterializedRunForkFanOut(ctx context.Context, tx *sql.Tx, postgres bool, forkRunID string, plan runfork.RunForkPlan, planRefs map[runtimecontracts.FanOutElementRef]runtimecontracts.FanOutPlanRef, original semanticview.OriginalLoopCarriage, targetBundleHash string, data *storedurabledata.Owner, pins []runtimedata.Pin) error {
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
		projectedCapsule, _, err := projectRunForkFanOutCapsule(ctx, tx, forkRunID, plan, obligation, original)
		if err != nil {
			return err
		}
		planRef := planRefs[sourceIntent.Request.PlanRef.ElementRef]
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
		index := 0
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
				strings.TrimSpace(inheritedDisposition.String) != string(want.InheritedDisposition) || !equalOptionalJSON(failure, want.Failure) || !present || createdAt.IsZero() {
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

func resolveRunForkFanOutPlanRefs(plan runfork.RunForkPlan, targetBundleHash string, proofs []runtimecontracts.FanOutPlanRef) (map[runtimecontracts.FanOutElementRef]runtimecontracts.FanOutPlanRef, error) {
	targetBundleHash = strings.TrimSpace(targetBundleHash)
	proofByElement := make(map[runtimecontracts.FanOutElementRef]runtimecontracts.FanOutPlanRef, len(proofs))
	for _, proof := range proofs {
		if _, err := proof.ElementRef.DeclarationIdentity(); err != nil {
			return nil, fmt.Errorf("selected fan-out plan proof: %w", err)
		}
		if strings.TrimSpace(proof.BundleHash) != targetBundleHash || strings.TrimSpace(proof.SemanticDigest) == "" {
			return nil, fmt.Errorf("selected fan-out plan proof must belong to target bundle %s", targetBundleHash)
		}
		if prior, duplicate := proofByElement[proof.ElementRef]; duplicate && prior != proof {
			return nil, fmt.Errorf("selected fan-out plan proof is contradictory for %s", fanOutElementLabel(proof.ElementRef))
		}
		proofByElement[proof.ElementRef] = proof
	}
	resolved := make(map[runtimecontracts.FanOutElementRef]runtimecontracts.FanOutPlanRef, len(plan.FanOutObligations))
	for _, obligation := range plan.FanOutObligations {
		if obligation.Intent.Request.Deployment != nil {
			if obligation.Intent.Request.PlanRef != (runtimecontracts.FanOutPlanRef{}) {
				return nil, fmt.Errorf("deployment feed cannot carry a handler plan proof")
			}
			continue
		}
		source := obligation.Intent.Request.PlanRef
		if source.BundleHash == targetBundleHash {
			if proof, present := proofByElement[source.ElementRef]; present {
				if proof.SemanticDigest != source.SemanticDigest {
					return nil, fmt.Errorf("selected bundle changed pending fan_out declaration %s semantic digest", fanOutElementLabel(source.ElementRef))
				}
				resolved[source.ElementRef] = proof
			} else {
				resolved[source.ElementRef] = source
			}
			continue
		}
		proof, ok := proofByElement[source.ElementRef]
		if !ok {
			return nil, fmt.Errorf("selected bundle %s has no proof for pending fan_out declaration %s", targetBundleHash, fanOutElementLabel(source.ElementRef))
		}
		if proof.SemanticDigest != source.SemanticDigest {
			return nil, fmt.Errorf("selected bundle changed pending fan_out declaration %s semantic digest", fanOutElementLabel(source.ElementRef))
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
	attempt *mutationprotocol.Attempt,
	barriers runForkFanOutBarrierOwner,
	forkRunID string,
	plan runfork.RunForkPlan,
	planRefs map[runtimecontracts.FanOutElementRef]runtimecontracts.FanOutPlanRef,
	original semanticview.OriginalLoopCarriage,
	targetBundleHash string,
	data *storedurabledata.Owner,
	pins []runtimedata.Pin,
	now time.Time,
) (int, error) {
	if err := runfork.ValidateFanOutPendingReplayAdmission(plan); err != nil {
		return 0, err
	}
	for _, obligation := range plan.FanOutObligations {
		if obligation.Intent.Request.Deployment != nil {
			continue
		}
		intent := obligation.Intent
		capsuleProjection, generation, err := projectRunForkFanOutCapsule(ctx, tx, forkRunID, plan, obligation, original)
		if err != nil {
			return 0, err
		}
		intent.Request.Capsule = capsuleProjection
		planRef, ok := planRefs[intent.Request.PlanRef.ElementRef]
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
		if obligation.Barrier != nil {
			if barriers == nil {
				return 0, fmt.Errorf("fork fan-out barrier requires selected-store pipeline owner")
			}
			if err := barriers.MaterializeRunForkFanOutBarrierTx(ctx, attempt, forkRunID, *obligation.Barrier, planRef, generation, now); err != nil {
				return 0, err
			}
		}
	}
	count, err := materializeRunForkDeploymentFeeds(ctx, tx, postgres, attempt, barriers, data, forkRunID, targetBundleHash, plan, pins, now)
	if err != nil {
		return 0, err
	}
	return len(plan.FanOutObligations) - countRunForkSourceDeploymentFeeds(plan) + count, nil
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
			if err := bindRunForkDeploymentPendingReplays(ctx, tx, attempt, forkRunID, obligation, now); err != nil {
				return err
			}
			continue
		}
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
					run_id, triggering_delivery_id, flow_path, declaration_family, semantic_path,
					ordinal, outcome_kind, event_id, source_event_id, inherited_disposition, failure, created_at
				) VALUES ($1,$2,$3,$4,$5,$6,'committed',$7,NULL,NULL,NULL,$8)
				ON CONFLICT (run_id, triggering_delivery_id, flow_path, declaration_family, semantic_path, ordinal) DO NOTHING
			`, forkRunID, obligation.Intent.Request.Key.TriggeringDeliveryID,
				obligation.Intent.Request.Key.ElementRef.FlowPath, obligation.Intent.Request.Key.ElementRef.Family, obligation.Intent.Request.Key.ElementRef.SemanticPath,
				replay.Ordinal, forkEventID, now.UTC())
			if err != nil {
				return fmt.Errorf("bind fork fan-out pending ordinal %d: %w", replay.Ordinal, err)
			}
			inserted, err := rowsAffected(result)
			if err != nil {
				return err
			}
			if inserted {
				key := obligation.Intent.Request.Key
				key.RunID = forkRunID
				ref, err := runforkrevision.FanOutOutcomeFact(key, replay.Ordinal)
				if err != nil {
					return err
				}
				if err := attempt.AddFacts(forkRunID, ref); err != nil {
					return err
				}
			}
			var ownedEvent, sourceEvent, inherited sql.NullString
			if err := tx.QueryRowContext(ctx, `
				SELECT event_id, source_event_id, inherited_disposition
				FROM fan_out_outcomes
				WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5 AND ordinal=$6
			`, forkRunID, obligation.Intent.Request.Key.TriggeringDeliveryID,
				obligation.Intent.Request.Key.ElementRef.FlowPath, obligation.Intent.Request.Key.ElementRef.Family, obligation.Intent.Request.Key.ElementRef.SemanticPath,
				replay.Ordinal).Scan(&ownedEvent, &sourceEvent, &inherited); err != nil {
				return err
			}
			if strings.TrimSpace(ownedEvent.String) != forkEventID || sourceEvent.Valid || inherited.Valid {
				return fmt.Errorf("fork fan-out pending ordinal %d conflicts with child-local replay", replay.Ordinal)
			}
		}
	}
	return nil
}

func fanOutElementLabel(ref runtimecontracts.FanOutElementRef) string {
	identity, err := ref.DeclarationIdentity()
	if err != nil {
		return "<invalid>"
	}
	return identity.Key()
}
