package runforkpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storeentity "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func requireRunForkRemovedArrivalDependents(ctx context.Context, attempt *mutationprotocol.Attempt, plan runfork.RunForkPlan, childRunID string, requests []storegenericschedule.ForkJoinRequest) error {
	for _, request := range requests {
		if request.Disposition != genericschedule.ForkJoinRuleRemoved {
			continue
		}
		_, ref, ok := timeridentity.ParseJoinHandle(request.Child.Payload.Interface().(map[string]any))
		if !ok {
			return fmt.Errorf("removed arrival lacks its exact child dependent")
		}
		expected, entityType, err := projectedRemovedArrivalArm(plan, childRunID, ref)
		if err != nil {
			return err
		}
		if err := requireRunForkRemovedArrivalArm(ctx, attempt, ref, entityType, expected); err != nil {
			return err
		}
	}
	return nil
}

func projectedRemovedArrivalArm(plan runfork.RunForkPlan, childRunID string, ref timeridentity.JoinRef) (joinruntime.Activation, string, error) {
	for _, entity := range plan.Entities {
		if entity.MaterializationMetadata == nil {
			return joinruntime.Activation{}, "", fmt.Errorf("removed arrival lacks fixed-cut construction evidence")
		}
		projection, err := projectRunForkEntityOwnership(plan.SourceRunID, childRunID, entity.EntityID, entity.MaterializationMetadata.FlowInstance)
		if err != nil {
			return joinruntime.Activation{}, "", err
		}
		if projection.Fork.EntityID != ref.StageEntry().EntityID {
			continue
		}
		_, accumulator, _, err := projectRunForkEntityExecutionState(entity, plan.SourceRunID, childRunID, projection)
		if err != nil {
			return joinruntime.Activation{}, "", err
		}
		if err := cancelRunForkRemovedArrivalState(accumulator, []timeridentity.JoinRef{ref}); err != nil {
			return joinruntime.Activation{}, "", err
		}
		buckets, err := joinruntime.PersistedBuckets(accumulator)
		if err != nil {
			return joinruntime.Activation{}, "", err
		}
		arm, found, err := joinruntime.Load(buckets, ref.Node(), ref.Key())
		if err != nil || !found {
			return joinruntime.Activation{}, "", fmt.Errorf("removed arrival lacks its projected dependent: %w", err)
		}
		return arm, entity.MaterializationMetadata.EntityType, nil
	}
	return joinruntime.Activation{}, "", fmt.Errorf("removed arrival lacks fixed-cut construction ownership")
}

func requireRunForkRemovedArrivalArm(ctx context.Context, attempt *mutationprotocol.Attempt, ref timeridentity.JoinRef, expectedEntityType string, expected joinruntime.Activation) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		entry := ref.StageEntry()
		var instancePath, template string
		var entityType sql.NullString
		var raw any
		if err := tx.QueryRowContext(ctx, `SELECT instance_path, flow_template, entity_type, accumulator FROM flow_instances WHERE run_id=$1 AND entity_id=$2`,
			entry.RunID, entry.EntityID).Scan(&instancePath, &template, &entityType, &raw); err != nil {
			return err
		}
		route := flowidentity.StoredRoute(template, "", instancePath)
		if entityType.String != expectedEntityType || entityType.Valid != (expectedEntityType != "") {
			return fmt.Errorf("removed arrival dependent changed its canonical entity type")
		}
		if err := entry.RequireOwner(entry.RunID, route.ScopeKey, route.InstanceID, route.InstancePath, entry.EntityID, entry.Stage); err != nil {
			return fmt.Errorf("removed arrival dependent belongs to another constructed owner: %w", err)
		}
		accumulator, err := storeentity.DecodeJSONMap(raw)
		if err != nil {
			return err
		}
		if err := requireRunForkRemovedArrivalArmEvidence(accumulator, ref, expected); err != nil {
			return err
		}
		if !entityType.Valid {
			return nil
		}
		if err := tx.QueryRowContext(ctx, `SELECT accumulator FROM entity_state WHERE run_id=$1 AND entity_id=$2 AND flow_instance=$3 AND entity_type=$4`,
			entry.RunID, entry.EntityID, instancePath, entityType.String).Scan(&raw); err != nil {
			return err
		}
		accumulator, err = storeentity.DecodeJSONMap(raw)
		if err != nil {
			return err
		}
		return requireRunForkRemovedArrivalArmEvidence(accumulator, ref, expected)
	})
}

func requireRunForkRemovedArrivalArmEvidence(accumulator map[string]any, ref timeridentity.JoinRef, expected joinruntime.Activation) error {
	buckets, err := joinruntime.PersistedBuckets(accumulator)
	if err != nil {
		return err
	}
	actual, found, err := joinruntime.Load(buckets, ref.Node(), ref.Key())
	if err != nil {
		return err
	}
	if !found || !actual.JoinRef().Equal(ref) || !expected.JoinRef().Equal(ref) ||
		expected.CloseReason != joinruntime.CloseReasonRuleRemoved {
		return fmt.Errorf("removed arrival lacks exact canceled dependent evidence")
	}
	want, err := canonicaljson.MarshalPreservingNumberKinds(expected)
	if err != nil {
		return err
	}
	got, err := canonicaljson.MarshalPreservingNumberKinds(actual)
	if err != nil || !bytes.Equal(want, got) {
		return fmt.Errorf("removed arrival dependent changed its retained cancellation or membership")
	}
	return nil
}
