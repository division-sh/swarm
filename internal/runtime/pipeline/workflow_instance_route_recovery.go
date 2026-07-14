package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

// WorkflowInstanceRouteRecoveryProjection is the run-independent persisted
// identity/config needed to restore one active materialized route at startup.
type WorkflowInstanceRouteRecoveryProjection struct {
	Identity runtimeflowidentity.Instance
	Config   map[string]any
}

func (s *WorkflowInstanceStore) LoadRouteRecoveryProjection(
	ctx context.Context,
	route runtimeflowidentity.Route,
) (WorkflowInstanceRouteRecoveryProjection, error) {
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if !route.Valid() {
		return WorkflowInstanceRouteRecoveryProjection{}, fmt.Errorf("flow-instance route recovery identity is required")
	}
	if s == nil || s.db == nil {
		return WorkflowInstanceRouteRecoveryProjection{}, fmt.Errorf("workflow instance store is required for route recovery %s", route.InstancePath)
	}

	var (
		workflowName string
		configRaw    []byte
		err          error
	)
	if s.isSQLite() {
		err = dbQueryRowContext(ctx, s.db, `
			SELECT flow_template, COALESCE(config, '{}')
			FROM flow_instances
			WHERE instance_id = ?
			  AND LOWER(TRIM(status)) = 'active'
			  AND terminated_at IS NULL
		`, route.InstancePath).Scan(&workflowName, &configRaw)
	} else {
		err = dbQueryRowContext(ctx, s.db, `
			SELECT flow_template, COALESCE(config, '{}'::jsonb)
			FROM flow_instances
			WHERE instance_id = $1
			  AND LOWER(BTRIM(status)) = 'active'
			  AND terminated_at IS NULL
		`, route.InstancePath).Scan(&workflowName, &configRaw)
	}
	if err == sql.ErrNoRows {
		return WorkflowInstanceRouteRecoveryProjection{}, fmt.Errorf("active flow instance not found for route recovery: %s", route.InstancePath)
	}
	if err != nil {
		return WorkflowInstanceRouteRecoveryProjection{}, fmt.Errorf("load active flow instance for route recovery %s: %w", route.InstancePath, err)
	}
	workflowName = strings.TrimSpace(workflowName)
	if workflowName == "" {
		return WorkflowInstanceRouteRecoveryProjection{}, fmt.Errorf("flow instance %s has empty flow_template for route recovery", route.InstancePath)
	}

	config, control, err := decodeWorkflowInstanceConfigPayload(configRaw, workflowInstancePersistedControl{
		StorageRef: route.InstancePath,
	})
	if err != nil {
		return WorkflowInstanceRouteRecoveryProjection{}, fmt.Errorf("decode flow instance for route recovery %s: %w", route.InstancePath, err)
	}
	persistedProjection := workflowInstancePersistedProjection{Config: config, Control: control}
	persistedIdentity, err := workflowInstancePersistedIdentity(nil, WorkflowInstance{
		StorageRef:   route.InstancePath,
		WorkflowName: workflowName,
		Config:       config,
		Metadata:     persistedProjection.Metadata(),
	})
	if err != nil {
		return WorkflowInstanceRouteRecoveryProjection{}, fmt.Errorf("derive flow instance identity for route recovery %s: %w", route.InstancePath, err)
	}
	if derived := persistedIdentity.Route(); derived != route {
		return WorkflowInstanceRouteRecoveryProjection{}, fmt.Errorf(
			"flow instance route recovery identity mismatch: persisted=%s/%s/%s requested=%s/%s/%s",
			derived.ScopeKey,
			derived.InstanceID,
			derived.InstancePath,
			route.ScopeKey,
			route.InstanceID,
			route.InstancePath,
		)
	}
	return WorkflowInstanceRouteRecoveryProjection{
		Identity: persistedIdentity.Instance,
		Config:   cloneStringAnyMap(config),
	}, nil
}
