package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

type flowInstanceDescriptorQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func normalizeFlowInstanceRouteRecord(route runtimebus.FlowInstanceRouteRecord) (runtimebus.FlowInstanceRouteRecord, error) {
	route.Identity = route.Identity.Normalize()
	if err := route.Identity.Validate(); err != nil {
		return runtimebus.FlowInstanceRouteRecord{}, fmt.Errorf("run_id, scope_key, instance_id, and instance_path are required")
	}
	route.EventPattern = strings.TrimSpace(route.EventPattern)
	route.SubscriberType = strings.TrimSpace(route.SubscriberType)
	route.SubscriberID = strings.TrimSpace(route.SubscriberID)
	route.SourceFlow = strings.TrimSpace(route.SourceFlow)
	if route.SourceFlow == "" {
		route.SourceFlow = route.Identity.Route.ScopeKey
	}
	if route.EventPattern == "" || route.SubscriberType == "" || route.SubscriberID == "" {
		return runtimebus.FlowInstanceRouteRecord{}, fmt.Errorf("flow-instance route record requires event pattern and subscriber identity")
	}
	return route, nil
}

func (s *PipelinePostgresOwner) ListFlowInstanceRouteRecords(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) ([]runtimebus.FlowInstanceRouteRecord, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres store is required for flow instance routes")
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return nil, fmt.Errorf("flow instance route identity is required")
	}
	q := flowInstanceDescriptorQueryer(s.backend)
	return listFlowInstanceRouteRecords(ctx, q, identity, `
		SELECT event_pattern, subscriber_type, subscriber_id, COALESCE(source_flow, '')
		FROM routing_rules
		JOIN flow_instances fi ON fi.run_id = routing_rules.run_id AND fi.instance_path = routing_rules.flow_instance
		WHERE routing_rules.run_id = $1::uuid AND routing_rules.flow_instance = $2
		  AND routing_rules.is_materialized = true
		  AND routing_rules.status = 'active'
		  AND fi.status = 'active'
		ORDER BY event_pattern, subscriber_type, subscriber_id, source_flow
	`)
}

func (s *PipelineSQLiteOwner) ListFlowInstanceRouteRecords(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) ([]runtimebus.FlowInstanceRouteRecord, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite runtime store is required for flow instance routes")
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return nil, fmt.Errorf("flow instance route identity is required")
	}
	q := flowInstanceDescriptorQueryer(s.backend)
	return listFlowInstanceRouteRecords(ctx, q, identity, `
		SELECT event_pattern, subscriber_type, subscriber_id, COALESCE(source_flow, '')
		FROM routing_rules
		JOIN flow_instances fi ON fi.run_id = routing_rules.run_id AND fi.instance_path = routing_rules.flow_instance
		WHERE routing_rules.run_id = ? AND routing_rules.flow_instance = ?
		  AND routing_rules.is_materialized = TRUE
		  AND routing_rules.status = 'active'
		  AND fi.status = 'active'
		ORDER BY event_pattern, subscriber_type, subscriber_id, source_flow
	`)
}

func listFlowInstanceRouteRecords(
	ctx context.Context,
	q flowInstanceDescriptorQueryer,
	identity runtimeflowidentity.RunScopedFlowInstance,
	query string,
) ([]runtimebus.FlowInstanceRouteRecord, error) {
	rows, err := q.QueryContext(ctx, query, identity.RunID, identity.Route.InstancePath)
	if err != nil {
		return nil, fmt.Errorf("list exact flow instance route records %s: %w", identity.Key(), err)
	}
	defer rows.Close()
	var out []runtimebus.FlowInstanceRouteRecord
	for rows.Next() {
		var record runtimebus.FlowInstanceRouteRecord
		record.Identity = identity
		if err := rows.Scan(&record.EventPattern, &record.SubscriberType, &record.SubscriberID, &record.SourceFlow); err != nil {
			return nil, fmt.Errorf("scan exact flow instance route record %s: %w", identity.Key(), err)
		}
		if strings.TrimSpace(record.SourceFlow) == "" {
			record.SourceFlow = identity.Route.ScopeKey
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate exact flow instance route records %s: %w", identity.Key(), err)
	}
	return out, nil
}

const postgresActiveFlowInstanceDescriptorsSQL = `
		SELECT fi.run_id::text, fi.instance_path, fi.flow_template, readiness.plan, readiness.plan_hash, readiness.activation_attempt_id, readiness.phase, readiness.activation_attempt_state,
		       run.bundle_hash, es.fields
		FROM flow_instances fi
		LEFT JOIN flow_instance_runtime_readiness readiness
		  ON readiness.run_id = fi.run_id AND readiness.instance_path = fi.instance_path
		JOIN runs run ON run.run_id = fi.run_id
		LEFT JOIN entity_state es
		  ON es.run_id = fi.run_id
		 AND es.flow_instance = fi.instance_path
		 AND es.entity_id = NULLIF(readiness.plan #>> '{identity,EntityID}', '')::uuid
		WHERE fi.run_id = $1::uuid
		  AND fi.status = 'active' AND fi.mode = 'template'
		  AND LOWER(BTRIM(run.status)) IN ('running', 'paused')
		  AND EXISTS (
			  SELECT 1
			  FROM entity_state owned
			  WHERE owned.run_id = fi.run_id
			    AND owned.flow_instance = fi.instance_path
		  )
	`

const sqliteActiveFlowInstanceDescriptorsSQL = `
		SELECT fi.run_id, fi.instance_path, fi.flow_template, readiness.plan, readiness.plan_hash, readiness.activation_attempt_id, readiness.phase, readiness.activation_attempt_state,
		       run.bundle_hash, es.fields
		FROM flow_instances fi
		LEFT JOIN flow_instance_runtime_readiness readiness
		  ON readiness.run_id = fi.run_id AND readiness.instance_path = fi.instance_path
		JOIN runs run ON run.run_id = fi.run_id
		LEFT JOIN entity_state es
		  ON es.run_id = fi.run_id
		 AND es.flow_instance = fi.instance_path
		 AND es.entity_id = json_extract(readiness.plan, '$.identity.EntityID')
		WHERE fi.run_id = ?
		  AND fi.status = 'active' AND fi.mode = 'template'
		  AND LOWER(TRIM(run.status)) IN ('running', 'paused')
		  AND EXISTS (
			  SELECT 1
			  FROM entity_state owned
			  WHERE owned.run_id = fi.run_id
			    AND owned.flow_instance = fi.instance_path
		  )
	`

const activeFlowInstanceDescriptorOrderSQL = ` ORDER BY fi.run_id, fi.instance_path ASC`

func (s *PipelinePostgresOwner) ListActiveFlowInstanceDescriptors(ctx context.Context, runID string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres store is required for active flow instance descriptors")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("active flow instance descriptors require exact run_id")
	}
	rows, err := s.backend.QueryContext(ctx, postgresActiveFlowInstanceDescriptorsSQL+activeFlowInstanceDescriptorOrderSQL, runID)
	if err != nil {
		return nil, fmt.Errorf("list active flow instance descriptors: %w", err)
	}
	return scanExactActiveFlowInstanceDescriptors(rows, "active flow instance descriptor")
}

func (s *PipelineSQLiteOwner) ListActiveFlowInstanceDescriptors(ctx context.Context, runID string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite runtime store is required for active flow instance descriptors")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("active flow instance descriptors require exact run_id")
	}
	rows, err := s.activeFlowDescriptors.QueryContext(ctx, s.backend, sqliteActiveFlowInstanceDescriptorsSQL+activeFlowInstanceDescriptorOrderSQL, runID)
	if err != nil {
		return nil, fmt.Errorf("list sqlite active flow instance descriptors: %w", err)
	}
	return scanExactActiveFlowInstanceDescriptors(rows, "sqlite active flow instance descriptor")
}

func exactScopeValues(label string, values []string) ([]string, error) {
	out := make([]string, 0, len(values))
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if value == "" || value != strings.TrimSpace(value) || label == "instance path" && value != strings.Trim(value, "/") {
			return nil, fmt.Errorf("scoped route lookup requires canonical %s", label)
		}
		if !seen[value] {
			out = append(out, value)
			seen[value] = true
		}
	}
	return out, nil
}

func exactScopePredicate(column string, postgres bool, first, count int) string {
	var query strings.Builder
	query.WriteString(column)
	query.WriteString(" IN (")
	for i := range count {
		if i > 0 {
			query.WriteByte(',')
		}
		if postgres {
			fmt.Fprintf(&query, "$%d", first+i)
		} else {
			query.WriteByte('?')
		}
	}
	query.WriteByte(')')
	return query.String()
}

func activeFlowInstanceDescriptorScope(postgres bool, templateIDs, instancePaths []string) string {
	clauses := make([]string, 0, 2)
	if len(templateIDs) > 0 {
		clauses = append(clauses, exactScopePredicate("fi.flow_template", postgres, 2, len(templateIDs)))
	}
	if len(instancePaths) > 0 {
		clauses = append(clauses, exactScopePredicate("fi.instance_path", postgres, 2+len(templateIDs), len(instancePaths)))
	}
	return " AND (" + strings.Join(clauses, " OR ") + ")" + activeFlowInstanceDescriptorOrderSQL
}

func (s *PipelinePostgresOwner) ListActiveFlowInstanceDescriptorsForScope(ctx context.Context, runID string, templateIDs, instancePaths []string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres store is required for active flow instance descriptors")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("active flow instance descriptors require exact run_id")
	}
	var err error
	if templateIDs, err = exactScopeValues("flow template", templateIDs); err != nil {
		return nil, err
	}
	if instancePaths, err = exactScopeValues("instance path", instancePaths); err != nil {
		return nil, err
	}
	if len(templateIDs)+len(instancePaths) == 0 {
		return nil, fmt.Errorf("active flow instance descriptor lookup requires graph-owned scope")
	}
	args := make([]any, 0, 1+len(templateIDs)+len(instancePaths))
	args = append(args, runID)
	for _, value := range templateIDs {
		args = append(args, value)
	}
	for _, value := range instancePaths {
		args = append(args, value)
	}
	rows, err := s.backend.QueryContext(ctx, postgresActiveFlowInstanceDescriptorsSQL+activeFlowInstanceDescriptorScope(true, templateIDs, instancePaths), args...)
	if err != nil {
		return nil, fmt.Errorf("list scoped active flow instance descriptors: %w", err)
	}
	return scanExactActiveFlowInstanceDescriptors(rows, "scoped active flow instance descriptor")
}

func (s *PipelineSQLiteOwner) ListActiveFlowInstanceDescriptorsForScope(ctx context.Context, runID string, templateIDs, instancePaths []string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite runtime store is required for active flow instance descriptors")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("active flow instance descriptors require exact run_id")
	}
	var err error
	if templateIDs, err = exactScopeValues("flow template", templateIDs); err != nil {
		return nil, err
	}
	if instancePaths, err = exactScopeValues("instance path", instancePaths); err != nil {
		return nil, err
	}
	if len(templateIDs)+len(instancePaths) == 0 {
		return nil, fmt.Errorf("active flow instance descriptor lookup requires graph-owned scope")
	}
	args := make([]any, 0, 1+len(templateIDs)+len(instancePaths))
	args = append(args, runID)
	for _, value := range templateIDs {
		args = append(args, value)
	}
	for _, value := range instancePaths {
		args = append(args, value)
	}
	rows, err := s.backend.QueryContext(ctx, sqliteActiveFlowInstanceDescriptorsSQL+activeFlowInstanceDescriptorScope(false, templateIDs, instancePaths), args...)
	if err != nil {
		return nil, fmt.Errorf("list sqlite scoped active flow instance descriptors: %w", err)
	}
	return scanExactActiveFlowInstanceDescriptors(rows, "sqlite scoped active flow instance descriptor")
}

// The SQL predicate is a conservative prefilter. DescriptorAddressFields and
// ConnectInstanceKeyDescriptorMatches own scalar normalization and final identity.
// Numeric/boolean candidates remain broad because JSON number lexemes differ
// between stores; excluding a canonical match here would hide a duplicate owner.
const postgresActiveFlowInstanceDescriptorKeySQL = `
	AND fi.flow_template = $2
	AND CASE
		WHEN es.entity_id IS NULL OR es.fields IS NULL THEN TRUE
		WHEN jsonb_typeof(es.fields) <> 'object' THEN TRUE
		ELSE EXISTS (
			SELECT 1 FROM jsonb_each(es.fields) AS field(key, value)
			WHERE strpos(field.key, $3) > 0
			  AND (jsonb_typeof(field.value) IN ('number', 'boolean')
			       OR (jsonb_typeof(field.value) = 'string' AND strpos(field.value #>> '{}', $4) > 0))
		)
	END`

const sqliteActiveFlowInstanceDescriptorKeySQL = `
	AND fi.flow_template = ?
	AND CASE
		WHEN es.entity_id IS NULL OR es.fields IS NULL THEN TRUE
		WHEN json_valid(es.fields) = 0 THEN TRUE
		WHEN json_type(es.fields) <> 'object' THEN TRUE
		ELSE EXISTS (
			SELECT 1 FROM json_each(es.fields) AS field
			WHERE instr(field.key, ?) > 0
			  AND (field.type IN ('integer', 'real', 'true', 'false')
			       OR (field.type = 'text' AND instr(field.value, ?) > 0))
		)
	END`

func exactDescriptorKeyLookup(runID, templateID, keyField, keyValue string) (string, string, string, string, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return "", "", "", "", fmt.Errorf("active flow instance descriptors require exact run_id")
	}
	if templateID == "" || templateID != strings.TrimSpace(templateID) {
		return "", "", "", "", fmt.Errorf("keyed descriptor lookup requires canonical receiver template")
	}
	if !strings.HasPrefix(keyField, "entity.") {
		return "", "", "", "", fmt.Errorf("keyed descriptor lookup requires entity.<literal field>")
	}
	field := strings.TrimPrefix(keyField, "entity.")
	if field == "" || field != strings.TrimSpace(field) {
		return "", "", "", "", fmt.Errorf("keyed descriptor lookup requires canonical literal entity field")
	}
	// The compiled matcher strips surrounding slashes after whitespace. Its
	// normalized value must remain a substring of any matching raw JSON string.
	return runID, templateID, field, strings.Trim(strings.TrimSpace(keyValue), "/"), nil
}

func (s *PipelinePostgresOwner) ListActiveFlowInstanceDescriptorsForKey(ctx context.Context, runID, templateID, keyField, keyValue string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres store is required for active flow instance descriptors")
	}
	runID, templateID, field, value, err := exactDescriptorKeyLookup(runID, templateID, keyField, keyValue)
	if err != nil {
		return nil, err
	}
	query := postgresActiveFlowInstanceDescriptorsSQL + postgresActiveFlowInstanceDescriptorKeySQL + activeFlowInstanceDescriptorOrderSQL
	rows, err := s.backend.QueryContext(ctx, query, runID, templateID, field, value)
	if err != nil {
		return nil, fmt.Errorf("list keyed active flow instance descriptors: %w", err)
	}
	return scanExactActiveFlowInstanceDescriptors(rows, "keyed active flow instance descriptor")
}

func (s *PipelineSQLiteOwner) ListActiveFlowInstanceDescriptorsForKey(ctx context.Context, runID, templateID, keyField, keyValue string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite runtime store is required for active flow instance descriptors")
	}
	runID, templateID, field, value, err := exactDescriptorKeyLookup(runID, templateID, keyField, keyValue)
	if err != nil {
		return nil, err
	}
	query := sqliteActiveFlowInstanceDescriptorsSQL + sqliteActiveFlowInstanceDescriptorKeySQL + activeFlowInstanceDescriptorOrderSQL
	rows, err := s.backend.QueryContext(ctx, query, runID, templateID, field, value)
	if err != nil {
		return nil, fmt.Errorf("list sqlite keyed active flow instance descriptors: %w", err)
	}
	return scanExactActiveFlowInstanceDescriptors(rows, "sqlite keyed active flow instance descriptor")
}

const postgresSelectedRunTargetOwnersSQL = `
		SELECT fi.entity_id::text, fi.instance_path, fi.current_state,
		       fi.status, fi.terminated_at IS NOT NULL
		FROM flow_instances fi
		JOIN runs run ON run.run_id = fi.run_id
		WHERE fi.run_id = $1::uuid
		  AND LOWER(BTRIM(run.status)) IN ('running', 'paused')
	`

const sqliteSelectedRunTargetOwnersSQL = `
		SELECT fi.entity_id, fi.instance_path, fi.current_state,
		       fi.status, fi.terminated_at IS NOT NULL
		FROM flow_instances fi
		JOIN runs run ON run.run_id = fi.run_id
		WHERE fi.run_id = ?
		  AND LOWER(TRIM(run.status)) IN ('running', 'paused')
	`

const selectedRunTargetOwnerOrderSQL = ` ORDER BY fi.instance_path ASC, fi.entity_id ASC`

func (s *PipelinePostgresOwner) ListSelectedRunTargetOwners(ctx context.Context, runID string) ([]runtimebus.ActiveTargetDescriptor, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres store is required for selected-run target owners")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("selected-run target owners require exact run_id")
	}
	rows, err := s.backend.QueryContext(ctx, postgresSelectedRunTargetOwnersSQL+selectedRunTargetOwnerOrderSQL, runID)
	if err != nil {
		return nil, fmt.Errorf("list selected-run target owners: %w", err)
	}
	return scanSelectedRunTargetOwners(rows, "selected-run target owner")
}

func (s *PipelineSQLiteOwner) ListSelectedRunTargetOwners(ctx context.Context, runID string) ([]runtimebus.ActiveTargetDescriptor, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite runtime store is required for selected-run target owners")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("selected-run target owners require exact run_id")
	}
	rows, err := s.selectedRunTargetOwners.QueryContext(ctx, s.backend, sqliteSelectedRunTargetOwnersSQL+selectedRunTargetOwnerOrderSQL, runID)
	if err != nil {
		return nil, fmt.Errorf("list sqlite selected-run target owners: %w", err)
	}
	return scanSelectedRunTargetOwners(rows, "sqlite selected-run target owner")
}

func (s *PipelinePostgresOwner) ListSelectedRunTargetOwnersForScope(ctx context.Context, runID string, instancePaths []string, sourceEntityID string) ([]runtimebus.ActiveTargetDescriptor, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres store is required for selected-run target owners")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("selected-run target owners require exact run_id")
	}
	paths, err := exactScopeValues("instance path", instancePaths)
	if err != nil {
		return nil, err
	}
	sourceEntityID = strings.TrimSpace(sourceEntityID)
	if len(paths) == 0 && sourceEntityID == "" {
		return nil, fmt.Errorf("selected-run target owner lookup requires graph-owned paths or source entity")
	}
	if sourceEntityID != "" {
		parsed, err := uuid.Parse(sourceEntityID)
		if err != nil || parsed.String() != sourceEntityID {
			return nil, fmt.Errorf("selected-run source entity identity is not a canonical UUID")
		}
	}
	args := make([]any, 0, len(paths)+2)
	args = append(args, runID)
	predicates := make([]string, 0, 2)
	if len(paths) > 0 {
		predicates = append(predicates, exactScopePredicate("fi.instance_path", true, 2, len(paths)))
	}
	for _, path := range paths {
		args = append(args, path)
	}
	if sourceEntityID != "" {
		predicates = append(predicates, fmt.Sprintf("fi.entity_id=$%d::uuid", len(args)+1))
		args = append(args, sourceEntityID)
	}
	query := postgresSelectedRunTargetOwnersSQL + " AND (" + strings.Join(predicates, " OR ") + ")" + selectedRunTargetOwnerOrderSQL
	rows, err := s.backend.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list scoped selected-run target owners: %w", err)
	}
	return scanSelectedRunTargetOwners(rows, "scoped selected-run target owner")
}

func (s *PipelineSQLiteOwner) ListSelectedRunTargetOwnersForScope(ctx context.Context, runID string, instancePaths []string, sourceEntityID string) ([]runtimebus.ActiveTargetDescriptor, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite runtime store is required for selected-run target owners")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("selected-run target owners require exact run_id")
	}
	paths, err := exactScopeValues("instance path", instancePaths)
	if err != nil {
		return nil, err
	}
	sourceEntityID = strings.TrimSpace(sourceEntityID)
	if len(paths) == 0 && sourceEntityID == "" {
		return nil, fmt.Errorf("selected-run target owner lookup requires graph-owned paths or source entity")
	}
	if sourceEntityID != "" {
		parsed, err := uuid.Parse(sourceEntityID)
		if err != nil || parsed.String() != sourceEntityID {
			return nil, fmt.Errorf("selected-run source entity identity is not a canonical UUID")
		}
	}
	args := make([]any, 0, len(paths)+2)
	args = append(args, runID)
	predicates := make([]string, 0, 2)
	if len(paths) > 0 {
		predicates = append(predicates, exactScopePredicate("fi.instance_path", false, 2, len(paths)))
	}
	for _, path := range paths {
		args = append(args, path)
	}
	if sourceEntityID != "" {
		predicates = append(predicates, "fi.entity_id=?")
		args = append(args, sourceEntityID)
	}
	query := sqliteSelectedRunTargetOwnersSQL + " AND (" + strings.Join(predicates, " OR ") + ")" + selectedRunTargetOwnerOrderSQL
	rows, err := s.backend.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list sqlite scoped selected-run target owners: %w", err)
	}
	return scanSelectedRunTargetOwners(rows, "sqlite scoped selected-run target owner")
}

func scanSelectedRunTargetOwners(rows *sql.Rows, label string) ([]runtimebus.ActiveTargetDescriptor, error) {
	defer rows.Close()
	out := []runtimebus.ActiveTargetDescriptor{}
	for rows.Next() {
		var entityID, flowInstance, stage, status string
		var terminated bool
		if err := rows.Scan(&entityID, &flowInstance, &stage, &status, &terminated); err != nil {
			return nil, fmt.Errorf("scan %s: %w", label, err)
		}
		descriptor := (runtimebus.ActiveTargetDescriptor{
			ID: flowInstance, EntityID: entityID, FlowInstance: flowInstance,
			Availability: runtimepipeline.NewDeliveryTargetAvailability(stage, status, terminated),
		}).Normalized()
		if descriptor.EntityID == "" || descriptor.FlowInstance == "" {
			return nil, fmt.Errorf("%s is missing exact entity and flow-instance identity", label)
		}
		out = append(out, descriptor)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %ss: %w", label, err)
	}
	return out, nil
}

func scanExactActiveFlowInstanceDescriptors(rows *sql.Rows, label string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	defer rows.Close()
	out := []runtimebus.ActiveFlowInstanceDescriptor{}
	for rows.Next() {
		var runID, instancePath, templateID string
		var planRaw, bundleHash, fieldsRaw sql.NullString
		var attemptOrdinal sql.NullInt64
		var planHash sql.NullString
		var phase sql.NullString
		var attemptState sql.NullString
		if err := rows.Scan(&runID, &instancePath, &templateID, &planRaw, &planHash, &attemptOrdinal, &phase, &attemptState, &bundleHash, &fieldsRaw); err != nil {
			return nil, fmt.Errorf("scan %s: %w", label, err)
		}
		instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
		templateID = strings.TrimSpace(templateID)
		if instancePath == "" || templateID == "" {
			return nil, fmt.Errorf("%s is missing exact instance identity", label)
		}
		if !planRaw.Valid || strings.TrimSpace(planRaw.String) == "" {
			return nil, fmt.Errorf("%s %s is missing exact readiness plan", label, instancePath)
		}
		if !attemptOrdinal.Valid || attemptOrdinal.Int64 <= 0 {
			return nil, fmt.Errorf("%s %s is missing exact attachment attempt", label, instancePath)
		}
		if !bundleHash.Valid {
			return nil, fmt.Errorf("%s %s is missing exact run source artifact", label, instancePath)
		}
		readiness, err := runtimepipeline.DecodeDynamicFlowRuntimeReadinessPersistenceRecord(
			runtimepipeline.DynamicFlowRuntimeReadinessPersistenceRecord{
				RunID: runID, InstancePath: instancePath, Plan: []byte(planRaw.String),
				AttemptOrdinal:      uint64(attemptOrdinal.Int64),
				PlanHash:            planHash.String,
				Phase:               runtimepipeline.FlowAttachmentPhase(phase.String),
				AttemptState:        attemptState.String,
				OwningRunBundleHash: bundleHash.String,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("validate %s %s readiness plan: %w", label, instancePath, err)
		}
		plan := readiness.Plan
		if plan.RunID != runID || plan.Identity.InstancePath != instancePath || plan.Identity.TemplateID != templateID {
			return nil, fmt.Errorf("%s %s readiness identity does not match persisted owner", label, instancePath)
		}
		if !fieldsRaw.Valid {
			return nil, fmt.Errorf("%s %s is missing exact entity state", label, instancePath)
		}
		persistedSource, err := runtimecorrelation.DecodeSourceArtifactFact(bundleHash.String)
		if err != nil {
			return nil, fmt.Errorf("%s %s run bundle source: %w", label, instancePath, err)
		}
		planSource, err := runtimecorrelation.DecodeSourceArtifactFact(plan.BundleHash)
		if err != nil || planSource != persistedSource {
			return nil, fmt.Errorf("%s %s readiness source does not match persisted run", label, instancePath)
		}
		addressFields, err := exactDescriptorAddressFields(fieldsRaw.String)
		if err != nil {
			return nil, fmt.Errorf("%s %s entity fields: %w", label, instancePath, err)
		}
		out = append(out, runtimebus.ActiveFlowInstanceDescriptor{
			Identity:   plan.Identity,
			RunID:      runID,
			InstanceID: plan.Identity.InstanceID, EntityID: plan.Identity.EntityID,
			FlowInstance: instancePath, FlowTemplate: templateID,
			BundleHash:      plan.BundleHash,
			WorkflowVersion: plan.WorkflowVersion, AddressFields: addressFields,
		}.Normalized())
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %ss: %w", label, err)
	}
	return out, nil
}

func exactDescriptorAddressFields(raw any) (map[string]string, error) {
	values, err := decodeDescriptorJSONMap(raw)
	if err != nil {
		return nil, err
	}
	return runtimepinrouting.DescriptorAddressFields(values)
}

func decodeDescriptorJSONMap(raw any) (map[string]any, error) {
	data := jsonRawMessageValue(raw)
	if len(data) == 0 || strings.TrimSpace(string(data)) == "" || strings.TrimSpace(string(data)) == "null" {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	if out == nil {
		return map[string]any{}, nil
	}
	return out, nil
}
