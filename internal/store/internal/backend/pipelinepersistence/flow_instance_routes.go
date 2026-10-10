package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

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
