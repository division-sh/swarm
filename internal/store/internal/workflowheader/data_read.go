package workflowheader

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// DataReadColumns and DataReadFrom preserve data-query membership. The OR join
// exposes conflicting identity/path owners instead of relabeling them imports.
const DataReadColumns = `CAST(es.run_id AS TEXT), CAST(es.entity_id AS TEXT),
	es.flow_instance, es.entity_type, es.slug, es.name, es.fields, es.created_at, es.updated_at,
	CAST(fi.entity_id AS TEXT), fi.instance_path, fi.entity_type, fi.flow_template,
	fi.mode, fi.status, fi.config, fi.current_state, fi.stage_defined, fi.revision,
	fi.entered_state_at, fi.created_at, fi.updated_at, fi.terminated_at,
	fi.gates, fi.bookkeeping, fi.accumulator, fi.parent_instance, fi.instance_key,
	(SELECT COUNT(*) FROM flow_instances owner
	 WHERE owner.run_id = es.run_id AND (owner.entity_id = es.entity_id OR owner.instance_path = es.flow_instance))`

const DataReadFrom = `FROM entity_state es LEFT JOIN flow_instances fi
	ON fi.run_id = es.run_id AND (fi.entity_id = es.entity_id OR fi.instance_path = es.flow_instance)`

type dataScanner interface{ Scan(...any) error }

// ScanData shares exact companion admission across operator, agent and handler
// reads. decodeTime is only the existing backend's timestamp transport codec.
func ScanData(row dataScanner, decodeTime func(any) (time.Time, bool, error), tail ...any) (operatorread.OperatorEntityFull, error) {
	var runID string
	var state pipeline.WorkflowEntityStatePersistenceRecord
	var slug, name sql.NullString
	var header pipeline.WorkflowInstancePersistenceRecord
	var headerID, path, entityType, template, mode, status, stage sql.NullString
	var parentInstance, instanceKey sql.NullString
	var stageDefined sql.NullBool
	var revision sql.NullInt64
	var owners int
	var dataCreated, dataUpdated, entered, created, updated, terminated any
	var fields, config, gates, bookkeeping, accumulator []byte
	dest := []any{&runID, &state.EntityID, &state.FlowInstance, &state.EntityType, &slug, &name,
		&fields, &dataCreated, &dataUpdated, &headerID, &path, &entityType, &template,
		&mode, &status, &config, &stage, &stageDefined, &revision, &entered, &created, &updated,
		&terminated, &gates, &bookkeeping, &accumulator, &parentInstance, &instanceKey, &owners}
	if err := row.Scan(append(dest, tail...)...); err != nil {
		return operatorread.OperatorEntityFull{}, err
	}
	state.Fields = fields
	header.Config, header.Gates, header.Bookkeeping, header.Accumulator = config, gates, bookkeeping, accumulator
	if owners > 1 || (owners == 1) != headerID.Valid {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("entity has conflicting constructed header owners")
	}
	state.Slug, state.Name = slug.String, name.String
	for _, clock := range []struct {
		name string
		raw  any
		dst  *time.Time
	}{
		{"data created_at", dataCreated, &state.CreatedAt}, {"data updated_at", dataUpdated, &state.UpdatedAt},
		{"entered_state_at", entered, &header.EnteredStageAt}, {"lifecycle created_at", created, &header.CreatedAt},
		{"lifecycle updated_at", updated, &header.UpdatedAt}, {"terminated_at", terminated, &header.TerminatedAt},
	} {
		at, _, err := decodeTime(clock.raw)
		if err != nil {
			return operatorread.OperatorEntityFull{}, fmt.Errorf("decode %s: %w", clock.name, err)
		}
		*clock.dst = at
	}
	target := pipeline.WorkflowTargetPersistenceRecord{Presence: pipeline.WorkflowTargetPersistenceStateOnly, State: state}
	if headerID.Valid {
		if !stageDefined.Valid || !revision.Valid {
			return operatorread.OperatorEntityFull{}, fmt.Errorf("constructed lifecycle presence is incomplete")
		}
		header.EntityID, header.FlowInstance, header.EntityType = headerID.String, path.String, entityType.String
		header.WorkflowName, header.Mode, header.Status = template.String, mode.String, status.String
		header.ParentInstance, header.InstanceKey = parentInstance.String, instanceKey.String
		header.CurrentState, header.StageDefined, header.Revision = stage.String, stageDefined.Bool, revision.Int64
		target.Presence = pipeline.WorkflowTargetPersistenceComplete
		target.Lifecycle = pipeline.WorkflowLifecycleCompanionPersistenceRecord{
			ParentInstance: header.ParentInstance, InstanceKey: header.InstanceKey,
			FlowInstance: header.FlowInstance, WorkflowName: header.WorkflowName, Mode: header.Mode, Status: header.Status,
			Config: header.Config, CreatedAt: header.CreatedAt, TerminatedAt: header.TerminatedAt, State: header, StageDefined: header.StageDefined,
		}
		for _, object := range []struct {
			name string
			raw  any
		}{{"gates", header.Gates}, {"bookkeeping", header.Bookkeeping}, {"accumulator", header.Accumulator}} {
			if err := requireJSONObject(object.name, object.raw); err != nil {
				return operatorread.OperatorEntityFull{}, err
			}
		}
	}
	if err := target.Validate(flowidentity.RouteForInstancePath(state.FlowInstance), identity.NormalizeEntityID(state.EntityID)); err != nil {
		return operatorread.OperatorEntityFull{}, err
	}
	if headerID.Valid && header.UpdatedAt.Before(header.CreatedAt) {
		return operatorread.OperatorEntityFull{}, fmt.Errorf("constructed lifecycle clocks are inconsistent")
	}
	out := operatorread.OperatorEntityFull{Entity: operatorread.OperatorEntitySummary{
		EntityID: state.EntityID, RunID: runID, FlowInstance: state.FlowInstance, EntityType: state.EntityType,
		Slug: state.Slug, Name: state.Name, CreatedAt: state.CreatedAt, UpdatedAt: state.UpdatedAt,
	}}
	if err := canonicaljson.DecodePreservingNumberLexemes(state.Fields, &out.Fields); err != nil {
		return operatorread.OperatorEntityFull{}, err
	}
	if !headerID.Valid {
		return out, nil
	}
	instance, err := target.DecodeComplete(flowidentity.RouteForInstancePath(state.FlowInstance), identity.NormalizeEntityID(state.EntityID))
	if err != nil {
		return operatorread.OperatorEntityFull{}, err
	}
	out.Entity.StageDefined, out.Entity.Revision = &header.StageDefined, &header.Revision
	out.Entity.LifecycleCreatedAt, out.Entity.LifecycleUpdatedAt = &header.CreatedAt, &header.UpdatedAt
	if header.StageDefined {
		out.Entity.CurrentState, out.Entity.EnteredStateAt = &header.CurrentState, &header.EnteredStageAt
	}
	out.Gates, out.Bookkeeping = instance.Gates, instance.Bookkeeping
	loops, err := loopruntime.PublicActivations(instance.StateBuckets)
	if err != nil {
		return operatorread.OperatorEntityFull{}, err
	}
	out.Loops, out.Accumulated = loops, loopruntime.PublicStateBuckets(instance.StateBuckets)
	return out, nil
}
