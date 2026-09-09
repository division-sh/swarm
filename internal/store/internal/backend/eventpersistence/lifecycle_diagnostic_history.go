package eventpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/google/uuid"
)

type lifecycleDiagnosticEvidenceReader interface {
	LoadLifecycleDiagnosticEventTx(context.Context, *sql.Tx, string) (events.AdmittedEvent, bool, error)
	ValidateRuntimeLogRecordTx(context.Context, *sql.Tx, runtimepkg.RuntimeLogPersistenceRecord) error
	RequirePresentTx(context.Context, *sql.Tx, string) error
}

type lifecycleDiagnosticOccurrence struct {
	item                              diaglog.LifecycleDiagnostic
	projection                        []byte
	id, runID, operationID, eventName string
	result                            runtimemanager.AgentLifecycleTransitionResult
	createdAt                         time.Time
	projected                         bool
}

func loadLifecycleDiagnosticOccurrence(ctx context.Context, tx *sql.Tx, id string) (lifecycleDiagnosticOccurrence, error) {
	o := lifecycleDiagnosticOccurrence{id: id}
	var raw, provenance []byte
	var mode, agentID, nameOwner, nameSource, presence, scope, instanceID, path string
	var created, projected any
	err := tx.QueryRowContext(ctx, `SELECT run_id,operation_id,event_name,payload,provenance,execution_mode,created_at,projected_at,
		agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,flow_instance,projection
		FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id=$1`, id).Scan(&o.runID, &o.operationID, &o.eventName, &raw, &provenance, &mode, &created, &projected,
		&agentID, &nameOwner, &nameSource, &presence, &scope, &instanceID, &path, &o.projection)
	if err != nil {
		return o, fmt.Errorf("load diagnostic occurrence %s: %w", id, err)
	}
	if err := json.Unmarshal(raw, &o.result); err != nil {
		return o, err
	}
	var p runtimemanager.LifecycleDiagnosticProvenance
	if err := json.Unmarshal(provenance, &p); err != nil {
		return o, err
	}
	if err := p.Validate(); err != nil {
		return o, err
	}
	identity, err := agentidentity.FromStorageFields(agentidentity.StorageFields{RunID: o.runID, AgentID: agentID, NameOwner: nameOwner, NameSource: nameSource, RoutePresence: presence, FlowScopeKey: scope, FlowInstanceID: instanceID, FlowInstancePath: path})
	if err != nil {
		return o, err
	}
	if o.eventName != "platform.agent_lifecycle_transition" || o.result.OperationID != o.operationID || o.result.Identity != identity || o.result.AgentID != agentID || o.result.DiagnosticProvenance != p || string(p.EventMode) != mode || o.result.Replayed {
		return o, errors.New("lifecycle diagnostic occurrence facts conflict")
	}
	var payload map[string]any
	if err := canonicaljson.DecodeInto(raw, &payload); err != nil {
		return o, err
	}
	o.item = diaglog.LifecycleDiagnostic{OutboxID: id, OperationID: o.operationID, Identity: identity, AgentID: agentID, EventName: o.eventName, Payload: payload}
	o.createdAt, _, err = sqliteTimeValue(created)
	if err != nil || o.createdAt.IsZero() {
		return o, fmt.Errorf("lifecycle diagnostic timestamp invalid: %v", err)
	}
	o.item.CreatedAt = o.createdAt
	_, o.projected, err = sqliteTimeValue(projected)
	return o, err
}

func validateLifecycleDiagnosticHistory(ctx context.Context, tx *sql.Tx, o lifecycleDiagnosticOccurrence, origins selectedForkLineageOwner, eventOwner lifecycleDiagnosticEvidenceReader) error {
	if origins == nil || eventOwner == nil {
		return errors.New("lifecycle diagnostic evidence owners are required")
	}
	if err := o.result.ProcessBinding.Validate(); err != nil {
		return err
	}
	var raw []byte
	var state, runID string
	var operationTime, transitionTime any
	if err := tx.QueryRowContext(ctx, `SELECT result,state,run_id,created_at FROM agent_lifecycle_operations WHERE operation_id=$1`, o.operationID).Scan(&raw, &state, &runID, &operationTime); err != nil {
		return fmt.Errorf("load diagnostic operation: %w", err)
	}
	var result runtimemanager.AgentLifecycleTransitionResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return err
	}
	if state != "succeeded" || runID != o.runID || !reflect.DeepEqual(result, o.result) {
		return errors.New("diagnostic operation result conflicts with occurrence")
	}
	f, err := result.Identity.StorageFields()
	if err != nil {
		return err
	}
	var matching bool
	b := result.ProcessBinding
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM agent_lifecycle_transition_facts WHERE transition_id=$1 AND operation_id=$2 AND run_id=$3
		AND agent_id=$4 AND agent_name_owner=$5 AND agent_name_source=$6 AND agent_route_presence=$7 AND flow_scope_key=$8 AND flow_instance_id=$9 AND flow_instance=$10
		AND previous_phase=$11 AND next_phase=$12 AND previous_generation=$13 AND next_generation=$14 AND runtime_epoch=$15 AND config_revision=$16 AND run_mode=$17
		AND process_authority_id=$18 AND process_owner_id=$19 AND process_boot_id=$20 AND generation_grant_id=$21 AND bundle_hash=$22 AND runtime_instance_id=$23 AND runtime_generation=$24)`,
		result.TransitionID, o.operationID, o.runID, f.AgentID, f.NameOwner, f.NameSource, f.RoutePresence, f.FlowScopeKey, f.FlowInstanceID, f.FlowInstancePath,
		string(result.PreviousPhase), string(result.Phase), result.PreviousGeneration, result.Generation, result.RuntimeEpoch, result.ConfigRevision, string(result.RunMode),
		b.ProcessAuthorityID, b.ProcessOwnerID, b.ProcessBootID, b.GenerationGrantID, b.BundleHash, b.RuntimeInstanceID, b.RuntimeGeneration).Scan(&matching); err != nil {
		return err
	}
	if !matching {
		return errors.New("lifecycle diagnostic historical transition missing or conflicting")
	}
	if err := tx.QueryRowContext(ctx, `SELECT created_at FROM agent_lifecycle_transition_facts WHERE transition_id=$1`, result.TransitionID).Scan(&transitionTime); err != nil {
		return err
	}
	for _, rawTime := range []any{operationTime, transitionTime} {
		at, present, err := sqliteTimeValue(rawTime)
		if err != nil || !present || !at.UTC().Truncate(time.Microsecond).Equal(o.createdAt.UTC().Truncate(time.Microsecond)) {
			return errors.New("lifecycle diagnostic historical timestamp conflict")
		}
	}
	p := result.DiagnosticProvenance
	if p.Origin.Causality == runtimemanager.LifecycleDiagnosticAcceptedEvent {
		parent, found, err := eventOwner.LoadLifecycleDiagnosticEventTx(ctx, tx, p.Origin.ParentEventID)
		if err != nil {
			return err
		}
		if !found || parent.Event().RunID() != o.runID || parent.Event().ExecutionMode() != p.EventMode {
			return errors.New("lifecycle diagnostic historical parent missing or conflicting")
		}
	}
	_, err = origins.ValidateLifecycleDiagnosticOriginTx(ctx, tx, result, true)
	return err
}

func lifecycleDiagnosticLogRecord(o lifecycleDiagnosticOccurrence) (runtimepkg.RuntimeLogPersistenceRecord, error) {
	raw, err := runtimepkg.EncodeLifecycleDiagnosticLog(o.item)
	if err != nil {
		return runtimepkg.RuntimeLogPersistenceRecord{}, err
	}
	var receipt lifecycleDiagnosticProjection
	if json.Unmarshal(o.projection, &receipt) != nil || receipt.EventID != uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm:lifecycle-diagnostic:"+o.id)).String() ||
		receipt.RunID != o.runID || !receipt.CreatedAt.Equal(o.createdAt) || !receipt.ExecutionMode.Valid() || !sameDiagnosticJSON(receipt.Payload, raw) {
		return runtimepkg.RuntimeLogPersistenceRecord{}, errors.New("lifecycle diagnostic projection receipt conflict")
	}
	lineage, err := o.item.ProducerLineage()
	if err != nil {
		return runtimepkg.RuntimeLogPersistenceRecord{}, err
	}
	parent, disposition := lineage.ParentEventID, "causal_explicit"
	if parent == "" {
		parent, disposition = lineage.SubjectEventID, "causal_subject"
	}
	if parent == "" {
		disposition = "parentless"
	}
	if receipt.ParentEventID != parent || receipt.LineageDisposition != disposition {
		return runtimepkg.RuntimeLogPersistenceRecord{}, errors.New("lifecycle diagnostic projection lineage conflict")
	}
	return runtimepkg.RuntimeLogPersistenceRecord{EventID: receipt.EventID, RunID: receipt.RunID, ParentEventID: receipt.ParentEventID, CreatedAt: receipt.CreatedAt, ExecutionMode: receipt.ExecutionMode, Payload: raw}, nil
}

// Only acknowledged exact observations are harmless in a selected tree. The
// returned IDs must be excluded from the stray count, never added as tree roots.
func lifecycleObservationIDsTx(ctx context.Context, tx *sql.Tx, runID string, eventOwner lifecycleDiagnosticEvidenceReader, origins selectedForkLineageOwner) ([]string, error) {
	if eventOwner == nil || origins == nil {
		return nil, errors.New("lifecycle diagnostic owners are required")
	}
	if err := eventOwner.RequirePresentTx(ctx, tx, runID); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT outbox_id FROM agent_lifecycle_diagnostic_outbox WHERE run_id=$1 AND projected_at IS NOT NULL ORDER BY outbox_id`, runID)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	observations := []string{}
	for _, id := range ids {
		o, err := loadLifecycleDiagnosticOccurrence(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if err := validateLifecycleDiagnosticHistory(ctx, tx, o, origins, eventOwner); err != nil {
			return nil, err
		}
		record, err := lifecycleDiagnosticLogRecord(o)
		if err != nil {
			return nil, err
		}
		if err := eventOwner.ValidateRuntimeLogRecordTx(ctx, tx, record); err != nil {
			return nil, err
		}
		if o.result.DiagnosticProvenance.Origin.Owner == runtimemanager.LifecycleDiagnosticSelectedFork && o.result.DiagnosticProvenance.Origin.Causality == runtimemanager.LifecycleDiagnosticObservation {
			if record.ParentEventID == "" {
				observations = append(observations, record.EventID)
			}
		}
	}
	return observations, nil
}

func (s *EventPostgresOwner) LifecycleObservationIDsTx(ctx context.Context, tx *sql.Tx, runID string) ([]string, error) {
	return lifecycleObservationIDsTx(ctx, tx, runID, s, s.runFork)
}
func (s *EventSQLiteOwner) LifecycleObservationIDsTx(ctx context.Context, tx *sql.Tx, runID string) ([]string, error) {
	return lifecycleObservationIDsTx(ctx, tx, runID, s, s.runFork)
}
