package agentpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	revision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

type lifecycleDiagnosticOccurrence struct {
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
		agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,flow_instance
		FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id=$1`, id).Scan(&o.runID, &o.operationID, &o.eventName, &raw, &provenance, &mode, &created, &projected,
		&agentID, &nameOwner, &nameSource, &presence, &scope, &instanceID, &path)
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
	identity, err := IdentityFromColumns(o.runID, agentID, nameOwner, nameSource, presence, scope, instanceID, path)
	if err != nil {
		return o, err
	}
	if o.eventName != "platform.agent_lifecycle_transition" || o.result.OperationID != o.operationID || o.result.Identity != identity || o.result.AgentID != agentID || o.result.DiagnosticProvenance != p || string(p.EventMode) != mode || o.result.Replayed {
		return o, errors.New("lifecycle diagnostic occurrence facts conflict")
	}
	o.createdAt, _, err = sqliteTimeValue(created)
	if err != nil || o.createdAt.IsZero() {
		return o, fmt.Errorf("lifecycle diagnostic timestamp invalid: %v", err)
	}
	_, o.projected, err = sqliteTimeValue(projected)
	return o, err
}

func validateLifecycleDiagnosticHistory(ctx context.Context, tx *sql.Tx, o lifecycleDiagnosticOccurrence, origins LifecycleDiagnosticOriginValidator, eventOwner LifecycleDiagnosticEventOwner) error {
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
	f, err := IdentityFields(result.Identity)
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
	raw, err := json.Marshal(o.result)
	if err != nil {
		return runtimepkg.RuntimeLogPersistenceRecord{}, err
	}
	var detail map[string]any
	if err := json.Unmarshal(raw, &detail); err != nil {
		return runtimepkg.RuntimeLogPersistenceRecord{}, err
	}
	detail["outbox_id"], detail["operation_id"], detail["event_name"] = o.id, o.operationID, o.eventName
	p := o.result.DiagnosticProvenance
	record, _, err := runtimepkg.EncodeRuntimeLogRecord(runtimepkg.RuntimeLogFacts{
		EventID: o.id, CreatedAt: o.createdAt, RunID: o.runID, ParentEventID: p.Origin.ParentEventID, ExecutionMode: p.EventMode,
	}, runtimepkg.RuntimeLogEntry{Level: "info", Component: "agent-lifecycle", Action: o.eventName, AgentID: o.result.AgentID, Detail: detail})
	return record, err
}

func projectLifecycleDiagnosticTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *revision.Effects, events LifecycleDiagnosticEventOwner, origins LifecycleDiagnosticOriginValidator, postgres bool) (bool, error) {
	if events == nil || origins == nil {
		return false, errors.New("lifecycle diagnostic owners are required")
	}
	// Select and lock the run before selecting an occurrence. A queue hint is not
	// projection authority: discard may remove its rows while we wait for this lock.
	query := `SELECT r.run_id,o.outbox_id FROM runs r JOIN agent_lifecycle_diagnostic_outbox o ON o.run_id=r.run_id WHERE o.projected_at IS NULL ORDER BY o.created_at,o.outbox_id LIMIT 1`
	if postgres {
		query += ` FOR UPDATE OF r`
	}
	var runID, id string
	if err := tx.QueryRowContext(ctx, query).Scan(&runID, &id); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		var orphan bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM agent_lifecycle_diagnostic_outbox o WHERE NOT EXISTS(SELECT 1 FROM runs r WHERE r.run_id=o.run_id))`).Scan(&orphan); err != nil {
			return false, err
		}
		if orphan {
			return false, errors.New("lifecycle diagnostic run is missing")
		}
		return false, nil
	}
	if err := events.RequirePresentTx(ctx, tx, runID); err != nil {
		return false, err
	}
	query = `SELECT outbox_id FROM agent_lifecycle_diagnostic_outbox WHERE run_id=$1 AND outbox_id=$2`
	if postgres {
		query += ` FOR UPDATE`
	}
	if err := tx.QueryRowContext(ctx, query, runID, id).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	o, err := loadLifecycleDiagnosticOccurrence(ctx, tx, id)
	if err != nil {
		return false, err
	}
	if err := validateLifecycleDiagnosticHistory(ctx, tx, o, origins, events); err != nil {
		return false, err
	}
	record, err := lifecycleDiagnosticLogRecord(o)
	if err != nil {
		return false, err
	}
	if o.projected {
		return false, events.ValidateRuntimeLogRecordTx(ctx, tx, record)
	}
	if _, found, err := events.LoadLifecycleDiagnosticEventTx(ctx, tx, id); err != nil {
		return false, err
	} else if found {
		return false, errors.New("pending lifecycle diagnostic already has an event without acknowledgement")
	}
	if _, err := events.CommitRuntimeLogRecordTx(ctx, tx, story, effects, record); err != nil {
		return false, err
	}
	if err := events.ValidateRuntimeLogRecordTx(ctx, tx, record); err != nil {
		return false, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE agent_lifecycle_diagnostic_outbox SET projected_at=$2 WHERE outbox_id=$1 AND projected_at IS NULL`, id, time.Now().UTC())
	if err := requireSingleLifecycleDiagnosticProjection(res, err); err != nil {
		return false, err
	}
	return true, nil
}

func (s *AgentPostgresOwner) ProjectAgentLifecycleDiagnostics(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	for n := 0; n < limit; n++ {
		effects := revision.NewEffects()
		var projected bool
		err := s.runPrivateAuthorActivityMutation(ctx, effects, func(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			var err error
			projected, err = projectLifecycleDiagnosticTx(ctx, tx, story, effects, s.diagnosticEvents, s.diagnosticOrigins, true)
			return err
		})
		if err != nil || !projected {
			return n, err
		}
	}
	return limit, nil
}

func (s *AgentSQLiteOwner) ProjectAgentLifecycleDiagnostics(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	for n := 0; n < limit; n++ {
		effects := revision.NewEffects()
		var projected bool
		err := s.runPrivateAuthorActivityMutation(ctx, "sqlite project lifecycle diagnostic", effects, func(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			var err error
			projected, err = projectLifecycleDiagnosticTx(ctx, tx, story, effects, s.diagnosticEvents, s.diagnosticOrigins, false)
			return err
		})
		if err != nil || !projected {
			return n, err
		}
	}
	return limit, nil
}

// Only acknowledged exact observations are harmless in a selected tree. The
// returned IDs must be excluded from the stray count, never added as tree roots.
func lifecycleObservationIDsTx(ctx context.Context, tx *sql.Tx, runID string, eventOwner LifecycleDiagnosticEventOwner, origins LifecycleDiagnosticOriginValidator) ([]string, error) {
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
			observations = append(observations, id)
		}
	}
	return observations, nil
}

func (s *AgentPostgresOwner) LifecycleObservationIDsTx(ctx context.Context, tx *sql.Tx, runID string) ([]string, error) {
	return lifecycleObservationIDsTx(ctx, tx, runID, s.diagnosticEvents, s.diagnosticOrigins)
}
func (s *AgentSQLiteOwner) LifecycleObservationIDsTx(ctx context.Context, tx *sql.Tx, runID string) ([]string, error) {
	return lifecycleObservationIDsTx(ctx, tx, runID, s.diagnosticEvents, s.diagnosticOrigins)
}
