package eventpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/google/uuid"
)

type lifecycleDiagnosticProjection struct {
	ParentEventID      string             `json:"parent_event_id"`
	LineageDisposition string             `json:"lineage_disposition"`
	EventID            string             `json:"event_id"`
	RunID              string             `json:"run_id"`
	Payload            json.RawMessage    `json:"payload"`
	CreatedAt          time.Time          `json:"created_at"`
	ExecutionMode      executionmode.Mode `json:"execution_mode"`
}

func sameDiagnosticJSON(a, b []byte) bool {
	ac, ae := canonicaljson.Canonicalize(a)
	bc, be := canonicaljson.Canonicalize(b)
	return ae == nil && be == nil && bytes.Equal(ac, bc)
}

func persistLifecycleDiagnosticTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *revisionEffects, store eventCommitTxStore, postgres bool, item diaglog.LifecycleDiagnostic, record runtimepkg.RuntimeLogPersistenceRecord) (bool, error) {
	if err := item.Validate(); err != nil {
		return false, err
	}
	canonical, err := runtimepkg.EncodeLifecycleDiagnosticLog(item)
	if err != nil || !sameDiagnosticJSON(canonical, record.Payload) {
		return false, fmt.Errorf("noncanonical lifecycle diagnostic payload: %v", err)
	}
	if _, err := uuid.Parse(item.OutboxID); err != nil {
		return false, err
	}
	if _, err := uuid.Parse(item.OperationID); err != nil {
		return false, err
	}
	if record.RunID != "" || record.ParentEventID != "" || !record.ExecutionMode.Valid() {
		return false, fmt.Errorf("lifecycle diagnostic cannot inherit caller lineage")
	}
	query := `SELECT operation_id, run_id, agent_id, agent_name_owner, agent_name_source,
		agent_route_presence, flow_scope_key, flow_instance_id, flow_instance, event_name,
		payload, created_at, projected_at, projection
		FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id = ?`
	if postgres {
		query = `SELECT operation_id::text, run_id::text, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance, event_name,
			payload, created_at, projected_at, projection
			FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id = $1::uuid FOR UPDATE`
	}
	var fields agentidentity.StorageFields
	var operation, eventName string
	var payload, projection []byte
	var created, projected any
	if err := tx.QueryRowContext(ctx, query, item.OutboxID).Scan(&operation, &fields.RunID, &fields.AgentID,
		&fields.NameOwner, &fields.NameSource, &fields.RoutePresence, &fields.FlowScopeKey,
		&fields.FlowInstanceID, &fields.FlowInstancePath, &eventName, &payload, &created, &projected, &projection); err != nil {
		return false, fmt.Errorf("load exact lifecycle diagnostic: %w", err)
	}
	wantFields, err := item.Identity.StorageFields()
	if err != nil {
		return false, err
	}
	wantPayload, err := json.Marshal(item.Payload)
	if err != nil {
		return false, err
	}
	// Compare the complete immutable producer snapshot before either first settlement or replay.
	at, validTime, err := sqliteTimeValue(created)
	if err != nil || !validTime {
		return false, fmt.Errorf("invalid lifecycle diagnostic timestamp: %v", err)
	}
	if fields != wantFields || operation != item.OperationID || eventName != item.EventName ||
		!sameDiagnosticJSON(payload, wantPayload) || !at.Equal(item.CreatedAt) {
		return false, fmt.Errorf("lifecycle diagnostic snapshot conflict")
	}
	eventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm:lifecycle-diagnostic:"+item.OutboxID)).String()
	lineage, err := item.ProducerLineage()
	if err != nil {
		return false, err
	}
	parent, disposition := lineage.ParentEventID, "causal_explicit"
	if parent == "" {
		parent, disposition = lineage.SubjectEventID, "causal_subject"
	}
	if parent == "" {
		disposition = "parentless"
	} else if _, err := uuid.Parse(parent); err != nil {
		return false, fmt.Errorf("invalid lifecycle diagnostic producer parent: %w", err)
	}
	if projected != nil {
		var receipt lifecycleDiagnosticProjection
		if json.Unmarshal(projection, &receipt) != nil || receipt.EventID != eventID ||
			(receipt.RunID != "" && receipt.RunID != fields.RunID) || !receipt.ExecutionMode.Valid() ||
			!receipt.CreatedAt.Equal(at) || !sameDiagnosticJSON(receipt.Payload, record.Payload) {
			return false, fmt.Errorf("lifecycle diagnostic projection receipt conflict")
		}
		if receipt.LineageDisposition == "historical_cleanup" {
			if receipt.RunID != "" || receipt.ParentEventID != "" {
				return false, fmt.Errorf("historical lifecycle diagnostic receipt has live lineage")
			}
		} else if receipt.RunID != fields.RunID || receipt.ParentEventID != parent || receipt.LineageDisposition != disposition {
			return false, fmt.Errorf("lifecycle diagnostic projection lineage conflict")
		}
		return false, nil
	}
	if len(projection) != 0 {
		return false, fmt.Errorf("unacknowledged lifecycle diagnostic has projection receipt")
	}
	var existingRun string
	runQuery := "SELECT run_id FROM runs WHERE run_id = ?"
	if postgres {
		runQuery = "SELECT run_id::text FROM runs WHERE run_id = $1::uuid FOR KEY SHARE"
	}
	err = tx.QueryRowContext(ctx, runQuery, fields.RunID).Scan(&existingRun)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("admit lifecycle diagnostic history: %w", err)
	}
	receipt := lifecycleDiagnosticProjection{EventID: eventID, RunID: existingRun, Payload: record.Payload, CreatedAt: at, ExecutionMode: record.ExecutionMode}
	receipt.ParentEventID, receipt.LineageDisposition = parent, disposition
	facts := events.EventFacts{ID: eventID, Type: events.EventTypePlatformRuntimeLog,
		Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"},
		Payload:  record.Payload, CreatedAt: at, ExecutionMode: receipt.ExecutionMode}
	var event events.Event
	if existingRun == "" {
		receipt.ParentEventID, receipt.LineageDisposition = "", "historical_cleanup"
		event, err = events.NewStandaloneDiagnosticDirectEvent(events.StandaloneRuntimeEventInput{Facts: facts})
	} else if parent != "" {
		// The canonical append owner admits the persisted same-run reference in
		// this transaction; missing/foreign parents cannot be demoted to roots.
		event, err = events.NewCausalDiagnosticDirectEvent(events.CausalRuntimeEventInput{Facts: facts, Lineage: events.EventLineage{
			RunID: existingRun, ParentEventID: parent, ExecutionMode: receipt.ExecutionMode,
		}})
	} else {
		event, err = events.NewRunScopedDiagnosticDirectEvent(events.RunScopedRuntimeEventInput{Facts: facts, RunID: existingRun})
	}
	if err != nil {
		return false, err
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return false, err
	}
	settlement, err := events.NewNoDeliverySettlement(events.EventWriteRuntimeLogDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	if err != nil {
		return false, err
	}
	outcome, err := store.appendAdmittedEventTxOutcome(ctx, tx, runtimeAuthorActivityMutation(story), effects, admitted, settlement)
	if err != nil {
		return false, err
	}
	if outcome != runtimebus.EventAppendInserted {
		return false, fmt.Errorf("pending lifecycle diagnostic already has an event without acknowledgment")
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		return false, err
	}
	update := "UPDATE agent_lifecycle_diagnostic_outbox SET projected_at = ?, projection = ? WHERE outbox_id = ? AND projected_at IS NULL"
	args := []any{time.Now().UTC(), string(raw), item.OutboxID}
	if postgres {
		update = "UPDATE agent_lifecycle_diagnostic_outbox SET projected_at = $1, projection = $2::jsonb WHERE outbox_id = $3::uuid AND projected_at IS NULL"
	}
	result, err := tx.ExecContext(ctx, update, args...)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return false, fmt.Errorf("lifecycle diagnostic acknowledgment failed: rows=%d error=%v", n, err)
	}
	return true, nil
}

func (s *EventPostgresOwner) PersistLifecycleDiagnostic(ctx context.Context, item diaglog.LifecycleDiagnostic, record runtimepkg.RuntimeLogPersistenceRecord) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	if err := s.validateEventPayload(ctx, runtimeLogEventName, record.Payload); err != nil {
		return false, err
	}
	inserted := false
	err := s.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *revisionEffects) error {
		var err error
		inserted, err = persistLifecycleDiagnosticTx(txctx, tx, story, effects, s, true, item, record)
		return err
	})
	return inserted && err == nil, err
}

func (s *EventSQLiteOwner) PersistLifecycleDiagnostic(ctx context.Context, item diaglog.LifecycleDiagnostic, record runtimepkg.RuntimeLogPersistenceRecord) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	if err := s.validateEventPayload(ctx, runtimeLogEventName, record.Payload); err != nil {
		return false, err
	}
	inserted := false
	err := s.runPrivateAuthorActivityMutation(ctx, "sqlite lifecycle diagnostic settlement", func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *revisionEffects) error {
		var err error
		inserted, err = persistLifecycleDiagnosticTx(txctx, tx, story, effects, s, false, item, record)
		return err
	})
	return inserted && err == nil, err
}
