package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

// SQLiteRuntimeStore is the file-backed SQLite implementation of the selected
// runtime persistence contracts. SQLite is a local-dev backend: process-local
// mutexes provide startup/session serialization while persisted rows remain the
// canonical state consumed by the runtime.
type SQLiteRuntimeStore struct {
	*SQLiteSchemaStore

	eventPayloadValidator EventPayloadValidator
	startupMu             sync.Mutex
	sessionMu             sync.Mutex
	replayMu              sync.Mutex
	startupOwner          string
	replayClaims          map[string]struct{}
	sessionLockTTL        time.Duration
	nowFn                 func() time.Time
}

var _ SchemaBootstrapper = (*SQLiteRuntimeStore)(nil)
var _ runtimebus.EventStore = (*SQLiteRuntimeStore)(nil)
var _ runtimebus.RunLifecyclePersistence = (*SQLiteRuntimeStore)(nil)
var _ runtimebus.RunLifecycleReadPersistence = (*SQLiteRuntimeStore)(nil)
var _ runtimebus.StandaloneRuntimePlatformRunConvergencePersistence = (*SQLiteRuntimeStore)(nil)
var _ runtimebus.NormalRunCompletionConvergencePersistence = (*SQLiteRuntimeStore)(nil)
var _ runtimemanager.ManagerPersistence = (*SQLiteRuntimeStore)(nil)
var _ runtimepipeline.SchedulePersistence = (*SQLiteRuntimeStore)(nil)
var _ runtimetools.MailboxPersistence = (*SQLiteRuntimeStore)(nil)
var _ runtimeingress.Store = (*SQLiteRuntimeStore)(nil)
var _ runtimeruncontrol.Store = (*SQLiteRuntimeStore)(nil)
var _ runtimereplayclaim.Participation = (*SQLiteRuntimeStore)(nil)

var sqliteAPIIdempotencyLocks = struct {
	sync.Mutex
	byPath map[string]*sync.Mutex
}{byPath: map[string]*sync.Mutex{}}

func NewSQLiteRuntimeStore(path string) (*SQLiteRuntimeStore, error) {
	schemaStore, err := NewSQLiteSchemaStore(path)
	if err != nil {
		return nil, err
	}
	return &SQLiteRuntimeStore{
		SQLiteSchemaStore: schemaStore,
		replayClaims:      map[string]struct{}{},
		sessionLockTTL:    120 * time.Second,
		nowFn:             time.Now,
	}, nil
}

func (s *SQLiteRuntimeStore) schemaCapabilities(ctx context.Context) (StoreSchemaCapabilities, error) {
	if s == nil || s.DB == nil {
		return StoreSchemaCapabilities{}, fmt.Errorf("sqlite runtime store is required")
	}
	return s.ResolveSchemaCapabilities(ctx)
}

func (*SQLiteRuntimeStore) SupportsPersistedReplay() bool { return true }

func (s *SQLiteRuntimeStore) SetSessionLockTTL(ttl time.Duration) {
	if s == nil {
		return
	}
	if ttl <= 0 {
		ttl = 120 * time.Second
	}
	s.sessionLockTTL = ttl
}

func (s *SQLiteRuntimeStore) now() time.Time {
	if s == nil || s.nowFn == nil {
		return time.Now().UTC()
	}
	return s.nowFn().UTC()
}

func (s *SQLiteRuntimeStore) SetEventPayloadValidator(validator func(eventType string, payload []byte) error) {
	if s == nil {
		return
	}
	s.eventPayloadValidator = EventPayloadValidator(validator)
}

func (s *SQLiteRuntimeStore) validateEventPayload(eventType string, payload []byte) error {
	if s == nil || s.eventPayloadValidator == nil {
		return nil
	}
	if err := s.eventPayloadValidator(strings.TrimSpace(eventType), payload); err != nil {
		return fmt.Errorf("validate event payload: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) CanonicalRuntimeLogCapability(ctx context.Context) (bool, bool, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return false, false, err
	}
	if caps.Events.Log != SchemaFlavorCanonical {
		return false, false, nil
	}
	return true, caps.Events.LogRunID, nil
}

func (s *SQLiteRuntimeStore) CanonicalEventReceiptsCapability(ctx context.Context) (bool, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return false, err
	}
	return caps.Events.Log == SchemaFlavorCanonical && caps.Events.Receipts == SchemaFlavorCanonical, nil
}

func (s *SQLiteRuntimeStore) AppendEvent(ctx context.Context, evt events.Event) error {
	return s.AppendEventTx(ctx, nil, evt)
}

func (s *SQLiteRuntimeStore) BeginEventTx(ctx context.Context) (*sql.Tx, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("sqlite runtime store is required")
	}
	return s.DB.BeginTx(ctx, nil)
}

func (s *SQLiteRuntimeStore) AppendEventTx(ctx context.Context, tx *sql.Tx, evt events.Event) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if caps.Events.Log != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("events", caps.Events.Log)
	}
	id, runID, name, entityID, flowInstance, scope, payload, chainDepth, producedBy, producedByType, sourceEventID, createdAt, err := eventStorageEnvelope(evt)
	if err != nil {
		return err
	}
	if err := s.validateEventPayload(name, payload); err != nil {
		return err
	}
	sourceRoute, targetRoute, targetSet := eventRouteStorageEnvelope(evt)
	if eventHasRouteIdentity(evt) && !caps.Events.LogRouteIdentity {
		return fmt.Errorf("events source_route/target_route/target_set columns required for routed event")
	}
	ownedTx := tx == nil
	if ownedTx {
		tx, err = s.DB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin sqlite event tx: %w", err)
		}
	}
	committed := false
	defer func() {
		if ownedTx && !committed {
			_ = tx.Rollback()
		}
	}()
	if caps.Events.LogRunID {
		if err := sqliteEnsureRunRow(ctx, tx, runID, id, name, createdAt); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO events (
			event_id, run_id, event_name, entity_id, flow_instance, source_route, target_route, target_set,
			scope, payload, chain_depth, produced_by, produced_by_type, source_event_id, created_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, id, sqliteNullUUID(runID), name, sqliteNullUUID(entityID), sqliteNullString(flowInstance), string(sourceRoute), string(targetRoute), string(targetSet),
		scope, string(payload), chainDepth, sqliteNullString(producedBy), producedByType, sqliteNullUUID(sourceEventID), createdAt.UTC())
	if err != nil {
		return fmt.Errorf("append sqlite event: %w", err)
	}
	if ownedTx {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit sqlite event tx: %w", err)
		}
		committed = true
	}
	return nil
}

func (s *SQLiteRuntimeStore) InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error {
	return s.InsertEventDeliveriesTx(ctx, nil, eventID, agentIDs)
}

func (s *SQLiteRuntimeStore) InsertEventDeliveriesTx(ctx context.Context, tx *sql.Tx, eventID string, agentIDs []string) error {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" || len(agentIDs) == 0 {
		return nil
	}
	var runID sql.NullString
	if err := chooseRowQueryer(s.DB, tx).QueryRowContext(ctx, `SELECT run_id FROM events WHERE event_id = ?`, eventID).Scan(&runID); err != nil {
		return fmt.Errorf("load event run for sqlite delivery manifest: %w", err)
	}
	ownedTx := tx == nil
	var err error
	if ownedTx {
		tx, err = s.DB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin sqlite delivery manifest tx: %w", err)
		}
	}
	committed := false
	defer func() {
		if ownedTx && !committed {
			_ = tx.Rollback()
		}
	}()
	for _, agentID := range agentIDs {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO event_deliveries (
				delivery_id, run_id, event_id, subscriber_type, subscriber_id, delivery_target_route, status, created_at
			)
			VALUES (?, ?, ?, 'agent', ?, '{}', 'pending', ?)
		`, uuid.NewString(), sqliteNullString(runID.String), eventID, agentID, time.Now().UTC()); err != nil {
			return fmt.Errorf("insert sqlite event delivery: %w", err)
		}
	}
	if ownedTx {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit sqlite delivery manifest tx: %w", err)
		}
		committed = true
	}
	return nil
}

func (s *SQLiteRuntimeStore) ListEventDeliveryRecipients(ctx context.Context, eventID string) ([]string, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, runtimereplayclaim.ErrAuthoritativeRecipientManifestUnavailable
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT subscriber_id
		FROM event_deliveries
		WHERE event_id = ?
		  AND subscriber_type = 'agent'
		ORDER BY created_at ASC, subscriber_id ASC
	`, eventID)
	if err != nil {
		return nil, fmt.Errorf("query sqlite delivery recipients: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var recipient string
		if err := rows.Scan(&recipient); err != nil {
			return nil, fmt.Errorf("scan sqlite delivery recipient: %w", err)
		}
		out = append(out, strings.TrimSpace(recipient))
	}
	return out, rows.Err()
}

func (s *SQLiteRuntimeStore) UpsertAgent(ctx context.Context, rec runtimemanager.PersistedAgent) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if caps.Agents != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("agents", caps.Agents)
	}
	if rec.Config.ID == "" {
		return fmt.Errorf("agent id is required")
	}
	rec.Config.NormalizeEntityID()
	rec.Config.NormalizeRuntimeDescriptor()
	if _, err := runtimesessions.ValidateAgentSessionScopeConfig(rec.Config); err != nil {
		return fmt.Errorf("invalid agent session scope: %w", err)
	}
	projection, err := projectPersistedAgentConfig(rec.Config, rec.ParentAgentID)
	if err != nil {
		return err
	}
	startedAt := rec.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	_, err = s.DB.ExecContext(ctx, `
		INSERT INTO agents (
			agent_id, flow_instance, role, model, llm_backend, conversation_mode,
			parent_agent_id, entity_id, config, subscriptions, emit_events, tools, permissions,
			runtime_descriptor, status, turn_count, last_active_at, created_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
		ON CONFLICT(agent_id) DO UPDATE SET
			flow_instance = excluded.flow_instance,
			role = excluded.role,
			model = excluded.model,
			llm_backend = excluded.llm_backend,
			conversation_mode = excluded.conversation_mode,
			parent_agent_id = excluded.parent_agent_id,
			entity_id = excluded.entity_id,
			config = excluded.config,
			subscriptions = excluded.subscriptions,
			emit_events = excluded.emit_events,
			tools = excluded.tools,
			permissions = excluded.permissions,
			runtime_descriptor = excluded.runtime_descriptor,
			status = excluded.status,
			last_active_at = excluded.last_active_at
	`, projection.AgentID, sqliteNullString(projection.FlowInstance), projection.Role, projection.Model, projection.LLMBackend, projection.ConversationMode,
		sqliteNullString(projection.ParentAgentID), sqliteNullUUID(projection.EntityID), string(projection.ConfigJSON), string(projection.SubscriptionsJSON),
		string(projection.EmitEventsJSON), string(projection.ToolsJSON), string(projection.PermissionsJSON), string(projection.RuntimeDescriptor),
		agentPersistedStatus(rec.Status), time.Now().UTC(), startedAt.UTC())
	if err != nil {
		return fmt.Errorf("upsert sqlite agent: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT agent_id, COALESCE(flow_instance, ''), role, model, llm_backend, conversation_mode,
		       COALESCE(parent_agent_id, ''), COALESCE(entity_id, ''), config, COALESCE(runtime_descriptor, '{}'),
		       COALESCE(subscriptions, '[]'), COALESCE(emit_events, '[]'), COALESCE(tools, '[]'), COALESCE(permissions, '[]'),
		       COALESCE(status, 'active'), COALESCE(created_at, CURRENT_TIMESTAMP)
		FROM agents
		WHERE status NOT IN ('terminated', 'ephemeral')
		ORDER BY created_at ASC, agent_id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query sqlite agents: %w", err)
	}
	defer rows.Close()
	out := make([]runtimemanager.PersistedAgent, 0)
	for rows.Next() {
		var rec runtimemanager.PersistedAgent
		var row persistedAgentProjection
		var startedAt any
		if err := rows.Scan(&row.AgentID, &row.FlowInstance, &row.Role, &row.Model, &row.LLMBackend, &row.ConversationMode,
			&row.ParentAgentID, &row.EntityID, &row.ConfigJSON, &row.RuntimeDescriptor, &row.SubscriptionsJSON, &row.EmitEventsJSON,
			&row.ToolsJSON, &row.PermissionsJSON, &rec.Status, &startedAt); err != nil {
			return nil, fmt.Errorf("scan sqlite agent: %w", err)
		}
		if at, ok, err := sqliteTimeValue(startedAt); err != nil {
			return nil, fmt.Errorf("scan sqlite agent created_at: %w", err)
		} else if ok {
			rec.StartedAt = at
		}
		cfg, err := hydratePersistedAgentConfig(row)
		if err != nil {
			return nil, fmt.Errorf("hydrate sqlite agent row %s: %w", strings.TrimSpace(row.AgentID), err)
		}
		rec.ParentAgentID = row.ParentAgentID
		rec.Config = cfg
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *SQLiteRuntimeStore) MarkAgentTerminated(ctx context.Context, agentID string) error {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return fmt.Errorf("agent_id is required")
	}
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite runtime store is required")
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin sqlite mark agent terminated tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	now := s.now()
	if _, err := tx.ExecContext(ctx, `
		UPDATE agents
		SET status = 'terminated',
		    last_active_at = ?
		WHERE agent_id = ?
	`, now, agentID); err != nil {
		return fmt.Errorf("mark sqlite agent terminated: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE agent_sessions
		SET status = 'terminated',
		    termination_reason = 'cancelled',
		    terminated_at = COALESCE(terminated_at, ?),
		    lease_holder = NULL,
		    lease_expires_at = NULL,
		    updated_at = ?
		WHERE agent_id = ?
		  AND status IN ('active', 'suspended')
	`, now, now, agentID); err != nil {
		return fmt.Errorf("mark sqlite agent terminated sessions: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE agent_conversation_audits
		SET status = 'terminated',
		    updated_at = ?
		WHERE agent_id = ?
		  AND status = 'active'
	`, now, agentID); err != nil {
		return fmt.Errorf("mark sqlite agent terminated conversation audits: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sqlite mark agent terminated tx: %w", err)
	}
	committed = true
	return nil
}

func (s *SQLiteRuntimeStore) EnsureEntitySchema(ctx context.Context, entityID string) error {
	if strings.TrimSpace(entityID) == "" {
		return fmt.Errorf("entity_id is required")
	}
	if _, err := uuid.Parse(strings.TrimSpace(entityID)); err != nil {
		return nil
	}
	identity, err := runtimecurrentstate.RequireIdentity(ctx, entityID)
	if err != nil {
		return fmt.Errorf("lookup sqlite entity schema identity: %w", err)
	}
	var exists int
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM entity_state
		WHERE run_id = ?
		  AND entity_id = ?
	`, identity.RunID, identity.EntityID).Scan(&exists); err != nil {
		return fmt.Errorf("lookup sqlite entity schema row: %w", err)
	}
	if exists == 0 {
		return fmt.Errorf("lookup sqlite entity schema row: sql: no rows in result set")
	}
	return nil
}

func (s *SQLiteRuntimeStore) EnsureRuntimeIngressState(ctx context.Context, now time.Time) (runtimeingress.State, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if _, err := s.DB.ExecContext(ctx, `
		INSERT OR IGNORE INTO runtime_ingress_state (id, status, controlled_by, updated_at)
		VALUES (1, 'running', 'runtime', ?)
	`, now.UTC()); err != nil {
		return runtimeingress.State{}, fmt.Errorf("ensure sqlite runtime ingress state: %w", err)
	}
	return s.LoadRuntimeIngressState(ctx)
}

func (s *SQLiteRuntimeStore) LoadRuntimeIngressState(ctx context.Context) (runtimeingress.State, error) {
	state, err := scanRuntimeIngressState(s.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason, ''), controlled_by, COALESCE(transition_event_id, ''), updated_at
		FROM runtime_ingress_state
		WHERE id = 1
	`))
	if err == sql.ErrNoRows {
		return runtimeingress.State{}, runtimeingress.ErrStateNotInitialized
	}
	if err != nil {
		return runtimeingress.State{}, fmt.Errorf("load sqlite runtime ingress state: %w", err)
	}
	return state, nil
}

func (s *SQLiteRuntimeStore) TransitionRuntimeIngressState(ctx context.Context, target runtimeingress.Status, reason, controlledBy string, now time.Time) (runtimeingress.State, bool, error) {
	if target != runtimeingress.StatusRunning && target != runtimeingress.StatusPaused {
		return runtimeingress.State{}, false, fmt.Errorf("unsupported runtime ingress status: %s", target)
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if _, err := s.EnsureRuntimeIngressState(ctx, now); err != nil {
		return runtimeingress.State{}, false, err
	}
	current, err := s.LoadRuntimeIngressState(ctx)
	if err != nil {
		return runtimeingress.State{}, false, err
	}
	if current.Status == target {
		return current, false, nil
	}
	if controlledBy = strings.TrimSpace(controlledBy); controlledBy == "" {
		controlledBy = "runtime"
	}
	_, err = s.DB.ExecContext(ctx, `
		UPDATE runtime_ingress_state
		SET status = ?, reason = ?, controlled_by = ?, transition_event_id = NULL, updated_at = ?
		WHERE id = 1
	`, string(target), sqliteNullString(reason), controlledBy, now.UTC())
	if err != nil {
		return runtimeingress.State{}, false, fmt.Errorf("update sqlite runtime ingress state: %w", err)
	}
	state, err := s.LoadRuntimeIngressState(ctx)
	return state, true, err
}

func (s *SQLiteRuntimeStore) SetRuntimeIngressTransitionEvent(ctx context.Context, target runtimeingress.Status, eventID string, transitionAt time.Time) (bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return false, nil
	}
	res, err := s.DB.ExecContext(ctx, `
		UPDATE runtime_ingress_state
		SET transition_event_id = ?, updated_at = ?
		WHERE id = 1
		  AND status = ?
		  AND updated_at = ?
	`, eventID, transitionAt.UTC(), string(target), transitionAt.UTC())
	if err != nil {
		return false, fmt.Errorf("set sqlite runtime ingress transition event: %w", err)
	}
	rows, err := res.RowsAffected()
	return rows > 0, err
}

func (s *SQLiteRuntimeStore) StopRunControl(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	return s.sqliteRunControlTransition(ctx, req, "stop")
}

func (s *SQLiteRuntimeStore) PauseRunControl(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	return s.sqliteRunControlTransition(ctx, req, "pause")
}

func (s *SQLiteRuntimeStore) ContinueRunControl(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	return s.sqliteRunControlTransition(ctx, req, "continue")
}

func (s *SQLiteRuntimeStore) RunDispatchBlocked(ctx context.Context, runID string) (bool, error) {
	runID = nullUUIDString(runID)
	if runID == "" {
		return false, nil
	}
	var blocked bool
	if err := s.DB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM run_control_state
			WHERE run_id = ? AND control_status IN ('paused', 'stopped')
		)
	`, runID).Scan(&blocked); err != nil {
		return false, fmt.Errorf("load sqlite run dispatch control state: %w", err)
	}
	return blocked, nil
}

func (s *SQLiteRuntimeStore) sqliteRunControlTransition(ctx context.Context, req runtimeruncontrol.TransitionRequest, action string) (runtimeruncontrol.State, error) {
	runID := nullUUIDString(req.RunID)
	if runID == "" {
		return runtimeruncontrol.State{}, fmt.Errorf("run_id is required")
	}
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	}
	if req.Reason = strings.TrimSpace(req.Reason); req.Reason == "" {
		req.Reason = "operator_request"
	}
	if req.ControlledBy = strings.TrimSpace(req.ControlledBy); req.ControlledBy == "" {
		req.ControlledBy = "api.v1"
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("begin sqlite run control transition: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	state, err := sqliteLoadRunControlState(ctx, tx, runID)
	if err != nil {
		return runtimeruncontrol.State{}, err
	}
	switch action {
	case "pause":
		state, err = sqlitePauseRunControl(ctx, tx, state, req)
	case "continue":
		state, err = sqliteContinueRunControl(ctx, tx, state, req)
	case "stop":
		state, err = sqliteStopRunControl(ctx, tx, state, req)
	default:
		err = fmt.Errorf("unsupported run control action %q", action)
	}
	if err != nil {
		return runtimeruncontrol.State{}, err
	}
	if err := tx.Commit(); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("commit sqlite run control transition: %w", err)
	}
	committed = true
	return state, nil
}

func (s *SQLiteRuntimeStore) WithAPIIdempotency(ctx context.Context, req APIIdempotencyRequest, execute func(context.Context) (APIIdempotencyCompletion, error)) (APIIdempotencyCompletion, bool, error) {
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		completion, err := execute(ctx)
		return completion, false, err
	}
	if s == nil || s.DB == nil {
		return APIIdempotencyCompletion{}, false, fmt.Errorf("sqlite runtime store is required")
	}
	if execute == nil {
		return APIIdempotencyCompletion{}, false, fmt.Errorf("api idempotency executor is required")
	}
	req.Method = strings.TrimSpace(req.Method)
	req.ActorTokenID = strings.TrimSpace(req.ActorTokenID)
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	req.RequestHash = strings.TrimSpace(req.RequestHash)
	req.ResourceID = strings.TrimSpace(req.ResourceID)
	if req.Method == "" || req.ActorTokenID == "" || req.RequestHash == "" {
		return APIIdempotencyCompletion{}, false, fmt.Errorf("method, actor token id, and request hash are required")
	}
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	}
	if req.TTL <= 0 {
		req.TTL = 24 * time.Hour
	}

	// SQLite is local-dev only here: serialize idempotent callbacks in-process
	// per database file, but never keep a SQLite write transaction open while
	// the callback writes through the selected runtime store.
	lock := sqliteAPIIdempotencyLockForPath(s.Path())
	lock.Lock()
	defer lock.Unlock()

	if err := purgeExpiredSQLiteAPIIdempotency(ctx, s.DB, req.Now); err != nil {
		return APIIdempotencyCompletion{}, false, err
	}
	existing, ok, err := sqliteLoadAPIIdempotency(ctx, s.DB, req)
	if err != nil {
		return APIIdempotencyCompletion{}, false, err
	}
	if ok {
		if existing.RequestHash != req.RequestHash {
			return APIIdempotencyCompletion{}, false, &APIIdempotencyConflictError{
				OriginalRequestHash:    existing.RequestHash,
				ConflictingRequestHash: req.RequestHash,
				Method:                 req.Method,
				ResourceID:             existing.ResourceID,
			}
		}
		return APIIdempotencyCompletion{
			ResourceID: existing.ResourceID,
			Response:   append(json.RawMessage(nil), existing.Response...),
		}, true, nil
	}
	completion, err := execute(ctx)
	if err != nil {
		return APIIdempotencyCompletion{}, false, err
	}
	if len(completion.Response) == 0 {
		return APIIdempotencyCompletion{}, false, fmt.Errorf("api idempotency response is required")
	}
	if strings.TrimSpace(completion.ResourceID) == "" {
		completion.ResourceID = req.ResourceID
	}
	if err := sqliteStoreAPIIdempotency(ctx, s.DB, req, completion); err != nil {
		return APIIdempotencyCompletion{}, false, err
	}
	return completion, false, nil
}

func purgeExpiredSQLiteAPIIdempotency(ctx context.Context, q execQueryer, now time.Time) error {
	if _, err := q.ExecContext(ctx, `DELETE FROM api_idempotency WHERE expires_at <= ?`, now.UTC()); err != nil {
		return fmt.Errorf("purge expired sqlite api idempotency: %w", err)
	}
	return nil
}

func sqliteAPIIdempotencyLockForPath(path string) *sync.Mutex {
	key := sqliteAPIIdempotencyLockKey(path)
	sqliteAPIIdempotencyLocks.Lock()
	defer sqliteAPIIdempotencyLocks.Unlock()
	lock := sqliteAPIIdempotencyLocks.byPath[key]
	if lock == nil {
		lock = &sync.Mutex{}
		sqliteAPIIdempotencyLocks.byPath[key] = lock
	}
	return lock
}

func sqliteAPIIdempotencyLockKey(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "<unknown>"
	}
	cleanPath := filepath.Clean(path)
	if abs, err := filepath.Abs(cleanPath); err == nil {
		return abs
	}
	return cleanPath
}

func sqliteLoadAPIIdempotency(ctx context.Context, q execQueryer, req APIIdempotencyRequest) (apiIdempotencyRecord, bool, error) {
	var record apiIdempotencyRecord
	var response []byte
	err := q.QueryRowContext(ctx, `
		SELECT request_hash, resource_id, response
		FROM api_idempotency
		WHERE method = ? AND actor_token_id = ? AND idempotency_key = ? AND expires_at > ?
	`, req.Method, req.ActorTokenID, req.IdempotencyKey, req.Now.UTC()).Scan(&record.RequestHash, &record.ResourceID, &response)
	if err == sql.ErrNoRows {
		return apiIdempotencyRecord{}, false, nil
	}
	if err != nil {
		return apiIdempotencyRecord{}, false, fmt.Errorf("load sqlite api idempotency response: %w", err)
	}
	record.Response = json.RawMessage(response)
	return record, true, nil
}

func sqliteEnsureRunRow(ctx context.Context, tx *sql.Tx, runID, triggerEventID, triggerEventType string, now time.Time) error {
	runID = nullUUIDString(runID)
	if runID == "" {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	_, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO runs (run_id, status, bundle_source, trigger_event_id, trigger_event_type, started_at)
		VALUES (?, 'running', 'legacy', ?, ?, ?)
	`, runID, sqliteNullUUID(triggerEventID), sqliteNullString(triggerEventType), now.UTC())
	if err != nil {
		return fmt.Errorf("ensure sqlite run row: %w", err)
	}
	return nil
}

func sqliteNullString(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return raw
}

func sqliteNullUUID(raw string) any {
	raw = nullUUIDString(raw)
	if raw == "" {
		return nil
	}
	return raw
}

func sqliteTimeValue(raw any) (time.Time, bool, error) {
	switch v := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		if v.IsZero() {
			return time.Time{}, false, nil
		}
		return v.UTC(), true, nil
	case string:
		return parseSQLiteTimeString(v)
	case []byte:
		return parseSQLiteTimeString(string(v))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported sqlite time value %T", raw)
	}
}

func parseSQLiteTimeString(raw string) (time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false, nil
	}
	formats := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
	}
	var lastErr error
	for _, layout := range formats {
		parsed, err := time.Parse(layout, raw)
		if err == nil {
			return parsed.UTC(), true, nil
		}
		lastErr = err
	}
	return time.Time{}, false, fmt.Errorf("parse sqlite time %q: %w", raw, lastErr)
}
