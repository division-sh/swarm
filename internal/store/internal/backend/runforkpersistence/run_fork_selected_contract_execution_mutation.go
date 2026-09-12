package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimecanonicaljson "github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

func prepareRunForkSelectedContractRouteResolution(
	plan runfork.RunForkPlan,
	forkRunID string,
	selection runfork.RunForkContractSelection,
	frontier runfork.RunForkContractFrontierAdmission,
	topology runfork.RunForkSelectedContractRouteTopology,
	planning runfork.RunForkSelectedContractRecipientPlanning,
) (runfork.RunForkSelectedContractRouteRecovery, bool, error) {
	switch strings.TrimSpace(plan.RouteHistory.State) {
	case runfork.RunForkRouteHistoryNotApplicable:
		return runfork.RunForkSelectedContractRouteRecovery{}, false, nil
	case runfork.RunForkRouteHistoryUnknownUnversioned:
	default:
		return runfork.RunForkSelectedContractRouteRecovery{}, false, fmt.Errorf("selected-contract route resolution received unsupported route history state %q", plan.RouteHistory.State)
	}
	if strings.TrimSpace(frontier.Owner) != runfork.RunForkContractFrontierAdmissionOwner || !frontier.NonMutating {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, runForkReplayResumeError(
			runfork.RunForkBlockerFlowRouteHistoryUnproven,
			runfork.RunForkReplayResumeFactRouteHistory,
			"selected-contract route resolution requires canonical frontier admission",
		)
	}
	if !topology.StaticTopologySupported || !topology.DynamicTopologySupported {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, runForkReplayResumeError(
			runfork.RunForkBlockerFlowRouteHistoryUnproven,
			runfork.RunForkReplayResumeFactRouteHistory,
			"selected-contract route resolution requires complete static and dynamic topology proof",
		)
	}
	if err := validateRunForkSelectedContractRouteRecoverySelection("route resolution frontier", selection, frontier.ContractSelection); err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, err
	}
	count, eventIDs, fingerprint, err := runfork.RunForkContractFrontierEvidenceBinding(frontier)
	if err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, err
	}
	if count != topology.FrontierEventCount || !equalTrimmedStrings(eventIDs, topology.FrontierSourceEventIDs) || fingerprint != strings.TrimSpace(topology.FrontierEvidenceFingerprint) {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, fmt.Errorf("selected-contract route topology does not match the fixed-event frontier")
	}
	historicalEventIDs, ok := plan.HistoricalEventIDs(plan.ForkPoint.Revision)
	if !ok {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, fmt.Errorf("selected-contract route resolution requires the fixed-event revision snapshot")
	}
	historicalEvents := map[string]struct{}{}
	for _, eventID := range historicalEventIDs {
		historicalEvents[strings.TrimSpace(eventID)] = struct{}{}
	}
	for _, eventID := range eventIDs {
		if _, ok := historicalEvents[strings.TrimSpace(eventID)]; !ok {
			return runfork.RunForkSelectedContractRouteRecovery{}, false, fmt.Errorf("selected-contract route frontier event %s is outside fixed revision %d", eventID, plan.ForkPoint.Revision)
		}
	}
	record, err := normalizeRunForkSelectedContractRouteRecovery(runfork.RunForkSelectedContractRouteRecoveryRequest{
		ForkRunID:         forkRunID,
		SourceRunID:       plan.SourceRunID,
		ForkEventID:       plan.ForkPoint.EventID,
		ContractSelection: selection,
		RouteTopology:     topology,
		RecipientPlanning: planning,
	}, time.Now().UTC())
	if err != nil {
		return runfork.RunForkSelectedContractRouteRecovery{}, false, err
	}
	return record, true, nil
}

func equalTrimmedStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if strings.TrimSpace(left[i]) != strings.TrimSpace(right[i]) {
			return false
		}
	}
	return true
}

func (s *RunForkPostgresOwner) requireRunForkSelectedContractExecutionAccess() error {
	return s.requireCurrentSchema()
}

func (s *RunForkPostgresOwner) MaterializeRunForkForSelectedContractExecution(ctx context.Context, req runforkreadiness.MaterializeRequest) (runfork.RunForkMaterialization, error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkMaterialization{}, fmt.Errorf("postgres store is required")
	}
	return materializeRunForkForSelectedContractExecution(ctx, req, postgresRunForkSelectedContractMaterializationPort(s))
}

func (s *RunForkSQLiteOwner) MaterializeRunForkForSelectedContractExecution(ctx context.Context, req runforkreadiness.MaterializeRequest) (materialization runfork.RunForkMaterialization, err error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkMaterialization{}, fmt.Errorf("sqlite store is required")
	}
	return materializeRunForkForSelectedContractExecution(ctx, req, sqliteRunForkSelectedContractMaterializationPort(s))
}

type selectedContractWorkflowState struct {
	RunID           string
	EntityID        string
	EntityType      string
	WorkflowName    string
	WorkflowVersion string
	ExecutionMode   executionmode.Mode
	Mode            string
	Route           string
	Config          map[string]any
	Agents          []runfork.RunForkSelectedContractAgentExpectation
}

func selectedContractAdmittedWorkflowStates(plan runfork.RunForkPlan, forkRunID string, admission runforkreadiness.Admission) ([]selectedContractWorkflowState, error) {
	projection, err := admission.Projection()
	if err != nil {
		return nil, err
	}
	metadata, err := loadRunForkEntityMetadata(plan)
	if err != nil {
		return nil, err
	}
	out := make([]selectedContractWorkflowState, 0, len(projection.States))
	for _, state := range projection.States {
		meta, ok := metadata[state.EntityID]
		if !ok {
			return nil, fmt.Errorf("admitted selected-contract state has no fixed-revision entity %s", state.EntityID)
		}
		owner, err := ProjectRunForkEntityOwnership(plan.SourceRunID, forkRunID, state.EntityID, meta.FlowInstance)
		if err != nil {
			return nil, err
		}
		route, err := selectedContractProjectedWorkflowStateRoute(forkRunID, state)
		if err != nil {
			return nil, err
		}
		if route.InstancePath != owner.Fork.FlowInstance || state.EntityType != meta.EntityType {
			return nil, fmt.Errorf("admitted selected-contract state disagrees with child entity projection")
		}
		out = append(out, selectedContractWorkflowState{
			RunID: forkRunID, EntityID: owner.Fork.EntityID, EntityType: state.EntityType,
			WorkflowName: state.FlowID, WorkflowVersion: state.WorkflowVersion,
			ExecutionMode: state.ExecutionMode, Mode: state.Mode, Route: route.InstancePath,
			Config: state.Config, Agents: state.Agents,
		})
	}
	return out, nil
}

func selectedContractProjectedWorkflowStateRoute(forkRunID string, state runfork.RunForkSelectedContractWorkflowState) (runtimeflowidentity.Route, error) {
	switch state.AddressKind {
	case runfork.RunForkSelectedContractWorkflowStateRunScope:
		if state.Route.Valid() {
			return runtimeflowidentity.Route{}, fmt.Errorf("selected-contract run-scope workflow state cannot carry an exact pre-materialization route")
		}
		route := runtimeflowidentity.StoredRoute(forkRunID, runtimeflowidentity.LogicalInstanceID(forkRunID), forkRunID)
		if !route.Valid() {
			return runtimeflowidentity.Route{}, fmt.Errorf("selected-contract run-scope workflow state requires exact fork run identity")
		}
		return route, nil
	case runfork.RunForkSelectedContractWorkflowStateExact:
		route := runtimeflowidentity.StoredRoute(state.Route.ScopeKey, state.Route.InstanceID, state.Route.InstancePath)
		if !route.Valid() {
			return runtimeflowidentity.Route{}, fmt.Errorf("selected-contract workflow state requires exact route")
		}
		return route, nil
	default:
		return runtimeflowidentity.Route{}, fmt.Errorf("selected-contract workflow state has unsupported address kind %q", state.AddressKind)
	}
}

func selectedContractWorkflowStateConfig(state selectedContractWorkflowState) ([]byte, error) {
	descriptor := state.Config
	if state.Mode != "template" {
		descriptor = map[string]any{
			"flow_path":        state.Route,
			"instance_id":      runtimeflowidentity.LogicalInstanceID(state.Route),
			"storage_ref":      state.Route,
			"workflow_version": state.WorkflowVersion,
		}
	} else if descriptor == nil {
		return nil, fmt.Errorf("selected-contract template workflow state requires exact activation config")
	}
	route := runtimeflowidentity.RouteForInstancePath(state.Route)
	descriptor, err := runtimepipeline.WorkflowInstanceConfigPayloadForRoute(route, state.WorkflowVersion, descriptor)
	if err != nil {
		return nil, fmt.Errorf("encode selected-contract workflow route config: %w", err)
	}
	config, err := json.Marshal(descriptor)
	if err != nil {
		return nil, fmt.Errorf("encode selected-contract root descriptor: %w", err)
	}
	return config, nil
}

func requireSelectedContractWorkflowEntity(ctx context.Context, tx *sql.Tx, postgres bool, state selectedContractWorkflowState) error {
	query := `SELECT flow_instance, entity_type FROM entity_state WHERE run_id = ? AND entity_id = ?`
	if postgres {
		query = `SELECT flow_instance, entity_type FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid FOR UPDATE`
	}
	var route, entityType string
	if err := tx.QueryRowContext(ctx, query, state.RunID, state.EntityID).Scan(&route, &entityType); err != nil {
		return fmt.Errorf("verify selected-contract restored entity state: %w", err)
	}
	if state.EntityType == "" || route != state.Route || entityType != state.EntityType {
		return fmt.Errorf("selected-contract restored entity %s disagrees with exact child route/type", state.EntityID)
	}
	return nil
}

func requireSelectedContractWorkflowCompanion(ctx context.Context, tx *sql.Tx, postgres bool, state selectedContractWorkflowState, config []byte) (bool, error) {
	var persistedWorkflow, persistedMode, persistedStatus string
	var persistedConfig []byte
	var unterminated bool
	selectFlow := `SELECT flow_template, mode, config, status, terminated_at IS NULL FROM flow_instances WHERE run_id = ? AND instance_path = ?`
	if postgres {
		selectFlow = `SELECT flow_template, mode, config, status, terminated_at IS NULL FROM flow_instances WHERE run_id = $1::uuid AND instance_path = $2 FOR UPDATE`
	}
	err := tx.QueryRowContext(ctx, selectFlow, state.RunID, state.Route).Scan(&persistedWorkflow, &persistedMode, &persistedConfig, &persistedStatus, &unterminated)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("verify selected-contract workflow instance: %w", err)
	}
	if persistedWorkflow != state.WorkflowName || persistedMode != state.Mode || persistedStatus != "active" || !unterminated ||
		!workflowCommitJSONEqual(persistedConfig, config) {
		return false, fmt.Errorf("selected-contract workflow instance %s disagrees with exact descriptor", state.Route)
	}
	return true, nil
}

func requireSelectedContractWorkflowState(ctx context.Context, tx *sql.Tx, postgres bool, source runtimecorrelation.SourceArtifactFact, state selectedContractWorkflowState) ([]runfork.RunForkSelectedContractAgentTopology, error) {
	if err := requireSelectedContractWorkflowEntity(ctx, tx, postgres, state); err != nil {
		return nil, err
	}
	config, err := selectedContractWorkflowStateConfig(state)
	if err != nil {
		return nil, err
	}
	found, err := requireSelectedContractWorkflowCompanion(ctx, tx, postgres, state, config)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("selected-contract workflow instance %s is missing from existing child materialization", state.Route)
	}
	_, topologies, encoded, err := selectedContractWorkflowReadiness(source, state)
	if err != nil {
		return nil, err
	}
	if encoded != nil {
		if err := requireSelectedContractWorkflowReadiness(ctx, tx, postgres, state, encoded); err != nil {
			return nil, err
		}
	}
	return topologies, nil
}

func materializeSelectedContractWorkflowState(ctx context.Context, tx *sql.Tx, postgres bool, source runtimecorrelation.SourceArtifactFact, state selectedContractWorkflowState, now time.Time) ([]runfork.RunForkSelectedContractAgentTopology, error) {
	if err := requireSelectedContractWorkflowEntity(ctx, tx, postgres, state); err != nil {
		return nil, err
	}
	config, err := selectedContractWorkflowStateConfig(state)
	if err != nil {
		return nil, err
	}
	found, err := requireSelectedContractWorkflowCompanion(ctx, tx, postgres, state, config)
	if err != nil {
		return nil, err
	}
	if found {
		return requireSelectedContractWorkflowState(ctx, tx, postgres, source, state)
	}
	transition, err := runtimepipeline.WorkflowEngineStateTransitionForPresence(runtimepipeline.WorkflowTargetPersistenceStateOnly)
	if err != nil || transition != runtimepipeline.WorkflowEngineStateTransitionUpdateStateCreateCompanion {
		return nil, fmt.Errorf("selected-contract workflow state requires the canonical state-only companion transition")
	}
	insertFlow := `
		INSERT INTO flow_instances (run_id, instance_path, flow_template, mode, config, status, created_at)
			VALUES (?, ?, ?, ?, ?, 'active', ?)
		ON CONFLICT (run_id, instance_path) DO NOTHING`
	if postgres {
		insertFlow = `
			INSERT INTO flow_instances (run_id, instance_path, flow_template, mode, config, status, created_at)
				VALUES ($1::uuid, $2, $3, $4, $5, 'active', $6)
			ON CONFLICT (run_id, instance_path) DO NOTHING`
	}
	if _, err := tx.ExecContext(ctx, insertFlow, state.RunID, state.Route, state.WorkflowName, state.Mode, string(config), now); err != nil {
		return nil, fmt.Errorf("insert selected-contract workflow instance: %w", err)
	}
	if found, err := requireSelectedContractWorkflowCompanion(ctx, tx, postgres, state, config); err != nil {
		return nil, err
	} else if !found {
		return nil, fmt.Errorf("selected-contract workflow companion %s was not created", state.Route)
	}
	topologies, err := materializeSelectedContractWorkflowReadiness(ctx, tx, postgres, source, state, now)
	if err != nil {
		return nil, err
	}
	return topologies, nil
}

func materializeSelectedContractWorkflowReadiness(ctx context.Context, tx *sql.Tx, postgres bool, source runtimecorrelation.SourceArtifactFact, state selectedContractWorkflowState, now time.Time) ([]runfork.RunForkSelectedContractAgentTopology, error) {
	plan, topologies, encoded, err := selectedContractWorkflowReadiness(source, state)
	if err != nil || plan == nil {
		return topologies, err
	}
	query := `
		INSERT INTO flow_instance_runtime_readiness (
			run_id, instance_path, plan, topology_ready_at, creation_event_emitted_at, created_at, updated_at
		) VALUES (?, ?, ?, NULL, NULL, ?, ?)
		ON CONFLICT (run_id, instance_path) DO NOTHING`
	args := []any{state.RunID, state.Route, encoded, now, now}
	if postgres {
		query = `
			INSERT INTO flow_instance_runtime_readiness (
				run_id, instance_path, plan, topology_ready_at, creation_event_emitted_at, created_at, updated_at
			) VALUES ($1::uuid, $2, $3::jsonb, NULL, NULL, $4, $4)
			ON CONFLICT (run_id, instance_path) DO NOTHING`
		args = []any{state.RunID, state.Route, encoded, now}
	}
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return nil, fmt.Errorf("insert selected-contract workflow readiness: %w", err)
	}
	if err := requireSelectedContractWorkflowReadiness(ctx, tx, postgres, state, encoded); err != nil {
		return nil, err
	}
	return topologies, nil
}

func requireSelectedContractWorkflowReadiness(ctx context.Context, tx *sql.Tx, postgres bool, state selectedContractWorkflowState, expected []byte) error {
	if state.Mode != "template" {
		if len(state.Agents) != 0 {
			return fmt.Errorf("selected-contract static workflow state cannot carry dynamic agent expectations")
		}
		return nil
	}
	query := `SELECT plan FROM flow_instance_runtime_readiness WHERE run_id = ? AND instance_path = ?`
	if postgres {
		query = `SELECT plan FROM flow_instance_runtime_readiness WHERE run_id = $1::uuid AND instance_path = $2`
	}
	var persisted []byte
	if err := tx.QueryRowContext(ctx, query, state.RunID, state.Route).Scan(&persisted); err != nil {
		return fmt.Errorf("verify selected-contract workflow readiness: %w", err)
	}
	if !workflowCommitJSONEqual(persisted, expected) {
		return fmt.Errorf("selected-contract workflow readiness %s disagrees with exact plan", state.Route)
	}
	return nil
}

func selectedContractWorkflowReadiness(source runtimecorrelation.SourceArtifactFact, state selectedContractWorkflowState) (*runtimepipeline.DynamicFlowRuntimeReadinessPlan, []runfork.RunForkSelectedContractAgentTopology, []byte, error) {
	if state.Mode != "template" {
		if len(state.Agents) != 0 {
			return nil, nil, nil, fmt.Errorf("selected-contract static workflow state cannot carry dynamic agent expectations")
		}
		return nil, nil, nil, nil
	}
	if err := source.Validate(); err != nil {
		return nil, nil, nil, fmt.Errorf("selected-contract workflow readiness source: %w", err)
	}
	route := runtimeflowidentity.StoredRoute(
		runtimeflowidentity.SemanticScopeFromInstancePath(state.Route),
		runtimeflowidentity.LogicalInstanceID(state.Route),
		state.Route,
	)
	if !route.Valid() {
		return nil, nil, nil, fmt.Errorf("selected-contract workflow readiness requires exact route")
	}
	identity := runtimeflowidentity.Instance{
		TemplateID: state.WorkflowName, ScopeKey: route.ScopeKey, InstanceID: route.InstanceID,
		InstancePath: route.InstancePath, EntityID: state.EntityID, HasStoredPath: true,
	}
	bundleHash := source.BundleHash()
	plan := runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: identity, RunID: state.RunID, BundleHash: bundleHash,
		WorkflowVersion: state.WorkflowVersion, ExecutionMode: state.ExecutionMode,
		Agents: make([]runtimepipeline.DynamicFlowRuntimeAgentExpectation, 0, len(state.Agents)),
	}
	agentRoute, err := route.AgentIdentityRoute()
	if err != nil {
		return nil, nil, nil, err
	}
	for _, expected := range state.Agents {
		declaration := expected.Plan.Normalize()
		if err := declaration.Validate(); err != nil || declaration.Route != agentRoute {
			return nil, nil, nil, fmt.Errorf("selected-contract workflow readiness has invalid agent declaration for %s", state.Route)
		}
		live, err := declaration.Live(state.RunID)
		if err != nil {
			return nil, nil, nil, err
		}
		plan.Agents = append(plan.Agents, runtimepipeline.DynamicFlowRuntimeAgentExpectation{
			Identity: live, ConfigRevision: strings.TrimSpace(expected.ConfigRevision),
		})
	}
	normalized, err := plan.Normalized()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("normalize selected-contract workflow readiness: %w", err)
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encode selected-contract workflow readiness: %w", err)
	}
	fingerprint, err := runtimecanonicaljson.Hash(normalized)
	if err != nil {
		return nil, nil, nil, err
	}
	admission, err := runtimeagenttopology.FlowReadinessAdmission(state.RunID, state.Route, fingerprint)
	if err != nil {
		return nil, nil, nil, err
	}
	topologies := make([]runfork.RunForkSelectedContractAgentTopology, 0, len(normalized.Agents))
	for _, expected := range normalized.Agents {
		topologies = append(topologies, runfork.RunForkSelectedContractAgentTopology{
			Identity: expected.Identity, Admission: admission,
		})
	}
	return &normalized, topologies, encoded, nil
}

func (s *RunForkPostgresOwner) ActivateRunForkForSelectedContractExecution(ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkActivation{}, fmt.Errorf("postgres store is required")
	}
	return activateRunForkForSelectedContractExecution(ctx, req, postgresRunForkSelectedContractActivationPort(s))
}

func (s *RunForkPostgresOwner) DiscardMaterializedSelectedContractExecutionFork(ctx context.Context, forkRunID string) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres store is required")
	}
	return discardMaterializedSelectedContractExecutionFork(ctx, forkRunID, postgresRunForkSelectedContractDiscardPort(s))
}

func loadSQLiteRunForkSelectedContractEvents(ctx context.Context, q eventrecordsqlite.Queryer, eventIDs []string) ([]events.Event, error) {
	records, err := eventrecordsqlite.LoadMany(ctx, q, eventIDs)
	if err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(records))
	for _, record := range records {
		admitted, err := record.Decode()
		if err != nil {
			return nil, fmt.Errorf("decode selected-contract source event %s: %w", record.EventID, err)
		}
		out = append(out, admitted.Event())
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt().Equal(out[j].CreatedAt()) {
			return out[i].ID() < out[j].ID()
		}
		return out[i].CreatedAt().Before(out[j].CreatedAt())
	})
	return out, nil
}

func (s *RunForkPostgresOwner) LoadRunForkSelectedContractSourceEvents(ctx context.Context, sourceRunID, forkRunID string, sourceEventIDs []string, original semanticview.OriginalLoopCarriage) ([]runfork.RunForkSelectedContractSourceEvent, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	sourceRunID = strings.TrimSpace(sourceRunID)
	if sourceRunID == "" {
		return nil, fmt.Errorf("source run_id is required")
	}
	forkRunID = strings.TrimSpace(forkRunID)
	if forkRunID == "" {
		return nil, fmt.Errorf("fork run_id is required")
	}
	ids := uniqueNonEmptyStrings(sourceEventIDs)
	if len(ids) == 0 {
		return nil, nil
	}
	tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin selected-contract source event preparation: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	story, err := privateauthoractivity.Begin(ctx, tx, privateauthoractivity.DialectPostgres)
	if err != nil {
		return nil, err
	}
	var sourceStatus string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid FOR SHARE`, sourceRunID).Scan(&sourceStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &runtimerunlifecycle.RunNotFoundError{RunID: sourceRunID}
		}
		return nil, fmt.Errorf("load selected-contract source event preparation status: %w", err)
	}
	if !runForkSelectedContractBranchSourceStatusSupported(sourceStatus) {
		state, parseErr := runtimerunlifecycle.ParseState(sourceStatus)
		if parseErr != nil {
			return nil, parseErr
		}
		return nil, fmt.Errorf("selected-contract source event preparation state %s is unsupported", state)
	}
	if err := requirePostgresRunActive(ctx, tx, forkRunID); err != nil {
		return nil, fmt.Errorf("admit selected-contract source event preparation fork: %w", err)
	}
	stateAdmission, err := loadRunForkSourceStateAdmission(ctx, tx, forkRunID, runforkrevision.ValidateCompletePostgres, resolveRunForkRevisionPoint, true)
	if err != nil {
		return nil, err
	}
	if stateAdmission.snapshot.RunID != sourceRunID {
		return nil, fmt.Errorf("source event preparation run disagrees with selected fork binding")
	}
	fact, err := s.RunLifecyclePostgresOwner.RequirePresentSourceTx(ctx, tx, sourceRunID)
	if err != nil {
		return nil, err
	}
	if err := original.RequireSource(fact.BundleHash()); err != nil {
		return nil, err
	}
	stateAdmission.carriage = original
	records, err := eventrecordpostgres.LoadMany(ctx, tx, ids)
	if err != nil {
		return nil, fmt.Errorf("load selected-contract source events: %w", err)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].CreatedAt.Equal(records[j].CreatedAt) {
			return records[i].EventID < records[j].EventID
		}
		return records[i].CreatedAt.Before(records[j].CreatedAt)
	})
	out := make([]runfork.RunForkSelectedContractSourceEvent, 0, len(ids))
	for _, record := range records {
		admitted, err := record.Decode()
		if err != nil {
			return nil, fmt.Errorf("decode selected-contract source event %s: %w", record.EventID, err)
		}
		event := admitted.Event()
		if event.RunID() != sourceRunID {
			return nil, fmt.Errorf("selected-contract source event %s does not belong to source run %s", event.ID(), sourceRunID)
		}
		out = append(out, runfork.RunForkSelectedContractSourceEvent{
			SourceEventID: event.ID(), EventName: string(event.Type()), ExecutionMode: event.ExecutionMode(),
			Scope:         string(event.Scope()),
			RoutingSource: event.RoutingSource(),
			Payload:       event.Payload(),
		})
		out[len(out)-1].InputPublication, err = runfork.InputPublicationFromEvent(event)
		if err != nil {
			return nil, err
		}
	}
	for idx := range out {
		prepared, err := prepareRunForkSelectedContractSourceEvent(ctx, tx, story, stateAdmission, out[idx])
		if err != nil {
			return nil, err
		}
		out[idx] = prepared
	}
	if err := story.Finalize(ctx); err != nil {
		return nil, fmt.Errorf("finalize selected-contract source event author activity: %w", err)
	}
	if _, err := runforkrevision.FinalizePostgres(ctx, tx, runforkrevision.NewEffects()); err != nil {
		return nil, fmt.Errorf("finalize selected-contract source event preparation revisions: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit selected-contract source event preparation: %w", err)
	}
	committed = true
	return out, nil
}

func (s *RunForkPostgresOwner) LoadRunForkSelectedContractSourceEventModes(ctx context.Context, sourceRunID string, sourceEventIDs []string) ([]executionmode.Mode, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	sourceRunID = strings.TrimSpace(sourceRunID)
	if sourceRunID == "" {
		return nil, fmt.Errorf("source run_id is required")
	}
	ids := uniqueNonEmptyStrings(sourceEventIDs)
	if len(ids) == 0 {
		return nil, nil
	}
	var sourceStatus string
	if err := s.backend.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &runtimerunlifecycle.RunNotFoundError{RunID: sourceRunID}
		}
		return nil, fmt.Errorf("load selected-contract source event admission status: %w", err)
	}
	if !runForkSelectedContractBranchSourceStatusSupported(sourceStatus) {
		state, parseErr := runtimerunlifecycle.ParseState(sourceStatus)
		if parseErr != nil {
			return nil, parseErr
		}
		return nil, fmt.Errorf("selected-contract source event admission state %s is unsupported", state)
	}
	records, err := eventrecordpostgres.LoadMany(ctx, s.backend, ids)
	if err != nil {
		return nil, fmt.Errorf("load selected-contract source event modes: %w", err)
	}
	if len(records) != len(ids) {
		return nil, fmt.Errorf("selected-contract source event admission found %d of %d events", len(records), len(ids))
	}
	modes := make([]executionmode.Mode, 0, len(records))
	for _, record := range records {
		admitted, err := record.Decode()
		if err != nil {
			return nil, fmt.Errorf("decode selected-contract source event %s: %w", record.EventID, err)
		}
		event := admitted.Event()
		if event.RunID() != sourceRunID {
			return nil, fmt.Errorf("selected-contract source event %s does not belong to source run %s", event.ID(), sourceRunID)
		}
		modes = append(modes, event.ExecutionMode())
	}
	return modes, nil
}

func normalizeSelectedForkExecutionLineage(lineage runfork.RunForkSelectedContractExecutionLineage) (runfork.RunForkSelectedContractExecutionLineage, error) {
	lineage.ForkRunID = strings.TrimSpace(lineage.ForkRunID)
	lineage.SourceRunID = strings.TrimSpace(lineage.SourceRunID)
	lineage.SourceEventID = strings.TrimSpace(lineage.SourceEventID)
	lineage.ForkEventID = strings.TrimSpace(lineage.ForkEventID)
	lineage.EventName = strings.TrimSpace(lineage.EventName)
	lineage.SelectionAuthority = strings.TrimSpace(lineage.SelectionAuthority)
	for name, value := range map[string]string{
		"fork_run_id": lineage.ForkRunID, "source_run_id": lineage.SourceRunID,
		"source_event_id": lineage.SourceEventID, "fork_event_id": lineage.ForkEventID,
		"event_name": lineage.EventName, "selection_authority": lineage.SelectionAuthority,
	} {
		if value == "" {
			return runfork.RunForkSelectedContractExecutionLineage{}, fmt.Errorf("selected-fork execution lineage requires %s", name)
		}
	}
	createdAt := lineage.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	lineage.CreatedAt = createdAt.UTC()
	return lineage, nil
}

func insertPostgresSelectedForkExecutionLineageTx(ctx context.Context, tx *sql.Tx, lineage runfork.RunForkSelectedContractExecutionLineage) error {
	lineage, err := normalizeSelectedForkExecutionLineage(lineage)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_executions (
			execution_id, fork_run_id, source_run_id, source_event_id, fork_event_id,
			event_name, selection_authority, created_at
		)
		SELECT $1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::uuid, $6, $7, $8
		FROM events source
		WHERE source.event_id = $4::uuid AND source.run_id = $3::uuid
	`, uuid.NewString(), lineage.ForkRunID, lineage.SourceRunID, lineage.SourceEventID,
		lineage.ForkEventID, lineage.EventName, lineage.SelectionAuthority, lineage.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert selected-contract execution lineage: %w", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		if err != nil {
			return fmt.Errorf("read selected-contract execution lineage rows: %w", err)
		}
		return fmt.Errorf("selected-contract source event %s does not belong to source run %s", lineage.SourceEventID, lineage.SourceRunID)
	}
	return nil
}

func (s *RunForkPostgresOwner) InsertSelectedForkExecutionLineageTx(ctx context.Context, tx *sql.Tx, lineage runfork.RunForkSelectedContractExecutionLineage) error {
	return insertPostgresSelectedForkExecutionLineageTx(ctx, tx, lineage)
}

func insertSQLiteSelectedForkExecutionLineageTx(ctx context.Context, tx *sql.Tx, lineage runfork.RunForkSelectedContractExecutionLineage) error {
	lineage, err := normalizeSelectedForkExecutionLineage(lineage)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_executions (
			execution_id, fork_run_id, source_run_id, source_event_id, fork_event_id,
			event_name, selection_authority, created_at
		)
		SELECT ?, ?, ?, ?, ?, ?, ?, ?
		FROM events source
		WHERE source.event_id = ? AND source.run_id = ?
	`, uuid.NewString(), lineage.ForkRunID, lineage.SourceRunID, lineage.SourceEventID,
		lineage.ForkEventID, lineage.EventName, lineage.SelectionAuthority, lineage.CreatedAt,
		lineage.SourceEventID, lineage.SourceRunID)
	if err != nil {
		return fmt.Errorf("insert sqlite selected-contract execution lineage: %w", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		if err != nil {
			return fmt.Errorf("read sqlite selected-contract execution lineage rows: %w", err)
		}
		return fmt.Errorf("selected-contract source event %s does not belong to source run %s", lineage.SourceEventID, lineage.SourceRunID)
	}
	return nil
}

func (s *RunForkSQLiteOwner) InsertSelectedForkExecutionLineageTx(ctx context.Context, tx *sql.Tx, lineage runfork.RunForkSelectedContractExecutionLineage) error {
	return insertSQLiteSelectedForkExecutionLineageTx(ctx, tx, lineage)
}

func runForkSelectedContractBranchSourceStatusSupported(status string) bool {
	state, err := runtimerunlifecycle.ParseState(status)
	return err == nil && state != runtimerunlifecycle.StateForked
}

func collectRunForkSelectedContractSourceAdvancedFacts(ctx context.Context, tx *sql.Tx, lineage runForkActivationLineage) ([]string, error) {
	facts, err := collectRunForkSourceAdvancedFacts(ctx, tx, lineage)
	if err != nil {
		return nil, err
	}
	state, stateErr := runtimerunlifecycle.ParseState(lineage.SourceRunStatus)
	if stateErr == nil && state.Terminal() && state != runtimerunlifecycle.StateForked {
		facts = append(facts, "source_run_terminal_at_activation")
	}
	return uniqueNonEmptyStrings(facts), nil
}

func insertRunForkSelectedContractBranchDivergence(ctx context.Context, tx *sql.Tx, divergence runfork.RunForkSelectedContractBranchDivergence) error {
	if divergence.CreatedAt.IsZero() {
		divergence.CreatedAt = time.Now().UTC()
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_branch_divergences (
			fork_run_id,
			source_run_id,
			fork_event_id,
			owner,
			policy,
			source_run_status_at_activation,
			source_run_status_after_activation,
			source_frozen,
			source_advanced_facts,
			created_at
		)
		VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7, $8, $9::text[], $10)
		ON CONFLICT (fork_run_id) DO UPDATE
		SET owner = EXCLUDED.owner,
		    policy = EXCLUDED.policy,
		    source_run_status_at_activation = EXCLUDED.source_run_status_at_activation,
		    source_run_status_after_activation = EXCLUDED.source_run_status_after_activation,
		    source_frozen = EXCLUDED.source_frozen,
		    source_advanced_facts = EXCLUDED.source_advanced_facts,
		    created_at = EXCLUDED.created_at
	`, divergence.ForkRunID,
		divergence.SourceRunID,
		divergence.ForkEventID,
		divergence.Owner,
		divergence.Policy,
		divergence.SourceRunStatusAtActivation,
		divergence.SourceRunStatusAfterActivation,
		divergence.SourceFrozen,
		pq.Array(divergence.SourceAdvancedFacts),
		divergence.CreatedAt)
	if err != nil {
		return fmt.Errorf("record selected-contract branch divergence: %w", err)
	}
	return nil
}

func runForkSelectedContractExecutionPlanBlockersFromAdmission(plan runfork.RunForkPlan, admission runfork.RunForkReplayResumeAdmission, allowedSourceEventIDs []string) []runfork.RunForkUnsupportedBlocker {
	allowedEvents := map[string]struct{}{}
	for _, eventID := range allowedSourceEventIDs {
		if eventID = strings.TrimSpace(eventID); eventID != "" {
			allowedEvents[eventID] = struct{}{}
		}
	}
	blockers := []runfork.RunForkUnsupportedBlocker{}
	for _, blocker := range admission.UnsupportedBlockers {
		switch strings.TrimSpace(blocker.Code) {
		case runfork.RunForkBlockerDeliveryHistoryUnproven, runfork.RunForkBlockerNonAgentDeliveryReplayUnsupported:
			continue
		default:
			blockers = appendRunForkBlocker(blockers, blocker)
		}
	}
	for _, item := range plan.PendingWork {
		classification := strings.TrimSpace(item.Classification)
		if item.RetainsTerminalBarrierHistory() {
			continue
		}
		if classification == runfork.RunForkPendingClassificationDeliveredCompleted {
			continue
		}
		if runfork.RunForkSelectedContractDiagnosticPlatformOutcomePolicyApplies(item) {
			continue
		}
		if classification == runfork.RunForkPendingClassificationCommittedReplay {
			if runfork.CommittedReplayScopeMarkerAdmitted(admission) {
				continue
			}
			blockers = appendRunForkBlocker(blockers, runForkReplayResumeBlocker(runfork.RunForkBlockerCommittedReplayScopeReplayUnsupported))
			continue
		}
		if runfork.ActiveSourceDeliveryConversationCouplingAdmitted(plan, item) {
			continue
		}
		if runfork.PendingWorkHasActiveDeliverySessionCoupling(item) {
			if blocker, ok := runForkSelectedContractAdmissionBlockerForPendingWork(admission, item); ok {
				blockers = appendRunForkBlocker(blockers, blocker)
			} else {
				blockers = appendRunForkBlocker(blockers, runForkReplayResumeBlocker(runfork.RunForkBlockerDeliveryHistoryUnproven))
			}
			continue
		}
		if len(allowedEvents) == 0 {
			continue
		}
		if _, ok := allowedEvents[strings.TrimSpace(item.EventID)]; !ok {
			blockers = appendRunForkBlocker(blockers, runfork.RunForkUnsupportedBlocker{
				Code:    runfork.RunForkBlockerDeliveryHistoryUnproven,
				Message: "selected-contract execution cannot absorb pending source delivery outside selected frontier evidence",
			})
		}
	}
	return blockers
}

func runForkSelectedContractAdmissionBlockerForPendingWork(admission runfork.RunForkReplayResumeAdmission, item runfork.RunForkPendingWork) (runfork.RunForkUnsupportedBlocker, bool) {
	key := runfork.PendingWorkKey(item)
	for _, disposition := range admission.Dispositions {
		if strings.TrimSpace(disposition.Disposition) != runfork.RunForkReplayResumeDispositionFailClosedBlocker {
			continue
		}
		if runfork.ReplayResumeDispositionKey(disposition) != key {
			continue
		}
		if code := strings.TrimSpace(disposition.BlockerCode); code != "" {
			return runForkReplayResumeBlocker(code), true
		}
	}
	return runfork.RunForkUnsupportedBlocker{}, false
}

func (s *RunForkPostgresOwner) EnsureRunForkNoPostForkCommittedReplayScopeMarkers(ctx context.Context, sourceRunID, forkEventID string) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres store is required")
	}
	tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := runforkrevision.ValidateCompletePostgres(ctx, tx, sourceRunID); err != nil {
		return err
	}
	point, err := resolveRunForkRevisionPoint(ctx, tx, sourceRunID, forkEventID)
	if err != nil {
		return fmt.Errorf("resolve committed replay-scope fork revision: %w", err)
	}
	return ensureRunForkNoPostForkCommittedReplayScopeMarkersAtRevision(ctx, tx, sourceRunID, point.Revision)
}

type selectedContractExecutionQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func ensureRunForkNoPostForkCommittedReplayScopeMarkersAtRevision(ctx context.Context, q selectedContractExecutionQueryer, sourceRunID string, forkRevision int64) error {
	var exists bool
	query := `
		SELECT EXISTS (
			SELECT 1
			FROM run_fork_fact_revisions
			WHERE run_id = $1
			  AND family = 'committed_replay_scopes'
			  AND revision > $2
			  AND present
		)
	`
	if err := q.QueryRowContext(ctx, query, sourceRunID, forkRevision).Scan(&exists); err != nil {
		return fmt.Errorf("check selected-contract source_committed_replay_scope_advanced_after_fork_point: %w", err)
	}
	if exists {
		code := "source_committed_replay_scope_advanced_after_fork_point"
		return runForkReplayResumeError(code, runfork.RunForkReplayResumeFactSourceAdvanced, fmt.Sprintf("selected-contract committed replay-scope marker policy blocked: %s", code))
	}
	return nil
}

func runForkSelectedContractConversationAdvancedFacts(facts []string) []string {
	out := []string{}
	for _, fact := range facts {
		switch strings.TrimSpace(fact) {
		case "source_sessions_advanced_after_fork_point", "source_conversation_audits_advanced_after_fork_point", "source_turns_advanced_after_fork_point":
			out = append(out, fact)
		}
	}
	return uniqueNonEmptyStrings(out)
}

func ensureRunForkNoPostForkActiveConversationDeliverySessionCoupling(ctx context.Context, q selectedContractExecutionQueryer, deliveries *storedelivery.Adapter, lineage runForkActivationLineage) error {
	snapshots, err := deliveries.ActiveCouplingSnapshotsForRun(ctx, q, lineage.SourceRunID)
	if err != nil {
		return fmt.Errorf("check selected-contract source delivery snapshots: %w", err)
	}
	for _, snapshot := range snapshots {
		var revision sql.NullInt64
		if err := q.QueryRowContext(ctx, `
			SELECT MAX(revision)
			FROM run_fork_fact_revisions
				WHERE run_id = $1
			  AND family = 'event_deliveries'
			  AND fact_key = $2
			  AND present
		`, lineage.SourceRunID, snapshot.DeliveryID).Scan(&revision); err != nil {
			return fmt.Errorf("check selected-contract source delivery revision: %w", err)
		}
		if revision.Valid && revision.Int64 > lineage.ForkEventRevision {
			code := "source_active_conversation_session_coupling_after_fork_point"
			return runForkReplayResumeError(code, runfork.RunForkReplayResumeFactSessionHistory, fmt.Sprintf("%s blocked unsafe post-T active source delivery/session coupling: %s", runfork.RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner, code))
		}
	}
	return nil
}

func runForkReplayResumeAdmissionWithSourceAdvancedConversationHistory(admission runfork.RunForkReplayResumeAdmission, facts []string) runfork.RunForkReplayResumeAdmission {
	if len(facts) == 0 {
		return admission
	}
	if strings.TrimSpace(admission.Owner) == "" {
		admission.Owner = runfork.RunForkReplayResumeAdmissionOwner
	}
	seen := map[string]struct{}{}
	for _, disposition := range admission.Dispositions {
		if strings.TrimSpace(disposition.Fact) != runfork.RunForkReplayResumeFactSourceAdvanced ||
			strings.TrimSpace(disposition.Disposition) != runfork.RunForkReplayResumeDispositionLineageOnly ||
			strings.TrimSpace(disposition.Owner) != runfork.RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner {
			continue
		}
		seen[strings.TrimSpace(disposition.Classification)] = struct{}{}
	}
	for _, fact := range uniqueNonEmptyStrings(facts) {
		if _, ok := seen[fact]; ok {
			continue
		}
		admission.Dispositions = append(admission.Dispositions, runfork.RunForkReplayResumeDisposition{
			Fact:           runfork.RunForkReplayResumeFactSourceAdvanced,
			Disposition:    runfork.RunForkReplayResumeDispositionLineageOnly,
			Owner:          runfork.RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner,
			Classification: fact,
			Message:        fmt.Sprintf("%s classifies post-T source conversation-history fact %s as selected-contract branch-divergence lineage only; fresh fork-local conversation rows must be created by normal runtime execution under the fork run_id", runfork.RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner, fact),
		})
	}
	return admission
}

func (s *RunForkPostgresOwner) ensureRunForkSelectedContractExecutionForkState(ctx context.Context, tx *sql.Tx, forkRunID string, allowedSourceEventIDs []string, source semanticview.Source) error {
	allowedEvents := uniqueNonEmptyStrings(allowedSourceEventIDs)
	if len(allowedEvents) == 0 {
		return ensureRunForkActivationNoForkReplayState(ctx, tx, postgresDeliveryAdapter, forkRunID)
	}

	// Materialization preflights empty fork-local replay state. At activation
	// time, sessions/turns/audits may be fresh outputs from selected execution.
	var missingLineage int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM unnest($2::uuid[]) AS allowed(source_event_id)
		WHERE NOT EXISTS (
			SELECT 1
			FROM run_fork_selected_contract_executions x
			WHERE x.fork_run_id = $1::uuid
			  AND x.source_event_id = allowed.source_event_id
		)
	`, forkRunID, pq.Array(allowedEvents)).Scan(&missingLineage); err != nil {
		return fmt.Errorf("check selected-contract execution lineage completeness: %w", err)
	}
	if missingLineage > 0 {
		return runForkReplayResumeError("fork_selected_contract_execution_lineage_missing", runfork.RunForkReplayResumeFactForkReplayState, "fork activation blocked: fork_selected_contract_execution_lineage_missing")
	}
	deliverySnapshots, err := postgresDeliveryAdapter.AgentSnapshotsForRun(ctx, tx, forkRunID)
	if err != nil {
		return fmt.Errorf("check selected-contract fork delivery snapshots: %w", err)
	}
	for _, snapshot := range deliverySnapshots {
		if snapshot.Status == runtimedelivery.StatusDelivered {
			continue
		}
		return runForkReplayResumeError(
			"fork_selected_contract_agent_delivery_incomplete",
			runfork.RunForkReplayResumeFactForkReplayState,
			fmt.Sprintf("fork activation blocked: fork_selected_contract_agent_delivery_incomplete: delivery %s for %s/%s is %s", snapshot.DeliveryID, snapshot.SubscriberClass, snapshot.SubscriberID, snapshot.Status),
		)
	}
	selectedAgentRunIDs := []string{}
	selectedAgentIDs := []string{}
	selectedAgentFlowInstances := []string{}
	for _, snapshot := range deliverySnapshots {
		var selected bool
		if err := tx.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM run_fork_selected_contract_executions x
				WHERE x.fork_event_id = $1::uuid
				  AND x.fork_run_id = $2::uuid
				  AND x.source_event_id = ANY($3::uuid[])
			)
		`, snapshot.EventID, forkRunID, pq.Array(allowedEvents)).Scan(&selected); err != nil {
			return fmt.Errorf("check selected-contract delivery lineage: %w", err)
		}
		if selected {
			identity := snapshot.Route.AgentIdentity.Normalize()
			if err := identity.Validate(); err != nil {
				return fmt.Errorf("check selected-contract delivery agent identity: %w", err)
			}
			if identity.RunID != strings.TrimSpace(forkRunID) {
				return fmt.Errorf("check selected-contract delivery agent identity: run %q conflicts with fork run %q", identity.RunID, forkRunID)
			}
			if identity.AgentID() != strings.TrimSpace(snapshot.SubscriberID) {
				return fmt.Errorf("check selected-contract delivery agent identity: subscriber %q conflicts with %s", snapshot.SubscriberID, identity.Description())
			}
			selectedAgentRunIDs = append(selectedAgentRunIDs, identity.RunID)
			selectedAgentIDs = append(selectedAgentIDs, identity.AgentID())
			selectedAgentFlowInstances = append(selectedAgentFlowInstances, identity.FlowInstance())
		}
	}

	activityIDs, err := selectedContractActivityLineage(ctx, tx, true, forkRunID, allowedEvents, source)
	if err != nil {
		return err
	}
	if s.lifecycleDiagnostics == nil {
		return fmt.Errorf("lifecycle observation owner is required")
	}
	observations, err := s.lifecycleDiagnostics.LifecycleObservationIDsTx(ctx, tx, forkRunID)
	if err != nil {
		return err
	}
	var strayEvents int
	var strayEventEvidence string
	if err := tx.QueryRowContext(ctx, `
		WITH RECURSIVE selected_agents AS (
			SELECT run_id, agent_id, flow_instance
			FROM unnest($4::uuid[], $5::text[], $6::text[]) AS selected(run_id, agent_id, flow_instance)
		),
		selected_tree AS (
			SELECT e.event_id
			FROM events e
			INNER JOIN run_fork_selected_contract_executions x
				ON x.fork_event_id = e.event_id
			   AND x.fork_run_id = $1::uuid
			   AND x.source_event_id = ANY($2::uuid[])
			WHERE e.run_id = $1::uuid
			UNION
			SELECT e.event_id
			FROM events e
			INNER JOIN selected_agents a
				ON a.run_id = e.run_id
			   AND a.agent_id = e.produced_by
			   AND a.flow_instance = COALESCE(e.source_route->>'flow_instance', '')
			WHERE e.run_id = $1::uuid
			  AND e.produced_by_type = 'agent'
			UNION
			SELECT child.event_id
			FROM events child
			INNER JOIN selected_tree parent ON child.source_event_id = parent.event_id
			WHERE child.run_id = $1::uuid
			  AND (
				child.event_name NOT LIKE 'platform.%'
				OR child.event_name = ANY($3::text[])
				OR child.event_id = ANY($7::uuid[])
			  )
		)
		SELECT COUNT(*), COALESCE(string_agg(e.event_name || ':' || e.event_id::text, ',' ORDER BY e.event_name, e.event_id), '')
		FROM events e
		WHERE e.run_id = $1::uuid
		  AND NOT (e.event_id = ANY($8::uuid[]))
		  AND NOT EXISTS (
			SELECT 1 FROM selected_tree tree WHERE tree.event_id = e.event_id
		  )
	`, forkRunID, pq.Array(allowedEvents), pq.Array(runForkSelectedContractForkLocalRuntimePlatformEventNames()), pq.Array(selectedAgentRunIDs), pq.Array(selectedAgentIDs), pq.Array(selectedAgentFlowInstances), pq.Array(activityIDs), pq.Array(observations)).Scan(&strayEvents, &strayEventEvidence); err != nil {
		return fmt.Errorf("check selected-contract fork event lineage: %w", err)
	}
	if strayEvents > 0 {
		return runForkReplayResumeError("fork_events_not_selected_contract_lineage", runfork.RunForkReplayResumeFactForkReplayState, fmt.Sprintf("fork activation blocked: fork_events_not_selected_contract_lineage: %s", strayEventEvidence))
	}

	checkedEvents := map[string]struct{}{}
	for _, snapshot := range deliverySnapshots {
		if _, ok := checkedEvents[snapshot.EventID]; ok {
			continue
		}
		checkedEvents[snapshot.EventID] = struct{}{}
		var belongs bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM events WHERE run_id = $1::uuid AND event_id = $2::uuid)`, forkRunID, snapshot.EventID).Scan(&belongs); err != nil {
			return fmt.Errorf("check selected-contract fork delivery event: %w", err)
		}
		if !belongs {
			return runForkReplayResumeError("fork_deliveries_not_selected_contract_lineage", runfork.RunForkReplayResumeFactForkReplayState, "fork activation blocked: fork_deliveries_not_selected_contract_lineage")
		}
	}
	return nil
}

func runForkSelectedContractForkLocalRuntimePlatformEventNames() []string {
	// runtime.run_fork.selected_contract_execution.fork_local_runtime_platform_event_lineage_policy:
	// fresh runtime platform/control outputs are fork-local lineage only when
	// they remain causally parented to selected-fork execution.
	return []string{
		"platform.agent_failed",
		"platform.agent_panic",
		"platform.agent_started",
		"platform.auth_required",
		"platform.budget_threshold_crossed",
		"platform.dead_letter_escalation",
		"platform.event_quarantined",
		"platform.paused",
		"platform.resumed",
		"platform.run_stalled",
		"platform.runtime_log",
	}
}

func uniqueNonEmptyStrings(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, value := range in {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
