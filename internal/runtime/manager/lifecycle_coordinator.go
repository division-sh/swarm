package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

type runtimeLifecyclePhase string

const (
	runtimeLifecycleStopped      runtimeLifecyclePhase = "stopped"
	runtimeLifecycleRunning      runtimeLifecyclePhase = "running"
	runtimeLifecycleShuttingDown runtimeLifecyclePhase = "shutting_down"
	runtimeLifecycleResetting    runtimeLifecyclePhase = "resetting"
)

type runtimeLifecycleTransitionKind string

const (
	runtimeLifecycleTransitionShutdown runtimeLifecycleTransitionKind = "shutdown"
	runtimeLifecycleTransitionReset    runtimeLifecycleTransitionKind = "reset"
)

type runtimeLifecycleTransition struct {
	kind     runtimeLifecycleTransitionKind
	done     chan struct{}
	claimed  bool
	complete bool
	result   error
}

func newRuntimeLifecycleTransition(kind runtimeLifecycleTransitionKind) *runtimeLifecycleTransition {
	return &runtimeLifecycleTransition{kind: kind, done: make(chan struct{})}
}

type agentLifecycleCell struct {
	identity       runtimeagentidentity.Identity
	opMu           sync.Mutex
	epoch          int64
	generation     uint64
	phase          AgentLifecyclePhase
	configRevision string
	runMode        AgentRunMode
	topology       runtimeagenttopology.Admission
	processBinding ProcessExecutionBinding
	execution      *agentExecutionProjection
}

type agentExecutionProjection struct {
	agent             Agent
	config            models.AgentConfig
	subscriptions     []events.EventType
	admission         semanticview.FlowOwnedAgentSubscriptionAdmission
	startedAt         time.Time
	token             runtimeeffects.LifecycleToken
	standingOwner     *worklifetime.StandingOccurrence
	generationCtx     context.Context
	cancelGeneration  context.CancelFunc
	loopCancel        context.CancelFunc
	loopDone          chan struct{}
	loopSettled       chan struct{}
	stopAfterAccepted chan struct{}
	route             <-chan *worklifetime.EventDelivery
	routeToken        runtimeeffects.LifecycleToken
	fenced            bool
	leases            int
	leaseDrained      chan struct{}
	deferredTerminal  *deferredAgentTermination
}

type deferredAgentTermination struct {
	trigger  string
	target   AgentLifecyclePhase
	topology runtimeagenttopology.Admission
}

type AgentRouteBus interface {
	PrepareAgentRoute(runtimeeffects.LifecycleToken, semanticview.FlowOwnedAgentSubscriptionAdmission) runtimebus.AgentRoutePreparation
	FenceAgentRoute(runtimeeffects.LifecycleToken)
	RemoveAgentRoute(runtimeeffects.LifecycleToken)
}

type agentExecutionSnapshot struct {
	Agent         Agent
	Config        models.AgentConfig
	Subscriptions []events.EventType
	Admission     semanticview.FlowOwnedAgentSubscriptionAdmission
	StartedAt     time.Time
	Token         runtimeeffects.LifecycleToken
	StandingOwner *worklifetime.StandingOccurrence
}

type executableAgentReadinessKind string

const (
	executableAgentPreparedBeforeRun         executableAgentReadinessKind = "prepared_before_run"
	executableAgentRunnableCurrentOccurrence executableAgentReadinessKind = "runnable_current_occurrence"
)

type executableAgentReadiness struct {
	Kind  executableAgentReadinessKind
	State AgentLifecycleState
}

func (c *agentLifecycleCoordinator) executableReadinessByIdentity(identity runtimeagentidentity.Identity) (executableAgentReadiness, error) {
	if c == nil {
		return executableAgentReadiness{}, errors.New("agent lifecycle coordinator is required")
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return executableAgentReadiness{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.sourceSetTransitionConflictLocked("verify_executable_readiness", identity); err != nil {
		return executableAgentReadiness{}, err
	}
	cell := c.cells[identity]
	if cell == nil || cell.execution == nil || cell.execution.agent == nil {
		return executableAgentReadiness{}, fmt.Errorf("agent %s has no executable lifecycle projection", identity.Description())
	}
	state := lifecycleStateFromCell(cell)
	switch c.phase {
	case runtimeLifecycleStopped:
		if !executionPreparedBeforeRunLocked(cell) {
			return executableAgentReadiness{}, executableReadinessError(identity, c.phase, state, "projection is not an exact pre-run preparation")
		}
		return executableAgentReadiness{Kind: executableAgentPreparedBeforeRun, State: state}, nil
	case runtimeLifecycleRunning:
		if !c.executionRunnableCurrentOccurrenceLocked(cell) {
			return executableAgentReadiness{}, executableReadinessError(identity, c.phase, state, "projection is not reachable in the current manager occurrence")
		}
		return executableAgentReadiness{Kind: executableAgentRunnableCurrentOccurrence, State: state}, nil
	default:
		return executableAgentReadiness{}, executableReadinessError(identity, c.phase, state, "manager lifecycle does not admit executable readiness")
	}
}

// committedRouteReadinessByIdentity observes an already executable occurrence
// without requesting lifecycle mutation authority. Source-set transitions still
// fence delivery admission; this observation only lets EventBus retain work for
// an exact route that was executable before the transition began.
func (c *agentLifecycleCoordinator) committedRouteReadinessByIdentity(identity runtimeagentidentity.Identity) (executableAgentReadiness, error) {
	if c == nil {
		return executableAgentReadiness{}, errors.New("agent lifecycle coordinator is required")
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return executableAgentReadiness{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cell := c.cells[identity]
	if cell == nil || cell.execution == nil || cell.execution.agent == nil {
		return executableAgentReadiness{}, fmt.Errorf("agent %s has no executable lifecycle projection", identity.Description())
	}
	state := lifecycleStateFromCell(cell)
	switch c.phase {
	case runtimeLifecycleStopped:
		if !executionPreparedBeforeRunLocked(cell) {
			return executableAgentReadiness{}, executableReadinessError(identity, c.phase, state, "projection is not an exact pre-run preparation")
		}
		return executableAgentReadiness{Kind: executableAgentPreparedBeforeRun, State: state}, nil
	case runtimeLifecycleRunning:
		if !c.executionRunnableCurrentOccurrenceLocked(cell) {
			return executableAgentReadiness{}, executableReadinessError(identity, c.phase, state, "projection is not reachable in the current manager occurrence")
		}
		return executableAgentReadiness{Kind: executableAgentRunnableCurrentOccurrence, State: state}, nil
	default:
		return executableAgentReadiness{}, executableReadinessError(identity, c.phase, state, "manager lifecycle does not admit executable readiness")
	}
}

func lifecycleStateFromCell(cell *agentLifecycleCell) AgentLifecycleState {
	if cell == nil {
		return AgentLifecycleState{}
	}
	return AgentLifecycleState{
		Identity: cell.identity, AgentID: cell.identity.AgentID(), RuntimeEpoch: cell.epoch,
		Generation: cell.generation, Phase: cell.phase, ConfigRevision: cell.configRevision,
		RunMode: cell.runMode, Topology: cell.topology, ProcessBinding: cell.processBinding,
	}
}

func executionPreparedBeforeRunLocked(cell *agentLifecycleCell) bool {
	if cell == nil || cell.execution == nil {
		return false
	}
	execution := cell.execution
	token := lifecycleToken(cell.identity, cell.epoch, cell.generation)
	phasePrepared := (cell.phase == AgentLifecycleRegistered && cell.runMode == AgentRunModeStopped) ||
		cell.phase == AgentLifecycleRunning
	return phasePrepared &&
		execution.agent != nil && execution.token == token && !execution.fenced &&
		execution.generationCtx != nil && execution.generationCtx.Err() == nil &&
		execution.loopCancel == nil && execution.loopDone == nil && execution.loopSettled == nil &&
		execution.stopAfterAccepted == nil && execution.route == nil && !execution.routeToken.Valid()
}

func (c *agentLifecycleCoordinator) executionRunnableCurrentOccurrenceLocked(cell *agentLifecycleCell) bool {
	if c == nil || cell == nil || cell.execution == nil || c.phase != runtimeLifecycleRunning ||
		c.runCtx == nil || c.runCtx.Err() != nil {
		return false
	}
	execution := cell.execution
	token := lifecycleToken(cell.identity, cell.epoch, cell.generation)
	return cell.phase == AgentLifecycleRunning && cell.runMode == c.runMode &&
		execution.agent != nil && execution.token == token && execution.routeToken == token &&
		!execution.fenced && execution.generationCtx != nil && execution.generationCtx.Err() == nil &&
		execution.loopDone != nil && signalPending(execution.loopDone) &&
		execution.loopSettled != nil && signalPending(execution.loopSettled) &&
		execution.stopAfterAccepted != nil && execution.route != nil
}

func signalPending(signal <-chan struct{}) bool {
	if signal == nil {
		return false
	}
	select {
	case <-signal:
		return false
	default:
		return true
	}
}

func executableReadinessError(identity runtimeagentidentity.Identity, managerPhase runtimeLifecyclePhase, state AgentLifecycleState, reason string) error {
	return runtimefailures.New(
		runtimefailures.ClassLifecycleConflict,
		"agent_execution_not_ready",
		"agent-lifecycle",
		"verify_executable_readiness",
		map[string]any{
			"agent": identity.Description(), "manager_phase": string(managerPhase),
			"lifecycle_phase": string(state.Phase), "run_mode": string(state.RunMode), "reason": reason,
		},
	)
}

func (c *agentLifecycleCoordinator) stateByIdentity(identity runtimeagentidentity.Identity) (AgentLifecycleState, bool) {
	if c == nil {
		return AgentLifecycleState{}, false
	}
	identity = identity.Normalize()
	c.mu.Lock()
	defer c.mu.Unlock()
	cell := c.cells[identity]
	if cell == nil {
		return AgentLifecycleState{}, false
	}
	return lifecycleStateFromCell(cell), true
}

type agentExecutionLease struct {
	agentExecutionSnapshot
	Context context.Context
	release func()
}

func (l *agentExecutionLease) Release() {
	if l == nil {
		return
	}
	if l.release != nil {
		l.release()
	}
}

type agentLifecycleCoordinator struct {
	mu                  sync.Mutex
	storeMu             sync.RWMutex
	workMu              sync.Mutex
	executionPublishMu  sync.Mutex
	sourceSetPublishMu  sync.RWMutex
	store               AgentLifecyclePersistence
	stateReader         AgentLifecycleStateReader
	effectsStore        runtimeeffects.Store
	sessions            runtimesessions.LifecycleProjection
	phase               runtimeLifecyclePhase
	runMode             AgentRunMode
	runCtx              context.Context
	baseContext         context.Context
	cancelRun           context.CancelFunc
	runParentContext    context.Context
	runParent           worklifetime.Occurrence
	runOwner            *worklifetime.ManagerRunOccurrence
	transitionExecutor  *worklifetime.Lease
	runGeneration       uint64
	workRetiring        bool
	watcherExpected     bool
	transition          *runtimeLifecycleTransition
	pendingReset        *runtimeLifecycleTransition
	retryDone           <-chan struct{}
	sourceSetTransition SourceSetTransitionAdmission
	cells               map[runtimeagentidentity.Identity]*agentLifecycleCell
	routes              AgentRouteBus
	executionPosture    executionposture.Posture
}

func sourceSetTransitionPending(admission SourceSetTransitionAdmission) bool {
	if admission == nil || admission.Done() == nil {
		return false
	}
	select {
	case <-admission.Done():
		return false
	default:
		return true
	}
}

func validateSourceSetTransitionAdmission(admission SourceSetTransitionAdmission) error {
	if admission == nil {
		return errors.New("source-set transition admission is required")
	}
	if _, err := uuid.Parse(strings.TrimSpace(admission.TransitionID())); err != nil {
		return fmt.Errorf("source-set transition identity is invalid: %w", err)
	}
	if strings.TrimSpace(admission.SourceSetRevision()) == "" {
		return errors.New("source-set transition revision is required")
	}
	if admission.Done() == nil {
		return errors.New("source-set transition completion is required")
	}
	return nil
}

func (c *agentLifecycleCoordinator) installSourceSetTransitionAdmission(admission SourceSetTransitionAdmission, resume bool) error {
	if c == nil {
		return errors.New("agent lifecycle coordinator is required")
	}
	if err := validateSourceSetTransitionAdmission(admission); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	current := c.sourceSetTransition
	if sourceSetTransitionPending(current) {
		if !resume || current.TransitionID() != admission.TransitionID() ||
			current.SourceSetRevision() != admission.SourceSetRevision() {
			return runtimefailures.New(
				runtimefailures.ClassLifecycleConflict,
				"source_set_transition_conflict",
				"agent-lifecycle",
				"prepare_source_set_transition",
				map[string]any{"source_set_revision": admission.SourceSetRevision()},
			)
		}
		return nil
	}
	if resume {
		return runtimefailures.New(
			runtimefailures.ClassLifecycleConflict,
			"source_set_transition_not_pending",
			"agent-lifecycle",
			"resume_source_set_transition",
			map[string]any{"source_set_revision": admission.SourceSetRevision()},
		)
	}
	c.sourceSetTransition = admission
	return nil
}

func (c *agentLifecycleCoordinator) sourceSetTransitionConflictLocked(action string, identity runtimeagentidentity.Identity) error {
	admission := c.sourceSetTransition
	if !sourceSetTransitionPending(admission) {
		return nil
	}
	detail := map[string]any{
		"transition_id":       admission.TransitionID(),
		"source_set_revision": admission.SourceSetRevision(),
	}
	if identity.Validate() == nil {
		detail["agent"] = identity.Description()
	}
	return runtimefailures.New(
		runtimefailures.ClassLifecycleConflict,
		"source_set_transition_pending",
		"agent-lifecycle",
		strings.TrimSpace(action),
		detail,
	)
}

func (c *agentLifecycleCoordinator) sourceSetTransitionConflict(action string) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sourceSetTransitionConflictLocked(action, runtimeagentidentity.Identity{})
}

func (c *agentLifecycleCoordinator) waitForSourceSetTransition() error {
	for {
		c.mu.Lock()
		admission := c.sourceSetTransition
		c.mu.Unlock()
		if !sourceSetTransitionPending(admission) {
			return nil
		}
		<-admission.Done()
	}
}

func (c *agentLifecycleCoordinator) installPersistence(store AgentLifecyclePersistence) error {
	if c == nil || store == nil {
		return errors.New("agent lifecycle persistence is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase != runtimeLifecycleStopped || len(c.cells) != 0 {
		return errors.New("agent lifecycle persistence must be installed before recovery or execution")
	}
	c.replacePersistence(store)
	return nil
}

func (c *agentLifecycleCoordinator) persistence() AgentLifecyclePersistence {
	if c == nil {
		return nil
	}
	c.storeMu.RLock()
	defer c.storeMu.RUnlock()
	return c.store
}

func (c *agentLifecycleCoordinator) replacePersistence(store AgentLifecyclePersistence) {
	if c == nil {
		return
	}
	c.storeMu.Lock()
	c.store = store
	c.storeMu.Unlock()
}

func (c *agentLifecycleCoordinator) context() context.Context {
	if c != nil && c.baseContext != nil {
		return c.baseContext
	}
	return context.Background()
}

func newAgentLifecycleCoordinator(store AgentLifecyclePersistence, sessionLifecycle runtimesessions.LifecycleProjection, routes AgentRouteBus, stateReader AgentLifecycleStateReader, effectsStore runtimeeffects.Store) *agentLifecycleCoordinator {
	coordinator := &agentLifecycleCoordinator{
		store: store, phase: runtimeLifecycleStopped, runMode: AgentRunModeStopped,
		cells: map[runtimeagentidentity.Identity]*agentLifecycleCell{}, sessions: sessionLifecycle,
		routes: routes, stateReader: stateReader, effectsStore: effectsStore,
	}
	return coordinator
}

func lifecycleToken(
	identity runtimeagentidentity.Identity,
	epoch int64,
	generation uint64,
) runtimeeffects.LifecycleToken {
	return runtimeeffects.LifecycleToken{
		RuntimeEpoch: epoch,
		Identity:     identity.Normalize(),
		AgentID:      identity.AgentID(),
		Generation:   generation,
	}
}

func (c *agentLifecycleCoordinator) resolveAgentTargetLocked(
	runID string,
	agentID string,
	flowInstance string,
	includeTerminated bool,
) (runtimeagentidentity.Identity, *agentLifecycleCell, error) {
	runID = strings.TrimSpace(runID)
	agentID = strings.TrimSpace(agentID)
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if runID == "" {
		return runtimeagentidentity.Identity{}, nil, fmt.Errorf("run_id is required")
	}
	if agentID == "" {
		return runtimeagentidentity.Identity{}, nil, fmt.Errorf("agent_id is required")
	}
	var matched runtimeagentidentity.Identity
	var matchedCell *agentLifecycleCell
	candidates := make([]runtimeagentidentity.Identity, 0, 2)
	for identity, cell := range c.cells {
		if identity.RunID != runID || !identity.MatchesAgentID(agentID) || cell == nil {
			continue
		}
		if flowInstance != "" && identity.FlowInstance() != flowInstance {
			continue
		}
		if !includeTerminated && (cell.phase == AgentLifecycleDraining || cell.phase == AgentLifecycleTerminated || cell.phase == AgentLifecycleFailed) {
			continue
		}
		candidates = append(candidates, identity.Normalize())
		matched, matchedCell = identity, cell
	}
	if len(candidates) > 1 {
		sort.Slice(candidates, func(left, right int) bool {
			return runtimeagentidentity.Less(candidates[left], candidates[right])
		})
		descriptions := make([]string, 0, len(candidates))
		for _, identity := range candidates {
			descriptions = append(descriptions, identity.Description())
		}
		return runtimeagentidentity.Identity{}, nil, fmt.Errorf(
			"agent_id %q in run %q is ambiguous across multiple live flow instances; provide flow_instance; candidates: %s",
			agentID,
			runID,
			strings.Join(descriptions, ", "),
		)
	}
	if matchedCell == nil {
		target := agentID
		if flowInstance != "" {
			target += "@" + flowInstance
		}
		return runtimeagentidentity.Identity{}, nil, fmt.Errorf("%w: %s", ErrAgentNotFound, target)
	}
	return matched, matchedCell, nil
}

func (c *agentLifecycleCoordinator) resolveAgentTarget(runID, agentID, flowInstance string, includeTerminated bool) (runtimeagentidentity.Identity, error) {
	if c == nil {
		return runtimeagentidentity.Identity{}, fmt.Errorf("%w: %s", ErrAgentNotFound, strings.TrimSpace(agentID))
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	identity, _, err := c.resolveAgentTargetLocked(runID, agentID, flowInstance, includeTerminated)
	return identity, err
}

func (c *agentLifecycleCoordinator) prepareRunOwner(parent context.Context, owner worklifetime.Occurrence) error {
	if c == nil {
		return errors.New("agent lifecycle coordinator is required")
	}
	c.workMu.Lock()
	defer c.workMu.Unlock()
	if c.runParent != nil {
		return nil
	}
	if owner == nil {
		return errors.New("manager run occurrence requires a runtime work occurrence")
	}
	c.runParentContext = parent
	c.runParent = owner
	return nil
}

func (c *agentLifecycleCoordinator) ensureRunAuthoritiesLocked(parent context.Context, owner worklifetime.Occurrence) error {
	if c.workRetiring {
		return errRuntimeShuttingDown
	}
	if c.runOwner != nil {
		return nil
	}
	if owner == nil {
		owner = c.runParent
	}
	if parent == nil {
		parent = c.runParentContext
	}
	runGeneration := c.runGeneration + 1
	runOwner, transitionExecutor, err := prepareManagerRunAuthorities(parent, owner, runGeneration)
	if err != nil {
		return err
	}
	c.runOwner = runOwner
	c.transitionExecutor = transitionExecutor
	c.runGeneration = runGeneration
	return nil
}

func prepareManagerRunAuthorities(parent context.Context, owner worklifetime.Occurrence, generation uint64) (*worklifetime.ManagerRunOccurrence, *worklifetime.Lease, error) {
	if owner == nil {
		return nil, nil, errors.New("manager run occurrence requires a runtime work occurrence")
	}
	if parent == nil {
		parent = context.Background()
	}
	root := runtimebus.WithRuntimeEpoch(context.WithoutCancel(parent), runtimebus.CurrentRuntimeEpoch())
	transitionExecutor, err := owner.BeginStanding(root)
	if err != nil {
		return nil, nil, fmt.Errorf("reserve manager transition executor: %w", err)
	}
	runOwner, err := worklifetime.NewManagerRunOccurrence(root, owner, worklifetime.ManagerRunIdentity{Generation: generation})
	if err != nil {
		return nil, nil, errors.Join(err, transitionExecutor.Done())
	}
	return runOwner, transitionExecutor, nil
}

func lifecycleConfigRevision(rec PersistedAgent) (string, error) {
	identity, err := rec.Config.ConcreteIdentity()
	if err != nil {
		return "", err
	}
	plan, err := identity.Plan()
	if err != nil {
		return "", err
	}
	return AgentConfigPlanRevision(rec.Config, plan)
}

// AgentConfigPlanRevision returns the run-independent revision admitted by
// declaration and readiness topology owners before a concrete run exists.
func AgentConfigPlanRevision(config models.AgentConfig, plan runtimeagentidentity.Plan) (string, error) {
	plan = plan.Normalize()
	if err := plan.Validate(); err != nil {
		return "", err
	}
	raw, err := canonicaljson.Bytes(config)
	if err != nil {
		return "", err
	}
	var projection map[string]any
	if err := canonicaljson.DecodeInto(raw, &projection); err != nil {
		return "", err
	}
	projection["identity"] = plan
	raw, err = canonicaljson.Bytes(projection)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func agentConfigPlanRevision(config models.AgentConfig, plan runtimeagentidentity.Plan) (string, error) {
	return AgentConfigPlanRevision(config, plan)
}

func lifecycleRequestHash(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func lifecycleTopologyIdentity(topology runtimeagenttopology.Admission) string {
	if err := topology.Validate(); err != nil {
		return ""
	}
	raw, err := canonicaljson.Bytes(topology)
	if err != nil {
		return ""
	}
	return string(raw)
}

func lifecycleProjectionEqual(
	revision string,
	topology runtimeagenttopology.Admission,
	currentRevision string,
	currentTopology runtimeagenttopology.Admission,
) bool {
	return revision == currentRevision && topology.Equal(currentTopology)
}

func lifecycleRequestHashWithTopology(topology runtimeagenttopology.Admission, parts ...string) string {
	parts = append(parts, lifecycleTopologyIdentity(topology))
	return lifecycleRequestHash(parts...)
}

func lifecycleRequestHashForIdentity(
	identity runtimeagentidentity.Identity,
	topology runtimeagenttopology.Admission,
	parts ...string,
) string {
	identity = identity.Normalize()
	parts = append(parts,
		identity.RunID,
		identity.Name.AgentID,
		identity.Name.Owner,
		string(identity.Name.Source),
		string(identity.Route.Presence),
		identity.Route.ScopeKey,
		identity.Route.InstanceID,
		identity.Route.InstancePath,
	)
	return lifecycleRequestHashWithTopology(topology, parts...)
}

func normalizedLifecycleSubordinate(plan runtimesessions.LifecycleMutationPlan) (runtimesessions.LifecycleMutationPlan, string, error) {
	normalized, err := plan.Normalize()
	if err != nil {
		return runtimesessions.LifecycleMutationPlan{}, "", err
	}
	raw, err := json.Marshal(normalized)
	if err != nil {
		return runtimesessions.LifecycleMutationPlan{}, "", err
	}
	return normalized, string(raw), nil
}

func lifecycleReconfigureOperationID(identity runtimeagentidentity.Identity, epoch int64, generation uint64, phase AgentLifecyclePhase, operationKind, revision, planIdentity string, topology runtimeagenttopology.Admission, target ProcessExecutionBinding) string {
	fingerprint, _ := identity.Fingerprint()
	parts := []string{
		"agent-lifecycle-reconfigure-occurrence-v1",
		fingerprint,
		strconv.FormatInt(epoch, 10),
		strconv.FormatUint(generation, 10),
		string(phase),
		strings.TrimSpace(operationKind),
		strings.TrimSpace(revision),
		planIdentity,
		target.ProcessAuthorityID,
		target.ProcessBootID,
		target.GenerationGrantID,
	}
	parts = append(parts, lifecycleTopologyIdentity(topology))
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join(parts, "\x00"))).String()
}

func lifecycleReintroductionOperationID(identity runtimeagentidentity.Identity, epoch int64, generation uint64, phase AgentLifecyclePhase, operationKind, revision, planIdentity string, topology runtimeagenttopology.Admission, target ProcessExecutionBinding) string {
	fingerprint, _ := identity.Fingerprint()
	parts := []string{
		"agent-lifecycle-reintroduction-occurrence-v1",
		fingerprint,
		strconv.FormatInt(epoch, 10),
		strconv.FormatUint(generation, 10),
		string(phase),
		strings.TrimSpace(operationKind),
		strings.TrimSpace(revision),
		planIdentity,
		target.ProcessAuthorityID,
		target.ProcessBootID,
		target.GenerationGrantID,
	}
	parts = append(parts, lifecycleTopologyIdentity(topology))
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join(parts, "\x00"))).String()
}

func lifecycleReintroductionAuthority(store AgentLifecyclePersistence, previous ProcessExecutionBinding) (string, ProcessExecutionBinding, error) {
	return lifecycleMutationExecutionAuthority(store, previous, "restart", false)
}

func lifecycleMutationExecutionAuthority(store AgentLifecyclePersistence, previous ProcessExecutionBinding, nominalKind string, terminal bool) (string, ProcessExecutionBinding, error) {
	provider, ok := store.(processExecutionBindingProvider)
	if !ok {
		return "", ProcessExecutionBinding{}, errors.New("lifecycle mutation requires process execution binding")
	}
	target, err := provider.ProcessExecutionBinding()
	if err != nil {
		return "", ProcessExecutionBinding{}, fmt.Errorf("load lifecycle reintroduction process binding: %w", err)
	}
	if err := previous.Validate(); err != nil {
		return "", ProcessExecutionBinding{}, fmt.Errorf("previous lifecycle process binding: %w", err)
	}
	if err := target.Validate(); err != nil {
		return "", ProcessExecutionBinding{}, fmt.Errorf("target lifecycle process binding: %w", err)
	}
	if previous.Equal(target) {
		return strings.TrimSpace(nominalKind), target, nil
	}
	if sameProcessExecutionOwner(previous, target) {
		if terminal {
			return "source_set_retire", target, nil
		}
		return "source_set_rebind", target, nil
	}
	return "process_takeover", target, nil
}

func (c *agentLifecycleCoordinator) registerExecution(ctx context.Context, rec PersistedAgent, persist bool, agent Agent, admission semanticview.FlowOwnedAgentSubscriptionAdmission) error {
	return c.registerExecutionWithTopology(ctx, rec, persist, agent, admission, rec.Topology)
}

func (c *agentLifecycleCoordinator) registerExecutionWithTopology(
	ctx context.Context,
	rec PersistedAgent,
	persist bool,
	agent Agent,
	admission semanticview.FlowOwnedAgentSubscriptionAdmission,
	topology runtimeagenttopology.Admission,
) error {
	if c == nil {
		return fmt.Errorf("agent lifecycle coordinator is required")
	}
	c.sourceSetPublishMu.RLock()
	defer c.sourceSetPublishMu.RUnlock()
	c.executionPublishMu.Lock()
	defer c.executionPublishMu.Unlock()
	if err := topology.Validate(); err != nil {
		return fmt.Errorf("agent lifecycle topology admission: %w", err)
	}
	rec.Topology = topology
	identity, err := rec.Config.ConcreteIdentity()
	if err != nil {
		return err
	}
	agentID := strings.TrimSpace(rec.Config.ID)
	if !admission.ValidForAgent(agentID) {
		return fmt.Errorf("agent %s missing subscription admission", agentID)
	}
	revision, err := lifecycleConfigRevision(rec)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.sourceSetTransitionConflictLocked("register_execution", identity); err != nil {
		return err
	}
	if c.phase == runtimeLifecycleShuttingDown || c.phase == runtimeLifecycleResetting {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "agent-lifecycle", "register_execution", map[string]any{"agent_id": agentID})
	}
	existingCell := c.cells[identity]
	if existingCell != nil && existingCell.phase != AgentLifecycleTerminated {
		return fmt.Errorf("%w: %s", ErrAgentAlreadyExists, agentID)
	}
	epoch := rec.LifecycleEpoch
	generation := rec.LifecycleGeneration
	phase := rec.LifecyclePhase
	mode := rec.LifecycleRunMode
	processBinding := rec.ProcessBinding
	store := c.persistence()
	if epoch <= 0 {
		epoch = runtimebus.CurrentRuntimeEpoch()
	}
	if generation == 0 && persist {
		generation = 1
	}
	if generation == 0 && store == nil {
		generation = 1
	}
	if phase == "" {
		phase = AgentLifecycleRegistered
	}
	if mode == "" {
		mode = AgentRunModeStopped
	}
	now := time.Now().UTC()
	plan, planHash, err := normalizedLifecycleSubordinate(runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		return err
	}
	operationID := uuid.NewString()
	requestHash := lifecycleRequestHashForIdentity(identity, topology, "spawn", revision, planHash)
	if persist && store != nil {
		previous, terminated, err := c.terminatedLifecycleState(ctx, identity, existingCell)
		if err != nil {
			return err
		}
		transition := AgentLifecycleTransition{
			OperationID: operationID, OperationKind: "spawn", Identity: identity, AgentID: agentID, Trigger: "spawn",
			RequestHash: requestHash, TargetEpoch: epoch,
			TargetGeneration: generation, TargetPhase: AgentLifecycleRegistered,
			ConfigRevision: revision, RunMode: AgentRunModeStopped, Agent: &rec, Subordinate: plan,
			Topology: topology, Now: now,
		}
		if terminated {
			operationKind, targetBinding, authorityErr := lifecycleReintroductionAuthority(store, previous.ProcessBinding)
			if authorityErr != nil {
				return authorityErr
			}
			operationID = lifecycleReintroductionOperationID(
				identity,
				previous.RuntimeEpoch,
				previous.Generation,
				previous.Phase,
				operationKind,
				revision,
				planHash,
				topology,
				targetBinding,
			)
			epoch = runtimebus.CurrentRuntimeEpoch()
			generation = previous.Generation + 1
			requestHash = lifecycleRequestHashForIdentity(
				identity, topology, operationKind, revision, planHash,
				previous.ProcessBinding.ProcessAuthorityID, previous.ProcessBinding.ProcessBootID,
				targetBinding.ProcessAuthorityID, targetBinding.ProcessBootID, targetBinding.GenerationGrantID,
			)
			transition = AgentLifecycleTransition{
				OperationID: operationID, OperationKind: operationKind, Identity: identity, AgentID: agentID, Trigger: operationKind,
				RequestHash:   requestHash,
				ExpectedEpoch: previous.RuntimeEpoch, ExpectedGeneration: previous.Generation, ExpectedPhase: previous.Phase,
				TargetEpoch: epoch, TargetGeneration: generation, TargetPhase: AgentLifecycleRegistered,
				ConfigRevision: revision, RunMode: AgentRunModeStopped, Agent: &rec, Subordinate: plan,
				Topology: topology, Now: now,
			}
		}
		result, err := store.CommitAgentLifecycleTransition(ctx, transition)
		if err != nil {
			return err
		}
		epoch, generation, phase, mode = result.RuntimeEpoch, result.Generation, result.Phase, result.RunMode
		processBinding = result.ProcessBinding
	} else if c.sessions != nil {
		if _, _, err := c.sessions.ApplyLifecycleProjection(ctx, runtimesessions.LifecycleProjectionRequest{
			OperationID: operationID, RequestHash: requestHash,
			Target:      lifecycleToken(identity, epoch, generation),
			TargetPhase: string(AgentLifecycleRegistered), Plan: plan, Now: now,
		}); err != nil {
			return err
		}
	}
	generationCtx, cancelGeneration := context.WithCancel(c.context())
	var standingOwner *worklifetime.StandingOccurrence
	if owner, ok := worklifetime.OccurrenceFromContext(ctx); ok {
		standingOwner, _ = worklifetime.StandingProjection(owner)
	}
	startedAt := rec.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now()
	}
	execution := &agentExecutionProjection{
		agent: agent, config: rec.Config, admission: admission, startedAt: startedAt,
		token: lifecycleToken(identity, epoch, generation), standingOwner: standingOwner,
		generationCtx: generationCtx, cancelGeneration: cancelGeneration,
	}
	execution.subscriptions = admittedSubscriptionEventTypes(admission)
	c.cells[identity] = &agentLifecycleCell{
		identity: identity, epoch: epoch, generation: generation, phase: phase,
		configRevision: revision, runMode: mode, topology: topology,
		processBinding: processBinding, execution: execution,
	}
	return nil
}

func (c *agentLifecycleCoordinator) terminatedLifecycleState(
	ctx context.Context,
	identity runtimeagentidentity.Identity,
	cell *agentLifecycleCell,
) (AgentLifecycleState, bool, error) {
	agentID := identity.AgentID()
	if cell != nil {
		return AgentLifecycleState{
			Identity: identity, AgentID: agentID, RuntimeEpoch: cell.epoch, Generation: cell.generation,
			Phase: cell.phase, ConfigRevision: cell.configRevision, RunMode: cell.runMode, Topology: cell.topology,
			ProcessBinding: cell.processBinding,
		}, cell.phase == AgentLifecycleTerminated, nil
	}
	reader := c.stateReader
	if reader == nil {
		return AgentLifecycleState{}, false, nil
	}
	state, found, err := reader.LoadAgentLifecycleState(ctx, identity)
	if err != nil {
		return AgentLifecycleState{}, false, err
	}
	if !found {
		return AgentLifecycleState{}, false, nil
	}
	if state.Phase != AgentLifecycleTerminated {
		return AgentLifecycleState{}, false, fmt.Errorf("%w: %s", ErrAgentAlreadyExists, agentID)
	}
	return state, true, nil
}

func (c *agentLifecycleCoordinator) beginRun(parent context.Context, mode AgentRunMode, owner worklifetime.Occurrence) (context.Context, bool, error) {
	c.sourceSetPublishMu.RLock()
	defer c.sourceSetPublishMu.RUnlock()
	c.executionPublishMu.Lock()
	defer c.executionPublishMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.sourceSetTransitionConflictLocked("begin_manager_run", runtimeagentidentity.Identity{}); err != nil {
		return nil, false, err
	}
	if c.phase != runtimeLifecycleStopped {
		return c.runCtx, false, nil
	}
	c.workMu.Lock()
	if err := c.ensureRunAuthoritiesLocked(parent, owner); err != nil {
		c.workMu.Unlock()
		return nil, false, err
	}
	c.workMu.Unlock()
	root := runtimebus.WithRuntimeEpoch(parent, runtimebus.CurrentRuntimeEpoch())
	runCtx, cancelRun := context.WithCancel(root)
	c.runCtx, c.cancelRun = runCtx, cancelRun
	c.phase = runtimeLifecycleRunning
	c.runMode = mode
	c.transition = nil
	c.pendingReset = nil
	c.retryDone = nil
	c.watcherExpected = true
	return c.runCtx, true, nil
}

func (c *agentLifecycleCoordinator) runSnapshot() (context.Context, AgentRunMode, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.runCtx, c.runMode, c.phase == runtimeLifecycleRunning
}

func (c *agentLifecycleCoordinator) phaseSnapshot() runtimeLifecyclePhase {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase
}

func (c *agentLifecycleCoordinator) abortRunStart(startErr error) error {
	c.mu.Lock()
	if c.cancelRun != nil {
		c.cancelRun()
	}
	c.workMu.Lock()
	runOwner := c.runOwner
	transitionExecutor := c.transitionExecutor
	c.runOwner = nil
	c.transitionExecutor = nil
	c.workRetiring = true
	c.workMu.Unlock()
	transitions := []*runtimeLifecycleTransition{c.transition, c.pendingReset}
	for _, transition := range transitions {
		if transition == nil || transition.complete {
			continue
		}
		transition.claimed = true
	}
	c.phase = runtimeLifecycleStopped
	c.runMode = AgentRunModeStopped
	c.runCtx = nil
	c.cancelRun = nil
	c.watcherExpected = false
	c.transition = nil
	c.pendingReset = nil
	c.retryDone = nil
	c.mu.Unlock()
	var settleErr error
	if transitionExecutor != nil {
		settleErr = transitionExecutor.Done()
	}
	if runOwner != nil {
		if err := runOwner.RetireAndWait(context.Background()); err != nil {
			settleErr = errors.Join(settleErr, fmt.Errorf("retire aborted manager run occurrence: %w", err))
		}
	}
	transitionResult := errors.Join(startErr, settleErr)
	for _, transition := range transitions {
		if transition == nil || transition.complete {
			continue
		}
		transition.result = transitionResult
		transition.complete = true
		close(transition.done)
	}
	c.workMu.Lock()
	c.workRetiring = false
	c.workMu.Unlock()
	return settleErr
}

func (c *agentLifecycleCoordinator) takeShutdownWatcherExecutor() (*worklifetime.Lease, error) {
	if c == nil {
		return nil, errors.New("agent lifecycle coordinator is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.watcherExpected || c.phase != runtimeLifecycleRunning {
		return nil, errors.New("manager shutdown watcher is not expected")
	}
	c.workMu.Lock()
	defer c.workMu.Unlock()
	if c.transitionExecutor == nil {
		return nil, errors.New("manager shutdown watcher has no reserved transition executor")
	}
	executor := c.transitionExecutor
	c.transitionExecutor = nil
	return executor, nil
}

func (c *agentLifecycleCoordinator) claimUnwatchedTransition(transition *runtimeLifecycleTransition, kind runtimeLifecycleTransitionKind) (*worklifetime.Lease, bool, error) {
	if c == nil || transition == nil {
		return nil, false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.watcherExpected {
		return nil, false, nil
	}
	if c.transition != transition || transition.kind != kind || transition.claimed || transition.complete {
		return nil, false, nil
	}
	c.workMu.Lock()
	defer c.workMu.Unlock()
	if c.transitionExecutor == nil {
		return nil, false, errors.New("manager transition has no reserved executor")
	}
	transition.claimed = true
	executor := c.transitionExecutor
	c.transitionExecutor = nil
	return executor, true, nil
}

func (c *agentLifecycleCoordinator) beginWork(ctx context.Context, companion worklifetime.Occurrence) (*worklifetime.Lease, error) {
	if c == nil {
		return nil, errors.New("agent lifecycle coordinator is required")
	}
	c.workMu.Lock()
	defer c.workMu.Unlock()
	if err := c.ensureRunAuthoritiesLocked(nil, nil); err != nil {
		return nil, err
	}
	return c.runOwner.Begin(ctx, companion)
}

func (c *agentLifecycleCoordinator) beginStandingWork(ctx context.Context) (*worklifetime.Lease, error) {
	if c == nil {
		return nil, errors.New("agent lifecycle coordinator is required")
	}
	c.workMu.Lock()
	defer c.workMu.Unlock()
	if err := c.ensureRunAuthoritiesLocked(nil, nil); err != nil {
		return nil, err
	}
	return c.runOwner.BeginStanding(ctx)
}

func (c *agentLifecycleCoordinator) retireRunOwner(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.workMu.Lock()
	owner := c.runOwner
	c.workMu.Unlock()
	if owner == nil {
		return nil
	}
	return owner.RetireAndWait(ctx)
}

func (c *agentLifecycleCoordinator) waitForWork(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.workMu.Lock()
	owner := c.runOwner
	c.workMu.Unlock()
	if owner == nil {
		return nil
	}
	return owner.WaitForQuiescence(ctx)
}

func (c *agentLifecycleCoordinator) retireWorkAdmission() bool {
	c.workMu.Lock()
	defer c.workMu.Unlock()
	if c.runOwner == nil {
		return false
	}
	c.workRetiring = true
	c.runOwner.Retire()
	return true
}

func (c *agentLifecycleCoordinator) requestShutdownTransition() *runtimeLifecycleTransition {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	var cancel context.CancelFunc
	var transition *runtimeLifecycleTransition
	switch c.phase {
	case runtimeLifecycleRunning:
		c.retireWorkAdmission()
		transition = newRuntimeLifecycleTransition(runtimeLifecycleTransitionShutdown)
		c.transition = transition
		c.phase = runtimeLifecycleShuttingDown
		cancel = c.cancelRun
	case runtimeLifecycleShuttingDown, runtimeLifecycleResetting:
		transition = c.transition
	case runtimeLifecycleStopped:
		if c.retireWorkAdmission() {
			transition = newRuntimeLifecycleTransition(runtimeLifecycleTransitionShutdown)
			c.transition = transition
			c.phase = runtimeLifecycleShuttingDown
			cancel = c.cancelRun
		}
	}
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return transition
}

func (c *agentLifecycleCoordinator) requestResetTransition() (*runtimeLifecycleTransition, *runtimeLifecycleTransition) {
	if c == nil {
		return nil, nil
	}
	c.mu.Lock()
	var cancel context.CancelFunc
	var shutdown *runtimeLifecycleTransition
	var reset *runtimeLifecycleTransition
	switch c.phase {
	case runtimeLifecycleRunning:
		c.retireWorkAdmission()
		shutdown = newRuntimeLifecycleTransition(runtimeLifecycleTransitionShutdown)
		reset = newRuntimeLifecycleTransition(runtimeLifecycleTransitionReset)
		c.transition = shutdown
		c.pendingReset = reset
		c.phase = runtimeLifecycleShuttingDown
		cancel = c.cancelRun
	case runtimeLifecycleShuttingDown:
		shutdown = c.transition
		if c.pendingReset == nil {
			c.pendingReset = newRuntimeLifecycleTransition(runtimeLifecycleTransitionReset)
		}
		reset = c.pendingReset
	case runtimeLifecycleResetting:
		reset = c.transition
	case runtimeLifecycleStopped:
		if c.retireWorkAdmission() {
			shutdown = newRuntimeLifecycleTransition(runtimeLifecycleTransitionShutdown)
			reset = newRuntimeLifecycleTransition(runtimeLifecycleTransitionReset)
			c.transition = shutdown
			c.pendingReset = reset
			c.phase = runtimeLifecycleShuttingDown
			cancel = c.cancelRun
		} else {
			reset = newRuntimeLifecycleTransition(runtimeLifecycleTransitionReset)
			c.transition = reset
			c.phase = runtimeLifecycleResetting
		}
	}
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return shutdown, reset
}

func (c *agentLifecycleCoordinator) claimTransition(transition *runtimeLifecycleTransition, kind runtimeLifecycleTransitionKind) bool {
	if c == nil || transition == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.transition != transition || transition.kind != kind || transition.claimed || transition.complete {
		return false
	}
	transition.claimed = true
	return true
}

func (c *agentLifecycleCoordinator) setRetryDone(done <-chan struct{}) bool {
	if c == nil || done == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase != runtimeLifecycleRunning || c.retryDone != nil {
		return false
	}
	c.retryDone = done
	return true
}

func (c *agentLifecycleCoordinator) cancelShutdownWork() (context.Context, []<-chan struct{}) {
	c.mu.Lock()
	if c.cancelRun != nil {
		c.cancelRun()
		c.cancelRun = nil
	}
	done := make([]<-chan struct{}, 0, len(c.cells))
	for _, cell := range c.cells {
		execution := cell.execution
		if execution == nil {
			continue
		}
		execution.fenced = true
		if execution.cancelGeneration != nil {
			execution.cancelGeneration()
		}
		if execution.loopDone != nil {
			done = append(done, execution.loopDone)
		}
		if execution.loopSettled != nil {
			done = append(done, execution.loopSettled)
		}
		if execution.leases > 0 && execution.leaseDrained != nil {
			done = append(done, execution.leaseDrained)
		}
	}
	if c.retryDone != nil {
		done = append(done, c.retryDone)
	}
	ctx := c.runCtx
	c.mu.Unlock()
	return ctx, done
}

func (c *agentLifecycleCoordinator) completeShutdownTransition(transition *runtimeLifecycleTransition, result error) {
	if c == nil || transition == nil {
		return
	}
	c.mu.Lock()
	if c.transition != transition || transition.kind != runtimeLifecycleTransitionShutdown || transition.complete {
		c.mu.Unlock()
		return
	}
	c.workMu.Lock()
	if c.transitionExecutor != nil {
		result = errors.Join(result, c.transitionExecutor.Done())
	}
	transition.result = result
	transition.complete = true
	c.runMode = AgentRunModeStopped
	c.runCtx = nil
	c.cancelRun = nil
	c.runOwner = nil
	c.transitionExecutor = nil
	c.watcherExpected = false
	c.retryDone = nil
	if c.pendingReset != nil {
		c.workRetiring = true
		c.phase = runtimeLifecycleResetting
		c.transition = c.pendingReset
		c.pendingReset = nil
	} else {
		c.workRetiring = false
		c.phase = runtimeLifecycleStopped
		c.transition = nil
	}
	c.workMu.Unlock()
	close(transition.done)
	c.mu.Unlock()
}

func (c *agentLifecycleCoordinator) completeResetTransition(transition *runtimeLifecycleTransition, result error, clearCells bool) {
	if c == nil || transition == nil {
		return
	}
	c.mu.Lock()
	if c.transition != transition || transition.kind != runtimeLifecycleTransitionReset || transition.complete {
		c.mu.Unlock()
		return
	}
	transition.result = result
	transition.complete = true
	if clearCells {
		c.cells = map[runtimeagentidentity.Identity]*agentLifecycleCell{}
	}
	c.phase = runtimeLifecycleStopped
	c.runMode = AgentRunModeStopped
	c.runCtx = nil
	c.cancelRun = nil
	c.workMu.Lock()
	c.runOwner = nil
	c.transitionExecutor = nil
	c.workRetiring = false
	c.workMu.Unlock()
	c.watcherExpected = false
	c.transition = nil
	c.pendingReset = nil
	c.retryDone = nil
	close(transition.done)
	c.mu.Unlock()
}

func (c *agentLifecycleCoordinator) prepareLoopTokenLocked(identity runtimeagentidentity.Identity, lockedCell *agentLifecycleCell) (runtimeeffects.LifecycleToken, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return runtimeeffects.LifecycleToken{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cell := c.cells[identity]
	if cell == nil || cell != lockedCell || cell.identity != identity || cell.phase == AgentLifecycleDraining || cell.phase == AgentLifecycleTerminated || cell.phase == AgentLifecycleFailed {
		return runtimeeffects.LifecycleToken{}, fmt.Errorf("%w: %s", ErrAgentNotFound, identity.Description())
	}
	return lifecycleToken(identity, runtimebus.CurrentRuntimeEpoch(), cell.generation+1), nil
}

func (c *agentLifecycleCoordinator) lockIdentityOperation(identity runtimeagentidentity.Identity) (*agentLifecycleCell, error) {
	return c.lockIdentityOperationMode(identity, false, false, false)
}

// lockIdentityTopologyOperation admits failed durable cells because topology
// rebinding does not make them executable. Ordinary lifecycle operations keep
// treating failed cells as terminal until startup explicitly reintroduces a
// still-declared identity as a fresh registered generation.
func (c *agentLifecycleCoordinator) lockIdentityTopologyOperation(identity runtimeagentidentity.Identity) (*agentLifecycleCell, error) {
	return c.lockIdentityOperationMode(identity, true, false, false)
}

func (c *agentLifecycleCoordinator) lockIdentitySourceSetOperation(identity runtimeagentidentity.Identity) (*agentLifecycleCell, error) {
	return c.lockIdentityOperationMode(identity, true, true, true)
}

func (c *agentLifecycleCoordinator) lockIdentityOperationMode(
	identity runtimeagentidentity.Identity,
	includeFailed bool,
	includeDraining bool,
	ignoreSourceSetTransition bool,
) (*agentLifecycleCell, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if !ignoreSourceSetTransition {
		if err := c.sourceSetTransitionConflictLocked("lifecycle_mutation", identity); err != nil {
			c.mu.Unlock()
			return nil, err
		}
	}
	cell := c.cells[identity]
	c.mu.Unlock()
	if cell == nil {
		return nil, fmt.Errorf("%w: %s", ErrAgentNotFound, identity.Description())
	}
	cell.opMu.Lock()
	c.mu.Lock()
	current := c.cells[identity]
	valid := current == cell && current.phase != AgentLifecycleTerminated &&
		(includeDraining || current.phase != AgentLifecycleDraining) &&
		(includeFailed || current.phase != AgentLifecycleFailed)
	if valid && !ignoreSourceSetTransition {
		if err := c.sourceSetTransitionConflictLocked("lifecycle_mutation", identity); err != nil {
			c.mu.Unlock()
			cell.opMu.Unlock()
			return nil, err
		}
	}
	c.mu.Unlock()
	if !valid {
		cell.opMu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrAgentNotFound, identity.Description())
	}
	return cell, nil
}

func (c *agentLifecycleCoordinator) executionSnapshotByIdentity(identity runtimeagentidentity.Identity) (agentExecutionSnapshot, bool) {
	if c == nil {
		return agentExecutionSnapshot{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cell := c.cells[identity.Normalize()]
	if cell == nil || cell.execution == nil || cell.execution.agent == nil ||
		cell.phase == AgentLifecycleDraining || cell.phase == AgentLifecycleTerminated || cell.phase == AgentLifecycleFailed {
		return agentExecutionSnapshot{}, false
	}
	return snapshotExecution(cell.execution), true
}

func (c *agentLifecycleCoordinator) executionIdentities() []runtimeagentidentity.Identity {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	identities := make([]runtimeagentidentity.Identity, 0, len(c.cells))
	for identity, cell := range c.cells {
		if cell != nil && cell.execution != nil && cell.execution.agent != nil && cell.phase != AgentLifecycleDraining && cell.phase != AgentLifecycleTerminated && cell.phase != AgentLifecycleFailed {
			identities = append(identities, identity)
		}
	}
	sort.Slice(identities, func(i, j int) bool {
		return identities[i].Description() < identities[j].Description()
	})
	return identities
}

func (c *agentLifecycleCoordinator) executionConfigs() []models.AgentConfig {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	configs := make([]models.AgentConfig, 0, len(c.cells))
	for _, cell := range c.cells {
		if cell != nil && cell.execution != nil && cell.execution.agent != nil && cell.phase != AgentLifecycleDraining && cell.phase != AgentLifecycleTerminated && cell.phase != AgentLifecycleFailed {
			configs = append(configs, cell.execution.config)
		}
	}
	return configs
}

func snapshotExecution(execution *agentExecutionProjection) agentExecutionSnapshot {
	if execution == nil {
		return agentExecutionSnapshot{}
	}
	return agentExecutionSnapshot{
		Agent: execution.agent, Config: execution.config,
		Subscriptions: append([]events.EventType(nil), execution.subscriptions...),
		Admission:     execution.admission,
		StartedAt:     execution.startedAt, Token: execution.token, StandingOwner: execution.standingOwner,
	}
}

func (c *agentLifecycleCoordinator) acquireExecutionIdentity(ctx context.Context, identity runtimeagentidentity.Identity, purpose string, requireRunning bool) (*agentExecutionLease, error) {
	cell, err := c.lockIdentityOperation(identity)
	if err != nil {
		return nil, err
	}
	return c.acquireExecutionLocked(ctx, cell, identity.Description(), purpose, requireRunning, runtimeeffects.LifecycleToken{}, false)
}

func (c *agentLifecycleCoordinator) acquireDeliveryExecution(ctx context.Context, token runtimeeffects.LifecycleToken) (*agentExecutionLease, error) {
	if !token.Valid() {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "delivery_execution_token_invalid", "agent-lifecycle", "admit_delivery_execution", nil)
	}
	for {
		if err := c.waitForSourceSetTransition(); err != nil {
			return nil, err
		}
		cell, err := c.lockIdentityOperationMode(token.Identity, false, false, true)
		if err != nil {
			return nil, err
		}
		lease, err := c.acquireExecutionLocked(ctx, cell, token.Identity.Description(), "admit_delivery_execution", true, token, true)
		if errors.Is(err, errSourceSetTransitionAdmissionPending) {
			continue
		}
		return lease, err
	}
}

var errSourceSetTransitionAdmissionPending = errors.New("source-set transition admission pending")

func (c *agentLifecycleCoordinator) acquireExecutionLocked(
	ctx context.Context,
	cell *agentLifecycleCell,
	target, purpose string,
	requireRunning bool,
	exactRouteToken runtimeeffects.LifecycleToken,
	waitForSourceSetTransition bool,
) (*agentExecutionLease, error) {
	defer cell.opMu.Unlock()
	c.mu.Lock()
	if sourceSetTransitionPending(c.sourceSetTransition) {
		if waitForSourceSetTransition {
			c.mu.Unlock()
			return nil, errSourceSetTransitionAdmissionPending
		}
		err := c.sourceSetTransitionConflictLocked(purpose, cell.identity)
		c.mu.Unlock()
		return nil, err
	}
	execution := cell.execution
	running := cell.phase == AgentLifecycleRunning
	exactRoute := !exactRouteToken.Valid() || (execution != nil && execution.token == exactRouteToken && execution.routeToken == exactRouteToken)
	generationLive := execution != nil && execution.generationCtx != nil && execution.generationCtx.Err() == nil
	if execution == nil || execution.agent == nil || execution.fenced || !generationLive || (requireRunning && !running) || !exactRoute {
		c.mu.Unlock()
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_generation_not_running", "agent-lifecycle", purpose, map[string]any{"agent": strings.TrimSpace(target)})
	}
	if execution.leases == 0 {
		execution.leaseDrained = make(chan struct{})
	}
	execution.leases++
	snapshot := snapshotExecution(execution)
	generationCtx := execution.generationCtx
	runCtx := c.runCtx
	c.mu.Unlock()

	if ctx == nil {
		ctx = c.context()
	}
	leaseCtx, cancel := context.WithCancel(ctx)
	stopGenerationCancel := context.AfterFunc(generationCtx, cancel)
	if admission, ok := managedexecution.FromContext(runCtx); ok {
		leaseCtx = managedexecution.WithAdmission(leaseCtx, admission)
	}
	leaseCtx = runtimeeffects.WithLifecycleToken(leaseCtx, snapshot.Token)
	if c.effectsStore != nil {
		leaseCtx = runtimeeffects.WithController(leaseCtx, runtimeeffects.NewController(c.effectsStore).WithExecutionPosture(c.executionPosture))
	}
	lease := &agentExecutionLease{agentExecutionSnapshot: snapshot, Context: leaseCtx}
	lease.release = sync.OnceFunc(func() {
		stopGenerationCancel()
		cancel()
		c.mu.Lock()
		if execution.leases > 0 {
			execution.leases--
			if execution.leases == 0 && execution.leaseDrained != nil {
				close(execution.leaseDrained)
				execution.leaseDrained = nil
			}
		}
		c.mu.Unlock()
	})
	return lease, nil
}

func (c *agentLifecycleCoordinator) replaceLoopLocked(
	ctx context.Context,
	agentID, trigger, operationID string,
	rec *PersistedAgent,
	subordinate runtimesessions.LifecycleMutationPlan,
	topology *runtimeagenttopology.Admission,
	lockedCell *agentLifecycleCell,
	preparedToken runtimeeffects.LifecycleToken,
) (context.Context, runtimeeffects.LifecycleToken, chan struct{}, error) {
	plan, planHash, err := normalizedLifecycleSubordinate(subordinate)
	if err != nil {
		return nil, runtimeeffects.LifecycleToken{}, nil, err
	}
	identity := lockedCell.identity
	c.mu.Lock()
	cell := c.cells[identity]
	if cell == nil || cell != lockedCell || cell.phase == AgentLifecycleDraining || cell.phase == AgentLifecycleTerminated || cell.phase == AgentLifecycleFailed {
		c.mu.Unlock()
		return nil, runtimeeffects.LifecycleToken{}, nil, fmt.Errorf("%w: %s", ErrAgentNotFound, agentID)
	}
	if c.phase == runtimeLifecycleShuttingDown || c.phase == runtimeLifecycleResetting {
		c.mu.Unlock()
		return nil, runtimeeffects.LifecycleToken{}, nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "agent-lifecycle", trigger, map[string]any{"agent_id": agentID})
	}
	previousEpoch, previousGeneration, previousPhase := cell.epoch, cell.generation, cell.phase
	transitionTopology := cell.topology
	if topology != nil {
		transitionTopology = *topology
	}
	if err := transitionTopology.Validate(); err != nil {
		c.mu.Unlock()
		return nil, runtimeeffects.LifecycleToken{}, nil, fmt.Errorf("agent lifecycle topology admission: %w", err)
	}
	if rec != nil {
		rec.Topology = transitionTopology
	}
	previousExecution := cell.execution
	var previousDone, previousLeasesDone <-chan struct{}
	var previousCancel context.CancelFunc
	var previousRouteToken runtimeeffects.LifecycleToken
	if previousExecution != nil {
		previousDone = previousExecution.loopDone
		previousCancel = previousExecution.cancelGeneration
		previousRouteToken = previousExecution.routeToken
	}
	runCtx, mode, running := c.runCtx, c.runMode, c.phase == runtimeLifecycleRunning
	nextEpoch := runtimebus.CurrentRuntimeEpoch()
	nextGeneration := previousGeneration + 1
	if preparedToken.Valid() {
		if preparedToken.Identity != identity || preparedToken.Generation != nextGeneration {
			c.mu.Unlock()
			return nil, runtimeeffects.LifecycleToken{}, nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "prepared_execution_token_mismatch", "agent-lifecycle", trigger, map[string]any{"agent_id": agentID})
		}
		nextEpoch = preparedToken.RuntimeEpoch
	}
	revision := cell.configRevision
	if rec != nil {
		var err error
		revision, err = lifecycleConfigRevision(*rec)
		if err != nil {
			c.mu.Unlock()
			return nil, runtimeeffects.LifecycleToken{}, nil, err
		}
	}
	if trigger == "reconfigure" && lifecycleProjectionEqual(revision, transitionTopology, cell.configRevision, cell.topology) {
		token := lifecycleToken(identity, cell.epoch, cell.generation)
		c.mu.Unlock()
		return nil, token, nil, nil
	}
	store := c.persistence()
	operationKind := trigger
	targetBinding := cell.processBinding
	if store != nil {
		operationKind, targetBinding, err = lifecycleMutationExecutionAuthority(store, cell.processBinding, trigger, false)
		if err != nil {
			c.mu.Unlock()
			return nil, runtimeeffects.LifecycleToken{}, nil, err
		}
	}
	if operationID == "" {
		if trigger == "reconfigure" {
			operationID = lifecycleReconfigureOperationID(identity, previousEpoch, previousGeneration, previousPhase, operationKind, revision, planHash, transitionTopology, targetBinding)
		} else {
			operationID = uuid.NewString()
		}
	}
	targetPhase := AgentLifecycleRunning
	targetMode := mode
	if !running || runCtx == nil {
		targetPhase = AgentLifecycleRegistered
		targetMode = AgentRunModeStopped
	}
	now := time.Now().UTC()
	requestHash := lifecycleRequestHashForIdentity(
		identity, transitionTopology, operationKind, trigger, revision, planHash,
		cell.processBinding.ProcessAuthorityID, cell.processBinding.ProcessBootID,
		targetBinding.ProcessAuthorityID, targetBinding.ProcessBootID, targetBinding.GenerationGrantID,
	)
	result := AgentLifecycleTransitionResult{
		OperationID: operationID, Identity: identity, AgentID: agentID,
		PreviousEpoch: previousEpoch, RuntimeEpoch: nextEpoch,
		PreviousGeneration: previousGeneration, Generation: nextGeneration,
		PreviousPhase: previousPhase, Phase: targetPhase, ConfigRevision: revision, RunMode: targetMode,
		Subordinate: runtimesessions.LifecycleMutationOutcome{Action: plan.Action},
	}
	if store != nil {
		var err error
		result, err = store.CommitAgentLifecycleTransition(context.WithoutCancel(ctx), AgentLifecycleTransition{
			OperationID: operationID, OperationKind: operationKind, RequestHash: requestHash, Identity: identity,
			AgentID: agentID, Trigger: trigger, ExpectedEpoch: previousEpoch, ExpectedGeneration: previousGeneration,
			ExpectedPhase: previousPhase, TargetEpoch: nextEpoch, TargetGeneration: nextGeneration,
			TargetPhase: targetPhase, ConfigRevision: revision, RunMode: targetMode, Agent: rec, Subordinate: plan,
			Topology: transitionTopology, Now: now,
		})
		if err != nil {
			c.mu.Unlock()
			return nil, runtimeeffects.LifecycleToken{}, nil, err
		}
	} else if c.sessions != nil {
		outcome, replayed, err := c.sessions.ApplyLifecycleProjection(context.WithoutCancel(ctx), runtimesessions.LifecycleProjectionRequest{
			OperationID: operationID, RequestHash: requestHash,
			Expected:    lifecycleToken(identity, previousEpoch, previousGeneration),
			Target:      lifecycleToken(identity, nextEpoch, nextGeneration),
			TargetPhase: string(targetPhase), Plan: plan, Now: now,
		})
		if err != nil {
			c.mu.Unlock()
			return nil, runtimeeffects.LifecycleToken{}, nil, err
		}
		result.Subordinate = outcome
		result.Replayed = replayed
	}
	if result.Replayed {
		if result.RuntimeEpoch == cell.epoch && result.Generation == cell.generation && result.Phase == cell.phase {
			token := lifecycleToken(identity, cell.epoch, cell.generation)
			c.mu.Unlock()
			return nil, token, nil, nil
		}
		if result.OperationID != operationID || result.AgentID != agentID ||
			result.PreviousEpoch != cell.epoch || result.PreviousGeneration != cell.generation || result.PreviousPhase != cell.phase ||
			result.RuntimeEpoch != nextEpoch || result.Generation != nextGeneration || result.Phase != targetPhase ||
			result.ConfigRevision != revision || result.RunMode != targetMode {
			c.mu.Unlock()
			return nil, runtimeeffects.LifecycleToken{}, nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_replay_projection_conflict", "agent-lifecycle", trigger, map[string]any{"agent_id": agentID, "operation_id": operationID})
		}
	}
	cell.epoch, cell.generation, cell.phase, cell.configRevision, cell.runMode, cell.topology = result.RuntimeEpoch, result.Generation, result.Phase, result.ConfigRevision, result.RunMode, transitionTopology
	cell.processBinding = result.ProcessBinding
	if previousExecution != nil {
		previousExecution.fenced = true
		if previousExecution.leases > 0 {
			previousLeasesDone = previousExecution.leaseDrained
		}
	}
	if previousCancel != nil {
		previousCancel()
	}
	c.mu.Unlock()
	if c.routes != nil && previousRouteToken.Valid() {
		c.routes.RemoveAgentRoute(previousRouteToken)
	}
	if previousDone != nil {
		<-previousDone
	}
	if previousLeasesDone != nil {
		<-previousLeasesDone
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cell = c.cells[identity]
	if cell == nil || cell.epoch != result.RuntimeEpoch || cell.generation != result.Generation || cell.phase != result.Phase {
		return nil, runtimeeffects.LifecycleToken{}, nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "agent-lifecycle", trigger, map[string]any{"agent_id": agentID})
	}
	token := lifecycleToken(identity, result.RuntimeEpoch, result.Generation)
	baseCtx := c.context()
	if result.Phase == AgentLifecycleRunning {
		baseCtx = runCtx
	}
	generationCtx, cancelGeneration := context.WithCancel(baseCtx)
	nextExecution := &agentExecutionProjection{token: token, generationCtx: generationCtx, cancelGeneration: cancelGeneration}
	if previousExecution != nil {
		nextExecution.agent = previousExecution.agent
		nextExecution.config = previousExecution.config
		nextExecution.subscriptions = append([]events.EventType(nil), previousExecution.subscriptions...)
		nextExecution.startedAt = previousExecution.startedAt
		nextExecution.standingOwner = previousExecution.standingOwner
	}
	if rec != nil {
		nextExecution.config = rec.Config
	}
	cell.execution = nextExecution
	if result.Phase != AgentLifecycleRunning {
		return nil, token, nil, nil
	}
	loopCtx := runtimeeffects.WithLifecycleToken(generationCtx, token)
	if c.effectsStore != nil {
		loopCtx = runtimeeffects.WithController(loopCtx, runtimeeffects.NewController(c.effectsStore).WithExecutionPosture(c.executionPosture))
	}
	done := make(chan struct{})
	settled := make(chan struct{})
	stopAfterAccepted := make(chan struct{})
	nextExecution.loopCancel, nextExecution.loopDone = cancelGeneration, done
	nextExecution.loopSettled = settled
	nextExecution.stopAfterAccepted = stopAfterAccepted
	return loopCtx, token, done, nil
}

func (c *agentLifecycleCoordinator) releaseLoop(token runtimeeffects.LifecycleToken, done chan struct{}) error {
	if c == nil {
		return nil
	}
	var cell *agentLifecycleCell
	for {
		if err := c.waitForSourceSetTransition(); err != nil {
			return err
		}
		// Source-set preparation takes the exclusive side before publishing its
		// admission fence. The shared side keeps the recheck and loop settlement
		// atomic without blocking an ordinary replacement that is joining this loop.
		c.sourceSetPublishMu.RLock()
		c.mu.Lock()
		pending := sourceSetTransitionPending(c.sourceSetTransition)
		c.mu.Unlock()
		if !pending {
			break
		}
		c.sourceSetPublishMu.RUnlock()
	}
	if c.routes != nil {
		c.routes.RemoveAgentRoute(token)
	}
	close(done)
	c.mu.Lock()
	cell = c.cells[token.Identity.Normalize()]
	c.mu.Unlock()
	if cell == nil {
		c.sourceSetPublishMu.RUnlock()
		return nil
	}
	cell.opMu.Lock()
	c.sourceSetPublishMu.RUnlock()
	defer cell.opMu.Unlock()
	c.mu.Lock()
	cell = c.cells[token.Identity.Normalize()]
	if cell == nil || cell.epoch != token.RuntimeEpoch || cell.generation != token.Generation || cell.execution == nil || cell.execution.loopDone != done {
		c.mu.Unlock()
		return nil
	}
	execution := cell.execution
	execution.fenced = true
	if execution.cancelGeneration != nil {
		execution.cancelGeneration()
	}
	var leasesDone <-chan struct{}
	if execution.leases > 0 {
		leasesDone = execution.leaseDrained
	}
	c.mu.Unlock()
	if leasesDone != nil {
		<-leasesDone
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cell = c.cells[token.Identity.Normalize()]
	if cell == nil || cell.execution != execution || cell.epoch != token.RuntimeEpoch || cell.generation != token.Generation {
		return nil
	}
	store := c.persistence()
	if execution.deferredTerminal != nil {
		pending := *execution.deferredTerminal
		result, err := c.commitDeferredAgentTerminationLocked(c.context(), cell, pending)
		if err != nil {
			cell.phase = AgentLifecycleFailed
			cell.runMode = AgentRunModeStopped
			cell.execution.loopCancel = nil
			cell.execution.loopDone = nil
			cell.execution.route = nil
			cell.execution.routeToken = runtimeeffects.LifecycleToken{}
			return fmt.Errorf("persist deferred agent termination: %w", err)
		}
		cell.epoch = result.RuntimeEpoch
		cell.generation = result.Generation
		cell.phase = result.Phase
		cell.runMode = result.RunMode
		cell.topology = result.Topology
		cell.processBinding = result.ProcessBinding
		execution.deferredTerminal = nil
	} else if cell.phase == AgentLifecycleRunning && store != nil {
		plan, planHash, err := normalizedLifecycleSubordinate(runtimesessions.LifecycleMutationPlan{})
		if err != nil {
			return err
		}
		_, err = store.CommitAgentLifecycleTransition(c.context(), AgentLifecycleTransition{
			OperationID: uuid.NewString(), OperationKind: "self_release",
			RequestHash: lifecycleRequestHashForIdentity(token.Identity, cell.topology, "self_release", cell.configRevision, planHash),
			Identity:    token.Identity,
			AgentID:     token.AgentID, Trigger: "self_release", ExpectedEpoch: cell.epoch, ExpectedGeneration: cell.generation,
			ExpectedPhase: cell.phase, TargetEpoch: cell.epoch, TargetGeneration: cell.generation,
			TargetPhase: AgentLifecycleRegistered, ConfigRevision: cell.configRevision, RunMode: AgentRunModeStopped, Subordinate: plan, Topology: cell.topology, Now: time.Now().UTC(),
		})
		if err != nil {
			cell.phase = AgentLifecycleFailed
			cell.runMode = AgentRunModeStopped
			cell.execution.loopCancel = nil
			cell.execution.loopDone = nil
			cell.execution.route = nil
			cell.execution.routeToken = runtimeeffects.LifecycleToken{}
			return fmt.Errorf("persist agent loop self-release: %w", err)
		}
		cell.phase = AgentLifecycleRegistered
		cell.runMode = AgentRunModeStopped
	} else if cell.phase == AgentLifecycleRunning {
		if c.sessions != nil {
			plan, planHash, err := normalizedLifecycleSubordinate(runtimesessions.LifecycleMutationPlan{})
			if err != nil {
				return err
			}
			operationID := uuid.NewString()
			if _, _, err := c.sessions.ApplyLifecycleProjection(c.context(), runtimesessions.LifecycleProjectionRequest{
				OperationID: operationID,
				RequestHash: lifecycleRequestHashForIdentity(token.Identity, cell.topology, "self_release", cell.configRevision, planHash),
				Expected:    token, Target: token, TargetPhase: string(AgentLifecycleRegistered), Plan: plan, Now: time.Now().UTC(),
			}); err != nil {
				return err
			}
		}
		cell.phase = AgentLifecycleRegistered
		cell.runMode = AgentRunModeStopped
	}
	cell.execution.loopCancel = nil
	cell.execution.loopDone = nil
	cell.execution.stopAfterAccepted = nil
	cell.execution.route = nil
	cell.execution.routeToken = runtimeeffects.LifecycleToken{}
	return nil
}

func (c *agentLifecycleCoordinator) commitDeferredAgentTerminationLocked(
	ctx context.Context,
	cell *agentLifecycleCell,
	pending deferredAgentTermination,
) (AgentLifecycleTransitionResult, error) {
	if cell == nil || cell.phase != AgentLifecycleRunning {
		return AgentLifecycleTransitionResult{}, errors.New("deferred agent termination requires the exact running lifecycle cell")
	}
	if err := pending.topology.Validate(); err != nil {
		return AgentLifecycleTransitionResult{}, fmt.Errorf("deferred agent termination topology: %w", err)
	}
	operationKind, err := lifecycleTerminationOperationKind(pending.target)
	if err != nil {
		return AgentLifecycleTransitionResult{}, err
	}
	plan, planHash, err := normalizedLifecycleSubordinate(runtimesessions.LifecycleMutationPlan{
		Action:            runtimesessions.LifecycleMutationTerminateCurrentSet,
		TerminationReason: runtimesessions.TerminationReasonNormal,
		TerminationDetail: pending.trigger,
	})
	if err != nil {
		return AgentLifecycleTransitionResult{}, err
	}
	nextEpoch, nextGeneration := runtimebus.CurrentRuntimeEpoch(), cell.generation+1
	operationID := uuid.NewString()
	store := c.persistence()
	if store != nil {
		targetBinding := cell.processBinding
		operationKind, targetBinding, err = lifecycleMutationExecutionAuthority(store, cell.processBinding, operationKind, true)
		if err != nil {
			return AgentLifecycleTransitionResult{}, err
		}
		requestHash := lifecycleRequestHashForIdentity(
			cell.identity, pending.topology, operationKind, pending.trigger, cell.configRevision, planHash,
			cell.processBinding.ProcessAuthorityID, cell.processBinding.ProcessBootID,
			targetBinding.ProcessAuthorityID, targetBinding.ProcessBootID, targetBinding.GenerationGrantID,
		)
		return store.CommitAgentLifecycleTransition(context.WithoutCancel(ctx), AgentLifecycleTransition{
			OperationID: operationID, OperationKind: operationKind, RequestHash: requestHash,
			Identity: cell.identity, AgentID: cell.identity.AgentID(), Trigger: pending.trigger,
			ExpectedEpoch: cell.epoch, ExpectedGeneration: cell.generation, ExpectedPhase: cell.phase,
			TargetEpoch: nextEpoch, TargetGeneration: nextGeneration, TargetPhase: pending.target,
			ConfigRevision: cell.configRevision, RunMode: AgentRunModeStopped, Subordinate: plan,
			Topology: pending.topology, Now: time.Now().UTC(),
		})
	}
	if c.sessions != nil {
		requestHash := lifecycleRequestHashForIdentity(cell.identity, pending.topology, pending.trigger, cell.configRevision, planHash)
		if _, _, err := c.sessions.ApplyLifecycleProjection(context.WithoutCancel(ctx), runtimesessions.LifecycleProjectionRequest{
			OperationID: operationID, RequestHash: requestHash,
			Expected:    lifecycleToken(cell.identity, cell.epoch, cell.generation),
			Target:      lifecycleToken(cell.identity, nextEpoch, nextGeneration),
			TargetPhase: string(pending.target), Plan: plan, Now: time.Now().UTC(),
		}); err != nil {
			return AgentLifecycleTransitionResult{}, err
		}
	}
	return AgentLifecycleTransitionResult{
		OperationID: operationID, Identity: cell.identity, AgentID: cell.identity.AgentID(),
		PreviousEpoch: cell.epoch, RuntimeEpoch: nextEpoch,
		PreviousGeneration: cell.generation, Generation: nextGeneration,
		PreviousPhase: cell.phase, Phase: pending.target,
		ConfigRevision: cell.configRevision, RunMode: AgentRunModeStopped,
		Topology: pending.topology, ProcessBinding: cell.processBinding,
	}, nil
}

func (c *agentLifecycleCoordinator) abortUnlaunchedLoopLocked(ctx context.Context, identity runtimeagentidentity.Identity, token runtimeeffects.LifecycleToken, done chan struct{}, lockedCell *agentLifecycleCell) error {
	if c == nil || lockedCell == nil || !token.Valid() {
		return errors.New("unlaunched agent execution requires lifecycle cell and token")
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return err
	}
	c.mu.Lock()
	cell := c.cells[identity]
	if cell == nil || cell != lockedCell || cell.identity != identity || cell.execution == nil || cell.execution.token != token {
		c.mu.Unlock()
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "agent-lifecycle", "abort_unlaunched_loop", map[string]any{"agent_identity": identity.Description()})
	}
	execution := cell.execution
	execution.fenced = true
	if execution.cancelGeneration != nil {
		execution.cancelGeneration()
	}
	plan, planHash, err := normalizedLifecycleSubordinate(runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		c.mu.Unlock()
		return err
	}
	operationID := uuid.NewString()
	requestHash := lifecycleRequestHashForIdentity(cell.identity, cell.topology, "start_failed", cell.configRevision, planHash)
	store := c.persistence()
	if store != nil {
		_, err = store.CommitAgentLifecycleTransition(context.WithoutCancel(ctx), AgentLifecycleTransition{
			OperationID: operationID, OperationKind: "start_failed", RequestHash: requestHash, Identity: cell.identity,
			AgentID: identity.AgentID(), Trigger: "start_failed", ExpectedEpoch: cell.epoch, ExpectedGeneration: cell.generation,
			ExpectedPhase: cell.phase, TargetEpoch: cell.epoch, TargetGeneration: cell.generation,
			TargetPhase: AgentLifecycleRegistered, ConfigRevision: cell.configRevision, RunMode: AgentRunModeStopped,
			Subordinate: plan, Topology: cell.topology, Now: time.Now().UTC(),
		})
	} else if c.sessions != nil {
		_, _, err = c.sessions.ApplyLifecycleProjection(context.WithoutCancel(ctx), runtimesessions.LifecycleProjectionRequest{
			OperationID: operationID, RequestHash: requestHash,
			Expected: token, Target: token, TargetPhase: string(AgentLifecycleRegistered), Plan: plan, Now: time.Now().UTC(),
		})
	}
	if err != nil {
		cell.phase = AgentLifecycleFailed
		cell.runMode = AgentRunModeStopped
	} else {
		cell.phase = AgentLifecycleRegistered
		cell.runMode = AgentRunModeStopped
	}
	execution.loopCancel = nil
	execution.loopDone = nil
	execution.route = nil
	execution.routeToken = runtimeeffects.LifecycleToken{}
	settled := execution.loopSettled
	execution.loopSettled = nil
	c.mu.Unlock()
	if done != nil {
		close(done)
	}
	if settled != nil {
		close(settled)
	}
	if err != nil {
		return fmt.Errorf("persist fail-closed unlaunched agent execution: %w", err)
	}
	return nil
}

func (c *agentLifecycleCoordinator) terminateIdentityWithTopology(
	ctx context.Context,
	identity runtimeagentidentity.Identity,
	trigger string,
	target AgentLifecyclePhase,
	topology *runtimeagenttopology.Admission,
) (models.AgentConfig, error) {
	return c.terminateIdentityWithTopologyExpected(ctx, identity, trigger, target, topology, nil, false)
}

func (c *agentLifecycleCoordinator) terminateIdentityWithTopologyExpected(
	ctx context.Context,
	identity runtimeagentidentity.Identity,
	trigger string,
	target AgentLifecyclePhase,
	topology *runtimeagenttopology.Admission,
	expected *models.AgentConfig,
	deferRouteRetirement bool,
) (models.AgentConfig, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return models.AgentConfig{}, err
	}
	agentID := identity.AgentID()
	cell, err := c.lockIdentityOperation(identity)
	if err != nil {
		return models.AgentConfig{}, err
	}
	defer cell.opMu.Unlock()
	c.mu.Lock()
	cell = c.cells[identity]
	if cell == nil {
		c.mu.Unlock()
		return models.AgentConfig{}, fmt.Errorf("%w: %s", ErrAgentNotFound, agentID)
	}
	epoch, generation, phase, revision := cell.epoch, cell.generation, cell.phase, cell.configRevision
	transitionTopology := cell.topology
	if topology != nil {
		transitionTopology = *topology
	}
	if err := transitionTopology.Validate(); err != nil {
		c.mu.Unlock()
		return models.AgentConfig{}, fmt.Errorf("agent lifecycle topology admission: %w", err)
	}
	execution := cell.execution
	var done, leasesDone <-chan struct{}
	var cancel context.CancelFunc
	var routeToken runtimeeffects.LifecycleToken
	if execution != nil {
		done = execution.loopDone
		cancel = execution.cancelGeneration
		routeToken = execution.routeToken
	}
	nextEpoch, nextGeneration := runtimebus.CurrentRuntimeEpoch(), generation+1
	plan, planHash, err := normalizedLifecycleSubordinate(runtimesessions.LifecycleMutationPlan{
		Action: runtimesessions.LifecycleMutationTerminateCurrentSet, TerminationReason: runtimesessions.TerminationReasonNormal,
		TerminationDetail: trigger,
	})
	if err != nil {
		c.mu.Unlock()
		return models.AgentConfig{}, err
	}
	operationID := uuid.NewString()
	now := time.Now().UTC()
	operationKind, err := lifecycleTerminationOperationKind(target)
	if err != nil {
		c.mu.Unlock()
		return models.AgentConfig{}, err
	}
	var previousConfig models.AgentConfig
	if execution != nil {
		previousConfig = snapshotExecution(execution).Config
	}
	if expected != nil {
		if execution == nil {
			c.mu.Unlock()
			return models.AgentConfig{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_generation_not_running", "agent-lifecycle", trigger, map[string]any{"agent_id": agentID})
		}
		expectedRevision, revisionErr := lifecycleConfigRevision(PersistedAgent{Config: *expected})
		if revisionErr != nil {
			c.mu.Unlock()
			return models.AgentConfig{}, fmt.Errorf("validate expected agent configuration: %w", revisionErr)
		}
		if expectedRevision != revision {
			c.mu.Unlock()
			return models.AgentConfig{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "agent_config_changed", "agent-lifecycle", trigger, map[string]any{"agent_id": agentID})
		}
	}
	if deferRouteRetirement && execution != nil {
		if target != AgentLifecycleTerminated || execution.loopDone == nil || execution.stopAfterAccepted == nil || !routeToken.Valid() {
			c.mu.Unlock()
			return models.AgentConfig{}, errors.New("deferred route retirement requires one running terminal agent execution")
		}
		if execution.deferredTerminal != nil {
			c.mu.Unlock()
			return models.AgentConfig{}, errors.New("agent execution already has a deferred terminal transition")
		}
		execution.fenced = true
		execution.deferredTerminal = &deferredAgentTermination{trigger: trigger, target: target, topology: transitionTopology}
		stopAfterAccepted := execution.stopAfterAccepted
		c.mu.Unlock()
		if c.routes != nil {
			c.routes.FenceAgentRoute(routeToken)
		}
		close(stopAfterAccepted)
		return previousConfig, nil
	}
	store := c.persistence()
	effectivePhase := target
	processBinding := cell.processBinding
	if store != nil {
		targetBinding := cell.processBinding
		operationKind, targetBinding, err = lifecycleMutationExecutionAuthority(store, cell.processBinding, operationKind, true)
		if err != nil {
			c.mu.Unlock()
			return models.AgentConfig{}, err
		}
		requestHash := lifecycleRequestHashForIdentity(
			identity, transitionTopology, operationKind, trigger, revision, planHash,
			cell.processBinding.ProcessAuthorityID, cell.processBinding.ProcessBootID,
			targetBinding.ProcessAuthorityID, targetBinding.ProcessBootID, targetBinding.GenerationGrantID,
		)
		result, err := store.CommitAgentLifecycleTransition(context.WithoutCancel(ctx), AgentLifecycleTransition{
			OperationID: operationID, OperationKind: operationKind, RequestHash: requestHash, Identity: identity,
			AgentID: agentID, Trigger: trigger, ExpectedEpoch: epoch, ExpectedGeneration: generation, ExpectedPhase: phase,
			TargetEpoch: nextEpoch, TargetGeneration: nextGeneration, TargetPhase: target,
			ConfigRevision: revision, RunMode: AgentRunModeStopped, Subordinate: plan,
			Topology: transitionTopology, Now: now,
		})
		if err != nil {
			c.mu.Unlock()
			return models.AgentConfig{}, err
		}
		nextEpoch, nextGeneration = result.RuntimeEpoch, result.Generation
		effectivePhase = result.Phase
		processBinding = result.ProcessBinding
	} else if c.sessions != nil {
		requestHash := lifecycleRequestHashForIdentity(identity, transitionTopology, trigger, revision, planHash)
		if _, _, err := c.sessions.ApplyLifecycleProjection(context.WithoutCancel(ctx), runtimesessions.LifecycleProjectionRequest{
			OperationID: operationID, RequestHash: requestHash,
			Expected:    lifecycleToken(identity, epoch, generation),
			Target:      lifecycleToken(identity, nextEpoch, nextGeneration),
			TargetPhase: string(target), Plan: plan, Now: now,
		}); err != nil {
			c.mu.Unlock()
			return models.AgentConfig{}, err
		}
	}
	cell.epoch, cell.generation, cell.phase, cell.runMode, cell.topology = nextEpoch, nextGeneration, effectivePhase, AgentRunModeStopped, transitionTopology
	cell.processBinding = processBinding
	if execution != nil {
		execution.fenced = true
		if execution.leases > 0 {
			leasesDone = execution.leaseDrained
		}
	}
	if cancel != nil {
		cancel()
	}
	c.mu.Unlock()
	if c.routes != nil && routeToken.Valid() {
		c.routes.FenceAgentRoute(routeToken)
		if deferRouteRetirement {
			// The current delivery owns a lease on this route. Fence admission
			// now, then let releaseLoop retire it after the delivery settles.
			return previousConfig, nil
		}
		c.routes.RemoveAgentRoute(routeToken)
	}
	if done != nil {
		<-done
	}
	if leasesDone != nil {
		<-leasesDone
	}
	c.mu.Lock()
	if current := c.cells[identity]; current == cell && current.execution == execution && (current.phase == target || current.phase == AgentLifecycleDraining) {
		current.execution = nil
	}
	c.mu.Unlock()
	return previousConfig, nil
}

func (c *agentLifecycleCoordinator) observeProviderDrainFinalization(finalization runtimeeffects.ProviderDrainFinalization) {
	if c == nil || !finalization.Token.Valid() || !finalization.Target.Valid() {
		return
	}
	identity := finalization.Token.Identity.Normalize()
	c.mu.Lock()
	defer c.mu.Unlock()
	cell := c.cells[identity]
	if cell == nil || cell.epoch != finalization.Token.RuntimeEpoch || cell.generation != finalization.Token.Generation || cell.phase != AgentLifecycleDraining {
		return
	}
	cell.phase = AgentLifecyclePhase(finalization.Target)
}

func (c *agentLifecycleCoordinator) refreshRecoveredProviderDrainFinalizations(ctx context.Context) error {
	if c == nil || c.stateReader == nil {
		return nil
	}
	c.mu.Lock()
	identities := make([]runtimeagentidentity.Identity, 0)
	for identity, cell := range c.cells {
		if cell != nil && cell.phase == AgentLifecycleDraining {
			identities = append(identities, identity)
		}
	}
	c.mu.Unlock()
	for _, identity := range identities {
		state, found, err := c.stateReader.LoadAgentLifecycleState(ctx, identity)
		if err != nil {
			return fmt.Errorf("refresh recovered provider-drain lifecycle %s: %w", identity.Description(), err)
		}
		if !found {
			return fmt.Errorf("refresh recovered provider-drain lifecycle %s: durable cell is absent", identity.Description())
		}
		c.mu.Lock()
		cell := c.cells[identity.Normalize()]
		if cell != nil && cell.phase == AgentLifecycleDraining {
			if cell.epoch != state.RuntimeEpoch || cell.generation != state.Generation {
				c.mu.Unlock()
				return fmt.Errorf("refresh recovered provider-drain lifecycle %s: durable generation changed", identity.Description())
			}
			switch state.Phase {
			case AgentLifecycleDraining:
			case AgentLifecycleTerminated, AgentLifecycleFailed:
				cell.phase = state.Phase
			default:
				c.mu.Unlock()
				return fmt.Errorf("refresh recovered provider-drain lifecycle %s: invalid durable phase %q", identity.Description(), state.Phase)
			}
		}
		c.mu.Unlock()
	}
	return nil
}

func lifecycleTerminationOperationKind(target AgentLifecyclePhase) (string, error) {
	switch target {
	case AgentLifecycleTerminated:
		return "teardown", nil
	case AgentLifecycleFailed:
		return "fail", nil
	default:
		return "", fmt.Errorf("lifecycle target phase %q is not terminal", target)
	}
}

func (c *agentLifecycleCoordinator) tokenIdentity(identity runtimeagentidentity.Identity) (runtimeeffects.LifecycleToken, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	identity = identity.Normalize()
	cell := c.cells[identity]
	if cell == nil || cell.phase != AgentLifecycleRunning {
		return runtimeeffects.LifecycleToken{}, false
	}
	return lifecycleToken(identity, cell.epoch, cell.generation), true
}
