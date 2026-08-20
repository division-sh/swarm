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
	execution      *agentExecutionProjection
}

type agentExecutionProjection struct {
	agent            Agent
	config           models.AgentConfig
	subscriptions    []events.EventType
	admission        semanticview.FlowOwnedAgentSubscriptionAdmission
	startedAt        time.Time
	token            runtimeeffects.LifecycleToken
	standingOwner    *worklifetime.StandingOccurrence
	generationCtx    context.Context
	cancelGeneration context.CancelFunc
	loopCancel       context.CancelFunc
	loopDone         chan struct{}
	loopSettled      chan struct{}
	route            <-chan *worklifetime.EventDelivery
	routeToken       runtimeeffects.LifecycleToken
	fenced           bool
	leases           int
	leaseDrained     chan struct{}
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
	return AgentLifecycleState{
		Identity: identity, AgentID: identity.AgentID(), RuntimeEpoch: cell.epoch,
		Generation: cell.generation, Phase: cell.phase, ConfigRevision: cell.configRevision,
		RunMode: cell.runMode, Topology: cell.topology,
	}, true
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
	mu                 sync.Mutex
	storeMu            sync.RWMutex
	workMu             sync.Mutex
	executionPublishMu sync.Mutex
	store              AgentLifecyclePersistence
	stateReader        AgentLifecycleStateReader
	effectsStore       runtimeeffects.Store
	sessions           runtimesessions.LifecycleProjection
	phase              runtimeLifecyclePhase
	runMode            AgentRunMode
	runCtx             context.Context
	baseContext        context.Context
	cancelRun          context.CancelFunc
	runParentContext   context.Context
	runParent          worklifetime.Occurrence
	runOwner           *worklifetime.ManagerRunOccurrence
	transitionExecutor *worklifetime.Lease
	runGeneration      uint64
	workRetiring       bool
	watcherExpected    bool
	transition         *runtimeLifecycleTransition
	pendingReset       *runtimeLifecycleTransition
	retryDone          <-chan struct{}
	cells              map[runtimeagentidentity.Identity]*agentLifecycleCell
	routes             AgentRouteBus
	executionPosture   executionposture.Posture
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
	agentID string,
	flowInstance string,
	includeTerminated bool,
) (runtimeagentidentity.Identity, *agentLifecycleCell, error) {
	agentID = strings.TrimSpace(agentID)
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if agentID == "" {
		return runtimeagentidentity.Identity{}, nil, fmt.Errorf("agent_id is required")
	}
	var matched runtimeagentidentity.Identity
	var matchedCell *agentLifecycleCell
	candidates := make([]runtimeagentidentity.Identity, 0, 2)
	for identity, cell := range c.cells {
		if !identity.MatchesAgentID(agentID) || cell == nil {
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
			"agent_id %q is ambiguous across multiple live flow instances; provide agent_id and flow_instance; candidates: %s",
			agentID,
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

func (c *agentLifecycleCoordinator) resolveAgentTarget(agentID, flowInstance string, includeTerminated bool) (runtimeagentidentity.Identity, error) {
	if c == nil {
		return runtimeagentidentity.Identity{}, fmt.Errorf("%w: %s", ErrAgentNotFound, strings.TrimSpace(agentID))
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	identity, _, err := c.resolveAgentTargetLocked(agentID, flowInstance, includeTerminated)
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
	raw, err := canonicaljson.Bytes(rec.Config)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
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

func lifecycleReconfigureOperationID(identity runtimeagentidentity.Identity, epoch int64, generation uint64, phase AgentLifecyclePhase, revision, planIdentity string, topology runtimeagenttopology.Admission) string {
	fingerprint, _ := identity.Fingerprint()
	parts := []string{
		"agent-lifecycle-reconfigure-occurrence-v1",
		fingerprint,
		strconv.FormatInt(epoch, 10),
		strconv.FormatUint(generation, 10),
		string(phase),
		strings.TrimSpace(revision),
		planIdentity,
	}
	parts = append(parts, lifecycleTopologyIdentity(topology))
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join(parts, "\x00"))).String()
}

func lifecycleReintroductionOperationID(identity runtimeagentidentity.Identity, epoch int64, generation uint64, phase AgentLifecyclePhase, revision, planIdentity string, topology runtimeagenttopology.Admission) string {
	fingerprint, _ := identity.Fingerprint()
	parts := []string{
		"agent-lifecycle-reintroduction-occurrence-v1",
		fingerprint,
		strconv.FormatInt(epoch, 10),
		strconv.FormatUint(generation, 10),
		string(phase),
		strings.TrimSpace(revision),
		planIdentity,
	}
	parts = append(parts, lifecycleTopologyIdentity(topology))
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.Join(parts, "\x00"))).String()
}

func (c *agentLifecycleCoordinator) register(ctx context.Context, rec PersistedAgent, persist bool) error {
	admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(nil, semanticview.FlowOwnedAgentSubscriptionRequest{
		AgentID: rec.Config.ID, FlowID: rec.Config.FlowID, FlowPath: rec.Config.CanonicalFlowPath(), Subscriptions: rec.Config.Subscriptions,
	})
	if err != nil {
		return err
	}
	return c.registerExecution(ctx, rec, persist, nil, admission)
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
	existingCell := c.cells[identity]
	if existingCell != nil && existingCell.phase != AgentLifecycleTerminated {
		return fmt.Errorf("%w: %s", ErrAgentAlreadyExists, agentID)
	}
	epoch := rec.LifecycleEpoch
	generation := rec.LifecycleGeneration
	phase := rec.LifecyclePhase
	mode := rec.LifecycleRunMode
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
			operationID = lifecycleReintroductionOperationID(
				identity,
				previous.RuntimeEpoch,
				previous.Generation,
				previous.Phase,
				revision,
				planHash,
				topology,
			)
			epoch = runtimebus.CurrentRuntimeEpoch()
			generation = previous.Generation + 1
			requestHash = lifecycleRequestHashForIdentity(identity, topology, "restart", revision, planHash)
			transition = AgentLifecycleTransition{
				OperationID: operationID, OperationKind: "restart", Identity: identity, AgentID: agentID, Trigger: "restart",
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
		configRevision: revision, runMode: mode, topology: topology, execution: execution,
	}
	return nil
}

func (c *agentLifecycleCoordinator) persistRegistration(ctx context.Context, rec PersistedAgent) (AgentLifecycleTransitionResult, error) {
	return c.persistRegistrationWithTopology(ctx, rec, rec.Topology)
}

func (c *agentLifecycleCoordinator) persistRegistrationWithTopology(
	ctx context.Context,
	rec PersistedAgent,
	topology runtimeagenttopology.Admission,
) (AgentLifecycleTransitionResult, error) {
	store := c.persistence()
	if c == nil || store == nil {
		return AgentLifecycleTransitionResult{}, fmt.Errorf("agent lifecycle persistence is required")
	}
	if err := topology.Validate(); err != nil {
		return AgentLifecycleTransitionResult{}, fmt.Errorf("agent lifecycle topology admission: %w", err)
	}
	rec.Topology = topology
	agentID := strings.TrimSpace(rec.Config.ID)
	revision, err := lifecycleConfigRevision(rec)
	if err != nil {
		return AgentLifecycleTransitionResult{}, err
	}
	epoch := rec.LifecycleEpoch
	if epoch <= 0 {
		epoch = runtimebus.CurrentRuntimeEpoch()
	}
	generation := rec.LifecycleGeneration
	if generation == 0 {
		generation = 1
	}
	plan, planHash, err := normalizedLifecycleSubordinate(runtimesessions.LifecycleMutationPlan{})
	if err != nil {
		return AgentLifecycleTransitionResult{}, err
	}
	identity, err := rec.Config.ConcreteIdentity()
	if err != nil {
		return AgentLifecycleTransitionResult{}, err
	}
	previous, terminated, err := c.terminatedLifecycleState(ctx, identity, nil)
	if err != nil {
		return AgentLifecycleTransitionResult{}, err
	}
	if terminated {
		operationID := lifecycleReintroductionOperationID(
			identity,
			previous.RuntimeEpoch,
			previous.Generation,
			previous.Phase,
			revision,
			planHash,
			topology,
		)
		return store.CommitAgentLifecycleTransition(ctx, AgentLifecycleTransition{
			OperationID: operationID, OperationKind: "restart", Identity: identity, AgentID: agentID, Trigger: "restart",
			RequestHash:   lifecycleRequestHashForIdentity(identity, topology, "restart", revision, planHash),
			ExpectedEpoch: previous.RuntimeEpoch, ExpectedGeneration: previous.Generation, ExpectedPhase: previous.Phase,
			TargetEpoch: runtimebus.CurrentRuntimeEpoch(), TargetGeneration: previous.Generation + 1,
			TargetPhase: AgentLifecycleRegistered, ConfigRevision: revision, RunMode: AgentRunModeStopped,
			Agent: &rec, Subordinate: plan, Topology: topology, Now: time.Now().UTC(),
		})
	}
	return store.CommitAgentLifecycleTransition(ctx, AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "spawn", Identity: identity, AgentID: agentID, Trigger: "spawn",
		RequestHash: lifecycleRequestHashForIdentity(identity, topology, "spawn", revision, planHash), TargetEpoch: epoch,
		TargetGeneration: generation, TargetPhase: AgentLifecycleRegistered,
		ConfigRevision: revision, RunMode: AgentRunModeStopped, Agent: &rec, Subordinate: plan,
		Topology: topology, Now: time.Now().UTC(),
	})
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

func (c *agentLifecycleCoordinator) unregisterIdentity(identity runtimeagentidentity.Identity) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.cells, identity.Normalize())
	c.mu.Unlock()
}

func (c *agentLifecycleCoordinator) beginRun(parent context.Context, mode AgentRunMode, owner worklifetime.Occurrence) (context.Context, bool, error) {
	c.executionPublishMu.Lock()
	defer c.executionPublishMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
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
	routeTokens := make([]runtimeeffects.LifecycleToken, 0, len(c.cells))
	for _, cell := range c.cells {
		execution := cell.execution
		if execution == nil {
			continue
		}
		execution.fenced = true
		if execution.cancelGeneration != nil {
			execution.cancelGeneration()
		}
		if c.routes != nil && execution.routeToken.Valid() {
			routeTokens = append(routeTokens, execution.routeToken)
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
	for _, token := range routeTokens {
		c.routes.RemoveAgentRoute(token)
	}
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
	return c.lockIdentityOperationWithFailed(identity, false)
}

// lockIdentityTopologyOperation admits failed durable cells because topology
// rebinding does not make them executable. Ordinary lifecycle operations keep
// treating failed cells as terminal until startup explicitly reintroduces a
// still-declared identity as a fresh registered generation.
func (c *agentLifecycleCoordinator) lockIdentityTopologyOperation(identity runtimeagentidentity.Identity) (*agentLifecycleCell, error) {
	return c.lockIdentityOperationWithFailed(identity, true)
}

func (c *agentLifecycleCoordinator) lockIdentityOperationWithFailed(identity runtimeagentidentity.Identity, includeFailed bool) (*agentLifecycleCell, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	cell := c.cells[identity]
	c.mu.Unlock()
	if cell == nil {
		return nil, fmt.Errorf("%w: %s", ErrAgentNotFound, identity.Description())
	}
	cell.opMu.Lock()
	c.mu.Lock()
	current := c.cells[identity]
	valid := current == cell && current.phase != AgentLifecycleDraining && current.phase != AgentLifecycleTerminated && (includeFailed || current.phase != AgentLifecycleFailed)
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
	return c.acquireExecutionLocked(ctx, cell, identity.Description(), purpose, requireRunning)
}

func (c *agentLifecycleCoordinator) acquireExecutionLocked(ctx context.Context, cell *agentLifecycleCell, target, purpose string, requireRunning bool) (*agentExecutionLease, error) {
	defer cell.opMu.Unlock()
	c.mu.Lock()
	execution := cell.execution
	running := cell.phase == AgentLifecycleRunning
	if execution == nil || execution.agent == nil || execution.fenced || (requireRunning && !running) {
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
	if operationID == "" {
		if trigger == "reconfigure" {
			operationID = lifecycleReconfigureOperationID(identity, previousEpoch, previousGeneration, previousPhase, revision, planHash, transitionTopology)
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
	requestHash := lifecycleRequestHashForIdentity(identity, transitionTopology, trigger, revision, planHash)
	result := AgentLifecycleTransitionResult{
		OperationID: operationID, Identity: identity, AgentID: agentID,
		PreviousEpoch: previousEpoch, RuntimeEpoch: nextEpoch,
		PreviousGeneration: previousGeneration, Generation: nextGeneration,
		PreviousPhase: previousPhase, Phase: targetPhase, ConfigRevision: revision, RunMode: targetMode,
		Subordinate: runtimesessions.LifecycleMutationOutcome{Action: plan.Action},
	}
	store := c.persistence()
	if store != nil {
		var err error
		result, err = store.CommitAgentLifecycleTransition(context.WithoutCancel(ctx), AgentLifecycleTransition{
			OperationID: operationID, OperationKind: trigger, RequestHash: requestHash, Identity: identity,
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
	nextExecution.loopCancel, nextExecution.loopDone = cancelGeneration, done
	nextExecution.loopSettled = settled
	return loopCtx, token, done, nil
}

func (c *agentLifecycleCoordinator) releaseLoop(token runtimeeffects.LifecycleToken, done chan struct{}) error {
	if c == nil {
		return nil
	}
	if c.routes != nil {
		c.routes.RemoveAgentRoute(token)
	}
	close(done)
	c.mu.Lock()
	cell := c.cells[token.Identity.Normalize()]
	c.mu.Unlock()
	if cell == nil {
		return nil
	}
	cell.opMu.Lock()
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
	if cell.phase == AgentLifecycleRunning && store != nil {
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
	cell.execution.route = nil
	cell.execution.routeToken = runtimeeffects.LifecycleToken{}
	return nil
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
	c.mu.Lock()
	cell := c.cells[identity]
	if cell == nil || cell.phase == AgentLifecycleDraining || cell.phase == AgentLifecycleTerminated || cell.phase == AgentLifecycleFailed {
		c.mu.Unlock()
		return models.AgentConfig{}, fmt.Errorf("%w: %s", ErrAgentNotFound, agentID)
	}
	c.mu.Unlock()
	cell.opMu.Lock()
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
	requestHash := lifecycleRequestHashForIdentity(identity, transitionTopology, trigger, revision, planHash)
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
	store := c.persistence()
	effectivePhase := target
	if store != nil {
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
	} else if c.sessions != nil {
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
