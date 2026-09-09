package manager

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

type preparedFlowInstanceDeactivation struct {
	mu       sync.Mutex
	consumed bool
	manager  *AgentManager
	lease    *worklifetime.Lease
	request  runtimepipeline.FlowInstanceDeactivationRequest
}

type preparedFlowTopologyRetirement struct {
	manager     *AgentManager
	lease       *worklifetime.Lease
	transferred bool
}

func (p *preparedFlowTopologyRetirement) abort() error {
	if p.transferred {
		return nil
	}
	return p.lease.Done()
}

func (p *preparedFlowTopologyRetirement) retire(flow runtimeflowidentity.RunScopedFlowInstance) error {
	if p.transferred {
		return errors.New("readiness topology retirement already transferred")
	}
	if err := flow.Validate(); err != nil {
		return err
	}
	am := p.manager
	plan := terminalFlowInstanceSideEffectPlan{RunID: flow.RunID, FlowPath: flow.Route.InstancePath}
	set, err := am.lifecycle.fenceTerminalFlow(plan)
	if err != nil && set == nil {
		return err
	}
	retireErr := errors.Join(err, am.retirePublishedDynamicFlowRoute(flow))
	if set == nil {
		return retireErr
	}
	set.trigger = "teardown"
	retireErr = errors.Join(retireErr, am.lifecycle.commitTerminalFlow(context.WithoutCancel(p.lease.Context()), set))
	p.transferred = true
	am.launchTerminalFlowCompletion(p.lease, set, retireErr)
	return retireErr
}

func (am *AgentManager) PrepareFlowInstanceDeactivation(ctx context.Context, req runtimepipeline.FlowInstanceDeactivationRequest) (runtimepipeline.PreparedFlowInstanceDeactivation, error) {
	if am == nil {
		return nil, errors.New("agent manager is required")
	}
	lease, err := am.beginWork(ctx, "prepared flow terminal completion")
	if err != nil {
		return nil, err
	}
	return &preparedFlowInstanceDeactivation{manager: am, lease: lease, request: req}, nil
}

func (p *preparedFlowInstanceDeactivation) Commit() error {
	p.mu.Lock()
	if p.consumed {
		p.mu.Unlock()
		return errors.New("flow terminal completion already consumed")
	}
	p.consumed = true
	p.mu.Unlock()
	return p.manager.deactivateFlowInstanceOwned(p.lease, p.request)
}

func (p *preparedFlowInstanceDeactivation) Abort() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.consumed {
		return nil
	}
	p.consumed = true
	return p.lease.Done()
}

// agentRetirement retains serialization authority without retaining opMu.
// The exact predecessor finalizer may acquire opMu while its join is pending.
type agentRetirement struct {
	cell      *agentLifecycleCell
	execution *agentExecutionProjection
	token     runtimeeffects.LifecycleToken
	done      <-chan struct{}
	settled   <-chan struct{}
	leases    <-chan struct{}
	failure   error
}

type agentTerminalCommit struct {
	config     models.AgentConfig
	retirement *agentRetirement
}

type agentTerminalDisposition uint8

const (
	terminalFenceAndSettle agentTerminalDisposition = iota + 1
	terminalSelfAuthor
	terminalJoinExisting
)

type terminalFlowMember struct {
	cell              *agentLifecycleCell
	token             runtimeeffects.LifecycleToken
	disposition       agentTerminalDisposition
	pendingTransition bool
}

type terminalFlowRetirement struct {
	runID       string
	flowPath    string
	trigger     string
	members     []terminalFlowMember
	retirements []*agentRetirement
}

func (c *agentLifecycleCoordinator) fenceTerminalFlow(plan terminalFlowInstanceSideEffectPlan) (*terminalFlowRetirement, error) {
	c.mu.Lock()
	set := &terminalFlowRetirement{runID: plan.RunID, flowPath: plan.FlowPath, trigger: "flow_instance_terminal"}
	identities := make([]runtimeagentidentity.Identity, 0)
	for identity, cell := range c.cells {
		if identity.RunID == plan.RunID && identity.FlowInstance() == plan.FlowPath && cell != nil && cell.execution != nil {
			identities = append(identities, identity)
		}
	}
	sort.Slice(identities, func(i, j int) bool { return runtimeagentidentity.Less(identities[i], identities[j]) })
	for _, identity := range identities {
		cell := c.cells[identity.Normalize()]
		if cell == nil {
			continue
		}
		if cell.terminalSet != nil && cell.terminalSet.runID == plan.RunID && cell.terminalSet.flowPath == plan.FlowPath {
			// The first request retained the complete exact set. A duplicate
			// cannot become a second finisher or reinterpret successor identities.
			c.mu.Unlock()
			return nil, nil
		}
		if cell.execution == nil || (cell.phase == AgentLifecycleTerminated && cell.retirement == nil) {
			continue
		}
		if cell.terminalSet != nil {
			c.mu.Unlock()
			return nil, fmt.Errorf("terminal flow %s has a pending exact agent transition: %s", plan.FlowPath, identity.Description())
		}
		self, _ := runtimeagentidentity.Equal(identity, plan.SelfRetiringAgent)
		disposition := terminalFenceAndSettle
		if self {
			disposition = terminalSelfAuthor
		}
		if cell.retirement != nil && (cell.phase == AgentLifecycleTerminated || cell.phase == AgentLifecycleDraining || cell.phase == AgentLifecycleFailed) {
			disposition = terminalJoinExisting
		}
		set.members = append(set.members, terminalFlowMember{cell: cell, token: cell.execution.token, disposition: disposition, pendingTransition: cell.retirement != nil})
	}
	for _, member := range set.members {
		member.cell.terminalSet = set
		member.cell.execution.fenced = true
	}
	c.mu.Unlock()
	var fenceErr error
	for _, member := range set.members {
		fenceErr = errors.Join(fenceErr, c.fenceRetiredAgentRoute(member.token))
	}
	return set, fenceErr
}

func (c *agentLifecycleCoordinator) fenceRetiredAgentRoute(token runtimeeffects.LifecycleToken) (result error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = fmt.Errorf("fence exact agent route %s: %v", token.Identity.Description(), recovered)
		}
	}()
	if c.routes != nil && token.Valid() {
		c.routes.FenceAgentRoute(token)
	}
	return result
}

func (c *agentLifecycleCoordinator) commitTerminalFlow(ctx context.Context, set *terminalFlowRetirement) error {
	var result error
	for _, member := range set.members {
		committed, err := c.commitTerminalFlowMember(ctx, set, member)
		set.retirements = append(set.retirements, committed.retirement)
		result = errors.Join(result, err)
	}
	return result
}

func (c *agentLifecycleCoordinator) commitTerminalFlowMember(ctx context.Context, set *terminalFlowRetirement, member terminalFlowMember) (committed agentTerminalCommit, result error) {
	cell := member.cell
	cell.opMu.Lock()
	defer cell.opMu.Unlock()
	defer func() {
		if recovered := recover(); recovered != nil {
			result = fmt.Errorf("terminal disposition panic for %s: %v", cell.identity.Description(), recovered)
		}
		if result == nil {
			return
		}
		// Durable flow termination already owns the full set. Even an
		// ambiguous member commit must retain cleanup and fence its suffix.
		c.mu.Lock()
		defer c.mu.Unlock()
		if cell.execution != nil && cell.execution.token == member.token {
			committed.retirement = retainAgentRetirement(cell, cell.execution)
			if cell.execution.cancelGeneration != nil {
				cell.execution.cancelGeneration()
			}
		}
	}()
	c.mu.Lock()
	exact := cell.terminalSet == set && cell.execution != nil && cell.execution.token == member.token
	c.mu.Unlock()
	if !exact {
		return committed, errors.New("terminal flow predecessor changed after fencing")
	}
	if member.disposition == terminalJoinExisting {
		c.mu.Lock()
		defer c.mu.Unlock()
		if cell.retirement == nil || cell.retirement.execution.token != member.token {
			return committed, errors.New("terminal flow lost retained predecessor retirement")
		}
		return agentTerminalCommit{retirement: cell.retirement}, nil
	}
	if member.pendingTransition {
		return committed, fmt.Errorf("terminal flow fenced pending predecessor transition for %s", cell.identity.Description())
	}
	return c.commitIdentityTerminationLocked(ctx, cell, cell.identity, set.trigger, AgentLifecycleTerminated, nil, nil, member.disposition)
}

func (am *AgentManager) launchTerminalFlowCompletion(lease *worklifetime.Lease, set *terminalFlowRetirement, commitErr error) {
	go func() {
		result := commitErr
		var diagnosticErr error
		defer func() {
			if recovered := recover(); recovered != nil {
				result = errors.Join(result, fmt.Errorf("terminal flow completion panic: %v", recovered))
			}
			am.lifecycle.recordTerminalCompletion(result)
			am.lifecycle.recordLifecycleDiagnosticFailure(diagnosticErr)
			am.lifecycle.recordTerminalCompletion(lease.Done())
		}()
		result = errors.Join(result, am.completeTerminalRetirements(context.WithoutCancel(lease.Context()), set.retirements))
		if result == nil {
			am.lifecycle.mu.Lock()
			for _, member := range set.members {
				if member.cell.terminalSet == set {
					member.cell.terminalSet = nil
				}
			}
			am.lifecycle.mu.Unlock()
		}
		diagnosticErr = am.projectLifecycleDiagnostics(context.WithoutCancel(lease.Context()))
	}()
}

func (am *AgentManager) requestPanicRetirement(ctx context.Context, identity runtimeagentidentity.Identity) error {
	lease, err := am.beginWork(ctx, "panic terminal completion")
	if err != nil {
		return err
	}
	token, ok := runtimeeffects.LifecycleTokenFromContext(ctx)
	if !ok || !token.Valid() || token.Identity != identity.Normalize() {
		return errors.Join(errors.New("panic retirement requires the exact originating lifecycle token"), lease.Done())
	}
	c := am.lifecycle
	cell, err := c.lockIdentityOperation(identity)
	if err != nil {
		return errors.Join(err, lease.Done())
	}
	c.mu.Lock()
	execution := cell.execution
	exact := execution != nil && execution.token == token && cell.epoch == token.RuntimeEpoch && cell.generation == token.Generation
	c.mu.Unlock()
	if !exact {
		cell.opMu.Unlock()
		return errors.Join(errors.New("panic retirement origin no longer owns the exact execution"), lease.Done())
	}
	committed, err := c.commitIdentityTerminationLocked(ctx, cell, identity, "agent_loop_panic_threshold", AgentLifecycleFailed, nil, nil, terminalFenceAndSettle)
	if err != nil && committed.retirement == nil {
		// Failure to persist is not permission to readmit a panicking loop.
		// Keep its exact predecessor fenced and the error owned through join.
		c.mu.Lock()
		execution.fenced = true
		committed.retirement = retainAgentRetirement(cell, execution)
		if execution.cancelGeneration != nil {
			execution.cancelGeneration()
		}
		committed.retirement.failure = err
		c.mu.Unlock()
		err = errors.Join(err, c.fenceRetiredAgentRoute(token))
	}
	cell.opMu.Unlock()
	am.launchTerminalFlowCompletion(lease, &terminalFlowRetirement{retirements: []*agentRetirement{committed.retirement}}, err)
	return err
}

func retainAgentRetirement(cell *agentLifecycleCell, execution *agentExecutionProjection) *agentRetirement {
	if execution == nil {
		return nil
	}
	if cell.retirement != nil && cell.retirement.execution == execution {
		return cell.retirement
	}
	retirement := &agentRetirement{
		cell: cell, execution: execution, token: execution.routeToken,
		done: execution.loopDone, settled: execution.loopSettled,
	}
	if execution.leases > 0 {
		retirement.leases = execution.leaseDrained
	}
	cell.retirement = retirement
	return retirement
}

func (c *agentLifecycleCoordinator) joinAgentRetirement(retirement *agentRetirement) error {
	if retirement == nil {
		return nil
	}
	// The loop settles its accepted carrier before removing its route. In
	// particular, retiring the self-author's route here would cancel that
	// carrier before its final delivery receipt could commit.
	if retirement.done != nil {
		<-retirement.done
	}
	if retirement.leases != nil {
		<-retirement.leases
	}
	routeErr := c.removeRetiredAgentRoute(retirement.token)
	if retirement.settled != nil {
		<-retirement.settled
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return errors.Join(routeErr, retirement.failure, retirement.execution.settlementErr)
}

func (c *agentLifecycleCoordinator) removeRetiredAgentRoute(token runtimeeffects.LifecycleToken) (result error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = fmt.Errorf("retire exact agent route %s: %v", token.Identity.Description(), recovered)
		}
	}()
	if c.routes != nil && token.Valid() {
		c.routes.RemoveAgentRoute(token)
	}
	return result
}

func (c *agentLifecycleCoordinator) finishAgentRetirement(retirement *agentRetirement) {
	if retirement == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cell := retirement.cell
	if cell.retirement != retirement {
		return
	}
	if cell.execution == retirement.execution {
		cell.execution = nil
	}
	cell.retirement = nil
}

// completeTerminalRetirements is called only by a separately admitted Manager
// operation. It never owns a carrier or execution lease of the routes it joins.
func (am *AgentManager) completeTerminalRetirements(ctx context.Context, retirements []*agentRetirement) error {
	var result error
	for _, retirement := range retirements {
		err := am.lifecycle.joinAgentRetirement(retirement)
		if err == nil {
			am.lifecycle.finishAgentRetirement(retirement)
		}
		result = errors.Join(result, err)
	}
	return result
}

func (c *agentLifecycleCoordinator) recordLifecycleDiagnosticFailure(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.terminalErr = errors.Join(c.terminalErr, fmt.Errorf("lifecycle diagnostic projection: %w", err))
}

func (c *agentLifecycleCoordinator) recordTerminalCompletion(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.terminalErr = errors.Join(c.terminalErr, fmt.Errorf("terminal retirement: %w", err))
}
