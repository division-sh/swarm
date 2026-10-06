package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type attachmentAgentAcquisitionRoutes struct {
	*attachmentAgentRouteProbe
	resource      string
	after         bool
	disposition   string
	enabled       atomic.Bool
	reached       atomic.Bool
	published     atomic.Int32
	flowPublished atomic.Int32
	discarded     atomic.Int32
	cancel        context.CancelFunc
	fault         error
}

func (r *attachmentAgentAcquisitionRoutes) PrepareAgentRoute(token effects.LifecycleToken, admission semanticview.FlowOwnedAgentSubscriptionAdmission) bus.AgentRoutePreparation {
	return &attachmentAgentAcquisitionPublication{AgentRoutePreparation: r.attachmentAgentRouteProbe.PrepareAgentRoute(token, admission), owner: r}
}

type attachmentAgentAcquisitionPublication struct {
	bus.AgentRoutePreparation
	owner *attachmentAgentAcquisitionRoutes
}

func (p *attachmentAgentAcquisitionPublication) Publish() error {
	if err := p.owner.inject("agent_route", false); err != nil {
		return err
	}
	if err := p.AgentRoutePreparation.Publish(); err != nil {
		return err
	}
	p.owner.published.Add(1)
	return p.owner.inject("agent_route", true)
}

func (r *attachmentAgentAcquisitionRoutes) inject(resource string, after bool) error {
	if r.resource != resource || r.after != after || !r.enabled.CompareAndSwap(true, false) {
		return nil
	}
	r.reached.Store(true)
	switch r.disposition {
	case "panic":
		panic(r.fault)
	case "caller_cancel":
		r.cancel()
		return nil
	default:
		return r.fault
	}
}

func (r *attachmentAgentAcquisitionRoutes) PublishPersistedFlowInstanceRouteForAttempt(ctx context.Context, req bus.FlowInstanceRouteMaterializationRequest, attempt pipeline.DynamicFlowRuntimeActivationAttempt) (bus.FlowRoutePublicationHandle, error) {
	if err := r.inject("flow_route", false); err != nil {
		return nil, err
	}
	publication, err := r.sqliteFlowActivationBus.PublishPersistedFlowInstanceRouteForAttempt(ctx, req, attempt)
	if err != nil {
		return publication, err
	}
	r.flowPublished.Add(1)
	return publication, r.inject("flow_route", true)
}

func (p *attachmentAgentAcquisitionPublication) Discard() error {
	p.owner.discarded.Add(1)
	return p.AgentRoutePreparation.Discard()
}

func TestFlowAttachmentAgentAcquisitionCutsBothStores(t *testing.T) {
	runFlowAttachmentAcquisitionCuts(t, "agent_route")
}

func TestFlowAttachmentRouteAcquisitionCutsBothStores(t *testing.T) {
	runFlowAttachmentAcquisitionCuts(t, "flow_route")
}

func runFlowAttachmentAcquisitionCuts(t *testing.T, resource string) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"before_publish", "after_publish"} {
			for _, disposition := range []string{"error", "panic", "caller_cancel"} {
				t.Run(backend+"/"+cut+"/"+disposition, func(t *testing.T) {
					var terminalEvidence error
					workflow := &attachmentPhaseFaultWorkflow{retryRelease: make(chan struct{}), abandoned: make(chan error, 2), ready: make(chan pipeline.DynamicFlowRuntimeActivationAttempt, 2)}
					probe := &attachmentAgentRouteProbe{prepared: make(map[effects.LifecycleToken]struct{}), fenced: make(map[effects.LifecycleToken]struct{}), removed: make(map[effects.LifecycleToken]struct{})}
					routes := &attachmentAgentAcquisitionRoutes{attachmentAgentRouteProbe: probe, resource: resource, after: cut == "after_publish", disposition: disposition, fault: errors.New("injected " + resource + " acquisition failure")}
					routes.enabled.Store(true)
					f := newReceiverConfigActivationFixtureWithOwnership(t, backend, true, map[string]string{
						"schema.yaml":          "name: agent-acquisition\n",
						"review/schema.yaml":   "name: review\ninstance: request_id\nstages:\n  pending: {}\npins:\n  inputs:\n    - task.started\n",
						"review/entities.yaml": "review_item:\n  request_id: text\n",
						"review/events.yaml":   "task.started:\n",
					}, nil, func(t *testing.T, am *manager.AgentManager) *manager.AgentManager {
						t.Cleanup(func() {
							close(workflow.retryRelease)
							if err := am.Shutdown(); err != nil && (terminalEvidence == nil || err.Error() != terminalEvidence.Error()) {
								t.Errorf("shutdown changed joined acquisition evidence: got=%v want=%v", err, terminalEvidence)
							}
						})
						return am
					}, nil, func(options *manager.AgentManagerOptions) {
						workflow.PipelineCoordinator = options.WorkflowInstances.(*pipeline.PipelineCoordinator)
						options.WorkflowInstances = workflow
						probe.sqliteFlowActivationBus = options.PersistenceRoles.AgentRoutes.(*sqliteFlowActivationBus)
						options.PersistenceRoles.AgentRoutes = routes
						options.PersistenceRoles.RouteRestorer = routes
					})
					binding, err := f.grant.ProcessExecutionBinding()
					if err != nil {
						t.Fatal(err)
					}
					admission, err := managedexecution.New(managedexecution.KindNormalRuntime, binding.RuntimeInstanceID, binding.RuntimeGeneration, "", "attachment-agent-acquisition", binding.BundleHash, nil)
					if err != nil {
						t.Fatal(err)
					}
					if err := f.manager.Run(managedexecution.WithAdmission(f.ctx, admission)); err != nil {
						t.Fatal(err)
					}
					req := f.request("business-key", "agent-acquisition", "unchanged")
					plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
					if err != nil {
						t.Fatal(err)
					}
					committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
					if err != nil || !committed.Acknowledged || !committed.Created {
						t.Fatalf("constructor commit: %+v %v", committed, err)
					}
					owner, err := flowidentity.NewRunScopedFlowInstance(plan.Readiness.RunID, plan.Identity.Route())
					if err != nil {
						t.Fatal(err)
					}
					initial, found, err := f.workflows.Load(f.ctx, owner)
					if err != nil || !found {
						t.Fatalf("initial header: %+v %t %v", initial, found, err)
					}
					ledger := assertActualMutationLedger(t, f.ctx, exactFactStore{db: f.db, postgres: backend == "postgres"}, owner.RunID, initial.EntityID)
					var receipt []byte
					if err := f.db.QueryRowContext(f.ctx, `SELECT projection FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2`, owner.RunID, owner.Route.InstancePath).Scan(&receipt); err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
					defer cancel()
					routes.cancel = cancel
					err = f.manager.FinalizeCommittedFlowInstanceActivation(ctx, committed)
					if !routes.reached.Load() {
						t.Fatal("acquisition cut was not reached")
					}
					wantOrdinal := uint64(1)
					if disposition == "caller_cancel" {
						if err != nil && !errors.Is(err, context.Canceled) {
							t.Fatalf("caller cancellation changed accepted work result: %v", err)
						}
					} else {
						// A launched agent releases the caller before its retirement
						// join. The owned worker still owes abandonment and error evidence.
						retiring := resource == "flow_route" && err != nil && err.Error() == "dynamic flow runtime readiness retains predecessor retirement"
						if err == nil || (!retiring && !strings.Contains(err.Error(), routes.fault.Error())) {
							t.Fatalf("acquisition failure lost its declared disposition: %v", err)
						}
						select {
						case err := <-workflow.abandoned:
							if err != nil {
								t.Fatalf("exact failed predecessor settlement: %v", err)
							}
						case <-time.After(5 * time.Second):
							t.Fatal("failed generation did not join and abandon")
						}
						joinCtx, cancelJoin := context.WithTimeout(f.ctx, 5*time.Second)
						terminalEvidence = f.manager.WaitForQuiescence(joinCtx)
						cancelJoin()
						if errors.Is(terminalEvidence, context.DeadlineExceeded) || (terminalEvidence != nil && !strings.Contains(terminalEvidence.Error(), routes.fault.Error())) || (retiring && terminalEvidence == nil) {
							t.Fatalf("failed acquisition retained work or changed error evidence: %v", terminalEvidence)
						}
						row, found, err := f.store.LoadDynamicFlowRuntimeReadiness(f.ctx, owner.RunID, owner.Route)
						wantPhase := pipeline.FlowAttachmentPlanned
						wantDiscarded := int32(1)
						if resource == "flow_route" {
							wantPhase, wantDiscarded = pipeline.FlowAttachmentAgentsRegistered, 0
						}
						if err != nil || !found || row.AttemptState != "aborted" || row.Phase != wantPhase || row.AttemptOrdinal != 1 || len(f.bus.routePaths()) != 0 || routes.discarded.Load() != wantDiscarded {
							t.Fatalf("failed acquisition retained executable progress: %+v found=%t err=%v routes=%v discarded=%d", row, found, err, f.bus.routePaths(), routes.discarded.Load())
						}
						if resource == "agent_route" {
							var operation, phase string
							var previousGeneration, nextGeneration uint64
							if err := f.db.QueryRowContext(f.ctx, `
							SELECT o.operation_kind, o.target_phase, f.previous_generation, f.next_generation
							FROM agent_lifecycle_operations o
							JOIN agent_lifecycle_transition_facts f ON f.operation_id = o.operation_id
							WHERE o.run_id = $1 AND o.flow_instance = $2 AND f.trigger = 'start_failed'
						`, owner.RunID, owner.Route.InstancePath).Scan(&operation, &phase, &previousGeneration, &nextGeneration); err != nil || operation != "self_release" || phase != "registered" || previousGeneration == 0 || previousGeneration != nextGeneration {
								t.Fatalf("unlaunched compensation did not self-release the exact generation: operation=%s phase=%s previous=%d next=%d err=%v", operation, phase, previousGeneration, nextGeneration, err)
							}
						}
						created, err := f.manager.EnsureFlowInstance(f.ctx, req)
						retiring = resource == "flow_route" && err != nil && err.Error() == "dynamic flow runtime readiness retains predecessor retirement"
						if created || (err != nil && !retiring) {
							t.Fatalf("attachment retry repeated construction: created=%t err=%v", created, err)
						}
						wantOrdinal++
					}
					select {
					case attempt := <-workflow.ready:
						if attempt.Ordinal() != wantOrdinal {
							t.Fatalf("ready attempt ordinal=%d want=%d", attempt.Ordinal(), wantOrdinal)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("accepted work did not reach ready")
					}
					wantPublished := int32(1)
					if (resource == "flow_route" || cut == "after_publish") && disposition != "caller_cancel" {
						wantPublished++
					}
					if routes.published.Load() != wantPublished {
						t.Fatalf("unexpected publication count: got=%d want=%d", routes.published.Load(), wantPublished)
					}
					wantFlowPublished := int32(1)
					if resource == "flow_route" && cut == "after_publish" && disposition != "caller_cancel" {
						wantFlowPublished++
					}
					if routes.flowPublished.Load() != wantFlowPublished {
						t.Fatalf("unexpected exact flow route acquisition count: got=%d want=%d", routes.flowPublished.Load(), wantFlowPublished)
					}
					current, found, err := f.workflows.Load(f.ctx, owner)
					if err != nil || !found || !reflect.DeepEqual(current, initial) {
						t.Fatalf("acquisition retry changed construction: %+v -> %+v %v", initial, current, err)
					}
					var currentReceipt []byte
					if err := f.db.QueryRowContext(f.ctx, `SELECT projection FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2`, owner.RunID, owner.Route.InstancePath).Scan(&currentReceipt); err != nil || string(currentReceipt) != string(receipt) {
						t.Fatalf("acquisition retry changed constructor receipt: %v", err)
					}
					if currentLedger := assertActualMutationLedger(t, f.ctx, exactFactStore{db: f.db, postgres: backend == "postgres"}, owner.RunID, initial.EntityID); !reflect.DeepEqual(currentLedger, ledger) {
						t.Fatal("acquisition retry repeated initial lifecycle")
					}
					assertConstructorRows(t, f, backend, 1)
				})
			}
		}
	}
}
