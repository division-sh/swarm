package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// Only the acknowledgment is faulted: Begin and resolution use real selected
// transactions. Manager resource retirement is exercised, not simulated.
type nativeManagerAdmissionFault struct {
	*pipeline.PipelineCoordinator
	connector  *stopCommitConnector
	failure    error
	committed  bool
	panicAfter bool
	unresolved atomic.Bool
}

func (s *nativeManagerAdmissionFault) BeginDynamicFlowRuntimeActivation(ctx context.Context, request pipeline.DynamicFlowRuntimeActivationRequest) (pipeline.DynamicFlowRuntimeActivationAdmissionResult, error) {
	s.connector.arm(func(tx driver.Tx) error {
		if s.committed {
			return errors.Join(s.failure, tx.Commit())
		}
		return errors.Join(s.failure, tx.Rollback())
	})
	result, err := s.PipelineCoordinator.BeginDynamicFlowRuntimeActivation(ctx, request)
	if s.panicAfter {
		panic(s.failure)
	}
	return result, err
}

func (s *nativeManagerAdmissionFault) ResolveDynamicFlowRuntimeActivation(ctx context.Context, request pipeline.DynamicFlowRuntimeActivationRequest) (pipeline.DynamicFlowRuntimeActivationResolution, error) {
	if s.unresolved.Load() {
		s.connector.arm(func(tx driver.Tx) error { return errors.Join(s.failure, tx.Rollback()) })
	}
	return s.PipelineCoordinator.ResolveDynamicFlowRuntimeActivation(ctx, request)
}

func TestManagerNativeLostAdmissionCleanupBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"commit", "rollback", "unresolved", "panic"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				selected, _, connector := newP16RaceStore(t, backend)
				fault := &nativeManagerAdmissionFault{connector: connector, failure: errors.New("native admission acknowledgment lost"), committed: cut != "rollback", panicAfter: cut == "panic"}
				fault.unresolved.Store(cut == "unresolved")
				var routes *bus.EventBus
				var nativeOptions manager.AgentManagerOptions
				f := newReceiverConfigActivationFixtureForStore(t, selected.(agentFixtureFlowStore), false, map[string]string{
					"schema.yaml":          "name: native-admission-manager\n",
					"review/schema.yaml":   "name: review\ninstance: request_id\nstages:\n  pending: {}\npins:\n  inputs:\n    - task.started\n",
					"review/entities.yaml": "review_item:\n  request_id: text\n",
					"review/events.yaml":   "task.started:\n",
					"review/agents.yaml":   "reviewer:\n  role: reviewer\n  intent: {inline: \"Review the task.\"}\n  model: regular\n  subscriptions: [task.started]\n",
				}, nil, func(t *testing.T, original *manager.AgentManager) *manager.AgentManager {
					if err := original.Shutdown(); err != nil {
						t.Fatal(err)
					}
					return ownStoreTestAgentManager(t, manager.NewAgentManagerWithOptions(routes, nil, nativeOptions, selected.(agentFixtureFlowStore)))
				}, nil, func(options *manager.AgentManagerOptions) {
					var ok bool
					fault.PipelineCoordinator, ok = options.WorkflowInstances.(*pipeline.PipelineCoordinator)
					if !ok {
						t.Fatal("fixture lost native workflow coordinator")
					}
					options.WorkflowInstances = fault
					var err error
					runtimeID, _ := correlation.RuntimeInstanceIDFromContext(options.BaseContext)
					routes, err = newStoreTestEventBus(t, selected.(storeTestDurableEventBusStore), bus.EventBusOptions{ContractBundle: options.SemanticSource, SourceArtifactFact: options.SourceArtifactFact, RuntimeInstanceID: runtimeID})
					if err != nil {
						t.Fatal(err)
					}
					options.PersistenceRoles.AgentRoutes = routes
					options.PersistenceRoles.FlowActivation = routes
					options.PersistenceRoles.RouteInstaller = routes
					options.PersistenceRoles.RouteVerifier = routes
					options.PersistenceRoles.RouteRestorer = routes
					nativeOptions = *options
				})
				grant, err := f.grant.Evidence()
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(correlation.WithRuntimeInstanceID(f.ctx, grant.RuntimeInstanceID), 20*time.Second)
				defer cancel()
				req := f.request("exact-key", "one", "unused")
				plan, err := f.manager.PrepareFlowInstanceActivation(ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				committed, err := routes.CommitFlowInstanceActivation(ctx, plan)
				if err != nil || !committed.Acknowledged {
					t.Fatalf("native construction: %+v %v", committed, err)
				}
				owner, err := flowidentity.NewRunScopedFlowInstance(correlation.RunIDFromContext(ctx), plan.Identity.Route())
				if err != nil {
					t.Fatal(err)
				}
				before, found, err := f.workflows.Load(ctx, owner)
				if err != nil || !found {
					t.Fatalf("constructed header: %+v %t %v", before, found, err)
				}
				ledger := assertActualMutationLedger(t, ctx, exactFactStore{db: f.db, postgres: backend == "postgres"}, owner.RunID, before.EntityID)
				var finalErr error
				var panicked any
				func() {
					defer func() { panicked = recover() }()
					finalErr = f.manager.FinalizeCommittedFlowInstanceActivation(ctx, committed)
				}()
				if cut == "panic" && (panicked != nil || finalErr == nil || !strings.Contains(finalErr.Error(), "dynamic flow readiness attempt panic: "+fault.failure.Error())) {
					t.Fatalf("owned attempt did not report its panic: panic=%v error=%v", panicked, finalErr)
				}
				if cut != "panic" && panicked == nil && !errors.Is(finalErr, fault.failure) {
					t.Fatalf("admission diagnostic lost: panic=%v error=%v", panicked, finalErr)
				}
				t.Logf("attachment result: %v", finalErr)
				if cut == "commit" && !routes.HasFlowInstanceRoute(owner) {
					t.Fatal("resolved committed admission failed to install exact route")
				}
				if cut != "commit" && routes.HasFlowInstanceRoute(owner) {
					t.Fatal("unresolved, cancelled or rolled-back admission installed topology")
				}
				if cut == "unresolved" {
					if err := f.manager.Shutdown(); !errors.Is(err, fault.failure) {
						t.Fatalf("joined shutdown lost unresolved responsibility: %v", err)
					}
					fault.unresolved.Store(false)
				}
				if err := f.manager.Shutdown(); err != nil {
					t.Fatalf("native exact retirement: %v", err)
				}
				if routes.HasFlowInstanceRoute(owner) {
					t.Fatal("joined retirement retained executable route")
				}
				row, found, err := f.workflows.LoadDynamicFlowRuntimeReadiness(ctx, owner.RunID, owner.Route)
				want := "aborted"
				if cut == "rollback" {
					want = "planned"
				} else if cut == "commit" {
					want = "retired"
				}
				phase := pipeline.FlowAttachmentPlanned
				if cut == "commit" {
					phase = pipeline.FlowAttachmentReady
				}
				if err != nil || !found || row.AttemptState != want || row.Phase != phase {
					t.Fatalf("exact durable disposition: %+v %t %v; want %s/%s", row, found, err, want, phase)
				}
				after, found, err := f.workflows.Load(ctx, owner)
				if err != nil || !found || !reflect.DeepEqual(before, after) {
					t.Fatalf("cleanup changed immutable construction: before=%+v after=%+v %t %v", before, after, found, err)
				}
				if after := assertActualMutationLedger(t, ctx, exactFactStore{db: f.db, postgres: backend == "postgres"}, owner.RunID, before.EntityID); !reflect.DeepEqual(ledger, after) {
					t.Fatal("cleanup repeated lifecycle initialization")
				}
			})
		}
	}
}
