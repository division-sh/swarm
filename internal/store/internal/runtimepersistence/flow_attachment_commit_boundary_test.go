package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
)

func TestFlowAttachmentNativeLostAckAfterRebindBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, _, connector := newP16RaceStore(t, backend)
			f := newReceiverConfigActivationFixtureForStore(t, selected.(agentFixtureFlowStore), false, map[string]string{
				"schema.yaml":          "name: attachment-rebind-ack\n",
				"review/schema.yaml":   "name: review\ninstance: request_id\nstages:\n  pending: {initial: true}\npins:\n  inputs:\n    - task.started\n",
				"review/entities.yaml": "review_item:\n  request_id: text\n",
				"review/events.yaml":   "task.started:\n",
			}, nil, ownStoreTestAgentManager, nil)
			request := f.request("business-key", "rebind-ack", "unused")
			plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if result, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil || !result.Acknowledged {
				t.Fatalf("construct: %+v %v", result, err)
			}
			binding, err := f.grant.ProcessExecutionBinding()
			if err != nil {
				t.Fatal(err)
			}
			admissionRequest := runtimepipeline.NewDynamicFlowRuntimeActivationRequest(plan.Readiness, 1, "planned", binding)
			fault := errors.New("injected physical COMMIT acknowledgment loss across rebind")
			arm := func() { connector.arm(func(tx driver.Tx) error { return errors.Join(fault, tx.Commit()) }) }
			arm()
			if result, err := f.workflows.BeginDynamicFlowRuntimeActivation(f.ctx, admissionRequest); !errors.Is(err, fault) || result.Acknowledged {
				t.Fatalf("lost admission acknowledgment: %+v %v", result, err)
			}
			process, err := agentfixture.ProcessCapability(t, f.ctx, f.store)
			if err != nil {
				t.Fatal(err)
			}
			sourceSet, found, err := process.CurrentSourceSet(f.ctx)
			if err != nil || !found {
				t.Fatalf("source set: found=%v err=%v", found, err)
			}
			successor, err := process.IssueGenerationGrant(f.ctx, startupownership.GrantRequest{BundleHash: binding.BundleHash, RuntimeInstanceID: binding.RuntimeInstanceID, RuntimeGeneration: binding.RuntimeGeneration + 1, SourceSetRevision: sourceSet.Revision})
			if err != nil {
				t.Fatal(err)
			}
			admitReceiverConfigFixtureGrant(t, f.ctx, successor)
			attempt, err := runtimepipeline.NewDynamicFlowRuntimeActivationAttempt("1", plan.Readiness.RunID, plan.Identity.InstancePath, binding)
			if err != nil {
				t.Fatal(err)
			}
			planHash, err := plan.Readiness.Hash()
			if err != nil {
				t.Fatal(err)
			}
			rebind := manager.FlowReadinessSourceSetRebindRequest{Attempt: attempt, PlanHash: planHash}
			owner := successor.(manager.FlowReadinessSourceSetRebindPersistence)
			arm()
			if result, err := owner.RebindFlowReadinessSourceSet(f.ctx, rebind); !errors.Is(err, fault) || result.Attempt.Validate() == nil {
				t.Fatalf("lost rebind acknowledgment leaked a receipt: %+v %v", result, err)
			}
			if backend == "postgres" {
				// An ambiguous retained session is force-discarded, not retried as
				// a live writer. Exact durable readback and joined settlement remain.
				if _, err := owner.RebindFlowReadinessSourceSet(f.ctx, rebind); err == nil {
					t.Fatal("ambiguous PostgreSQL session remained an executable rebind owner")
				}
				if err := successor.ProveCurrent(f.ctx); err == nil {
					t.Fatal("ambiguous PostgreSQL writer retained execution authority")
				}
				resolved, err := f.workflows.ResolveDynamicFlowRuntimeActivation(f.ctx, admissionRequest)
				if err != nil || resolved.Disposition != runtimepipeline.FlowActivationAdmitted || admissionRequest.ValidateResolution(resolved) != nil {
					t.Fatalf("lost admission after terminal writer: %+v %v", resolved, err)
				}
				if err := f.workflows.AbandonDynamicFlowRuntimeActivationAttempt(f.ctx, resolved.Attempt); err != nil {
					t.Fatal(err)
				}
				return
			}
			rebound, err := owner.RebindFlowReadinessSourceSet(f.ctx, rebind)
			if err != nil || rebound.Attempt.ID() != "1" {
				t.Fatalf("exact rebind retry: %+v %v", rebound, err)
			}
			if err := f.grant.Retire(f.ctx); err != nil {
				t.Fatal(err)
			}
			resolved, err := f.workflows.ResolveDynamicFlowRuntimeActivation(f.ctx, admissionRequest)
			if err != nil || resolved.Disposition != runtimepipeline.FlowActivationAdmitted || admissionRequest.ValidateResolution(resolved) != nil {
				t.Fatalf("lost admission after rebind: %+v %v", resolved, err)
			}
			if err := f.workflows.AbandonDynamicFlowRuntimeActivationAttempt(f.ctx, resolved.Attempt); err != nil {
				t.Fatal(err)
			}
			currentBinding, err := successor.ProcessExecutionBinding()
			if err != nil {
				t.Fatal(err)
			}
			next, err := f.workflows.BeginDynamicFlowRuntimeActivation(f.ctx, runtimepipeline.NewDynamicFlowRuntimeActivationRequest(plan.Readiness, 1, "aborted", currentBinding))
			if err != nil || !next.Acknowledged || next.Attempt.ID() != "2" {
				t.Fatalf("settled successor: %+v %v", next, err)
			}
			if err := f.workflows.VerifyDynamicFlowRuntimeActivationAttempt(f.ctx, rebound.Attempt); err == nil {
				t.Fatal("previous attempt callback revived after successor admission")
			}
			if old, err := f.workflows.ResolveDynamicFlowRuntimeActivation(f.ctx, admissionRequest); err != nil || old.Disposition != runtimepipeline.FlowActivationForeign {
				t.Fatalf("old request adopted successor: %+v %v", old, err)
			}
			if err := f.workflows.AbandonDynamicFlowRuntimeActivationAttempt(f.ctx, next.Attempt); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFlowAttachmentNativeCommitAcknowledgmentBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, predecessor := range []string{"first", "retired", "aborted"} {
			for _, committed := range []bool{false, true} {
				cut := "rollback_before_ack"
				if committed {
					cut = "commit_before_lost_ack"
				}
				t.Run(backend+"/"+predecessor+"/"+cut, func(t *testing.T) {
					selected, _, connector := newP16RaceStore(t, backend)
					f := newReceiverConfigActivationFixtureForStore(t, selected.(agentFixtureFlowStore), false, map[string]string{
						"schema.yaml":          "name: attachment-commit-boundary\n",
						"review/schema.yaml":   "name: review\ninstance: request_id\nstages:\n  pending: {initial: true}\npins:\n  inputs:\n    - task.started\n",
						"review/entities.yaml": "review_item:\n  request_id: text\n",
						"review/events.yaml":   "task.started:\n",
					}, nil, ownStoreTestAgentManager, nil)
					ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
					defer cancel()
					req := f.request("business-key", "commit-boundary", "unused")
					plan, err := f.manager.PrepareFlowInstanceActivation(ctx, req)
					if err != nil {
						t.Fatal(err)
					}
					constructed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(ctx, plan)
					if err != nil || !constructed.Acknowledged || !constructed.Created {
						t.Fatalf("construct: %+v %v", constructed, err)
					}
					owner, err := flowidentity.NewRunScopedFlowInstance(correlation.RunIDFromContext(ctx), plan.Identity.Route())
					if err != nil {
						t.Fatal(err)
					}
					initial, found, err := f.workflows.Load(ctx, owner)
					if err != nil || !found {
						t.Fatalf("constructed header: %+v %t %v", initial, found, err)
					}
					ledger := assertActualMutationLedger(t, ctx, exactFactStore{db: f.db, postgres: backend == "postgres"}, owner.RunID, initial.EntityID)
					binding, err := f.grant.ProcessExecutionBinding()
					if err != nil {
						t.Fatal(err)
					}
					expectedOrdinal := uint64(1)
					observedDisposition := "planned"
					if predecessor != "first" {
						previous, err := f.workflows.BeginDynamicFlowRuntimeActivation(ctx, runtimepipeline.NewDynamicFlowRuntimeActivationRequest(plan.Readiness, 1, "planned", binding))
						if err != nil || !previous.Acknowledged {
							t.Fatalf("predecessor: %+v %v", previous, err)
						}
						var settleErr error
						if predecessor == "retired" {
							settleErr = f.workflows.RetireDynamicFlowRuntimeActivationAttempt(ctx, previous.Attempt)
						} else {
							settleErr = f.workflows.AbandonDynamicFlowRuntimeActivationAttempt(ctx, previous.Attempt)
						}
						if settleErr != nil {
							t.Fatal(settleErr)
						}
						expectedOrdinal = 2
						observedDisposition = predecessor
					}
					fault := errors.New("injected physical attachment COMMIT acknowledgment loss")
					var commits atomic.Int32
					arm := func() {
						connector.arm(func(tx driver.Tx) error {
							commits.Add(1)
							if committed {
								return errors.Join(fault, tx.Commit())
							}
							return errors.Join(fault, tx.Rollback())
						})
					}
					arm()
					request := runtimepipeline.NewDynamicFlowRuntimeActivationRequest(plan.Readiness, 1, observedDisposition, binding)
					admitted, err := f.workflows.BeginDynamicFlowRuntimeActivation(ctx, request)
					if !errors.Is(err, fault) || admitted.Acknowledged || admitted.Attempt.Validate() == nil || commits.Load() != 1 {
						t.Fatalf("uncertain admission leaked executable authority: %+v %v commits=%d", admitted, err, commits.Load())
					}
					cancelledCtx, cancelResolution := context.WithCancel(ctx)
					cancelResolution()
					if resolved, err := f.workflows.ResolveDynamicFlowRuntimeActivation(cancelledCtx, request); !errors.Is(err, context.Canceled) || resolved.Disposition != runtimepipeline.FlowActivationUnresolved {
						t.Fatalf("cancelled resolution leaked evidence: %+v %v", resolved, err)
					}
					resolved, resolveErr := f.workflows.ResolveDynamicFlowRuntimeActivation(ctx, request)
					want := runtimepipeline.FlowActivationUnadmitted
					if committed {
						want = runtimepipeline.FlowActivationAdmitted
					}
					if resolveErr != nil || resolved.Disposition != want {
						t.Fatalf("resolve uncertain admission: %+v %v", resolved, resolveErr)
					}
					if again, err := f.workflows.ResolveDynamicFlowRuntimeActivation(ctx, request); err != nil || again != resolved {
						t.Fatalf("duplicate resolution changed exact evidence: %+v %v", again, err)
					}
					wrongBinding := binding
					wrongBinding.RuntimeInstanceID = "99999999-9999-4999-8999-999999999999"
					wrong := runtimepipeline.NewDynamicFlowRuntimeActivationRequest(plan.Readiness, 1, observedDisposition, wrongBinding)
					if result, err := f.workflows.ResolveDynamicFlowRuntimeActivation(ctx, wrong); err == nil || result.Disposition != runtimepipeline.FlowActivationUnresolved || result.Attempt.Validate() == nil {
						t.Fatalf("forged runtime received resolution authority: %+v %v", result, err)
					}
					admitted, err = f.workflows.BeginDynamicFlowRuntimeActivation(ctx, request)
					if err != nil || !admitted.Acknowledged || admitted.Reused != committed || admitted.Attempt.Ordinal() != expectedOrdinal || commits.Load() != 1 {
						t.Fatalf("exact admission reconciliation: %+v %v commits=%d", admitted, err, commits.Load())
					}
					foreign := runtimepipeline.NewDynamicFlowRuntimeActivationRequest(plan.Readiness, 1, "planned", binding)
					if resolved, err := f.workflows.ResolveDynamicFlowRuntimeActivation(ctx, foreign); err != nil || resolved.Disposition != runtimepipeline.FlowActivationForeign || resolved.Attempt.Validate() == nil {
						t.Fatalf("foreign request adopted same-grant attempt: %+v %v", resolved, err)
					}
					if result, err := f.workflows.BeginDynamicFlowRuntimeActivation(ctx, foreign); err == nil || result.Acknowledged {
						t.Fatalf("foreign request reused exact attempt: %+v %v", result, err)
					}
					arm()
					if err := f.workflows.AbandonDynamicFlowRuntimeActivationAttempt(ctx, admitted.Attempt); !errors.Is(err, fault) || commits.Load() != 2 {
						t.Fatalf("uncertain abandonment: %v commits=%d", err, commits.Load())
					}
					if err := f.workflows.AbandonDynamicFlowRuntimeActivationAttempt(ctx, admitted.Attempt); err != nil {
						t.Fatalf("exact abandonment reconciliation: %v", err)
					}
					successor, err := f.workflows.BeginDynamicFlowRuntimeActivation(ctx, runtimepipeline.NewDynamicFlowRuntimeActivationRequest(plan.Readiness, admitted.Attempt.Ordinal(), "aborted", binding))
					if err != nil || !successor.Acknowledged || successor.Reused || successor.Attempt.Ordinal() != expectedOrdinal+1 {
						t.Fatalf("acknowledged settled successor: %+v %v", successor, err)
					}
					if err := f.workflows.AbandonDynamicFlowRuntimeActivationAttempt(ctx, successor.Attempt); err != nil {
						t.Fatal(err)
					}
					current, found, err := f.workflows.Load(ctx, owner)
					if err != nil || !found || !reflect.DeepEqual(initial, current) {
						t.Fatalf("attachment reconciliation changed construction: before=%+v after=%+v found=%t err=%v", initial, current, found, err)
					}
					if after := assertActualMutationLedger(t, ctx, exactFactStore{db: f.db, postgres: backend == "postgres"}, owner.RunID, initial.EntityID); !reflect.DeepEqual(ledger, after) {
						t.Fatal("attachment reconciliation repeated constructor lifecycle evidence")
					}
					f.bus.mu.Lock()
					staged, published := len(f.bus.stagedRequests), len(f.bus.routeRequests)
					f.bus.mu.Unlock()
					if staged != 0 || published != 0 {
						t.Fatalf("store acknowledgment reconciliation installed process routes: staged=%d published=%d", staged, published)
					}
				})
			}
		}
	}
}
