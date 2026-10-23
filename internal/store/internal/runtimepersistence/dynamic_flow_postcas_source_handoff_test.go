package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

// Reach: both native stores settle a canceled post-CAS predecessor through its
// Manager before replacing its captured source; a canceled successor still finishes.
func TestDynamicFlowPostCASSourceHandoffSettlesManagerBeforeReplacementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			base := newReceiverConfigActivationFixtureWithOptions(t, backend, true, false)
			base.constructKeylessRoot(t)
			req := base.request("postcas", "postcas", "captured")
			plan, err := base.manager.PrepareFlowInstanceActivation(base.ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			committed, err := (agentFixtureFlowActivationCommitter{store: base.store}).CommitFlowInstanceActivation(base.ctx, plan)
			if err != nil || !committed.Acknowledged || !committed.Created {
				t.Fatalf("native initial construction: %+v %v", committed, err)
			}
			if err := base.manager.Shutdown(); err != nil {
				t.Fatal(err)
			}
			source := func(label string) *contracts.WorkflowContractBundle {
				t.Helper()
				files := make(map[string]string)
				for _, member := range base.bundle.SourceArtifact.Entries() {
					files[member.Label()] = string(member.Bytes())
				}
				files["review/agents.yaml"] += "\n# captured post-CAS source " + label + "\n"
				bundle := loadLifecyclePersistenceFixtureForTest(t, files)
				if err := ensureRunFixtureSourceArtifactForTest(base.ctx, base.store, bundle.SourceArtifact.BundleHash(), bundle.SourceArtifact); err != nil {
					t.Fatal(err)
				}
				return bundle
			}
			bundleB, bundleC := source("B"), source("C")
			factB := mustStoreTestSourceArtifactFact(bundleB.SourceArtifact.BundleHash())
			factC := mustStoreTestSourceArtifactFact(bundleC.SourceArtifact.BundleHash())
			if factB.Matches(factC) || factB.BundleHash() == base.bundle.SourceArtifact.BundleHash() || factC.BundleHash() == base.bundle.SourceArtifact.BundleHash() {
				t.Fatal("handoff requires three distinct captured sources")
			}
			rootB, err := bundleB.RootedSource()
			if err != nil {
				t.Fatal(err)
			}
			rootC, err := bundleC.RootedSource()
			if err != nil {
				t.Fatal(err)
			}
			runID := correlation.RunIDFromContext(base.ctx)
			ctxB := correlation.WithRootAdmission(correlation.WithRunID(storeTestWorkContext(t, testAuthorActivityContextForBundle(factB.BundleHash())), runID), rootB.Admission())
			ctxC := correlation.WithRootAdmission(correlation.WithRunID(storeTestWorkContext(t, testAuthorActivityContextForBundle(factC.BundleHash())), runID), rootC.Admission())
			owner, err := flowidentity.NewRunScopedFlowInstance(runID, plan.Identity.Route())
			if err != nil {
				t.Fatal(err)
			}
			originalConstruction, err := base.workflows.LoadFlowConstructionPublication(base.ctx, owner, plan.Identity.EntityID)
			if err != nil || originalConstruction.BundleHash != base.bundle.SourceArtifact.BundleHash() {
				t.Fatalf("original immutable construction: %+v %v", originalConstruction, err)
			}
			atA, atC := make(chan pipeline.DynamicFlowRuntimeActivationAttempt, 1), make(chan pipeline.DynamicFlowRuntimeActivationAttempt, 1)
			releaseA, releaseC := make(chan struct{}), make(chan struct{})
			var releaseAOnce, releaseCOnce sync.Once
			openA := func() { releaseAOnce.Do(func() { close(releaseA) }) }
			openC := func() { releaseCOnce.Do(func() { close(releaseC) }) }
			var barrier context.Context
			workflowA := &readinessPostcommitFaultWorkflow{PipelineCoordinator: base.workflows}
			workflowA.afterReady = func(_ context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt) error {
				if attempt.InstancePath() != plan.Identity.InstancePath {
					return nil
				}
				atA <- attempt
				select {
				case <-releaseA:
					return nil
				case <-barrier.Done():
					return barrier.Err()
				}
			}
			first := base.restartedReceiverConfigManager(t, nil, workflowA)
			callerA, cancelA := context.WithCancel(first.ctx)
			defer cancelA()
			callerC, cancelC := context.WithCancel(ctxC)
			defer cancelC()
			barrier, cancelBarrier := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelBarrier()
			resultA, resultC := make(chan error, 1), make(chan error, 1)
			doneA, doneC := make(chan struct{}), make(chan struct{})
			var successor *receiverConfigActivationFixture
			startedC := false
			t.Cleanup(func() {
				cancelA()
				cancelC()
				openA()
				openC()
				join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				select {
				case <-doneA:
				case <-join.Done():
					t.Errorf("join native caller A: %v", join.Err())
				}
				if startedC {
					select {
					case <-doneC:
					case <-join.Done():
						t.Errorf("join native caller C: %v", join.Err())
					}
				}
				if err := first.manager.WaitForQuiescence(join); err != nil {
					t.Errorf("join native predecessor work: %v", err)
				}
				if successor != nil {
					if err := successor.manager.WaitForQuiescence(join); err != nil {
						t.Errorf("join native successor work: %v", err)
					}
				}
			})
			go func() {
				defer close(doneA)
				resultA <- first.manager.FinalizeCommittedFlowInstanceActivation(callerA, committed)
			}()
			var oldAttempt pipeline.DynamicFlowRuntimeActivationAttempt
			select {
			case oldAttempt = <-atA:
				t.Logf("held A after native READY: attempt=%s ordinal=%d", oldAttempt.ID(), oldAttempt.Ordinal())
			case err := <-resultA:
				t.Fatalf("A returned before post-CAS hold: %v", err)
			case <-barrier.Done():
				t.Fatal(barrier.Err())
			}
			lifecycle := first.store.(runlifecycle.OperationOwner)
			if _, err := lifecycle.ReviseRunSource(first.ctx, runlifecycle.SourceRevisionRequest{RunID: runID, Source: factB, RootAdmission: rootB.Admission()}); err != nil {
				t.Fatalf("A-to-B explicit revision: %v", err)
			}
			if _, err := lifecycle.ReviseRunSource(ctxB, runlifecycle.SourceRevisionRequest{RunID: runID, Source: factC, RootAdmission: rootC.Admission()}); err != nil {
				t.Fatalf("B-to-C explicit revision: %v", err)
			}
			cancelA()
			select {
			case err := <-resultA:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("native caller A cancellation: %v", err)
				}
			case <-barrier.Done():
				t.Fatalf("native caller A did not observe cancellation: %v", barrier.Err())
			}
			select {
			case <-doneA:
			case <-barrier.Done():
				t.Fatalf("join canceled native caller A: %v", barrier.Err())
			}
			openA()
			if err := first.manager.WaitForQuiescence(barrier); err != nil {
				t.Fatalf("predecessor cleanup: %v", err)
			}
			if err := first.manager.Shutdown(); err != nil {
				t.Fatalf("settle predecessor Manager before successor admission: %v", err)
			}
			candidate := first
			candidate.ctx, candidate.bundle = ctxC, bundleC
			capability, err := agentfixture.ProcessCapability(t, ctxC, first.store)
			if err != nil {
				t.Fatal(err)
			}
			oldSet, found, err := capability.CurrentSourceSet(ctxC)
			if err != nil || !found {
				t.Fatalf("exact predecessor source set: found=%t err=%v", found, err)
			}
			coordinateC, err := agenttopology.NewSourceCoordinate(rootC.Admission())
			if err != nil {
				t.Fatal(err)
			}
			setC, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinateC}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := capability.RestoreSourceSet(ctxC, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: oldSet.Revision, Plan: setC}); err != nil {
				t.Fatalf("explicit source-set replacement: %v", err)
			}
			workflowC := &readinessPostcommitFaultWorkflow{PipelineCoordinator: configureAgentFixtureFlowLifecycle(t, first.store, &sqliteFlowActivationBus{}, bundleC)}
			workflowC.beforeReady = func(_ context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt) error {
				if attempt.InstancePath() != plan.Identity.InstancePath {
					return nil
				}
				atC <- attempt
				select {
				case <-releaseC:
					return nil
				case <-barrier.Done():
					return barrier.Err()
				}
			}
			candidate = candidate.restartedReceiverConfigManager(t, nil, workflowC)
			successor = &candidate
			startedC = true
			go func() {
				defer close(doneC)
				resultC <- candidate.manager.ReconcileDynamicFlowRuntimeReadinessPlansForRun(callerC, time.Now().UTC())
			}()
			var newAttempt pipeline.DynamicFlowRuntimeActivationAttempt
			select {
			case newAttempt = <-atC:
				t.Logf("held C before native READY: attempt=%s ordinal=%d", newAttempt.ID(), newAttempt.Ordinal())
			case err := <-resultC:
				t.Fatalf("C returned before owned successor hold: %v", err)
			case <-barrier.Done():
				t.Fatalf("C did not reach successor hold: %v", barrier.Err())
			}
			cancelC()
			select {
			case err := <-resultC:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("native caller C cancellation: %v", err)
				}
			case <-barrier.Done():
				t.Fatalf("native caller C did not observe cancellation: %v", barrier.Err())
			}
			openC()
			if err := candidate.manager.WaitForQuiescence(barrier); err != nil {
				t.Fatalf("successor completion: %v", err)
			}
			ready, found, err := candidate.store.LoadDynamicFlowRuntimeReadiness(ctxC, runID, plan.Identity.Route())
			if err != nil || !found || ready.Phase != pipeline.FlowAttachmentReady || ready.Pending() || ready.AttemptOrdinal != oldAttempt.Ordinal()+1 || newAttempt.Ordinal() != ready.AttemptOrdinal || !ready.OwningRunSource.Matches(factC) || ready.Plan.BundleHash != factC.BundleHash() {
				t.Fatalf("native exact successor readiness: %+v found=%t err=%v", ready, found, err)
			}
			if err := candidate.workflows.VerifyDynamicFlowRuntimeActivationAttempt(ctxC, newAttempt); err != nil {
				t.Fatalf("native successor executable attachment: %v", err)
			}
			if err := candidate.workflows.VerifyDynamicFlowRuntimeActivationAttempt(ctxC, oldAttempt); err == nil || err.Error() != "flow activation generation grant source-set revision is not current" {
				t.Fatalf("predecessor did not retain its exact obsolete generation fence: %v", err)
			}
			agents, err := candidate.store.LoadAgents(ctxC)
			if err != nil || len(agents) != 1 || agents[0].Config.Identity.RunID != runID || !reflect.DeepEqual(ready.Plan.Agents[0].Identity, agents[0].Config.Identity) {
				t.Fatalf("native exact successor agent: %+v %v", agents, err)
			}
			for _, stale := range []context.Context{first.ctx, ctxB} {
				if err := candidate.manager.ReconcileDynamicFlowRuntimeReadinessPlansForRun(stale, time.Now().UTC()); err == nil {
					t.Fatal("stale source callback gained successor authority")
				}
			}
			for _, stale := range []context.Context{ctxB, ctxC} {
				if err := candidate.manager.FinalizeCommittedFlowInstanceActivation(stale, committed); err == nil {
					t.Fatal("stale committed plan gained successor authority")
				}
			}
			afterStale, found, err := candidate.store.LoadDynamicFlowRuntimeReadiness(ctxC, runID, plan.Identity.Route())
			if err != nil || !found || !reflect.DeepEqual(ready, afterStale) {
				t.Fatalf("stale callback changed exact successor: %+v found=%t err=%v", afterStale, found, err)
			}
			if err := candidate.workflows.VerifyDynamicFlowRuntimeActivationAttempt(ctxC, newAttempt); err != nil {
				t.Fatalf("stale callback invalidated successor attachment: %v", err)
			}
			afterAgents, err := candidate.store.LoadAgents(ctxC)
			if err != nil || !reflect.DeepEqual(agents, afterAgents) {
				t.Fatalf("stale callback changed successor agents: %+v err=%v", afterAgents, err)
			}
			historical, err := candidate.workflows.LoadFlowConstructionPublication(ctxC, owner, plan.Identity.EntityID)
			if err != nil || !reflect.DeepEqual(originalConstruction, historical) {
				t.Fatalf("source handoff changed immutable construction: %+v err=%v", historical, err)
			}
			if err := candidate.manager.Shutdown(); err != nil {
				t.Fatalf("settle completed successor Manager: %v", err)
			}
		})
	}
}
