package conformance

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

func TestFanOutServingM06SameBundleGenerationReplacementOnBothBackends(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			workers := 1
			old := newServingMatrixProbe(servingMatrixCommit, 1)
			old.commitWithoutCancel = true
			f := newServingMatrixFixture(t, backend, &workers, old)
			_, runID := f.startRun(t, 0)
			trigger := f.submit(t, 0, runID, "same-bundle-successor")
			turn := waitServingMatrixHeld(t, old)
			waitServingMatrixTriggerReceipt(t, f.db, trigger)
			if err := f.runtimes[0].manager.Shutdown(); err != nil {
				t.Fatalf("settle fixture manager before the deliberate grant replacement: %v", err)
			}
			before := readServingLifetimeState(t, f.db, runID)
			assertServingLifetimeUnissued(t, before)
			oldGrant := f.topology.grants[f.runtimes[0].sourceArtifactFact.BundleHash()]
			oldEvidence, err := oldGrant.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			successor := newServingMatrixProbe(servingMatrixUnheld, 0)
			replacement := newNotifyAllChildrenRuntime(t, f.selected, f.db, f.sources[0], time.Now, notifyAllChildrenRuntimeOptions{
				processTopology: f.topology, fanOutWorkers: &workers,
				fanOutExecutor: func(pc *pipeline.PipelineCoordinator) startupownership.FanOutExecutor {
					successor.PipelineCoordinator = pc
					return successor
				},
			})
			t.Cleanup(successor.releaseAll)
			newGrant := f.topology.grants[replacement.sourceArtifactFact.BundleHash()]
			newEvidence, err := newGrant.Evidence()
			if err != nil || newEvidence.BundleHash != oldEvidence.BundleHash || newEvidence.SourceSetRevision != oldEvidence.SourceSetRevision || newEvidence.GrantID == oldEvidence.GrantID || newEvidence.RuntimeGeneration <= oldEvidence.RuntimeGeneration {
				t.Fatalf("same-bundle replacement must keep actual source-set authority and change exact generation: before=%+v after=%+v err=%v", oldEvidence, newEvidence, err)
			}
			select {
			case <-oldGrant.Done():
			default:
				t.Fatal("same-bundle replacement left predecessor grant executable")
			}

			successorCtx := correlation.WithRunID(context.Background(), runID)
			if err := runNotifyAllChildrenManager(managedConformanceExecutionContextForBundle(t, successorCtx, "fan-out-same-bundle-successor", replacement.sourceArtifactFact), replacement.bus, replacement.manager); err != nil {
				t.Fatalf("start exact same-bundle successor manager: %v", err)
			}

			join := beginServingLifetimeJoin(f.runtimes[0], nil)
			assertServingJoinBlocked(t, join)
			replacement.fanOutServing.Wake()
			select {
			case attempt := <-successor.attempts:
				t.Fatalf("successor acquired before predecessor's exact held permit was returned: %+v", attempt)
			case <-time.After(100 * time.Millisecond):
			}
			turn.release()
			oldReceipt := waitServingMatrixReceipt(t, old, time.Now().Add(5*time.Second))
			assertServingRetiredTurn(t, oldReceipt, servingMatrixCommit)
			if oldReceipt.turn.cleanupErr != nil {
				t.Fatalf("predecessor exact cleanup under same live process: %v", oldReceipt.turn.cleanupErr)
			}
			assertServingJoinComplete(t, join, f.runtimes[0], nil)
			fresh := waitServingMatrixReceipt(t, successor, time.Now().Add(5*time.Second))
			if fresh.err != nil || !fresh.result.Refill || fresh.turn.key != turn.key || fresh.turn.claim.Generation <= turn.claim.Generation || fresh.turn.claim.Owner == turn.claim.Owner {
				t.Fatalf("same-bundle successor failed exact durable recovery: old=%+v fresh=%+v result=%+v err=%v", turn.claim, fresh.turn.claim, fresh.result, fresh.err)
			}
			proof := *f
			proof.runtimes = []notifyAllChildrenRuntime{replacement}
			proof.assertSettled(t, 0, runID, 1)
			assertServingRetiredRegistration(t, f.runtimes[0], old)
			_, _, duplicates, turns := successor.snapshot()
			if duplicates != 0 || turns != 1 {
				t.Fatalf("same-bundle recovery duplicated serving: turns=%d duplicates=%d", turns, duplicates)
			}
			t.Logf("M06 replacement: runtime generation %d -> %d, exact claim generation %d -> %d, one immutable publication and canonical history", oldEvidence.RuntimeGeneration, newEvidence.RuntimeGeneration, turn.claim.Generation, fresh.turn.claim.Generation)
		})
	}
}
