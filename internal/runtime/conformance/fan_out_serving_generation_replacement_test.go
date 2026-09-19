package conformance

import (
	"testing"
	"time"

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
			ctx, runID := f.startRun(t, 0)
			trigger := f.submit(t, 0, runID, "same-bundle-successor")
			turn := waitServingMatrixHeld(t, old)
			waitServingMatrixTriggerReceipt(t, f.db, trigger)
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
			if err := replacement.manager.Run(managedConformanceExecutionContextForBundle(t, ctx, "fan-out-same-bundle-successor", replacement.sourceArtifactFact)); err != nil {
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
