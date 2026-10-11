package bus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

func TestPublicCompletionPreparationCancellation(t *testing.T) {
	for _, variant := range []string{"runtime", "standing", "selected"} {
		for _, cut := range []string{"retirement", "caller", "fence"} {
			t.Run(variant+"/"+cut, func(t *testing.T) {
				process := worklifetime.NewProcess()
				runtime, err := process.NewRuntime(t.Context(), worklifetime.RuntimeIdentity{RuntimeInstanceID: authorActivityTestRuntimeInstanceID, BundleHash: authorActivityTestSourceArtifactFact.BundleHash()})
				if err != nil {
					t.Fatal(err)
				}
				var owner worklifetime.Occurrence = runtime
				retire, fence := runtime.Retire, runtime.Fence
				join := func() error { _, err := runtime.RetireAndWait(t.Context()); return err }
				switch variant {
				case "standing":
					standing, err := runtime.NewStanding(t.Context(), worklifetime.StandingIdentity{ServiceID: "service", RunID: "standing-run", Generation: 1})
					if err != nil {
						t.Fatal(err)
					}
					owner, join = standing, func() error { return standing.RetireAndWait(t.Context()) }
					retire, fence = standing.Retire, standing.Fence
				case "selected":
					selected, err := process.NewSelectedFork(t.Context(), worklifetime.SelectedForkIdentity{ExecutionID: "execution", RunID: "selected-run", Generation: 1})
					if err != nil {
						t.Fatal(err)
					}
					owner, join = selected, func() error { return selected.RetireAndWait(t.Context()) }
					retire, fence = selected.Retire, process.Fence
				}
				t.Cleanup(func() {
					if err := join(); err != nil {
						t.Error(err)
					}
					if _, err := runtime.RetireAndWait(t.Context()); err != nil {
						t.Error(err)
					}
					if _, err := process.Join(t.Context()); err != nil {
						t.Error(err)
					}
				})
				source, _ := acknowledgedRootInputEndpoint(t)
				bus, err := newScopedTestEventBus(&publicationAcknowledgementProbeStore{}, EventBusOptions{ContractBundle: source, WorkOwner: runtime})
				if err != nil {
					t.Fatal(err)
				}
				caller, cancel := context.WithCancel(worklifetime.WithOccurrence(testAuthorActivityContext(t.Context()), owner))
				defer cancel()
				preparing, lease, finishPrecommit, err := bus.beginPublicCompletion(caller)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := lease.Done(); err != nil {
						t.Error(err)
					}
				}()
				defer finishPrecommit()
				switch cut {
				case "retirement":
					retire()
				case "caller":
					cancel()
				case "fence":
					if err := fence(); err != nil {
						t.Fatal(err)
					}
					if preparing.Err() != nil || lease.Context().Err() != nil {
						t.Fatal("fence canceled admitted work")
					}
					return
				}
				select {
				case <-preparing.Done():
				case <-time.After(time.Second):
					t.Fatal("preparation did not observe cancellation")
				}
				if cut == "caller" && lease.Context().Err() != nil {
					t.Fatal("caller canceled the independent accepted lease")
				}
				if cut == "retirement" && !errors.Is(context.Cause(preparing), worklifetime.ErrRetired) {
					t.Fatalf("retirement lost its cause: %v", context.Cause(preparing))
				}
			})
		}
	}
}
