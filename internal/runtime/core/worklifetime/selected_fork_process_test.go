package worklifetime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/google/uuid"
)

func TestProcessSelectedForkSurvivesLoadedRuntimeReplacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	process := NewProcess()
	loaded, err := process.NewRuntime(ctx, RuntimeIdentity{RuntimeInstanceID: uuid.NewString(), BundleHash: "loaded-source"})
	if err != nil {
		t.Fatal(err)
	}
	request, cancelRequest := context.WithCancel(ctx)
	preparation, err := process.Begin(request)
	if err != nil {
		t.Fatal(err)
	}
	identity := SelectedForkIdentity{ExecutionID: uuid.NewString(), RunID: uuid.NewString(), Generation: 1}
	selected, err := process.NewSelectedFork(preparation.Context(), identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := preparation.Done(); err != nil {
		t.Fatal(err)
	}
	cancelRequest()
	if _, err := loaded.RetireAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	successor, err := process.NewRuntime(ctx, RuntimeIdentity{RuntimeInstanceID: uuid.NewString(), BundleHash: "changed-loaded-source"})
	if err != nil {
		t.Fatal(err)
	}
	work, err := selected.Begin(ctx)
	if err != nil {
		t.Fatalf("loaded replacement or request loss retired selected context: %v", err)
	}
	if selected.Identity() != identity {
		t.Fatal("selected identity changed with loaded replacement")
	}
	if work.Context().Err() != nil {
		t.Fatal("selected work retained cancelled request or retired runtime")
	}
	if owner, ok := ProcessFromContext(work.Context()); !ok || owner != process {
		t.Fatal("selected work lost exact process owner")
	}
	if err := work.Done(); err != nil {
		t.Fatal(err)
	}
	if err := selected.RetireAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	control, err := successor.Begin(ctx)
	if err != nil {
		t.Fatalf("selected retirement affected live successor: %v", err)
	}
	if err := control.Done(); err != nil {
		t.Fatal(err)
	}
	if _, err := successor.RetireAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	receipt, err := process.Join(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.ValidateJoinReceipt(receipt); err != nil {
		t.Fatal(err)
	}
	if err := NewProcess().ValidateJoinReceipt(receipt); err == nil {
		t.Fatal("foreign process accepted selected owner's join receipt")
	}
}

func TestProcessSelectedForkRetirementJoinsAcceptedWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	process := NewProcess()
	selected, err := process.NewSelectedFork(ctx, SelectedForkIdentity{ExecutionID: uuid.NewString(), RunID: uuid.NewString(), Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	work, err := selected.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	process.Retire()
	select {
	case <-work.Context().Done():
	case <-ctx.Done():
		t.Fatal("process retirement did not cancel selected work")
	}
	if _, err := selected.Begin(ctx); !errors.Is(err, ErrRetired) {
		t.Fatalf("selected admission after process retirement = %v", err)
	}
	// An already-cancelled wait is a deterministic probe of outstanding work;
	// no sleep or scheduler delay establishes whether retirement joined.
	noWait, cancelWait := context.WithCancel(ctx)
	cancelWait()
	if receipt, err := process.Join(noWait); !errors.Is(err, context.Canceled) || receipt != nil {
		t.Fatalf("process released with accepted selected work: receipt=%v err=%v", receipt, err)
	}
	if err := selected.RetireAndWait(noWait); !errors.Is(err, context.Canceled) {
		t.Fatalf("selected retirement ignored accepted work: %v", err)
	}
	if err := work.Done(); err != nil {
		t.Fatal(err)
	}
	if err := work.Done(); !errors.Is(err, ErrAlreadySettled) {
		t.Fatalf("duplicate selected settlement = %v", err)
	}
	if err := selected.RetireAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	receipt, err := process.Join(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.ValidateJoinReceipt(receipt); err != nil {
		t.Fatal(err)
	}
}

func TestProcessRetirementBeforeSelectedBindingRetainsPreparation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	process := NewProcess()
	preparation, err := process.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	process.Retire()
	if _, err := process.NewSelectedFork(ctx, SelectedForkIdentity{ExecutionID: uuid.NewString(), RunID: uuid.NewString(), Generation: 1}); !errors.Is(err, ErrRetired) {
		t.Fatalf("post-retirement selected binding = %v", err)
	}
	noWait, cancelWait := context.WithCancel(ctx)
	cancelWait()
	if receipt, err := process.Join(noWait); !errors.Is(err, context.Canceled) || receipt != nil {
		t.Fatalf("process released before preparation disposition: receipt=%v err=%v", receipt, err)
	}
	if err := preparation.Done(); err != nil {
		t.Fatal(err)
	}
	if _, err := process.Join(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSelectedForkWorkProjectsOwningProcess(t *testing.T) {
	for _, parent := range []string{"process", "runtime"} {
		t.Run(parent, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			process := NewProcess()
			foreign := WithProcess(ctx, NewProcess())
			preparation, err := process.Begin(foreign)
			if err != nil {
				t.Fatal(err)
			}
			if owner, ok := ProcessFromContext(preparation.Context()); !ok || owner != process {
				t.Fatal("preparation inherited foreign process")
			}
			if err := preparation.Done(); err != nil {
				t.Fatal(err)
			}
			var loaded *RuntimeOccurrence
			var selected *SelectedForkOccurrence
			identity := SelectedForkIdentity{ExecutionID: uuid.NewString(), RunID: uuid.NewString(), Generation: 1}
			if parent == "runtime" {
				loaded, err = process.NewRuntime(ctx, RuntimeIdentity{RuntimeInstanceID: uuid.NewString(), BundleHash: "loaded-source"})
				if err != nil {
					t.Fatal(err)
				}
				selected, err = loaded.NewSelectedFork(foreign, identity)
			} else {
				selected, err = process.NewSelectedFork(foreign, identity)
			}
			if err != nil {
				t.Fatal(err)
			}
			manager, err := NewManagerRunOccurrence(foreign, selected, ManagerRunIdentity{Generation: 1})
			if err != nil {
				t.Fatal(err)
			}
			route, err := selected.NewRoute(foreign, RouteIdentity{
				RuntimeEpoch: 1, Generation: 1, Agent: agentidentitytest.RootRuntime(t, "selected-agent", "selected-process-test"),
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, consumer := range []struct {
				name  string
				begin func(context.Context) (*Lease, error)
			}{
				{"selected", selected.Begin}, {"selected_standing", selected.BeginStanding},
				{"manager", func(ctx context.Context) (*Lease, error) { return manager.Begin(ctx, nil) }},
				{"manager_standing", manager.BeginStanding}, {"route", route.Begin},
			} {
				t.Run(consumer.name, func(t *testing.T) {
					lease, err := consumer.begin(foreign)
					if err != nil {
						t.Fatal(err)
					}
					if owner, ok := ProcessFromContext(lease.Context()); !ok || owner != process {
						t.Fatal("selected consumer lost or borrowed process authority")
					}
					if err := lease.Done(); err != nil {
						t.Fatal(err)
					}
				})
			}
			if err := route.RetireAndWait(ctx); err != nil {
				t.Fatal(err)
			}
			if err := manager.RetireAndWait(ctx); err != nil {
				t.Fatal(err)
			}
			if err := selected.RetireAndWait(ctx); err != nil {
				t.Fatal(err)
			}
			if loaded != nil {
				if _, err := loaded.RetireAndWait(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := process.Join(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
