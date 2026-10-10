package genericschedule

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/google/uuid"
)

type scopedWakeupProofStore struct {
	lifecycleProofStore
	rows       []Activation
	err        error
	runID      string
	fresh      []string
	globalRead bool
}

func (s *scopedWakeupProofStore) ListActiveGenericScheduleActivationsForRun(_ context.Context, runID string) ([]Activation, error) {
	s.runID = runID
	return s.rows, s.err
}

func (s *scopedWakeupProofStore) ListActiveGenericScheduleActivations(context.Context) ([]Activation, error) {
	s.globalRead = true
	return nil, errors.New("global census forbidden")
}

func (s *scopedWakeupProofStore) LoadGenericScheduleActivation(ctx context.Context, id string) (Activation, bool, error) {
	s.fresh = append(s.fresh, id)
	return s.lifecycleProofStore.LoadGenericScheduleActivation(ctx, id)
}

func TestRunWakeupReconciliationUsesExactChildAndFreshClaim(t *testing.T) {
	for _, progressed := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "cancelled_after_census"}[progressed], func(t *testing.T) {
			activation := forkJoinOriginTestActivation(t)
			store := &scopedWakeupProofStore{rows: []Activation{activation}, lifecycleProofStore: lifecycleProofStore{activation: activation}}
			if progressed {
				store.activation.Status, store.activation.CancelCause = StatusCancelled, "stage_exit"
				store.activation.CancelledAt = activation.AdmittedAt.Add(time.Second)
			}
			scheduler := &lifecycleProofScheduler{}
			lifecycle, err := NewLifecycle(store, scheduler, &lifecycleProofPlanner{}, &lifecycleProofDispatcher{}, nil, executionposture.Live)
			if err != nil {
				t.Fatal(err)
			}
			defer stopLifecycleProof(t, lifecycle)
			ctx := correlation.WithRunID(t.Context(), activation.Command.RunID)
			if err := lifecycle.ReconcileRunWakeups(ctx, activation.Command.RunID); err != nil {
				t.Fatal(err)
			}
			if store.globalRead || store.runID != activation.Command.RunID || !reflect.DeepEqual(store.fresh, []string{activation.ID}) {
				t.Fatalf("nonexact reconciliation: global=%t run=%s fresh=%v", store.globalRead, store.runID, store.fresh)
			}
			if progressed {
				if len(scheduler.registered) != 0 || len(scheduler.retired) != 1 {
					t.Fatal("census resurrected a cancelled child")
				}
			} else if len(scheduler.registered) != 1 || scheduler.registered[0].ActivationID() != activation.ID {
				t.Fatal("exact child was not freshly claimed")
			}
		})
	}
}

func TestRunWakeupReconciliationValidatesCompleteCensusBeforeClaims(t *testing.T) {
	for _, fault := range []string{"foreign_context", "foreign_row", "duplicate", "nonactive", "corrupt", "read_error"} {
		t.Run(fault, func(t *testing.T) {
			activation := forkJoinOriginTestActivation(t)
			bad := activation
			store := &scopedWakeupProofStore{lifecycleProofStore: lifecycleProofStore{activation: activation}}
			ctx := correlation.WithRunID(t.Context(), activation.Command.RunID)
			switch fault {
			case "foreign_context":
				ctx = correlation.WithRunID(ctx, uuid.NewString())
			case "foreign_row":
				bad = forkJoinOriginTestActivation(t)
			case "nonactive":
				bad.Status, bad.CancelCause, bad.CancelledAt = StatusCancelled, "stage_exit", bad.AdmittedAt
			case "corrupt":
				bad.ImmutableHash = "bad"
			case "read_error":
				store.err = errors.New("child census failed")
			}
			store.rows = []Activation{activation, bad}
			scheduler := &lifecycleProofScheduler{}
			lifecycle, err := NewLifecycle(store, scheduler, &lifecycleProofPlanner{}, &lifecycleProofDispatcher{}, nil, executionposture.Live)
			if err != nil {
				t.Fatal(err)
			}
			defer stopLifecycleProof(t, lifecycle)
			if err := lifecycle.ReconcileRunWakeups(ctx, activation.Command.RunID); err == nil {
				t.Fatal("invalid census installed child wakeups")
			}
			if len(store.fresh) != 0 || len(scheduler.registered) != 0 || store.globalRead {
				t.Fatal("invalid complete census performed partial reconciliation")
			}
		})
	}
}
