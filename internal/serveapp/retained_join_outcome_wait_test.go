package serveapp

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func readNativeRetainedRootJoins(t *testing.T, reader pipeline.WorkflowTargetPersistenceReader, runID string) (loopruntime.Activation, []joinruntime.Activation) {
	t.Helper()
	if reader == nil {
		t.Fatal("retained root join observation requires its original workflow reader")
	}
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(".", flowidentity.LogicalInstanceID(runID), runID))
	if err != nil {
		t.Fatal(err)
	}
	record, err := reader.LoadWorkflowTargetPersistence(t.Context(), owner, identity.NormalizeEntityID(runID))
	if err != nil {
		t.Fatal(err)
	}
	instance, err := record.DecodeComplete(owner.Route, identity.NormalizeEntityID(runID))
	if err != nil {
		t.Fatal(err)
	}
	loop, found, err := loopruntime.Load(instance.StateBuckets, ".", "revision")
	if err != nil || !found {
		t.Fatalf("missing exact root loop: found=%t err=%v", found, err)
	}
	joins, err := joinruntime.List(instance.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	return loop, joins
}

func waitServedRetainedJoinOutcome(ctx context.Context, runID string, want timeridentity.JoinRef, read func() (loopruntime.Activation, []joinruntime.Activation)) error {
	if read == nil {
		return fmt.Errorf("exact retained join wait requires its canonical reader")
	}
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		loop, joins := read()
		if err := ctx.Err(); err != nil {
			return err
		}
		complete, err := retainedJoinOutcomeComplete(runID, want, loop, joins)
		if err != nil || complete {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func retainedJoinOutcomeComplete(runID string, want timeridentity.JoinRef, loop loopruntime.Activation, joins []joinruntime.Activation) (bool, error) {
	entry := want.StageEntry()
	if runID == "" || !want.Valid() || entry.Validate() != nil || entry.RunID != runID || entry.EntityID != runID || entry.InstanceID != runID || entry.InstancePath != runID {
		return false, fmt.Errorf("exact retained join wait requires the captured root/run/entry reference")
	}
	if err := loop.Validate(); err != nil {
		return false, err
	}
	if loop.Generation() != want.Generation() {
		return false, fmt.Errorf("captured retained join generation is not the current loop generation")
	}
	for _, join := range joins {
		if !join.JoinRef().Equal(want) {
			continue
		}
		if err := join.Validate(); err != nil {
			return false, err
		}
		if join.Status == joinruntime.StatusClosed && join.CloseReason != joinruntime.CloseReasonComplete {
			return false, fmt.Errorf("captured retained join closed without its expected completion: %s", join.CloseReason)
		}
		return join.Status == joinruntime.StatusClosed && join.TimerCancelled && !join.OutcomePending && join.OutcomeFired, nil
	}
	return false, fmt.Errorf("captured retained join entry is missing")
}

func TestServedRetainedJoinOutcomeWaitRejectsUnrelatedOrUnfiredEvidence(t *testing.T) {
	at := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	loop, err := loopruntime.New("run", "run", ".", "revision", "revision_id", "start", "working", 3, at)
	if err != nil {
		t.Fatal(err)
	}
	first := retainedJoinWaitArm(t, loop, "controller", "first", at)
	first.TimerCancelled, first.OutcomePending, first.OutcomeFired = true, false, true
	current := loop
	if _, err := current.Repeat("working", "retry", at); err != nil {
		t.Fatal(err)
	}
	second := retainedJoinWaitArm(t, current, "controller", "second", at)
	sibling := retainedJoinWaitArm(t, loop, "sibling", "first", at)
	otherEntry := retainedJoinWaitArm(t, loop, "controller", "another-entry", at)
	open := first
	open.Status, open.CloseReason = joinruntime.StatusOpen, ""
	open.TimerCancelled, open.OutcomePending, open.OutcomeFired = false, false, false
	canceled := first
	canceled.CloseReason, canceled.OutcomeFired = joinruntime.CloseReasonStageExit, false
	malformed := first
	malformed.OutcomePending = true

	for _, tc := range []struct {
		name     string
		run      string
		want     timeridentity.JoinRef
		loop     loopruntime.Activation
		joins    []joinruntime.Activation
		complete bool
		invalid  bool
	}{
		{"exact_fired", "run", first.JoinRef(), loop, []joinruntime.Activation{first}, true, false},
		{"open", "run", first.JoinRef(), loop, []joinruntime.Activation{open}, false, false},
		{"pending", "run", second.JoinRef(), current, []joinruntime.Activation{first, second}, false, false},
		{"missing", "run", first.JoinRef(), loop, nil, false, true},
		{"foreign_run", "foreign", first.JoinRef(), loop, []joinruntime.Activation{first}, false, true},
		{"sibling", "run", first.JoinRef(), loop, []joinruntime.Activation{sibling}, false, true},
		{"wrong_entry", "run", first.JoinRef(), loop, []joinruntime.Activation{otherEntry}, false, true},
		{"historical_fired", "run", first.JoinRef(), current, []joinruntime.Activation{first, second}, false, true},
		{"malformed_reference", "run", timeridentity.JoinRef{}, loop, []joinruntime.Activation{first}, false, true},
		{"contradictory_outcome", "run", first.JoinRef(), loop, []joinruntime.Activation{malformed}, false, true},
		{"canceled_entry", "run", first.JoinRef(), loop, []joinruntime.Activation{canceled}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			complete, err := retainedJoinOutcomeComplete(tc.run, tc.want, tc.loop, tc.joins)
			if complete != tc.complete || (err != nil) != tc.invalid {
				t.Fatalf("exact outcome=%t err=%v, want complete=%t invalid=%t", complete, err, tc.complete, tc.invalid)
			}
		})
	}
}

func retainedJoinWaitArm(t *testing.T, loop loopruntime.Activation, node, entryID string, at time.Time) joinruntime.Activation {
	t.Helper()
	ref, err := timeridentity.NewJoinRef(identitytest.RootNode(t, node), "review.requested", "working", "reviews")
	if err != nil {
		t.Fatal(err)
	}
	ref, err = ref.BindStageEntry(timeridentity.StageEntryRef{
		RunID: "run", FlowScope: ".", InstanceID: "run", InstancePath: "run", EntityID: "run",
		Stage: "working", Cause: "delivery", EventID: entryID, OccurrenceID: entryID, TransitionID: "transition",
	}, loop.Generation())
	if err != nil {
		t.Fatal(err)
	}
	arm, err := joinruntime.NewActivation(ref, []string{"member"}, nil, at, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := arm.Add("member", map[string]any{"value": 1}); err != nil {
		t.Fatal(err)
	}
	arm.Close(joinruntime.CloseReasonComplete, true, false)
	return arm
}
