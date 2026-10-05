package effects

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/google/uuid"
)

type launchClockProbe struct {
	effectStoreProbe
	result ExternalAttemptLaunch
	err    error
}

func (p *launchClockProbe) MarkExternalAttemptLaunched(context.Context, Attempt, time.Time) (ExternalAttemptLaunch, error) {
	return p.result, p.err
}

func TestHandleRetainsAcknowledgedFirstLaunchClock(t *testing.T) {
	origin, err := DirectiveCompletionOrigin(agentcontrol.DirectiveExecutionOrigin{OperationID: uuid.NewString(), ExecutionOwnerID: uuid.NewString()})
	if err != nil {
		t.Fatal(err)
	}
	bound := &timeridentity.TurnTimeout{After: time.Minute, Emit: "investigation.aborted"}
	first := time.Now().UTC()
	clock := LogicalTurnClock{Origin: origin, FirstAttempt: uuid.NewString(), LaunchedAt: first, Timeout: bound, DeadlineAt: first.Add(bound.After), TimeoutEvent: uuid.NewString()}
	attempt := Attempt{AttemptID: clock.FirstAttempt, OperationID: uuid.NewString(), Origin: origin, TurnTimeout: timeridentity.CloneTurnTimeout(bound)}
	cleanup := errors.New("launch cleanup failed")
	probe := &launchClockProbe{result: ExternalAttemptLaunch{Committed: true, Turn: &clock}, err: NewPostCommitMutationError(MutationLaunch, attempt, cleanup)}
	handle := &Handle{controller: NewController(probe), attempt: attempt}
	if _, ok := handle.LogicalTurnClock(); ok {
		t.Fatal("unlaunched handle acquired a clock")
	}
	if err := handle.MarkLaunched(context.Background()); !CommittedMutationPhase(err, MutationLaunch, attempt) || !errors.Is(err, cleanup) {
		t.Fatalf("launch cleanup evidence lost: %v", err)
	}
	got, ok := handle.LogicalTurnClock()
	if !ok || got.Validate() != nil || !got.DeadlineAt.Equal(clock.DeadlineAt) || got.FirstAttempt != clock.FirstAttempt {
		t.Fatalf("acknowledged launch clock lost: %+v found=%t", got, ok)
	}
	got.Timeout.After = time.Hour
	again, _ := handle.LogicalTurnClock()
	if again.Timeout.After != time.Minute {
		t.Fatal("clock reader mutated admitted bound")
	}
}

func TestHandleLaunchRejectsMissingAndForeignClockEvidence(t *testing.T) {
	origin, _ := DirectiveCompletionOrigin(agentcontrol.DirectiveExecutionOrigin{OperationID: uuid.NewString(), ExecutionOwnerID: uuid.NewString()})
	other, _ := DirectiveCompletionOrigin(agentcontrol.DirectiveExecutionOrigin{OperationID: uuid.NewString(), ExecutionOwnerID: uuid.NewString()})
	bound := &timeridentity.TurnTimeout{After: time.Minute, Emit: "investigation.aborted"}
	now := time.Now().UTC()
	foreign := &LogicalTurnClock{Origin: other, FirstAttempt: uuid.NewString(), LaunchedAt: now, Timeout: bound, DeadlineAt: now.Add(bound.After), TimeoutEvent: uuid.NewString()}
	for _, tc := range []struct {
		name   string
		launch ExternalAttemptLaunch
	}{
		{"missing_acknowledgment", ExternalAttemptLaunch{}},
		{"missing_bound_clock", ExternalAttemptLaunch{Committed: true}},
		{"foreign_clock", ExternalAttemptLaunch{Committed: true, Turn: foreign}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &launchClockProbe{result: tc.launch}
			handle := &Handle{controller: NewController(probe), attempt: Attempt{AttemptID: uuid.NewString(), OperationID: uuid.NewString(), Origin: origin, TurnTimeout: bound}}
			if err := handle.MarkLaunched(context.Background()); err == nil {
				t.Fatal("invalid launch evidence admitted provider dispatch")
			}
			if _, ok := handle.LogicalTurnClock(); ok {
				t.Fatal("invalid launch evidence supplied a clock")
			}
		})
	}
}
