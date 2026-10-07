package effects

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

type turnExecutionProbe struct {
	launchClockProbe
	request func(context.Context, Attempt, time.Time) (TurnCancellation, error)
}

func (p *turnExecutionProbe) RequestTurnTimeout(ctx context.Context, attempt Attempt, at time.Time) (TurnCancellation, error) {
	return p.request(ctx, attempt, at)
}

func turnExecutionClock(t *testing.T) (LogicalTurnClock, Attempt) {
	return turnExecutionClockForOrigin(t, CompletionOrigin{Kind: CompletionOriginDirective, Directive: agentcontrol.DirectiveExecutionOrigin{OperationID: uuid.NewString(), ExecutionOwnerID: uuid.NewString()}})
}

func turnExecutionClockForOrigin(t *testing.T, origin CompletionOrigin) (LogicalTurnClock, Attempt) {
	t.Helper()
	bound := &timeridentity.TurnTimeout{After: time.Minute, Emit: "work.aborted"}
	first := time.Now().UTC()
	clock := LogicalTurnClock{Origin: origin, FirstAttempt: uuid.NewString(), LaunchedAt: first, Timeout: bound, DeadlineAt: first.Add(bound.After), TimeoutEvent: uuid.NewString()}
	if err := clock.Validate(); err != nil {
		t.Fatal(err)
	}
	return clock, Attempt{AttemptID: clock.FirstAttempt, OperationID: uuid.NewString(), Origin: origin, TurnTimeout: timeridentity.CloneTurnTimeout(bound)}
}

func TestTurnExecutionObservesOnlyExactCommittedCompletionCancellation(t *testing.T) {
	for _, mode := range []string{"timeout", "timestamp_precision", "terminate", "missing_ack", "foreign_origin", "foreign_timeout", "premature_timeout", "changed_intent", "closed"} {
		t.Run(mode, func(t *testing.T) {
			clock, attempt := turnExecutionClock(t)
			if mode == "timestamp_precision" {
				clock.LaunchedAt = clock.LaunchedAt.Truncate(time.Microsecond)
				clock.Timeout.After = time.Nanosecond
				clock.DeadlineAt = clock.LaunchedAt.Add(time.Nanosecond)
			}
			wake := make(chan time.Time)
			ctx, owner := newTurnExecution(context.Background(), func(time.Time) (<-chan time.Time, func()) { return wake, func() {} })
			defer func() { _, _ = owner.Finish() }()
			probe := &turnExecutionProbe{launchClockProbe: launchClockProbe{result: ExternalAttemptLaunch{Committed: true, Turn: &clock}}}
			handle := &Handle{controller: NewController(probe), attempt: attempt}
			if err := handle.MarkLaunched(ctx); err != nil {
				t.Fatal(err)
			}
			intent := TurnCancellation{Committed: true, Requested: true, Origin: attempt.Origin, Reason: deliverylifecycle.CancellationTurnTimeout, CauseEvent: clock.TimeoutEvent, RequestedAt: clock.DeadlineAt}
			switch mode {
			case "timestamp_precision":
				intent.RequestedAt = clock.DeadlineAt.Truncate(time.Microsecond)
			case "terminate":
				intent.Reason = deliverylifecycle.CancellationTerminate
				intent.CauseEvent = uuid.NewString()
			case "missing_ack":
				intent.Committed = false
			case "foreign_origin":
				intent.Origin.Directive.OperationID = uuid.NewString()
			case "foreign_timeout":
				intent.CauseEvent = uuid.NewString()
			case "premature_timeout":
				intent.RequestedAt = clock.LaunchedAt.Add(-time.Second)
			case "changed_intent":
				if err := observeTurnCancellation(ctx, intent); err != nil {
					t.Fatal(err)
				}
				intent.RequestedAt = intent.RequestedAt.Add(time.Second)
			case "closed":
				_, _ = owner.Finish()
			}
			err := observeTurnCancellation(ctx, intent)
			if mode == "timeout" || mode == "terminate" || mode == "timestamp_precision" {
				var authored *AuthoredTurnCancellationError
				if err != nil || !errors.As(context.Cause(ctx), &authored) || authored.Cancellation.Reason != intent.Reason {
					t.Fatalf("exact completion cancellation lost: cause=%v err=%v", context.Cause(ctx), err)
				}
				intent.CauseEvent = uuid.NewString()
				result, err := owner.Finish()
				if err != nil || result.Cancellation.CauseEvent == intent.CauseEvent {
					t.Fatalf("owner exposed mutable cancellation: %+v err=%v", result, err)
				}
			} else if err == nil {
				t.Fatalf("%s completion gained cancellation authority", mode)
			}
		})
	}
}

func TestTurnExecutionOwnsPrelaunchTerminationBeforeAuthorizationReturns(t *testing.T) {
	for _, mode := range []string{"before_ack", "after_ack", "missing_ack", "foreign_origin", "unlaunched_timeout", "closed"} {
		t.Run(mode, func(t *testing.T) {
			_, attempt := turnExecutionClock(t)
			attempt.AuthorizationAcknowledged = true
			ctx, owner := WithTurnExecution(WithDirectiveCompletionOrigin(context.Background(), attempt.Origin.Directive))
			defer func() { _, _ = owner.Finish() }()
			intent := TurnCancellation{Committed: true, Requested: true, Origin: attempt.Origin, Reason: deliverylifecycle.CancellationTerminate, CauseEvent: uuid.NewString(), RequestedAt: time.Now().UTC()}
			if mode == "after_ack" {
				if err := observeTurnAuthorization(ctx, attempt); err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case "missing_ack":
				intent.Committed = false
			case "foreign_origin":
				intent.Origin.Directive.OperationID = uuid.NewString()
			case "unlaunched_timeout":
				intent.Reason = deliverylifecycle.CancellationTurnTimeout
			case "closed":
				_, _ = owner.Finish()
			}
			matched, err := owner.RequestCancellation(intent)
			valid := mode == "before_ack" || mode == "after_ack"
			if valid {
				if !matched || err != nil || ctx.Err() == nil {
					t.Fatalf("prelaunch intent lost: matched=%t err=%v", matched, err)
				}
				if mode == "before_ack" {
					if err := observeTurnAuthorization(ctx, attempt); err != nil {
						t.Fatal(err)
					}
				}
				result, err := owner.Finish()
				if err != nil || result.Clock != nil || result.Attempt.AttemptID != attempt.AttemptID || !result.Cancellation.Origin.Same(attempt.Origin) {
					t.Fatalf("prelaunch cleanup lost exact evidence or fabricated clock: %+v err=%v", result, err)
				}
			} else if matched || ctx.Err() != nil && mode != "closed" {
				t.Fatalf("invalid/closed intent gained authority: matched=%t err=%v cause=%v", matched, err, context.Cause(ctx))
			}
		})
	}
}

func TestTurnExecutionStartsOnlyOnAcknowledgedLaunchAndNeverResets(t *testing.T) {
	clock, attempt := turnExecutionClock(t)
	wake := make(chan time.Time, 1)
	timers := 0
	ctx, owner := newTurnExecution(context.Background(), func(deadline time.Time) (<-chan time.Time, func()) {
		timers++
		if !deadline.Equal(clock.DeadlineAt) {
			t.Fatalf("deadline differs from first actual launch: %s", deadline)
		}
		return wake, func() {}
	})
	defer func() { _, _ = owner.Finish() }()
	probe := &turnExecutionProbe{request: func(context.Context, Attempt, time.Time) (TurnCancellation, error) {
		t.Error("a stopped turn requested timeout")
		return TurnCancellation{}, nil
	}}
	handle := &Handle{controller: NewController(probe), attempt: attempt}
	if err := handle.MarkLaunched(ctx); err == nil || timers != 0 {
		t.Fatalf("unacknowledged launch started the bound: timers=%d err=%v", timers, err)
	}
	probe.result = ExternalAttemptLaunch{Committed: true, Turn: &clock}
	if err := handle.MarkLaunched(ctx); err != nil || timers != 1 {
		t.Fatalf("acknowledged launch did not start the bound once: timers=%d err=%v", timers, err)
	}
	next := &Handle{controller: handle.controller, attempt: attempt}
	next.attempt.AttemptID = uuid.NewString()
	if err := next.MarkLaunched(ctx); err != nil || timers != 1 {
		t.Fatalf("tool round reset the bound: timers=%d err=%v", timers, err)
	}
	result, err := owner.Finish()
	if err != nil || result.Cancellation.Requested || result.Attempt.AttemptID != attempt.AttemptID || ctx.Err() == nil {
		t.Fatalf("finish lost owner or inferred cancellation: %+v err=%v", result, err)
	}
	if result.Clock == nil || result.Clock.Validate() != nil || result.Clock.Timeout.After != clock.Timeout.After {
		t.Fatal("logical carrier lost its exact acknowledged clock")
	}
	result.Clock.Timeout.After = time.Hour
	again, _ := owner.Finish()
	if again.Clock.Timeout.After != clock.Timeout.After {
		t.Fatal("carrier result exposed mutable clock evidence")
	}
	if err := next.MarkLaunched(ctx); err == nil {
		t.Fatal("closed logical turn admitted another execution")
	}
}

func TestTurnExecutionCancelsOnlyForExactAcknowledgedIntent(t *testing.T) {
	for _, name := range []string{"exact", "terminate_won", "cleanup_error", "foreign_origin", "missing_ack", "malformed_reason", "already_settled", "no_intent"} {
		t.Run(name, func(t *testing.T) {
			clock, attempt := turnExecutionClock(t)
			wake := make(chan time.Time, 1)
			ctx, owner := newTurnExecution(context.Background(), func(time.Time) (<-chan time.Time, func()) { return wake, func() {} })
			defer func() { _, _ = owner.Finish() }()
			cleanup := errors.New("timeout cleanup failed")
			probe := &turnExecutionProbe{launchClockProbe: launchClockProbe{result: ExternalAttemptLaunch{Committed: true, Turn: &clock}}, request: func(_ context.Context, got Attempt, at time.Time) (TurnCancellation, error) {
				if got.AttemptID != attempt.AttemptID || !at.Equal(clock.DeadlineAt) {
					t.Error("timeout substituted launch evidence")
				}
				intent := TurnCancellation{Committed: true, Requested: true, Origin: attempt.Origin, Reason: deliverylifecycle.CancellationTurnTimeout, CauseEvent: clock.TimeoutEvent, RequestedAt: at}
				var err error
				switch name {
				case "terminate_won":
					intent.Reason, intent.CauseEvent = deliverylifecycle.CancellationTerminate, uuid.NewString()
				case "cleanup_error":
					err = cleanup
				case "foreign_origin":
					intent.Origin.Directive.OperationID = uuid.NewString()
				case "missing_ack":
					intent.Committed = false
				case "malformed_reason":
					intent.Reason = "shutdown"
				case "already_settled":
					intent.OriginSettled = true
				case "no_intent":
					intent.Requested = false
				}
				return intent, err
			}}
			handle := &Handle{controller: NewController(probe), attempt: attempt}
			if err := handle.MarkLaunched(ctx); err != nil {
				t.Fatal(err)
			}
			wake <- clock.DeadlineAt
			select {
			case <-owner.done:
			case <-time.After(time.Second):
				t.Fatal("owned timeout did not finish")
			}
			var authored *AuthoredTurnCancellationError
			exact := name == "exact" || name == "cleanup_error" || name == "terminate_won"
			if errors.As(context.Cause(ctx), &authored) != exact {
				t.Fatalf("intent classification: cause=%v want authored=%t", context.Cause(ctx), exact)
			}
			result, err := owner.Finish()
			if result.Cancellation.Requested != exact || (name == "cleanup_error" && !errors.Is(err, cleanup)) {
				t.Fatalf("finish lost exact intent or cleanup: %+v err=%v", result, err)
			}
		})
	}
}

func TestTurnExecutionFinishJoinsInFlightTimeoutCommit(t *testing.T) {
	clock, attempt := turnExecutionClock(t)
	wake := make(chan time.Time, 1)
	entered, release := make(chan struct{}), make(chan struct{})
	ctx, owner := newTurnExecution(context.Background(), func(time.Time) (<-chan time.Time, func()) { return wake, func() {} })
	probe := &turnExecutionProbe{launchClockProbe: launchClockProbe{result: ExternalAttemptLaunch{Committed: true, Turn: &clock}}, request: func(context.Context, Attempt, time.Time) (TurnCancellation, error) {
		close(entered)
		<-release
		return TurnCancellation{Committed: true}, nil
	}}
	handle := &Handle{controller: NewController(probe), attempt: attempt}
	if err := handle.MarkLaunched(ctx); err != nil {
		t.Fatal(err)
	}
	wake <- clock.DeadlineAt
	<-entered
	finished := make(chan struct{})
	go func() { _, _ = owner.Finish(); close(finished) }()
	<-owner.stop
	select {
	case <-finished:
		t.Fatal("logical owner escaped an outstanding timeout transaction")
	default:
	}
	close(release)
	<-finished
}

func TestTurnExecutionShutdownDoesNotInventAuthoredCancellation(t *testing.T) {
	clock, attempt := turnExecutionClock(t)
	parent, stop := context.WithCancelCause(context.Background())
	wake := make(chan time.Time, 1)
	ctx, owner := newTurnExecution(parent, func(time.Time) (<-chan time.Time, func()) { return wake, func() {} })
	called := make(chan struct{}, 1)
	probe := &turnExecutionProbe{launchClockProbe: launchClockProbe{result: ExternalAttemptLaunch{Committed: true, Turn: &clock}}, request: func(context.Context, Attempt, time.Time) (TurnCancellation, error) {
		called <- struct{}{}
		return TurnCancellation{}, nil
	}}
	handle := &Handle{controller: NewController(probe), attempt: attempt}
	if err := handle.MarkLaunched(ctx); err != nil {
		t.Fatal(err)
	}
	shutdown := errors.New("operational shutdown")
	stop(shutdown)
	wake <- clock.DeadlineAt
	result, err := owner.Finish()
	if err != nil || result.Cancellation.Requested || !errors.Is(context.Cause(ctx), shutdown) {
		t.Fatalf("shutdown changed meaning: %+v cause=%v err=%v", result, context.Cause(ctx), err)
	}
	select {
	case <-called:
		t.Fatal("shutdown requested authored intent")
	default:
	}
}

func TestCompletionObservationRetainsImmutableCancellationEvidence(t *testing.T) {
	clock, _ := turnExecutionClock(t)
	intent := TurnCancellation{Committed: true, Requested: true, Origin: clock.Origin, Reason: deliverylifecycle.CancellationTurnTimeout, CauseEvent: clock.TimeoutEvent, RequestedAt: clock.DeadlineAt}
	ctx, observe := WithCompletionSettlementObserver(context.Background())
	recordCompletionSettlementObservation(ctx, CompletionSettlementObservation{Origin: clock.Origin, Cancellation: &intent})
	intent.Reason = "shutdown"
	first := observe()
	if first.Cancellation == nil || first.Cancellation.Reason != deliverylifecycle.CancellationTurnTimeout {
		t.Fatal("observer retained mutable producer cancellation data")
	}
	first.Cancellation.Reason = "panic"
	again := CompletionSettlementObservationFromContext(ctx)
	if again.Cancellation == nil || again.Cancellation.Reason != deliverylifecycle.CancellationTurnTimeout {
		t.Fatal("observer exposed mutable cancellation data to a reader")
	}
}
