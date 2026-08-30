package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

type fanOutFailureTestPlan string

func (p fanOutFailureTestPlan) DurablePublicationEventID() string { return string(p) }
func (p fanOutFailureTestPlan) ValidateDurablePublicationPlan() error {
	if p == "" {
		return fmt.Errorf("test plan requires identity")
	}
	return nil
}

type fanOutFailureTestPlanner struct {
	released [][]string
}

func (*fanOutFailureTestPlanner) PrepareEnginePublications(context.Context, []runtimeengine.EmitIntent) ([]runtimeengine.DurablePublicationPlan, error) {
	return nil, fmt.Errorf("failure-algebra test does not prepare plans")
}

func (p *fanOutFailureTestPlanner) ReleaseEnginePublications(_ context.Context, plans []runtimeengine.DurablePublicationPlan) error {
	ids := make([]string, 0, len(plans))
	for _, plan := range plans {
		ids = append(ids, plan.DurablePublicationEventID())
	}
	p.released = append(p.released, ids)
	return nil
}

func (*fanOutFailureTestPlanner) FinalizeEnginePublications(context.Context, []runtimeengine.CommittedDurablePublication) error {
	return fmt.Errorf("failure-algebra test does not finalize plans")
}

type fanOutFailureTestOwner struct {
	commit       func(FanOutChunkCommand) (CommittedFanOutChunk, error)
	commands     []FanOutChunkCommand
	retryRelease []FanOutRetryableRelease
	blocks       []FanOutBlockRequest
}

func (*fanOutFailureTestOwner) ClaimFanOutIntent(context.Context, FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
	return fanoutobligation.Intent{}, fanoutobligation.Claim{}, false, fmt.Errorf("failure-algebra test does not claim")
}

func (*fanOutFailureTestOwner) LoadFanOutEvaluation(context.Context, fanoutobligation.Claim) (FanOutEvaluationInput, error) {
	return FanOutEvaluationInput{}, fmt.Errorf("failure-algebra test does not load")
}

func (o *fanOutFailureTestOwner) CommitFanOutChunk(_ context.Context, command FanOutChunkCommand) (CommittedFanOutChunk, error) {
	o.commands = append(o.commands, command)
	return o.commit(command)
}

func (*fanOutFailureTestOwner) ReleaseFanOutClaim(context.Context, fanoutobligation.Claim) error {
	return fmt.Errorf("outer turn owns ordinary release")
}

func (o *fanOutFailureTestOwner) ReleaseFanOutRetryable(_ context.Context, release FanOutRetryableRelease) error {
	o.retryRelease = append(o.retryRelease, release)
	return nil
}

func (o *fanOutFailureTestOwner) BlockFanOutClaim(_ context.Context, block FanOutBlockRequest) error {
	o.blocks = append(o.blocks, block)
	return nil
}

func (*fanOutFailureTestOwner) CancelRunFanOut(context.Context, string, string, time.Time) error {
	return fmt.Errorf("failure-algebra test does not cancel")
}

func (*fanOutFailureTestOwner) FanOutRunSummary(context.Context, string, time.Time) (fanoutobligation.RunSummary, error) {
	return fanoutobligation.RunSummary{}, fmt.Errorf("failure-algebra test does not summarize")
}

func TestFanOutCommitFailureAlgebraIsClosed(t *testing.T) {
	claim := fanOutFailureTestClaim()
	now := time.Now().UTC()
	itemFailure := fanOutFailureEnvelope(t, runtimefailures.ClassSchemaInvalid, "fan_out_test_item_invalid")
	aggregateFailure := fanOutFailureEnvelope(t, runtimefailures.ClassSchemaInvalid, "fan_out_test_aggregate_invalid")
	retryableFailure := runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "fan_out_test_store_unavailable", "test", "commit", nil)
	uncertainFailure := runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "fan_out_test_commit_unconfirmed", "test", "commit", nil)

	t.Run("only exact item evidence rejects its ordinal", func(t *testing.T) {
		for _, ordinal := range []int{0, 1, 2} {
			t.Run(fmt.Sprintf("ordinal_%d", ordinal), func(t *testing.T) {
				planner := &fanOutFailureTestPlanner{}
				calls := 0
				owner := &fanOutFailureTestOwner{commit: func(command FanOutChunkCommand) (CommittedFanOutChunk, error) {
					calls++
					if calls == 1 {
						return CommittedFanOutChunk{}, NewFanOutItemSemanticError(ordinal, itemFailure, errors.New("item invalid"))
					}
					if command.Outcomes[ordinal].Publication != nil || len(command.Outcomes[ordinal].Failure) == 0 {
						t.Fatalf("second command did not isolate ordinal %d: %#v", ordinal, command.Outcomes[ordinal])
					}
					return fanOutFailureCommitted(command, fanoutobligation.StatusClosed), nil
				}}
				committed, suppress, err := new(PipelineCoordinator).commitFanOutRange(context.Background(), owner, planner, claim, fanOutFailureOutcomes(0, 3), now)
				if err != nil || suppress || committed.Intent.Cursor != 3 || len(owner.commands) != 2 || fmt.Sprint(planner.released) != fmt.Sprintf("[[event-%d]]", ordinal) || len(owner.blocks) != 0 {
					t.Fatalf("item isolation = committed:%#v suppress:%v err:%v commands:%d released:%v blocks:%d", committed.Intent, suppress, err, len(owner.commands), planner.released, len(owner.blocks))
				}
			})
		}
	})

	t.Run("multiple item failures preserve all other ordinal plans", func(t *testing.T) {
		planner := &fanOutFailureTestPlanner{}
		calls := 0
		owner := &fanOutFailureTestOwner{commit: func(command FanOutChunkCommand) (CommittedFanOutChunk, error) {
			calls++
			switch calls {
			case 1:
				return CommittedFanOutChunk{}, NewFanOutItemSemanticError(1, itemFailure, errors.New("first item invalid"))
			case 2:
				return CommittedFanOutChunk{}, NewFanOutItemSemanticError(3, itemFailure, errors.New("second item invalid"))
			default:
				for ordinal, outcome := range command.Outcomes {
					if ordinal == 1 || ordinal == 3 {
						if outcome.Publication != nil || len(outcome.Failure) == 0 {
							t.Fatalf("rejected ordinal %d = %#v", ordinal, outcome)
						}
					} else if outcome.Publication == nil || len(outcome.Failure) != 0 {
						t.Fatalf("valid ordinal %d = %#v", ordinal, outcome)
					}
				}
				return fanOutFailureCommitted(command, fanoutobligation.StatusClosed), nil
			}
		}}
		committed, suppress, err := new(PipelineCoordinator).commitFanOutRange(context.Background(), owner, planner, claim, fanOutFailureOutcomes(0, 5), now)
		if err != nil || suppress || committed.Intent.Cursor != 5 || len(owner.commands) != 3 || fmt.Sprint(planner.released) != "[[event-1] [event-3]]" || len(owner.blocks) != 0 {
			t.Fatalf("multi-item isolation = cursor:%d suppress:%v err:%v commands:%d released:%v blocks:%d", committed.Intent.Cursor, suppress, err, len(owner.commands), planner.released, len(owner.blocks))
		}
	})

	t.Run("only safe aggregate evidence bisects lower half first", func(t *testing.T) {
		planner := &fanOutFailureTestPlanner{}
		calls := 0
		owner := &fanOutFailureTestOwner{commit: func(command FanOutChunkCommand) (CommittedFanOutChunk, error) {
			calls++
			if calls == 1 {
				return CommittedFanOutChunk{}, NewFanOutSafeAggregateError(aggregateFailure, errors.New("aggregate invalid"))
			}
			if len(command.Outcomes) != 2 || command.Outcomes[0].Ordinal != 0 || command.Outcomes[1].Ordinal != 1 {
				t.Fatalf("bisection did not retry the lower half: %#v", command.Outcomes)
			}
			return fanOutFailureCommitted(command, fanoutobligation.StatusOpen), nil
		}}
		committed, suppress, err := new(PipelineCoordinator).commitFanOutRange(context.Background(), owner, planner, claim, fanOutFailureOutcomes(0, 4), now)
		if err != nil || suppress || committed.Intent.Cursor != 2 || fmt.Sprint(planner.released) != "[[event-2 event-3]]" || len(owner.blocks) != 0 {
			t.Fatalf("aggregate isolation = cursor:%d suppress:%v err:%v released:%v blocks:%d", committed.Intent.Cursor, suppress, err, planner.released, len(owner.blocks))
		}
	})

	t.Run("recursive aggregate bisection commits the lowest prefix first", func(t *testing.T) {
		planner := &fanOutFailureTestPlanner{}
		calls := 0
		owner := &fanOutFailureTestOwner{commit: func(command FanOutChunkCommand) (CommittedFanOutChunk, error) {
			calls++
			if calls <= 2 {
				return CommittedFanOutChunk{}, NewFanOutSafeAggregateError(aggregateFailure, errors.New("aggregate invalid"))
			}
			if len(command.Outcomes) != 2 || command.Outcomes[0].Ordinal != 0 || command.Outcomes[1].Ordinal != 1 {
				t.Fatalf("recursive bisection committed non-lowest prefix: %#v", command.Outcomes)
			}
			return fanOutFailureCommitted(command, fanoutobligation.StatusOpen), nil
		}}
		committed, suppress, err := new(PipelineCoordinator).commitFanOutRange(context.Background(), owner, planner, claim, fanOutFailureOutcomes(0, 8), now)
		if err != nil || suppress || committed.Intent.Cursor != 2 || len(owner.commands) != 3 || fmt.Sprint(planner.released) != "[[event-4 event-5 event-6 event-7] [event-2 event-3]]" {
			t.Fatalf("recursive bisection = cursor:%d suppress:%v err:%v commands:%d released:%v", committed.Intent.Cursor, suppress, err, len(owner.commands), planner.released)
		}
	})

	t.Run("size-one aggregate and unknown errors block without progress", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			err  error
		}{
			{name: "aggregate", err: NewFanOutSafeAggregateError(aggregateFailure, errors.New("aggregate invalid"))},
			{name: "unknown", err: errors.New("raw sql text must not become semantic authority")},
		} {
			t.Run(tc.name, func(t *testing.T) {
				planner := &fanOutFailureTestPlanner{}
				owner := &fanOutFailureTestOwner{commit: func(FanOutChunkCommand) (CommittedFanOutChunk, error) { return CommittedFanOutChunk{}, tc.err }}
				_, suppress, err := new(PipelineCoordinator).commitFanOutRange(context.Background(), owner, planner, claim, fanOutFailureOutcomes(0, 1), now)
				if err == nil || !suppress || len(owner.commands) != 1 || len(owner.blocks) != 1 || fmt.Sprint(planner.released) != "[[event-0]]" || len(owner.retryRelease) != 0 {
					t.Fatalf("blocking = suppress:%v err:%v commands:%d blocks:%d retry:%d released:%v", suppress, err, len(owner.commands), len(owner.blocks), len(owner.retryRelease), planner.released)
				}
			})
		}
	})

	t.Run("retryable store failure releases and halves without progress", func(t *testing.T) {
		planner := &fanOutFailureTestPlanner{}
		owner := &fanOutFailureTestOwner{commit: func(FanOutChunkCommand) (CommittedFanOutChunk, error) {
			return CommittedFanOutChunk{}, retryableFailure
		}}
		_, suppress, err := new(PipelineCoordinator).commitFanOutRange(context.Background(), owner, planner, claim, fanOutFailureOutcomes(0, 4), now)
		if err == nil || !suppress || len(owner.retryRelease) != 1 || len(owner.blocks) != 0 || fmt.Sprint(planner.released) != "[[event-0 event-1 event-2 event-3]]" {
			t.Fatalf("retryable = suppress:%v err:%v retry:%d blocks:%d released:%v", suppress, err, len(owner.retryRelease), len(owner.blocks), planner.released)
		}
	})

	t.Run("outcome uncertain retains claim and plans for reconciliation", func(t *testing.T) {
		planner := &fanOutFailureTestPlanner{}
		owner := &fanOutFailureTestOwner{commit: func(FanOutChunkCommand) (CommittedFanOutChunk, error) {
			return CommittedFanOutChunk{}, uncertainFailure
		}}
		_, suppress, err := new(PipelineCoordinator).commitFanOutRange(context.Background(), owner, planner, claim, fanOutFailureOutcomes(0, 4), now)
		if err == nil || !suppress || len(owner.retryRelease) != 0 || len(owner.blocks) != 0 || len(planner.released) != 0 {
			t.Fatalf("uncertain = suppress:%v err:%v retry:%d blocks:%d released:%v", suppress, err, len(owner.retryRelease), len(owner.blocks), planner.released)
		}
	})

	t.Run("caller cancellation releases plans without tuning or blocking", func(t *testing.T) {
		planner := &fanOutFailureTestPlanner{}
		owner := &fanOutFailureTestOwner{commit: func(FanOutChunkCommand) (CommittedFanOutChunk, error) {
			return CommittedFanOutChunk{}, context.Canceled
		}}
		_, suppress, err := new(PipelineCoordinator).commitFanOutRange(context.Background(), owner, planner, claim, fanOutFailureOutcomes(0, 2), now)
		if !errors.Is(err, context.Canceled) || suppress || len(owner.retryRelease) != 0 || len(owner.blocks) != 0 || fmt.Sprint(planner.released) != "[[event-0 event-1]]" {
			t.Fatalf("cancellation = suppress:%v err:%v retry:%d blocks:%d released:%v", suppress, err, len(owner.retryRelease), len(owner.blocks), planner.released)
		}
	})
}

func TestFanOutBlockedTurnCausePreservesTypedFailuresAndNamesRawFailureStage(t *testing.T) {
	intent := fanoutobligation.Intent{
		Request: fanoutobligation.IntentRequest{Key: fanoutobligation.IntentKey{
			RunID: "run-1", TriggeringDeliveryID: "delivery-1",
			ElementRef: runtimecontracts.FanOutElementRef{FlowPath: ".", Family: "fan_out", SemanticPath: `nodes["fanout"].handlers["work.requested"].fan_out`},
		}},
		Cursor: 4,
	}

	typed := runtimefailures.New(
		runtimefailures.ClassDependencyUnavailable,
		"fan_out_test_dependency_unavailable",
		"test.owner",
		"read",
		nil,
	)
	if got := fanOutBlockedTurnCause("load_evaluation", -1, intent, typed); got != typed {
		t.Fatalf("typed cause identity changed: got %v, want %v", got, typed)
	}

	raw := errors.New("driver returned an untyped failure")
	wrapped := fanOutBlockedTurnCause("prepare_publication", 7, intent, raw)
	if !errors.Is(wrapped, raw) {
		t.Fatalf("wrapped cause does not preserve source error: %v", wrapped)
	}
	failure, ok := runtimefailures.As(wrapped)
	if !ok {
		t.Fatalf("wrapped cause is not typed: %v", wrapped)
	}
	if failure.Failure.Class != runtimefailures.ClassInternalFailure ||
		failure.Failure.Detail.Code != "fan_out_prepare_publication_failed" ||
		failure.Failure.Component != "runtime.fan_out" ||
		failure.Failure.Operation != "prepare_publication" {
		t.Fatalf("failure identity = %#v", failure.Failure)
	}
	want := map[string]any{
		"run_id": "run-1", "triggering_delivery_id": "delivery-1",
		"flow_path": ".", "family": "fan_out",
		"semantic_path": `nodes["fanout"].handlers["work.requested"].fan_out`,
		"cursor":        4, "ordinal": 7, "cause": raw.Error(),
	}
	if fmt.Sprint(failure.Failure.Detail.Attributes) != fmt.Sprint(want) {
		t.Fatalf("failure attributes = %#v, want %#v", failure.Failure.Detail.Attributes, want)
	}
}

func TestFanOutPrecommitRetryReleasesPlansAndAdaptivelyReleasesClaim(t *testing.T) {
	claim := fanOutFailureTestClaim()
	planner := &fanOutFailureTestPlanner{}
	owner := &fanOutFailureTestOwner{commit: func(FanOutChunkCommand) (CommittedFanOutChunk, error) {
		t.Fatal("precommit retry must not enter the selected-store commit")
		return CommittedFanOutChunk{}, nil
	}}
	cause := runtimefailures.New(
		runtimefailures.ClassDependencyUnavailable,
		"connect_route_snapshot_stale",
		"eventbus",
		"plan_connect_routes",
		map[string]any{"reason": "route_table_generation_changed"},
	)
	if _, disposition := fanOutPrecommitFailure(cause); disposition != fanOutFailureRetry {
		t.Fatalf("stale route generation disposition = %d, want retry", disposition)
	}

	released, err := releaseFanOutPrecommitRetry(
		context.Background(), owner, planner, claim,
		[]runtimeengine.DurablePublicationPlan{fanOutFailureTestPlan("event-0"), fanOutFailureTestPlan("event-1")},
		cause, time.Now().Add(-time.Millisecond),
	)
	if !released || err == nil || len(owner.commands) != 0 || len(owner.blocks) != 0 ||
		len(owner.retryRelease) != 1 || owner.retryRelease[0].Claim != claim ||
		owner.retryRelease[0].ObservedDuration <= 0 || fmt.Sprint(planner.released) != "[[event-0 event-1]]" {
		t.Fatalf("precommit retry = released:%v err:%v commands:%d blocks:%d releases:%#v plans:%v",
			released, err, len(owner.commands), len(owner.blocks), owner.retryRelease, planner.released)
	}
}

func fanOutFailureTestClaim() fanoutobligation.Claim {
	return fanoutobligation.Claim{
		Key: fanoutobligation.IntentKey{
			RunID: uuid.NewString(), TriggeringDeliveryID: uuid.NewString(),
			ElementRef: runtimecontracts.FanOutElementRef{FlowPath: ".", Family: "fan_out", SemanticPath: `nodes["fanout"].handlers["work.requested"].fan_out`},
		},
		Owner: "failure-test", Generation: 1, LeaseUntil: time.Now().Add(time.Minute),
	}
}

func fanOutFailureOutcomes(start, count int) []FanOutChunkOutcome {
	out := make([]FanOutChunkOutcome, 0, count)
	for ordinal := start; ordinal < start+count; ordinal++ {
		out = append(out, FanOutChunkOutcome{Ordinal: ordinal, Publication: fanOutFailureTestPlan(fmt.Sprintf("event-%d", ordinal))})
	}
	return out
}

func fanOutFailureCommitted(command FanOutChunkCommand, status fanoutobligation.Status) CommittedFanOutChunk {
	return CommittedFanOutChunk{Intent: fanoutobligation.Intent{Request: fanoutobligation.IntentRequest{Key: command.Claim.Key}, Cursor: command.Outcomes[len(command.Outcomes)-1].Ordinal + 1, Status: status}}
}

func fanOutFailureEnvelope(t *testing.T, class runtimefailures.Class, detail string) runtimefailures.Envelope {
	t.Helper()
	failure, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(class, detail, "test", "fan_out", nil))
	if !ok {
		t.Fatalf("construct failure envelope %s", detail)
	}
	return failure
}
