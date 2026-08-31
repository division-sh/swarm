package pipeline

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
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
	aggregateFailure := fanOutFailureEnvelope(t, runtimefailures.ClassSchemaInvalid, "fan_out_test_aggregate_invalid")
	retryableFailure := runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "fan_out_test_store_unavailable", "test", "commit", nil)
	uncertainFailure := runtimefailures.New(runtimefailures.ClassOutcomeUncertain, "fan_out_test_commit_unconfirmed", "test", "commit", nil)

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
			{name: "conflicting duplicate", err: runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "fan_out_commit_conflict", "test", "commit", nil)},
			{name: "non emit schema", err: runtimefailures.New(runtimefailures.ClassSchemaInvalid, "event_schema_invalid", "test", "commit", nil)},
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
			ElementRef: runtimecontracts.FanOutElementRef{PackageKey: "root", ElementID: "fan-out-1"},
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
		"package_key": "root", "element_id": "fan-out-1",
		"cursor": 4, "ordinal": 7, "cause": raw.Error(),
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

func TestFanOutPrecommitFailureAdmissionIsClosedToEmitContractEvidence(t *testing.T) {
	emitErr := &runtimeengine.EmitPayloadContractError{
		Event: "portfolio/account.registered", Kind: runtimeengine.EmitPayloadSchemaMismatch,
		Path: "$.score", Constraint: "type", Expected: "number", Actual: "string", Detail: "$.score must be number",
	}
	normalizedEmit := runtimeengine.NormalizeFailure(emitErr, "runtime.fan_out", "issue_ordinal")
	tests := []struct {
		name string
		err  error
		want fanOutFailureDisposition
	}{
		{name: "typed emit contract", err: emitErr, want: fanOutFailureItemSemantic},
		{name: "exact normalized emit envelope", err: normalizedEmit, want: fanOutFailureItemSemantic},
		{name: "forged emit code without typed attributes", err: runtimefailures.New(runtimefailures.ClassSchemaInvalid, "emit_payload_contract_violation", "test", "plan", nil), want: fanOutFailureBlock},
		{name: "forged emit kind", err: runtimefailures.New(runtimefailures.ClassSchemaInvalid, "emit_payload_contract_violation", "test", "plan", map[string]any{
			"event": "portfolio/account.registered", "kind": "other", "path": "$.score", "constraint": "type",
			"expected": "number", "actual": "string", "detail": "not a typed emit violation",
		}), want: fanOutFailureBlock},
		{name: "authorization", err: runtimefailures.New(runtimefailures.ClassAuthorizationDenied, "fan_out_authorization_denied", "test", "plan", map[string]any{"action": "publish"}), want: fanOutFailureBlock},
		{name: "conflicting duplicate", err: runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "fan_out_conflict", "test", "plan", nil), want: fanOutFailureBlock},
		{name: "non emit schema", err: runtimefailures.New(runtimefailures.ClassSchemaInvalid, "event_schema_invalid", "test", "plan", nil), want: fanOutFailureBlock},
		{name: "internal", err: runtimefailures.New(runtimefailures.ClassInternalFailure, "fan_out_internal", "test", "plan", nil), want: fanOutFailureBlock},
		{name: "unknown", err: errors.New("untyped planner failure"), want: fanOutFailureBlock},
		{name: "retryable", err: runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "fan_out_dependency_unavailable", "test", "plan", nil), want: fanOutFailureRetry},
		{name: "cancellation", err: context.Canceled, want: fanOutFailureYield},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, got := fanOutPrecommitFailure(test.err)
			if got != test.want {
				t.Fatalf("disposition = %d, want %d", got, test.want)
			}
			if test.want != fanOutFailureItemSemantic {
				if len(raw) != 0 {
					t.Fatalf("non-emit disposition persisted semantic evidence: %s", raw)
				}
				return
			}
			envelope, err := runtimefailures.UnmarshalEnvelope(raw)
			if err != nil || envelope.Detail.Code != "emit_payload_contract_violation" {
				t.Fatalf("semantic evidence = %#v err=%v", envelope, err)
			}
		})
	}
}

func TestFanOutEvaluatorAndPlannerFailuresConsumeClosedPrecommitOwner(t *testing.T) {
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate fan-out pump test")
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(current), "fan_out_pump.go"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "fan_out_pump.go", raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	arguments := map[string]int{}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "serveFanOutTurn" {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || len(call.Args) != 1 {
				return true
			}
			owner, ok := call.Fun.(*ast.Ident)
			argument, argumentOK := call.Args[0].(*ast.Ident)
			if ok && argumentOK && owner.Name == "fanOutPrecommitFailure" {
				arguments[argument.Name]++
			}
			return true
		})
	}
	if len(arguments) != 2 || arguments["evalErr"] != 1 || arguments["prepareErr"] != 1 {
		t.Fatalf("precommit failure callsites = %#v, want exact evaluator and planner consumers", arguments)
	}
}

func fanOutFailureTestClaim() fanoutobligation.Claim {
	return fanoutobligation.Claim{
		Key: fanoutobligation.IntentKey{
			RunID: uuid.NewString(), TriggeringDeliveryID: uuid.NewString(),
			ElementRef: runtimecontracts.FanOutElementRef{PackageKey: "root", ElementID: uuid.NewString()},
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
