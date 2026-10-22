package conformance

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

type selectedSettlementExecutorControl struct {
	serve  func(context.Context, pipeline.FanOutObligationOwner, fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error)
	report func(context.Context, error)
}

func (e selectedSettlementExecutorControl) ServeFanOutCandidate(ctx context.Context, owner pipeline.FanOutObligationOwner, key fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
	return e.serve(ctx, owner, key)
}

func (e selectedSettlementExecutorControl) ReportFanOutServingError(ctx context.Context, err error) {
	e.report(ctx, err)
}

var _ startupownership.FanOutExecutor = selectedSettlementExecutorControl{}

func TestSelectedSettlementExecutorPreservesBoundOwnerAndDelegation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bound := &selectedSettlementEvaluationControl{}
	key := fanoutobligation.IntentKey{RunID: uuid.NewString()}
	want := pipeline.FanOutTurnResult{Refill: true}
	wantErr := errors.New("delegated serve failure")
	observation := newSelectedDeploymentSettlementObservation()
	served, reported := 0, 0
	executor := &selectedDeploymentSettlementExecutor{
		observed: observation,
		FanOutExecutor: selectedSettlementExecutorControl{
			serve: func(gotCtx context.Context, owner pipeline.FanOutObligationOwner, gotKey fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
				served++
				wrapped, ok := owner.(selectedDeploymentSettlementOwner)
				if !ok || wrapped.FanOutObligationOwner != bound || wrapped.observed != observation || gotCtx != ctx || gotKey != key {
					t.Fatal("executor changed the bound owner, observation, key or context")
				}
				return want, wantErr
			},
			report: func(gotCtx context.Context, err error) {
				reported++
				if gotCtx != ctx || err != wantErr {
					t.Fatal("report changed context or error")
				}
			},
		},
	}
	got, err := executor.ServeFanOutCandidate(ctx, bound, key)
	if got != want || err != wantErr || served != 1 {
		t.Fatalf("serve result=%+v err=%v calls=%d", got, err, served)
	}
	executor.ReportFanOutServingError(ctx, err)
	if reported != 1 {
		t.Fatal("report was not delegated exactly once")
	}
}

type selectedSettlementEvaluationControl struct {
	pipeline.FanOutObligationOwner
	begin func(context.Context, fanoutobligation.Claim, func(pipeline.FanOutEvaluationInput) error) (pipeline.FanOutEvaluationInput, pipelineobligation.PublicationGroup, error)
}

func (o *selectedSettlementEvaluationControl) BeginFanOutEvaluation(ctx context.Context, claim fanoutobligation.Claim, validate func(pipeline.FanOutEvaluationInput) error) (pipeline.FanOutEvaluationInput, pipelineobligation.PublicationGroup, error) {
	return o.begin(ctx, claim, validate)
}

type selectedSettlementGroupControl struct {
	pipelineobligation.PublicationGroup
	settle func(context.Context, []pipelineobligation.PublicationSettlementMember) (pipelineobligation.PublicationGroupOutcome, error)
}

func (g *selectedSettlementGroupControl) Settle(ctx context.Context, members []pipelineobligation.PublicationSettlementMember) (pipelineobligation.PublicationGroupOutcome, error) {
	return g.settle(ctx, members)
}

func TestSelectedSettlementProbeRecordsOnlySuccessfulEvaluation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claim := fanoutobligation.Claim{Key: fanoutobligation.IntentKey{RunID: uuid.NewString()}}
	wantErr := errors.New("evaluation refused")
	group := &selectedSettlementGroupControl{}
	for _, test := range []struct {
		name  string
		group pipelineobligation.PublicationGroup
		err   error
	}{
		{"success", group, nil},
		{"no_group", nil, nil},
		{"failure", nil, wantErr},
		{"group_with_failure", group, wantErr},
	} {
		t.Run(test.name, func(t *testing.T) {
			observed := newSelectedDeploymentSettlementObservation()
			calls, validations := 0, 0
			input := pipeline.FanOutEvaluationInput{}
			owner := selectedDeploymentSettlementOwner{observed: observed, FanOutObligationOwner: &selectedSettlementEvaluationControl{
				begin: func(gotCtx context.Context, gotClaim fanoutobligation.Claim, validate func(pipeline.FanOutEvaluationInput) error) (pipeline.FanOutEvaluationInput, pipelineobligation.PublicationGroup, error) {
					calls++
					if gotCtx != ctx || gotClaim != claim || validate(input) != wantErr {
						t.Fatal("evaluation changed context, claim or validator")
					}
					return input, test.group, test.err
				},
			}}
			gotInput, gotGroup, err := owner.BeginFanOutEvaluation(ctx, claim, func(got pipeline.FanOutEvaluationInput) error {
				validations++
				if !reflect.DeepEqual(got, input) {
					t.Fatal("evaluation changed the validated input")
				}
				return wantErr
			})
			if !reflect.DeepEqual(gotInput, input) || err != test.err || calls != 1 || validations != 1 {
				t.Fatalf("evaluation result changed: input=%+v err=%v calls=%d validations=%d", gotInput, err, calls, validations)
			}
			evaluated, committed := observed.snapshot()
			if test.group == nil || test.err != nil {
				if gotGroup != test.group || len(evaluated) != 0 || len(committed) != 0 {
					t.Fatal("unsuccessful evaluation was observed or its group changed")
				}
			} else if wrapped, ok := gotGroup.(selectedDeploymentSettlementGroup); !ok || wrapped.PublicationGroup != test.group || !reflect.DeepEqual(evaluated, []string{claim.Key.RunID}) || len(committed) != 0 {
				t.Fatal("successful evaluation lost the original group or exact run witness")
			}
		})
	}
}

func TestSelectedSettlementProbePreservesCommittedEvidenceAndErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claim, err := pipelineobligation.NewClaimIssuer().Issue(uuid.NewString(), pipelineobligation.PurposeRecovery)
	if err != nil {
		t.Fatal(err)
	}
	postcommitErr := errors.New("committed with cleanup failure")
	for _, test := range []struct {
		name    string
		outcome pipelineobligation.SettlementOutcome
		err     error
		observe bool
	}{
		{"uncommitted", pipelineobligation.SettlementOutcome{}, nil, false},
		{"committed_without_handoff", pipelineobligation.CommittedSettlement(false), nil, false},
		{"committed", pipelineobligation.CommittedSettlement(true), nil, true},
		{"postcommit_error", pipelineobligation.CommittedSettlement(true), postcommitErr, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			observed := newSelectedDeploymentSettlementObservation()
			want := pipelineobligation.PublicationGroupOutcome{Results: []pipelineobligation.PublicationSettlementResult{{Claim: claim, Outcome: test.outcome}}}
			members := []pipelineobligation.PublicationSettlementMember{{Claim: claim}}
			calls := 0
			group := selectedDeploymentSettlementGroup{observed: observed, PublicationGroup: &selectedSettlementGroupControl{
				settle: func(gotCtx context.Context, gotMembers []pipelineobligation.PublicationSettlementMember) (pipelineobligation.PublicationGroupOutcome, error) {
					calls++
					if gotCtx != ctx || !reflect.DeepEqual(gotMembers, members) {
						t.Fatal("settlement changed context or members")
					}
					return want, test.err
				},
			}}
			got, err := group.Settle(ctx, members)
			if !reflect.DeepEqual(got, want) || err != test.err || calls != 1 {
				t.Fatalf("settlement result changed: outcome=%+v err=%v calls=%d", got, err, calls)
			}
			evaluated, committed := observed.snapshot()
			if len(evaluated) != 0 || (test.observe && !reflect.DeepEqual(committed, []string{claim.EventID()})) || (!test.observe && len(committed) != 0) {
				t.Fatalf("incorrect settlement witnesses: evaluated=%v committed=%v", evaluated, committed)
			}
		})
	}
}

func TestSelectedSettlementObservationIsLosslessAndDetached(t *testing.T) {
	observed := newSelectedDeploymentSettlementObservation()
	var writers sync.WaitGroup
	for i := 0; i < 64; i++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			observed.record("run", "event")
		}()
	}
	writers.Wait()
	evaluated, committed := observed.snapshot()
	if len(evaluated) != 64 || len(committed) != 64 {
		t.Fatalf("lost witnesses with no observer consuming notifications: %d/%d", len(evaluated), len(committed))
	}
	evaluated[0], committed[0] = "foreign", "foreign"
	evaluated, committed = observed.snapshot()
	if evaluated[0] != "run" || committed[0] != "event" {
		t.Fatal("snapshot mutation changed retained evidence")
	}
	if err := observed.waitCommitted(context.Background(), "event"); err != nil {
		t.Fatal(err)
	}
}
