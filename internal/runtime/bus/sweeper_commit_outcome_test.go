package bus

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type sweepCommitOutcomeOwner struct {
	*interceptorOutcomePipelineOwner
	event      events.Event
	scanIssuer *runtimepipelineobligation.ScanIssuer
	claimed    bool
	closed     int
}

func (o *sweepCommitOutcomeOwner) OpenScan(context.Context, runtimepipelineobligation.ScanRequest) (runtimepipelineobligation.Scan, error) {
	return o.scanIssuer.Issue()
}
func (o *sweepCommitOutcomeOwner) CloseScan(context.Context, runtimepipelineobligation.Scan) error {
	o.closed++
	return nil
}
func (o *sweepCommitOutcomeOwner) ClaimBatch(context.Context, runtimepipelineobligation.Scan, int) (runtimepipelineobligation.ScanBatch, error) {
	if o.claimed {
		return runtimepipelineobligation.ScanBatch{Exhausted: true}, nil
	}
	o.claimed = true
	claim, err := o.claim(o.event.ID(), runtimepipelineobligation.PurposeRecovery)
	return runtimepipelineobligation.ScanBatch{Examined: 1, Exhausted: true, Work: []runtimepipelineobligation.ClaimedWork{{Claim: claim, Event: o.event, Scope: runtimepipelineobligation.ScopeSubscribed}}}, err
}

func TestPipelineSweepRetainsResultAndRetryOwnershipWithError(t *testing.T) {
	for _, phase := range []string{"committed", "later_retry", "later_retry_release_failure"} {
		t.Run(phase, func(t *testing.T) {
			store := newTargetRouteMemoryStore()
			evt := deliveryContinuationProjectionEvent("sweep-commit", "custom.sweep_commit")
			seedCommittedNoDeliveryForTest(t, store, evt)
			cleanupErr, releaseErr := errors.New("committed interceptor cleanup"), errors.New("independent retry release failure")
			first := &outcomeTestInterceptor{outcome: runtimepipelineobligation.ExecutionOutcome{Committed: true}, err: cleanupErr}
			interceptors := []EventInterceptor{first}
			if phase != "committed" {
				interceptors = append(interceptors, &outcomeTestInterceptor{outcome: runtimepipelineobligation.ReleaseForRetry("later_not_ready", nil)})
			}
			bus, base := interceptorOutcomeBus(t, store, interceptors...)
			owner := &sweepCommitOutcomeOwner{interceptorOutcomePipelineOwner: base, event: evt, scanIssuer: runtimepipelineobligation.NewScanIssuer()}
			if phase == "later_retry_release_failure" {
				owner.releaseError[evt.ID()] = func(int) error { return releaseErr }
			}
			bus.pipelineObligations = owner
			result, err := bus.SweepPipelineObligations(context.Background(), 1)
			if !errors.Is(err, cleanupErr) || owner.closed != 1 || first.calls != 1 {
				t.Fatalf("result=%+v err=%v closed=%d calls=%d", result, err, owner.closed, first.calls)
			}
			if phase == "committed" {
				if result.Settled != 1 || !owner.settlements[evt.ID()].Successful() {
					t.Fatalf("acknowledged result lost: %+v", result)
				}
			} else if owner.releaseCalls[evt.ID()] != 1 || result.Settled != 0 {
				t.Fatalf("retry ownership lost: releases=%d settled=%d", owner.releaseCalls[evt.ID()], result.Settled)
			}
			if phase == "later_retry_release_failure" && !errors.Is(err, releaseErr) {
				t.Fatalf("release failure lost: %v", err)
			}
		})
	}
}
