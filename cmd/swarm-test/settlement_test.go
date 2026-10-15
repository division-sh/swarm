package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

type settlementLeaseProbe struct {
	join     func(context.Context) error
	complete func(context.Context, bool) error
}

func (p settlementLeaseProbe) Join(ctx context.Context) error { return p.join(ctx) }
func (p settlementLeaseProbe) Complete(ctx context.Context, success bool) error {
	return p.complete(ctx, success)
}

type settlementServiceProbe func(context.Context) error

func (p settlementServiceProbe) Close(ctx context.Context) error { return p(ctx) }

func TestRunSettlementSlowRemovalHasIndependentDeadline(t *testing.T) {
	oldJoin, oldRemoval := authoritySettlementTimeout, postgresRemovalTimeout
	authoritySettlementTimeout, postgresRemovalTimeout = 20*time.Millisecond, 10*time.Second
	t.Cleanup(func() { authoritySettlementTimeout, postgresRemovalTimeout = oldJoin, oldRemoval })
	var joined, removed, completed bool
	lease := settlementLeaseProbe{
		join: func(ctx context.Context) error {
			joined = true
			return ctx.Err()
		},
		complete: func(ctx context.Context, success bool) error {
			completed = true
			if !removed || !success {
				t.Fatalf("completion before successful removal: removed=%v success=%v", removed, success)
			}
			return ctx.Err()
		},
	}
	service := settlementServiceProbe(func(ctx context.Context) error {
		if !joined {
			t.Fatal("container removal preceded lease join")
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) <= authoritySettlementTimeout {
			t.Fatalf("container removal inherited join budget: deadline=%v", deadline)
		}
		timer := time.NewTimer(3 * authoritySettlementTimeout)
		defer timer.Stop()
		select {
		case <-timer.C:
			removed = true
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	serviceErr, leaseErr := settleRunAuthorities(lease, service, func() bool { return true })
	if err := errors.Join(serviceErr, leaseErr); err != nil || !completed {
		t.Fatalf("slow removal settlement: completed=%v err=%v", completed, err)
	}
}

func TestRunSettlementPreservesIndependentErrors(t *testing.T) {
	for _, joinFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "completion_failure", true: "join_failure"}[joinFails], func(t *testing.T) {
			leaseFailure := errors.New("injected lease failure")
			serviceFailure := errors.New("injected service failure")
			completed, resultCalled := false, false
			lease := settlementLeaseProbe{
				join: func(context.Context) error {
					if joinFails {
						return leaseFailure
					}
					return nil
				},
				complete: func(ctx context.Context, success bool) error {
					completed = true
					if success || ctx.Err() != nil {
						t.Fatalf("failed service published success or expired completion: success=%v err=%v", success, ctx.Err())
					}
					return leaseFailure
				},
			}
			serviceErr, leaseErr := settleRunAuthorities(lease, settlementServiceProbe(func(context.Context) error {
				return serviceFailure
			}), func() bool { resultCalled = true; return true })
			joinedErr := errors.Join(serviceErr, leaseErr)
			if !errors.Is(joinedErr, leaseFailure) || !errors.Is(joinedErr, serviceFailure) || !resultCalled || completed == joinFails {
				t.Fatalf("settlement lost independent failure/order: completed=%v result=%v err=%v", completed, resultCalled, joinedErr)
			}
		})
	}
}
