package main

import (
	"context"
	"time"
)

var postgresRemovalTimeout = 2 * time.Minute

type settlementLease interface {
	Join(context.Context) error
	Complete(context.Context, bool) error
}

func settleRunAuthorities(lease settlementLease, closeService func(context.Context) error, result func() bool) (serviceErr, leaseErr error) {
	joinCtx, cancelJoin := context.WithTimeout(context.Background(), authoritySettlementTimeout)
	leaseErr = lease.Join(joinCtx)
	cancelJoin()
	if closeService != nil {
		removalCtx, cancelRemoval := context.WithTimeout(context.Background(), postgresRemovalTimeout)
		serviceErr = closeService(removalCtx)
		cancelRemoval()
	}
	success := result()
	if leaseErr == nil {
		// Slow container removal must not consume the slot-publication budget.
		completeCtx, cancelComplete := context.WithTimeout(context.Background(), authoritySettlementTimeout)
		leaseErr = lease.Complete(completeCtx, success && serviceErr == nil)
		cancelComplete()
	}
	return serviceErr, leaseErr
}
