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

type settlementService interface {
	Close(context.Context) error
}

func settleRunAuthorities(lease settlementLease, service settlementService, result func() bool) (serviceErr, leaseErr error) {
	joinCtx, cancelJoin := context.WithTimeout(context.Background(), authoritySettlementTimeout)
	leaseErr = lease.Join(joinCtx)
	cancelJoin()
	if service != nil {
		removalCtx, cancelRemoval := context.WithTimeout(context.Background(), postgresRemovalTimeout)
		serviceErr = service.Close(removalCtx)
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
