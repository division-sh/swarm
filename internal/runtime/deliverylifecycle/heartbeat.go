package deliverylifecycle

import (
	"context"
	"fmt"
	"sync"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

const defaultClaimHeartbeatInterval = DefaultLeaseTTL / 3

// ClaimHeartbeat retains one exact claim while its handler is executing. Its
// process-local loop is admitted to and joined by the same runtime generation.
type ClaimHeartbeat struct {
	ctx       context.Context
	cancel    context.CancelCauseFunc
	stop      chan struct{}
	done      chan struct{}
	stopOnce  sync.Once
	errMu     sync.Mutex
	renewErr  error
	workLease *worklifetime.Lease
}

func StartClaimHeartbeat(ctx context.Context, owner worklifetime.Occurrence, store Store, claim Claim) (*ClaimHeartbeat, error) {
	return startClaimHeartbeat(ctx, owner, store, claim, defaultClaimHeartbeatInterval)
}

func startClaimHeartbeat(ctx context.Context, owner worklifetime.Occurrence, store Store, claim Claim, interval time.Duration) (*ClaimHeartbeat, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if contextual, ok := worklifetime.OccurrenceFromContext(ctx); ok {
		owner = contextual
	}
	if owner == nil {
		return nil, fmt.Errorf("delivery claim heartbeat requires a runtime work occurrence")
	}
	if store == nil || !claim.valid() {
		return nil, fmt.Errorf("delivery claim heartbeat requires a current claim owner")
	}
	if interval <= 0 || interval >= DefaultLeaseTTL {
		return nil, fmt.Errorf("delivery claim heartbeat interval must be positive and shorter than the claim lease")
	}
	workLease, err := owner.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("admit delivery claim heartbeat: %w", err)
	}
	if _, err := store.RenewClaim(workLease.Context(), claim); err != nil {
		_ = workLease.Done()
		return nil, fmt.Errorf("renew delivery claim before execution: %w", err)
	}
	heartbeatCtx, cancel := context.WithCancelCause(worklifetime.WithOccurrence(workLease.Context(), owner))
	h := &ClaimHeartbeat{
		ctx: heartbeatCtx, cancel: cancel, stop: make(chan struct{}), done: make(chan struct{}), workLease: workLease,
	}
	go h.run(store, claim, interval)
	return h, nil
}

func (h *ClaimHeartbeat) Context() context.Context {
	if h == nil || h.ctx == nil {
		return context.Background()
	}
	return h.ctx
}

func (h *ClaimHeartbeat) Stop() error {
	if h == nil {
		return nil
	}
	h.stopOnce.Do(func() { close(h.stop) })
	<-h.done
	h.errMu.Lock()
	defer h.errMu.Unlock()
	return h.renewErr
}

func (h *ClaimHeartbeat) run(store Store, claim Claim, interval time.Duration) {
	defer close(h.done)
	defer func() { _ = h.workLease.Done() }()
	defer h.cancel(nil)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-h.stop:
			if _, err := store.RenewClaim(h.ctx, claim); err != nil {
				h.recordRenewalFailure("renew delivery claim after execution", err)
			}
			return
		case <-ticker.C:
			if _, err := store.RenewClaim(h.ctx, claim); err != nil {
				h.recordRenewalFailure("renew delivery claim during execution", err)
				return
			}
		}
	}
}

func (h *ClaimHeartbeat) recordRenewalFailure(operation string, err error) {
	h.errMu.Lock()
	h.renewErr = fmt.Errorf("%s: %w", operation, err)
	h.errMu.Unlock()
	h.cancel(err)
}
