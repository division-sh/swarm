package deliverylifecycle

import (
	"context"
	"fmt"
	"sync"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

// ClaimHeartbeat retains one exact claim while its handler is executing. Its
// process-local loop is admitted to and joined by the same runtime generation.
type ClaimHeartbeat struct {
	ctx       context.Context
	cancel    context.CancelCauseFunc
	stop      chan struct{}
	done      chan struct{}
	stopOnce  sync.Once
	renewMu   sync.Mutex
	errMu     sync.Mutex
	renewErr  error
	workLease *worklifetime.Lease
	store     Store
	claim     Claim
	settled   bool
}

// ClaimSettlementGuard excludes periodic renewal while the exact selected-
// store settlement transaction commits. It is intentionally tied to one
// heartbeat and cannot settle or mutate a different claim.
type ClaimSettlementGuard struct {
	heartbeat *ClaimHeartbeat
	once      sync.Once
}

func StartClaimHeartbeat(ctx context.Context, owner worklifetime.Occurrence, store Store, claim Claim) (*ClaimHeartbeat, error) {
	return startClaimHeartbeat(ctx, owner, store, claim, 0)
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
	workLease, err := owner.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("admit delivery claim heartbeat: %w", err)
	}
	snapshot, err := store.RenewClaim(workLease.Context(), claim)
	if err != nil {
		_ = workLease.Done()
		return nil, fmt.Errorf("renew delivery claim before execution: %w", err)
	}
	if interval == 0 {
		leaseTTL := snapshot.ClaimExpiresAt.Sub(snapshot.UpdatedAt)
		if leaseTTL <= 0 {
			_ = workLease.Done()
			return nil, fmt.Errorf("renewed delivery claim did not report a positive lease")
		}
		interval = leaseTTL / 3
	}
	if interval <= 0 || interval >= DefaultLeaseTTL {
		_ = workLease.Done()
		return nil, fmt.Errorf("delivery claim heartbeat interval must be positive and shorter than the claim lease")
	}
	heartbeatCtx, cancel := context.WithCancelCause(worklifetime.WithOccurrence(workLease.Context(), owner))
	h := &ClaimHeartbeat{
		ctx: heartbeatCtx, cancel: cancel, stop: make(chan struct{}), done: make(chan struct{}), workLease: workLease,
		store: store, claim: claim,
	}
	go h.run(store, claim, interval)
	return h, nil
}

// BeginSettlement renews the claim immediately and prevents the heartbeat
// loop from issuing another renewal until the terminal/retry commit finishes.
func (h *ClaimHeartbeat) BeginSettlement() (*ClaimSettlementGuard, error) {
	if h == nil {
		return nil, fmt.Errorf("delivery claim settlement requires a heartbeat")
	}
	h.renewMu.Lock()
	if err := h.currentRenewalError(); err != nil {
		h.renewMu.Unlock()
		return nil, err
	}
	if h.settled {
		h.renewMu.Unlock()
		return nil, fmt.Errorf("delivery claim heartbeat is already settled")
	}
	if _, err := h.store.RenewClaim(h.ctx, h.claim); err != nil {
		h.recordRenewalFailure("renew delivery claim before settlement", err)
		h.renewMu.Unlock()
		return nil, h.currentRenewalError()
	}
	return &ClaimSettlementGuard{heartbeat: h}, nil
}

// Finish records whether the selected-store settlement committed, releases
// renewal exclusion, and joins the heartbeat. A failed settlement receives a
// final renewal so recovery retains an unambiguous current claim.
func (g *ClaimSettlementGuard) Finish(committed bool) error {
	if g == nil || g.heartbeat == nil {
		return fmt.Errorf("delivery claim settlement guard is required")
	}
	var finishErr error
	g.once.Do(func() {
		h := g.heartbeat
		if committed {
			h.settled = true
		}
		h.renewMu.Unlock()
		finishErr = h.Stop()
	})
	return finishErr
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
			h.renew(store, claim, "renew delivery claim after execution")
			return
		case <-ticker.C:
			if !h.renew(store, claim, "renew delivery claim during execution") {
				return
			}
		}
	}
}

func (h *ClaimHeartbeat) renew(store Store, claim Claim, operation string) bool {
	h.renewMu.Lock()
	defer h.renewMu.Unlock()
	if h.settled {
		return true
	}
	if _, err := store.RenewClaim(h.ctx, claim); err != nil {
		h.recordRenewalFailure(operation, err)
		return false
	}
	return true
}

func (h *ClaimHeartbeat) currentRenewalError() error {
	h.errMu.Lock()
	defer h.errMu.Unlock()
	return h.renewErr
}

func (h *ClaimHeartbeat) recordRenewalFailure(operation string, err error) {
	h.errMu.Lock()
	h.renewErr = fmt.Errorf("%s: %w", operation, err)
	h.errMu.Unlock()
	h.cancel(err)
}
