package sessions

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

const (
	minLeaseHeartbeatInterval = 5 * time.Second
	maxLeaseHeartbeatInterval = 45 * time.Second
)

func StartLeaseHeartbeat(ctx context.Context, sessions Registry, lease *Lease) func() {
	return StartLeaseHeartbeatWithErrorHandler(ctx, sessions, lease, nil)
}

func StartLeaseHeartbeatWithErrorHandler(ctx context.Context, sessions Registry, lease *Lease, onError func(error)) func() {
	if sessions == nil || lease == nil {
		return func() {}
	}
	current := *lease
	identity := current.Identity.Normalize()
	if identity.Validate() != nil || current.LockOwner == "" || current.GrantID == "" {
		return func() {}
	}

	interval := LeaseHeartbeatInterval(current.ExpiresAt)
	owner, ok := worklifetime.OccurrenceFromContext(ctx)
	if !ok {
		if onError != nil {
			onError(fmt.Errorf("session lease heartbeat requires a runtime work occurrence"))
		}
		return func() {}
	}
	workLease, err := owner.Begin(ctx)
	if err != nil {
		if onError != nil {
			onError(fmt.Errorf("admit session lease heartbeat: %w", err))
		}
		return func() {}
	}
	stopCh := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() { _ = workLease.Done() }()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-workLease.Context().Done():
				return
			case <-stopCh:
				return
			case <-ticker.C:
				refreshed, err := sessions.Renew(workLease.Context(), &current)
				if err != nil {
					if onError != nil {
						onError(err)
					} else {
						log.Printf("agent memory lease heartbeat failed: agent=%s run=%s flow_instance=%s err=%v", identity.AgentID(), identity.RunID, identity.FlowInstance(), err)
					}
					return
				}
				if refreshed != nil {
					current = *refreshed
				}
			}
		}
	}()
	return func() {
		once.Do(func() { close(stopCh) })
		wg.Wait()
	}
}

func LeaseHeartbeatInterval(expiresAt time.Time) time.Duration {
	if expiresAt.IsZero() {
		return 30 * time.Second
	}
	d := time.Until(expiresAt) / 3
	if d < minLeaseHeartbeatInterval {
		return minLeaseHeartbeatInterval
	}
	if d > maxLeaseHeartbeatInterval {
		return maxLeaseHeartbeatInterval
	}
	return d
}
