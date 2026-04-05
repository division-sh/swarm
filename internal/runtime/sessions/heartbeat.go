package sessions

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	minLeaseHeartbeatInterval = 5 * time.Second
	maxLeaseHeartbeatInterval = 45 * time.Second
)

func StartLeaseHeartbeat(ctx context.Context, sessions Registry, lease *Lease, runtimeMode RuntimeMode) func() {
	return StartLeaseHeartbeatWithErrorHandler(ctx, sessions, lease, runtimeMode, nil)
}

func StartLeaseHeartbeatWithErrorHandler(ctx context.Context, sessions Registry, lease *Lease, runtimeMode RuntimeMode, onError func(error)) func() {
	if sessions == nil || lease == nil {
		return func() {}
	}
	agentID := strings.TrimSpace(lease.AgentID)
	lockOwner := strings.TrimSpace(lease.LockOwner)
	scopeKey := strings.TrimSpace(lease.ScopeKey)
	sessionScope := lease.SessionScope
	if agentID == "" || lockOwner == "" || runtimeMode == "" {
		return func() {}
	}

	interval := LeaseHeartbeatInterval(lease.ExpiresAt)
	stopCh := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopCh:
				return
			case <-ticker.C:
				refreshed, err := sessions.Acquire(ctx, agentID, runtimeMode, sessionScope, lockOwner, scopeKey)
				if err != nil {
					if onError != nil {
						onError(err)
					} else {
						log.Printf("session lease heartbeat failed: agent=%s runtime=%s err=%v", agentID, runtimeMode.String(), err)
					}
					continue
				}
				if refreshed != nil {
					lease.ExpiresAt = refreshed.ExpiresAt
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
