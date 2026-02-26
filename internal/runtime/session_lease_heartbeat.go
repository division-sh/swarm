package runtime

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

func startLeaseHeartbeat(ctx context.Context, sessions SessionRegistry, lease *SessionLease, runtimeMode string) func() {
	if sessions == nil || lease == nil {
		return func() {}
	}
	agentID := strings.TrimSpace(lease.AgentID)
	lockOwner := strings.TrimSpace(lease.LockOwner)
	scopeKey := strings.TrimSpace(lease.ScopeKey)
	runtimeMode = strings.TrimSpace(runtimeMode)
	if agentID == "" || lockOwner == "" || runtimeMode == "" {
		return func() {}
	}

	interval := leaseHeartbeatInterval(lease.ExpiresAt)
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
				refreshed, err := sessions.Acquire(agentID, runtimeMode, lockOwner, scopeKey)
				if err != nil {
					log.Printf("session lease heartbeat failed: agent=%s runtime=%s err=%v", agentID, runtimeMode, err)
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

func leaseHeartbeatInterval(expiresAt time.Time) time.Duration {
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
