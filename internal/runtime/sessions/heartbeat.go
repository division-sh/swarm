package sessions

import (
	"context"
	"log"
	"sync"
	"time"
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
	identity := lease.Identity.Normalize()
	lockOwner := lease.LockOwner
	if identity.Validate() != nil || lockOwner == "" {
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
				refreshed, err := sessions.Acquire(ctx, identity, lockOwner)
				if err != nil {
					if onError != nil {
						onError(err)
					} else {
						log.Printf("agent memory lease heartbeat failed: agent=%s run=%s flow_instance=%s err=%v", identity.AgentID, identity.RunID, identity.FlowInstance, err)
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
