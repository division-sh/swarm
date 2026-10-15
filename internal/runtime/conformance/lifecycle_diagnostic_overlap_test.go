package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// Both consumers read the same real terminal row before either can settle it.
// Lifecycle production, logger, selected SQL transaction and shutdown stay real.
type lifecycleDiagnosticOverlap struct {
	runtimemanager.AgentLifecycleDiagnosticPersistence
	mu       sync.Mutex
	id       string
	arrivals int
	release  chan struct{}
}

func gateLifecycleDiagnosticOverlap(t *testing.T, selected notifyAllChildrenStore, roles runtimemanager.PersistenceRoles) runtimemanager.PersistenceRoles {
	t.Helper()
	probe := &lifecycleDiagnosticOverlap{AgentLifecycleDiagnosticPersistence: roles.LifecycleDiagnostics, release: make(chan struct{})}
	if probe.AgentLifecycleDiagnosticPersistence == nil {
		t.Fatal("real lifecycle diagnostic reader required")
	}
	t.Cleanup(func() {
		probe.mu.Lock()
		id, arrivals := probe.id, probe.arrivals
		probe.mu.Unlock()
		if id == "" || arrivals != 2 {
			t.Errorf("terminal diagnostic overlap: id=%q arrivals=%d, want two consumers", id, arrivals)
			return
		}
		payloads, err := storetest.ReadLifecycleProjectionLogPayloads(context.Background(), notifyAllChildrenNativeObservationOwner(selected))
		if err != nil {
			t.Errorf("read durable lifecycle logs: %v", err)
			return
		}
		count := 0
		for _, raw := range payloads {
			var payload struct {
				Details map[string]any `json:"details"`
			}
			if err := json.Unmarshal(raw, &payload); err != nil {
				t.Error(err)
				return
			}
			if payload.Details["outbox_id"] == id {
				count++
			}
		}
		if count != 1 {
			t.Errorf("outbox %s has %d durable log projections, want one", id, count)
		}
	})
	roles.LifecycleDiagnostics = probe
	return roles
}

func (s *lifecycleDiagnosticOverlap) ListPendingAgentLifecycleDiagnostics(ctx context.Context, limit int) ([]diaglog.LifecycleDiagnostic, error) {
	items, err := s.AgentLifecycleDiagnosticPersistence.ListPendingAgentLifecycleDiagnostics(ctx, limit)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.id == "" {
		for _, item := range items {
			if item.Payload["phase"] == "terminated" {
				s.id = item.OutboxID
				break
			}
		}
	}
	matches := false
	for _, item := range items {
		matches = matches || (s.id != "" && item.OutboxID == s.id)
	}
	if !matches || s.arrivals >= 2 {
		s.mu.Unlock()
		return items, nil
	}
	s.arrivals++
	if s.arrivals == 2 {
		close(s.release)
	}
	s.mu.Unlock()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-s.release:
		return items, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("second real terminal diagnostic consumer did not arrive")
	}
}

func requireSourceRevisionLifecycleDiagnosticReceipts(t *testing.T, selected notifyAllChildrenStore, runID string) {
	t.Helper()
	owner := notifyAllChildrenNativeObservationOwner(selected)
	rows, err := storetest.ReadSourceRevisionDiagnosticStorage(context.Background(), owner, runID)
	if err != nil {
		t.Fatal(err)
	}
	ids := make(map[string]int)
	for _, row := range rows {
		id, receipt := row.OutboxID, row.Projection
		if !row.Projected || len(receipt) == 0 {
			t.Fatalf("source revision diagnostic %s remained pending after real hydration", id)
		}
		ids[id] = 0
	}
	if len(ids) == 0 {
		t.Fatal("source revision did not exercise takeover/rebind diagnostic production")
	}
	logs, err := storetest.ReadRunLifecycleProjectionLogPayloads(context.Background(), owner, runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range logs {
		var event struct {
			Details map[string]any `json:"details"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			t.Fatal(err)
		}
		id, _ := event.Details["outbox_id"].(string)
		if _, ok := ids[id]; ok {
			ids[id]++
		}
	}
	for id, count := range ids {
		if count != 1 {
			t.Fatalf("source revision diagnostic %s log count=%d want=1", id, count)
		}
	}
}
