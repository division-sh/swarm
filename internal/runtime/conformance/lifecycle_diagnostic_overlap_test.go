package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
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

func gateLifecycleDiagnosticOverlap(t *testing.T, db *sql.DB, roles runtimemanager.PersistenceRoles) runtimemanager.PersistenceRoles {
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
		rows, err := db.Query("SELECT payload FROM events WHERE event_name = 'platform.runtime_log'")
		if err != nil {
			t.Errorf("read durable lifecycle logs: %v", err)
			return
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			var raw []byte
			var payload struct {
				Details map[string]any `json:"details"`
			}
			if err := rows.Scan(&raw); err != nil {
				t.Error(err)
				return
			}
			if err := json.Unmarshal(raw, &payload); err != nil {
				t.Error(err)
				return
			}
			if payload.Details["outbox_id"] == id {
				count++
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
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

func requireSourceRevisionLifecycleDiagnosticReceipts(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	rows, err := db.Query(`SELECT d.outbox_id, d.projected_at, d.projection
		FROM agent_lifecycle_diagnostic_outbox d
		JOIN agent_lifecycle_operations o ON o.operation_id = d.operation_id
		WHERE d.run_id = $1 AND o.operation_kind IN ('source_set_rebind', 'source_set_retire', 'process_takeover')`, runID)
	if err != nil {
		t.Fatal(err)
	}
	ids := make(map[string]int)
	for rows.Next() {
		var id string
		var at any
		var receipt []byte
		if err := rows.Scan(&id, &at, &receipt); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if at == nil || len(receipt) == 0 {
			rows.Close()
			t.Fatalf("source revision diagnostic %s remained pending after real hydration", id)
		}
		ids[id] = 0
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	if len(ids) == 0 {
		t.Fatal("source revision did not exercise takeover/rebind diagnostic production")
	}
	logs, err := db.Query("SELECT payload FROM events WHERE event_name = 'platform.runtime_log' AND run_id = $1", runID)
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	for logs.Next() {
		var raw []byte
		var event struct {
			Details map[string]any `json:"details"`
		}
		if err := logs.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			t.Fatal(err)
		}
		id, _ := event.Details["outbox_id"].(string)
		if _, ok := ids[id]; ok {
			ids[id]++
		}
	}
	if err := logs.Err(); err != nil {
		t.Fatal(err)
	}
	for id, count := range ids {
		if count != 1 {
			t.Fatalf("source revision diagnostic %s log count=%d want=1", id, count)
		}
	}
}
