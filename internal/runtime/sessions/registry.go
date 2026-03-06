package sessions

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Registry interface {
	Acquire(ctx context.Context, agentID, runtimeMode, lockOwner, scopeKey string) (*Lease, error)
	Release(ctx context.Context, lease *Lease) error
	Rotate(ctx context.Context, agentID, runtimeMode, lockOwner, summary, scopeKey string) (*Lease, error)
	IncrementTurn(ctx context.Context, agentID, runtimeMode, sessionID, scopeKey string) error
}

type Resetter interface {
	ResetAll(runtimeMode string) error
}

type Lease struct {
	SessionID   string
	AgentID     string
	RuntimeMode string
	LockOwner   string
	ScopeKey    string
	ExpiresAt   time.Time
}

type Record struct {
	SessionID         string
	AgentID           string
	RuntimeMode       string
	ScopeKey          string
	Status            string
	TurnCount         int
	CheckpointSummary string
	LockOwner         string
	LockExpiresAt     time.Time
	LastUsedAt        time.Time
}

// InMemoryRegistry is the process-local bootstrap implementation.
// It can be replaced by a Postgres-backed implementation with the same API.
type InMemoryRegistry struct {
	mu      sync.Mutex
	byKey   map[string]*Record
	lockTTL time.Duration
}

func NewInMemoryRegistry(lockTTL time.Duration) *InMemoryRegistry {
	if lockTTL <= 0 {
		lockTTL = 120 * time.Second
	}
	return &InMemoryRegistry{
		byKey:   make(map[string]*Record),
		lockTTL: lockTTL,
	}
}

func NewRegistry(lockTTL time.Duration) Registry {
	return NewInMemoryRegistry(lockTTL)
}

func registryKey(agentID, runtimeMode, scopeKey string) string {
	return strings.TrimSpace(agentID) + "|" + strings.TrimSpace(runtimeMode) + "|" + strings.TrimSpace(scopeKey)
}

func (sr *InMemoryRegistry) Acquire(_ context.Context, agentID, runtimeMode, lockOwner, scopeKey string) (*Lease, error) {
	if agentID == "" || runtimeMode == "" || lockOwner == "" {
		return nil, errors.New("agentID, runtimeMode, and lockOwner are required")
	}
	scopeKey = strings.TrimSpace(scopeKey)

	sr.mu.Lock()
	defer sr.mu.Unlock()

	now := time.Now()
	key := registryKey(agentID, runtimeMode, scopeKey)
	rec, ok := sr.byKey[key]
	if !ok || rec.Status != "active" {
		rec = &Record{
			SessionID:   uuid.NewString(),
			AgentID:     agentID,
			RuntimeMode: runtimeMode,
			ScopeKey:    scopeKey,
			Status:      "active",
		}
		sr.byKey[key] = rec
	}

	if rec.LockOwner != "" && rec.LockExpiresAt.After(now) && rec.LockOwner != lockOwner {
		return nil, fmt.Errorf("session already leased by %s", rec.LockOwner)
	}

	rec.LockOwner = lockOwner
	rec.LockExpiresAt = now.Add(sr.lockTTL)
	rec.LastUsedAt = now

	return &Lease{
		SessionID:   rec.SessionID,
		AgentID:     rec.AgentID,
		RuntimeMode: rec.RuntimeMode,
		LockOwner:   rec.LockOwner,
		ScopeKey:    rec.ScopeKey,
		ExpiresAt:   rec.LockExpiresAt,
	}, nil
}

func (sr *InMemoryRegistry) Release(_ context.Context, lease *Lease) error {
	if lease == nil {
		return errors.New("nil lease")
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()

	key := registryKey(lease.AgentID, lease.RuntimeMode, lease.ScopeKey)
	rec, ok := sr.byKey[key]
	if !ok {
		return fmt.Errorf("session for agent %s not found", lease.AgentID)
	}
	if rec.LockOwner != lease.LockOwner {
		return fmt.Errorf("lease owner mismatch: have=%s want=%s", rec.LockOwner, lease.LockOwner)
	}

	rec.LockOwner = ""
	rec.LockExpiresAt = time.Time{}
	rec.LastUsedAt = time.Now()
	return nil
}

func (sr *InMemoryRegistry) Rotate(_ context.Context, agentID, runtimeMode, lockOwner, summary, scopeKey string) (*Lease, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	scopeKey = strings.TrimSpace(scopeKey)
	key := registryKey(agentID, runtimeMode, scopeKey)
	rec, ok := sr.byKey[key]
	if !ok {
		return nil, fmt.Errorf("session for agent %s not found", agentID)
	}

	now := time.Now()
	if rec.LockOwner != "" && rec.LockOwner != lockOwner && rec.LockExpiresAt.After(now) {
		return nil, fmt.Errorf("cannot rotate: leased by %s", rec.LockOwner)
	}

	rec.Status = "rotating"
	rec.CheckpointSummary = summary
	rec.SessionID = uuid.NewString()
	rec.Status = "active"
	rec.TurnCount = 0
	rec.LockOwner = lockOwner
	rec.LockExpiresAt = now.Add(sr.lockTTL)
	rec.LastUsedAt = now

	return &Lease{
		SessionID:   rec.SessionID,
		AgentID:     rec.AgentID,
		RuntimeMode: rec.RuntimeMode,
		LockOwner:   rec.LockOwner,
		ScopeKey:    rec.ScopeKey,
		ExpiresAt:   rec.LockExpiresAt,
	}, nil
}

func (sr *InMemoryRegistry) IncrementTurn(_ context.Context, agentID, runtimeMode, sessionID, scopeKey string) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	key := registryKey(agentID, runtimeMode, strings.TrimSpace(scopeKey))
	if rec, ok := sr.byKey[key]; ok {
		if rec.SessionID != sessionID {
			return fmt.Errorf("session mismatch: have=%s want=%s", rec.SessionID, sessionID)
		}
		rec.TurnCount++
		rec.LastUsedAt = time.Now()
		return nil
	}
	return fmt.Errorf("session for agent %s not found", agentID)
}

func (sr *InMemoryRegistry) AdoptSessionID(_ context.Context, agentID, runtimeMode, lockOwner, newSessionID, scopeKey string) error {
	agentID = strings.TrimSpace(agentID)
	runtimeMode = strings.TrimSpace(runtimeMode)
	lockOwner = strings.TrimSpace(lockOwner)
	newSessionID = strings.TrimSpace(newSessionID)
	if agentID == "" || runtimeMode == "" || lockOwner == "" || newSessionID == "" {
		return errors.New("agentID, runtimeMode, lockOwner, and newSessionID are required")
	}
	scopeKey = strings.TrimSpace(scopeKey)

	sr.mu.Lock()
	defer sr.mu.Unlock()

	var (
		rec *Record
		ok  bool
	)
	for _, candidate := range sr.byKey {
		if candidate == nil {
			continue
		}
		if candidate.AgentID != agentID || candidate.RuntimeMode != runtimeMode {
			continue
		}
		if scopeKey != "" && candidate.ScopeKey != scopeKey {
			continue
		}
		if candidate.LockOwner == lockOwner {
			rec = candidate
			ok = true
			break
		}
		if !ok {
			rec = candidate
			ok = true
		}
	}
	if !ok {
		return fmt.Errorf("session for agent %s not found", agentID)
	}
	now := time.Now()
	if rec.LockOwner != "" && rec.LockOwner != lockOwner && rec.LockExpiresAt.After(now) {
		return fmt.Errorf("cannot adopt session id: leased by %s", rec.LockOwner)
	}
	rec.SessionID = newSessionID
	rec.LockOwner = lockOwner
	rec.LockExpiresAt = now.Add(sr.lockTTL)
	rec.LastUsedAt = now
	return nil
}

func (sr *InMemoryRegistry) Snapshot(agentID string) (*Record, bool) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	for _, rec := range sr.byKey {
		if rec == nil || rec.AgentID != agentID {
			continue
		}
		copy := *rec
		return &copy, true
	}
	return nil, false
}

func (sr *InMemoryRegistry) ResetAll(runtimeMode string) error {
	runtimeMode = strings.TrimSpace(runtimeMode)
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if runtimeMode == "" {
		sr.byKey = make(map[string]*Record)
		return nil
	}
	for key, rec := range sr.byKey {
		if rec == nil {
			delete(sr.byKey, key)
			continue
		}
		if strings.TrimSpace(rec.RuntimeMode) == runtimeMode {
			delete(sr.byKey, key)
		}
	}
	return nil
}
