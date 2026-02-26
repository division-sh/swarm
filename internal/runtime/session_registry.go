package runtime

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type SessionRegistry interface {
	Acquire(agentID, runtimeMode, lockOwner, scopeKey string) (*SessionLease, error)
	Release(lease *SessionLease) error
	Rotate(agentID, runtimeMode, lockOwner, summary, scopeKey string) (*SessionLease, error)
	IncrementTurn(agentID, runtimeMode, sessionID, scopeKey string) error
}

type SessionResetter interface {
	ResetAll(runtimeMode string) error
}

type SessionLease struct {
	SessionID   string
	AgentID     string
	RuntimeMode string
	LockOwner   string
	ScopeKey    string
	ExpiresAt   time.Time
}

type SessionRecord struct {
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

// InMemorySessionRegistry is the process-local bootstrap implementation.
// It can be replaced by a Postgres-backed implementation with the same API.
type InMemorySessionRegistry struct {
	mu      sync.Mutex
	byKey   map[string]*SessionRecord
	lockTTL time.Duration
}

func NewInMemorySessionRegistry(lockTTL time.Duration) *InMemorySessionRegistry {
	if lockTTL <= 0 {
		lockTTL = 120 * time.Second
	}
	return &InMemorySessionRegistry{
		byKey:   make(map[string]*SessionRecord),
		lockTTL: lockTTL,
	}
}

// NewSessionRegistry keeps backward compatibility with earlier bootstrap code.
func NewSessionRegistry(lockTTL time.Duration) SessionRegistry {
	return NewInMemorySessionRegistry(lockTTL)
}

func sessionRegistryKey(agentID, runtimeMode, scopeKey string) string {
	return strings.TrimSpace(agentID) + "|" + strings.TrimSpace(runtimeMode) + "|" + strings.TrimSpace(scopeKey)
}

func (sr *InMemorySessionRegistry) Acquire(agentID, runtimeMode, lockOwner, scopeKey string) (*SessionLease, error) {
	if agentID == "" || runtimeMode == "" || lockOwner == "" {
		return nil, errors.New("agentID, runtimeMode, and lockOwner are required")
	}
	scopeKey = strings.TrimSpace(scopeKey)

	sr.mu.Lock()
	defer sr.mu.Unlock()

	now := time.Now()
	key := sessionRegistryKey(agentID, runtimeMode, scopeKey)
	rec, ok := sr.byKey[key]
	if !ok || rec.Status != "active" {
		rec = &SessionRecord{
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

	return &SessionLease{
		SessionID:   rec.SessionID,
		AgentID:     rec.AgentID,
		RuntimeMode: rec.RuntimeMode,
		LockOwner:   rec.LockOwner,
		ScopeKey:    rec.ScopeKey,
		ExpiresAt:   rec.LockExpiresAt,
	}, nil
}

func (sr *InMemorySessionRegistry) Release(lease *SessionLease) error {
	if lease == nil {
		return errors.New("nil lease")
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()

	key := sessionRegistryKey(lease.AgentID, lease.RuntimeMode, lease.ScopeKey)
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

func (sr *InMemorySessionRegistry) Rotate(agentID, runtimeMode, lockOwner, summary, scopeKey string) (*SessionLease, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	scopeKey = strings.TrimSpace(scopeKey)
	key := sessionRegistryKey(agentID, runtimeMode, scopeKey)
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

	return &SessionLease{
		SessionID:   rec.SessionID,
		AgentID:     rec.AgentID,
		RuntimeMode: rec.RuntimeMode,
		LockOwner:   rec.LockOwner,
		ScopeKey:    rec.ScopeKey,
		ExpiresAt:   rec.LockExpiresAt,
	}, nil
}

func (sr *InMemorySessionRegistry) IncrementTurn(agentID, runtimeMode, sessionID, scopeKey string) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	key := sessionRegistryKey(agentID, runtimeMode, strings.TrimSpace(scopeKey))
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

func (sr *InMemorySessionRegistry) AdoptSessionID(agentID, runtimeMode, lockOwner, newSessionID, scopeKey string) error {
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
		rec *SessionRecord
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

func (sr *InMemorySessionRegistry) Snapshot(agentID string) (*SessionRecord, bool) {
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

func (sr *InMemorySessionRegistry) ResetAll(runtimeMode string) error {
	runtimeMode = strings.TrimSpace(runtimeMode)
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if runtimeMode == "" {
		sr.byKey = make(map[string]*SessionRecord)
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
