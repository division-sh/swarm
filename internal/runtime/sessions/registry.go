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
	Acquire(ctx context.Context, agentID string, runtimeMode RuntimeMode, sessionScope SessionScope, lockOwner, scopeKey string) (*Lease, error)
	Release(ctx context.Context, lease *Lease) error
	Rotate(ctx context.Context, agentID string, runtimeMode RuntimeMode, sessionScope SessionScope, lockOwner string, rotation RotationMetadata, scopeKey string) (*Lease, error)
	IncrementTurn(ctx context.Context, agentID string, runtimeMode RuntimeMode, sessionScope SessionScope, sessionID, scopeKey string) error
}

type Resetter interface {
	ResetAll(runtimeMode RuntimeMode, metadata ResetMetadata) (ResetSummary, error)
}

type ResetMetadata struct {
	Source string
}

type ResetDisposition struct {
	SessionID         string
	AgentID           string
	RuntimeMode       RuntimeMode
	ScopeKey          string
	PreviousStatus    string
	TerminationReason string
	TerminationDetail string
}

type ResetSummary struct {
	OrphanedSessions []ResetDisposition
}

func (s ResetSummary) OrphanedCount() int {
	return len(s.OrphanedSessions)
}

type Lease struct {
	SessionID            string
	ProviderSessionID    string
	AgentID              string
	RuntimeMode          RuntimeMode
	SessionScope         SessionScope
	RetryReason          string
	RetriesFromSessionID string
	LockOwner            string
	ScopeKey             string
	ExpiresAt            time.Time
}

type RotationMetadata struct {
	CheckpointSummary string
	RetryReason       string
	TerminationReason TerminationReason
	OperationID       string
}

type Record struct {
	SessionID            string
	ProviderSessionID    string
	AgentID              string
	RuntimeMode          RuntimeMode
	ScopeKey             string
	Status               string
	TurnCount            int
	CheckpointSummary    string
	RetryReason          string
	RetriesFromSessionID string
	LockOwner            string
	LockExpiresAt        time.Time
	LastUsedAt           time.Time
	TerminationReason    string
	TerminationDetail    string
	SuccessorSessionID   string
	TerminatedAt         time.Time
	RotationOperationID  string
}

// InMemoryRegistry is the process-local bootstrap implementation.
// It can be replaced by a Postgres-backed implementation with the same API.
type InMemoryRegistry struct {
	mu      sync.Mutex
	byKey   map[string]*Record
	history map[string][]*Record
	lockTTL time.Duration
}

func NewInMemoryRegistry(lockTTL time.Duration) *InMemoryRegistry {
	if lockTTL <= 0 {
		lockTTL = 120 * time.Second
	}
	return &InMemoryRegistry{
		byKey:   make(map[string]*Record),
		history: make(map[string][]*Record),
		lockTTL: lockTTL,
	}
}

var ErrSessionSuspended = errors.New("session exists in suspended state and cannot own live execution")

func NewRegistry(lockTTL time.Duration) Registry {
	return NewInMemoryRegistry(lockTTL)
}

func registryKey(agentID string, runtimeMode RuntimeMode, scopeKey string) string {
	return strings.TrimSpace(agentID) + "|" + runtimeMode.String() + "|" + strings.TrimSpace(scopeKey)
}

func (sr *InMemoryRegistry) Acquire(ctx context.Context, agentID string, runtimeMode RuntimeMode, sessionScope SessionScope, lockOwner, scopeKey string) (*Lease, error) {
	if agentID == "" || runtimeMode == "" || lockOwner == "" {
		return nil, errors.New("agentID, runtimeMode, and lockOwner are required")
	}
	resolved, err := ResolveScope(ctx, runtimeMode, sessionScope, scopeKey)
	if err != nil {
		return nil, err
	}
	if resolved.Stateless {
		return nil, errors.New("task-scoped sessions are stateless")
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()

	now := time.Now()
	key := registryKey(agentID, resolved.RuntimeMode, resolved.ScopeKey)
	rec, ok := sr.byKey[key]
	if ok && rec != nil && rec.Status == "suspended" {
		return nil, ErrSessionSuspended
	}
	if !ok || rec.Status == "" {
		rec = &Record{
			SessionID:   uuid.NewString(),
			AgentID:     agentID,
			RuntimeMode: resolved.RuntimeMode,
			ScopeKey:    resolved.ScopeKey,
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
		SessionID:            rec.SessionID,
		ProviderSessionID:    rec.ProviderSessionID,
		AgentID:              rec.AgentID,
		RuntimeMode:          rec.RuntimeMode,
		SessionScope:         resolved.Scope,
		RetryReason:          rec.RetryReason,
		RetriesFromSessionID: rec.RetriesFromSessionID,
		LockOwner:            rec.LockOwner,
		ScopeKey:             rec.ScopeKey,
		ExpiresAt:            rec.LockExpiresAt,
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

func (sr *InMemoryRegistry) Rotate(ctx context.Context, agentID string, runtimeMode RuntimeMode, sessionScope SessionScope, lockOwner string, rotation RotationMetadata, scopeKey string) (*Lease, error) {
	resolved, err := ResolveScope(ctx, runtimeMode, sessionScope, scopeKey)
	if err != nil {
		return nil, err
	}
	if resolved.Stateless {
		return nil, errors.New("task-scoped sessions are stateless")
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()

	key := registryKey(agentID, resolved.RuntimeMode, resolved.ScopeKey)
	rec, ok := sr.byKey[key]
	if !ok {
		return nil, fmt.Errorf("session for agent %s not found", agentID)
	}
	operationID := strings.TrimSpace(rotation.OperationID)
	if operationID != "" && rec.RotationOperationID == operationID && rec.Status == "active" {
		return &Lease{
			SessionID: rec.SessionID, ProviderSessionID: rec.ProviderSessionID, AgentID: rec.AgentID,
			RuntimeMode: rec.RuntimeMode, SessionScope: resolved.Scope, RetryReason: rec.RetryReason,
			RetriesFromSessionID: rec.RetriesFromSessionID, LockOwner: rec.LockOwner,
			ScopeKey: rec.ScopeKey, ExpiresAt: rec.LockExpiresAt,
		}, nil
	}

	now := time.Now()
	if rec.LockOwner != "" && rec.LockOwner != lockOwner && rec.LockExpiresAt.After(now) {
		return nil, fmt.Errorf("cannot rotate: leased by %s", rec.LockOwner)
	}

	retryReason := strings.TrimSpace(rotation.RetryReason)
	terminationReason := rotation.TerminationReason
	if terminationReason == "" {
		mappedReason, _, err := rotationTermination(retryReason)
		if err != nil {
			return nil, err
		}
		terminationReason = mappedReason
	}
	if err := validateRuntimeTerminationReason(terminationReason); err != nil {
		return nil, err
	}
	oldSessionID := rec.SessionID
	terminated := *rec
	terminated.Status = "terminated"
	terminated.LockOwner = ""
	terminated.LockExpiresAt = time.Time{}
	terminated.TerminationReason = terminationReason.String()
	terminated.TerminationDetail = retryReason
	terminated.SuccessorSessionID = uuid.NewString()
	terminated.TerminatedAt = now
	terminated.LastUsedAt = now
	sr.history[key] = append(sr.history[key], &terminated)

	rec.SessionID = terminated.SuccessorSessionID
	rec.ProviderSessionID = ""
	rec.Status = "active"
	rec.CheckpointSummary = strings.TrimSpace(rotation.CheckpointSummary)
	rec.RetryReason = retryReason
	rec.RetriesFromSessionID = oldSessionID
	rec.TurnCount = 0
	rec.LockOwner = lockOwner
	rec.LockExpiresAt = now.Add(sr.lockTTL)
	rec.LastUsedAt = now
	rec.TerminationReason = ""
	rec.TerminationDetail = ""
	rec.SuccessorSessionID = ""
	rec.TerminatedAt = time.Time{}
	rec.RotationOperationID = operationID

	return &Lease{
		SessionID:            rec.SessionID,
		ProviderSessionID:    rec.ProviderSessionID,
		AgentID:              rec.AgentID,
		RuntimeMode:          rec.RuntimeMode,
		SessionScope:         resolved.Scope,
		RetryReason:          rec.RetryReason,
		RetriesFromSessionID: rec.RetriesFromSessionID,
		LockOwner:            rec.LockOwner,
		ScopeKey:             rec.ScopeKey,
		ExpiresAt:            rec.LockExpiresAt,
	}, nil
}

func (sr *InMemoryRegistry) IncrementTurn(ctx context.Context, agentID string, runtimeMode RuntimeMode, sessionScope SessionScope, sessionID, scopeKey string) error {
	resolved, err := ResolveScope(ctx, runtimeMode, sessionScope, scopeKey)
	if err != nil {
		return err
	}
	if resolved.Stateless {
		return errors.New("task-scoped sessions are stateless")
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()
	key := registryKey(agentID, resolved.RuntimeMode, resolved.ScopeKey)
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

func (sr *InMemoryRegistry) AdoptSessionID(ctx context.Context, agentID string, runtimeMode RuntimeMode, sessionScope SessionScope, lockOwner, newSessionID, scopeKey string) error {
	agentID = strings.TrimSpace(agentID)
	lockOwner = strings.TrimSpace(lockOwner)
	newSessionID = strings.TrimSpace(newSessionID)
	if agentID == "" || runtimeMode == "" || lockOwner == "" || newSessionID == "" {
		return errors.New("agentID, runtimeMode, lockOwner, and newSessionID are required")
	}
	resolved, err := ResolveScope(ctx, runtimeMode, sessionScope, scopeKey)
	if err != nil {
		return err
	}
	if resolved.Stateless {
		return nil
	}

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
		if candidate.AgentID != agentID || candidate.RuntimeMode != resolved.RuntimeMode {
			continue
		}
		if resolved.ScopeKey != "" && candidate.ScopeKey != resolved.ScopeKey {
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
	rec.ProviderSessionID = newSessionID
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

func (sr *InMemoryRegistry) History(agentID string) []Record {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	out := make([]Record, 0)
	for _, entries := range sr.history {
		for _, rec := range entries {
			if rec == nil || rec.AgentID != agentID {
				continue
			}
			out = append(out, *rec)
		}
	}
	return out
}

func (sr *InMemoryRegistry) ResetAll(runtimeMode RuntimeMode, metadata ResetMetadata) (ResetSummary, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	summary := ResetSummary{}
	source := strings.TrimSpace(metadata.Source)
	if runtimeMode == "" {
		now := time.Now()
		for key, rec := range sr.byKey {
			if rec == nil {
				continue
			}
			terminated := *rec
			terminated.Status = "terminated"
			terminated.TerminationReason = TerminationReasonOrphaned.String()
			terminated.TerminationDetail = source
			terminated.TerminatedAt = now
			terminated.LockOwner = ""
			terminated.LockExpiresAt = time.Time{}
			terminated.SuccessorSessionID = ""
			sr.history[key] = append(sr.history[key], &terminated)
			summary.OrphanedSessions = append(summary.OrphanedSessions, ResetDisposition{
				SessionID:         terminated.SessionID,
				AgentID:           terminated.AgentID,
				RuntimeMode:       terminated.RuntimeMode,
				ScopeKey:          terminated.ScopeKey,
				PreviousStatus:    rec.Status,
				TerminationReason: terminated.TerminationReason,
				TerminationDetail: terminated.TerminationDetail,
			})
		}
		sr.byKey = make(map[string]*Record)
		return summary, nil
	}
	if runtimeMode == RuntimeModeTask {
		return summary, nil
	}
	now := time.Now()
	for key, rec := range sr.byKey {
		if rec == nil {
			delete(sr.byKey, key)
			continue
		}
		if rec.RuntimeMode == runtimeMode {
			terminated := *rec
			terminated.Status = "terminated"
			terminated.TerminationReason = TerminationReasonOrphaned.String()
			terminated.TerminationDetail = source
			terminated.TerminatedAt = now
			terminated.LockOwner = ""
			terminated.LockExpiresAt = time.Time{}
			terminated.SuccessorSessionID = ""
			sr.history[key] = append(sr.history[key], &terminated)
			summary.OrphanedSessions = append(summary.OrphanedSessions, ResetDisposition{
				SessionID:         terminated.SessionID,
				AgentID:           terminated.AgentID,
				RuntimeMode:       terminated.RuntimeMode,
				ScopeKey:          terminated.ScopeKey,
				PreviousStatus:    rec.Status,
				TerminationReason: terminated.TerminationReason,
				TerminationDetail: terminated.TerminationDetail,
			})
			delete(sr.byKey, key)
		}
	}
	return summary, nil
}
