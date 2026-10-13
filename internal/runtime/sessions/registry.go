package sessions

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/google/uuid"
)

type Registry interface {
	Acquire(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*Lease, error)
	Renew(ctx context.Context, lease *Lease) (*Lease, error)
	ReleaseOutcome(ctx context.Context, lease *Lease) (ReleaseResult, error)
	Rotate(ctx context.Context, lease *Lease, rotation RotationMetadata) (*Lease, error)
	IncrementTurnOutcome(ctx context.Context, lease *Lease) (TurnIncrementResult, error)
}

type ReleaseResult struct {
	// Acknowledged means the lease was cleared even if postcommit work failed.
	Acknowledged bool
}

type TurnIncrementResult struct {
	// Acknowledged means the turn count advanced even if postcommit work failed.
	Acknowledged bool
}

type Resetter interface {
	ResetAll(metadata ResetMetadata) (ResetSummary, error)
}

type ResetMetadata struct {
	Source string
}

type ResetDisposition struct {
	SessionID         string
	AgentID           string
	RunID             string
	FlowInstance      string
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
	GrantID              string
	ProviderSessionID    string
	Identity             agentmemory.Identity
	RetryReason          string
	RetriesFromSessionID string
	LockOwner            string
	ExpiresAt            time.Time
}

func (l *Lease) ValidateFor(identity agentmemory.Identity, sessionID string) error {
	if l == nil || strings.TrimSpace(l.GrantID) == "" || strings.TrimSpace(l.LockOwner) == "" ||
		strings.TrimSpace(l.SessionID) != strings.TrimSpace(sessionID) || l.Identity.Normalize() != identity.Normalize() {
		return errors.New("exact session grant does not match mutation target")
	}
	return nil
}

type RotationMetadata struct {
	CheckpointSummary string
	RetryReason       string
	TerminationReason TerminationReason
	OperationID       string
}

type LifecycleMutationAction string

const (
	LifecycleMutationNone                LifecycleMutationAction = "none"
	LifecycleMutationRotateCurrentSet    LifecycleMutationAction = "rotate_current_set"
	LifecycleMutationTerminateCurrentSet LifecycleMutationAction = "terminate_current_set"
)

type LifecycleMutationPlan struct {
	Action            LifecycleMutationAction `json:"action"`
	TerminationReason TerminationReason       `json:"termination_reason,omitempty"`
	TerminationDetail string                  `json:"termination_detail,omitempty"`
	CheckpointSummary string                  `json:"checkpoint_summary,omitempty"`
}

func (p LifecycleMutationPlan) Normalize() (LifecycleMutationPlan, error) {
	p.Action = LifecycleMutationAction(strings.TrimSpace(string(p.Action)))
	if p.Action == "" {
		p.Action = LifecycleMutationNone
	}
	p.TerminationDetail = strings.TrimSpace(p.TerminationDetail)
	p.CheckpointSummary = strings.TrimSpace(p.CheckpointSummary)
	switch p.Action {
	case LifecycleMutationNone:
		if p.TerminationReason != "" || p.TerminationDetail != "" || p.CheckpointSummary != "" {
			return LifecycleMutationPlan{}, fmt.Errorf("subordinate lifecycle action none cannot carry mutation metadata")
		}
	case LifecycleMutationRotateCurrentSet:
		if p.TerminationReason == "" {
			p.TerminationReason = TerminationReasonNormal
		}
		if err := validateRuntimeTerminationReason(p.TerminationReason); err != nil {
			return LifecycleMutationPlan{}, err
		}
	case LifecycleMutationTerminateCurrentSet:
		if p.TerminationReason == "" {
			p.TerminationReason = TerminationReasonNormal
		}
		if err := validateRuntimeTerminationReason(p.TerminationReason); err != nil {
			return LifecycleMutationPlan{}, err
		}
		if p.CheckpointSummary != "" {
			return LifecycleMutationPlan{}, fmt.Errorf("terminate_current_set cannot carry checkpoint_summary")
		}
	default:
		return LifecycleMutationPlan{}, fmt.Errorf("unknown subordinate lifecycle action %q", p.Action)
	}
	return p, nil
}

type LifecycleSessionMutation struct {
	PreviousSessionID  string                 `json:"previous_session_id"`
	SuccessorSessionID string                 `json:"successor_session_id,omitempty"`
	RunID              string                 `json:"run_id"`
	AgentIdentity      agentidentity.Identity `json:"agent_identity"`
	FlowInstance       string                 `json:"flow_instance"`
	PreviousStatus     string                 `json:"previous_status"`
	SuccessorStatus    string                 `json:"successor_status,omitempty"`
}

type LifecycleMutationOutcome struct {
	Action   LifecycleMutationAction    `json:"action"`
	Sessions []LifecycleSessionMutation `json:"sessions,omitempty"`
}

func LifecycleSuccessorSessionID(operationID, previousSessionID string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(strings.TrimSpace(operationID)+"\x00"+strings.TrimSpace(previousSessionID))).String()
}

type Record struct {
	SessionID            string
	ProviderSessionID    string
	Identity             agentmemory.Identity
	Status               string
	TurnCount            int
	CheckpointSummary    string
	RetryReason          string
	RetriesFromSessionID string
	LockOwner            string
	GrantID              string
	LockExpiresAt        time.Time
	LastUsedAt           time.Time
	TerminationReason    string
	TerminationDetail    string
	SuccessorSessionID   string
	TerminatedAt         time.Time
	RotationOperationID  string
}

// InMemoryRegistry owns transient session leases and history only. It never
// authorizes or projects a managed agent lifecycle.
type InMemoryRegistry struct {
	mu               sync.Mutex
	byKey            map[agentmemory.Identity]*Record
	history          map[agentmemory.Identity][]*Record
	rotationReceipts map[string]RotationReceipt
	lockTTL          time.Duration
}

func NewInMemoryRegistry(lockTTL time.Duration) *InMemoryRegistry {
	if lockTTL <= 0 {
		lockTTL = 120 * time.Second
	}
	return &InMemoryRegistry{
		byKey:            make(map[agentmemory.Identity]*Record),
		history:          make(map[agentmemory.Identity][]*Record),
		rotationReceipts: make(map[string]RotationReceipt),
		lockTTL:          lockTTL,
	}
}

var (
	ErrSessionLeased    = errors.New("session currently leased by another worker")
	ErrSessionSuspended = errors.New("session exists in suspended state and cannot own live execution")
)

func NewRegistry(lockTTL time.Duration) Registry {
	return NewInMemoryRegistry(lockTTL)
}

func registryKey(identity agentmemory.Identity) agentmemory.Identity {
	return identity.Normalize()
}

func (sr *InMemoryRegistry) Acquire(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*Lease, error) {
	identity = identity.Normalize()
	if err := agentmemory.ValidateIdentity(identity, true); err != nil {
		return nil, err
	}
	if strings.TrimSpace(lockOwner) == "" {
		return nil, errors.New("lockOwner is required")
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()

	now := time.Now()
	key := registryKey(identity)
	rec, ok := sr.byKey[key]
	if ok && rec != nil && rec.Status == "suspended" {
		return nil, ErrSessionSuspended
	}
	if !ok || rec.Status == "" {
		rec = &Record{
			SessionID: uuid.NewString(),
			Identity:  identity,
			Status:    "active",
		}
		sr.byKey[key] = rec
	}

	if rec.LockOwner != "" && rec.LockExpiresAt.After(now) && rec.LockOwner != lockOwner {
		return nil, fmt.Errorf("session already leased by %s", rec.LockOwner)
	}

	rec.LockOwner = lockOwner
	rec.GrantID = uuid.NewString()
	rec.LockExpiresAt = now.Add(sr.lockTTL)
	rec.LastUsedAt = now

	return &Lease{
		SessionID:            rec.SessionID,
		GrantID:              rec.GrantID,
		ProviderSessionID:    rec.ProviderSessionID,
		Identity:             rec.Identity,
		RetryReason:          rec.RetryReason,
		RetriesFromSessionID: rec.RetriesFromSessionID,
		LockOwner:            rec.LockOwner,
		ExpiresAt:            rec.LockExpiresAt,
	}, nil
}

func (sr *InMemoryRegistry) Renew(ctx context.Context, lease *Lease) (*Lease, error) {
	if lease == nil || strings.TrimSpace(lease.GrantID) == "" {
		return nil, errors.New("exact session grant is required")
	}
	sr.mu.Lock()
	defer sr.mu.Unlock()
	identity := lease.Identity.Normalize()
	rec := sr.byKey[registryKey(identity)]
	now := time.Now()
	if rec == nil || rec.Status != "active" || rec.SessionID != lease.SessionID || rec.LockOwner != lease.LockOwner || rec.GrantID != lease.GrantID || !rec.LockExpiresAt.After(now) {
		return nil, ErrSessionLeased
	}
	if next := now.Add(sr.lockTTL); next.After(rec.LockExpiresAt) {
		rec.LockExpiresAt = next
	}
	result := *lease
	result.ExpiresAt = rec.LockExpiresAt
	return &result, nil
}

func (sr *InMemoryRegistry) ReleaseOutcome(_ context.Context, lease *Lease) (ReleaseResult, error) {
	if lease == nil {
		return ReleaseResult{}, errors.New("nil lease")
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()

	key := registryKey(lease.Identity)
	rec, ok := sr.byKey[key]
	if !ok {
		return ReleaseResult{}, fmt.Errorf("session for agent %s not found", lease.Identity.AgentID())
	}
	if strings.TrimSpace(lease.GrantID) == "" || rec.Status != "active" || rec.SessionID != lease.SessionID || rec.LockOwner != lease.LockOwner || rec.GrantID != lease.GrantID {
		return ReleaseResult{}, ErrSessionLeased
	}

	rec.LockOwner = ""
	rec.GrantID = ""
	rec.LockExpiresAt = time.Time{}
	rec.LastUsedAt = time.Now()
	return ReleaseResult{Acknowledged: true}, nil
}

func (sr *InMemoryRegistry) Rotate(ctx context.Context, lease *Lease, rotation RotationMetadata) (*Lease, error) {
	if lease == nil || strings.TrimSpace(lease.GrantID) == "" {
		return nil, errors.New("exact session grant is required")
	}
	identity, lockOwner := lease.Identity, lease.LockOwner
	request, err := NormalizeRotationRequest(identity, lockOwner, rotation)
	if err != nil {
		return nil, err
	}
	identity, lockOwner, rotation = request.Identity, request.LockOwner, request.Metadata

	sr.mu.Lock()
	defer sr.mu.Unlock()

	key := registryKey(identity)
	rec, ok := sr.byKey[key]
	now := time.Now()
	if rotation.OperationID != "" {
		if receipt, found := sr.rotationReceipts[rotation.OperationID]; found {
			var current *Lease
			if ok && rec != nil && rec.Status == "active" {
				current = &Lease{SessionID: rec.SessionID, ProviderSessionID: rec.ProviderSessionID, Identity: rec.Identity,
					RetryReason: rec.RetryReason, RetriesFromSessionID: rec.RetriesFromSessionID,
					LockOwner: rec.LockOwner, GrantID: rec.GrantID, ExpiresAt: rec.LockExpiresAt}
			}
			return ReplayRotation(request, &receipt, current, now)
		}
	}
	if !ok || rec == nil {
		return nil, fmt.Errorf("session for agent %s not found", identity.AgentID())
	}
	if rec.SessionID != lease.SessionID || rec.LockOwner != lockOwner || rec.GrantID != lease.GrantID || !rec.LockExpiresAt.After(now) {
		return nil, ErrSessionLeased
	}

	retryReason := rotation.RetryReason
	oldSessionID := rec.SessionID
	terminated := *rec
	terminated.Status = "terminated"
	terminated.LockOwner = ""
	terminated.GrantID = ""
	terminated.LockExpiresAt = time.Time{}
	terminated.TerminationReason = rotation.TerminationReason.String()
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
	rec.GrantID = uuid.NewString()
	rec.LockExpiresAt = now.Add(sr.lockTTL)
	rec.LastUsedAt = now
	rec.TerminationReason = ""
	rec.TerminationDetail = ""
	rec.SuccessorSessionID = ""
	rec.TerminatedAt = time.Time{}
	rec.RotationOperationID = rotation.OperationID

	successor := Lease{
		SessionID:            rec.SessionID,
		GrantID:              rec.GrantID,
		ProviderSessionID:    rec.ProviderSessionID,
		Identity:             rec.Identity,
		RetryReason:          rec.RetryReason,
		RetriesFromSessionID: rec.RetriesFromSessionID,
		LockOwner:            rec.LockOwner,
		ExpiresAt:            rec.LockExpiresAt,
	}
	if rotation.OperationID != "" {
		sr.rotationReceipts[rotation.OperationID] = RotationReceipt{OperationID: rotation.OperationID, RequestDigest: request.Digest, Result: successor}
	}
	return &successor, nil
}

func (sr *InMemoryRegistry) IncrementTurnOutcome(ctx context.Context, lease *Lease) (TurnIncrementResult, error) {
	if lease == nil || strings.TrimSpace(lease.GrantID) == "" {
		return TurnIncrementResult{}, errors.New("exact session grant is required")
	}
	identity := lease.Identity.Normalize()
	if err := agentmemory.ValidateIdentity(identity, true); err != nil {
		return TurnIncrementResult{}, err
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()
	key := registryKey(identity)
	if rec, ok := sr.byKey[key]; ok {
		if rec.SessionID != lease.SessionID || rec.LockOwner != lease.LockOwner || rec.GrantID != lease.GrantID || !rec.LockExpiresAt.After(time.Now()) || rec.Status != "active" {
			return TurnIncrementResult{}, ErrSessionLeased
		}
		rec.TurnCount++
		rec.LastUsedAt = time.Now()
		return TurnIncrementResult{Acknowledged: true}, nil
	}
	return TurnIncrementResult{}, fmt.Errorf("session for agent %s not found", identity.AgentID())
}

func (sr *InMemoryRegistry) Snapshot(identity agentmemory.Identity) (*Record, bool) {
	identity = registryKey(identity)
	if err := agentmemory.ValidateIdentity(identity, true); err != nil {
		return nil, false
	}
	sr.mu.Lock()
	defer sr.mu.Unlock()
	rec := sr.byKey[identity]
	if rec == nil {
		return nil, false
	}
	copy := *rec
	return &copy, true
}

func (sr *InMemoryRegistry) History(identity agentmemory.Identity) []Record {
	identity = registryKey(identity)
	if err := agentmemory.ValidateIdentity(identity, true); err != nil {
		return nil
	}
	sr.mu.Lock()
	defer sr.mu.Unlock()
	entries := sr.history[identity]
	out := make([]Record, 0, len(entries))
	for _, rec := range entries {
		if rec != nil {
			out = append(out, *rec)
		}
	}
	return out
}

func (sr *InMemoryRegistry) ResetAll(metadata ResetMetadata) (ResetSummary, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	summary := ResetSummary{}
	source := strings.TrimSpace(metadata.Source)
	now := time.Now()
	for key, rec := range sr.byKey {
		if rec == nil {
			delete(sr.byKey, key)
			continue
		}
		terminated := *rec
		terminated.Status = "terminated"
		terminated.TerminationReason = TerminationReasonOrphaned.String()
		terminated.TerminationDetail = source
		terminated.TerminatedAt = now
		terminated.LockOwner = ""
		terminated.GrantID = ""
		terminated.LockExpiresAt = time.Time{}
		terminated.SuccessorSessionID = ""
		sr.history[key] = append(sr.history[key], &terminated)
		summary.OrphanedSessions = append(summary.OrphanedSessions, ResetDisposition{
			SessionID: terminated.SessionID, AgentID: terminated.Identity.AgentID(),
			RunID: terminated.Identity.RunID, FlowInstance: terminated.Identity.FlowInstance(),
			PreviousStatus: rec.Status, TerminationReason: terminated.TerminationReason,
			TerminationDetail: terminated.TerminationDetail,
		})
		delete(sr.byKey, key)
	}
	return summary, nil
}
