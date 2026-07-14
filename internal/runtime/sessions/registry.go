package sessions

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type Registry interface {
	Acquire(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*Lease, error)
	Release(ctx context.Context, lease *Lease) error
	Rotate(ctx context.Context, identity agentmemory.Identity, lockOwner string, rotation RotationMetadata) (*Lease, error)
	IncrementTurn(ctx context.Context, identity agentmemory.Identity, sessionID string) error
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
	ProviderSessionID    string
	Identity             agentmemory.Identity
	RetryReason          string
	RetriesFromSessionID string
	LockOwner            string
	ExpiresAt            time.Time
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
	PreviousSessionID  string `json:"previous_session_id"`
	SuccessorSessionID string `json:"successor_session_id,omitempty"`
	RunID              string `json:"run_id"`
	FlowInstance       string `json:"flow_instance"`
	PreviousStatus     string `json:"previous_status"`
	SuccessorStatus    string `json:"successor_status,omitempty"`
}

type LifecycleMutationOutcome struct {
	Action   LifecycleMutationAction    `json:"action"`
	Sessions []LifecycleSessionMutation `json:"sessions,omitempty"`
}

type LifecycleProjectionRequest struct {
	OperationID string
	RequestHash string
	AgentID     string
	Expected    runtimeeffects.LifecycleToken
	Target      runtimeeffects.LifecycleToken
	TargetPhase string
	Plan        LifecycleMutationPlan
	Now         time.Time
}

type LifecycleProjection interface {
	ApplyLifecycleProjection(context.Context, LifecycleProjectionRequest) (LifecycleMutationOutcome, bool, error)
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
	LockExpiresAt        time.Time
	LastUsedAt           time.Time
	TerminationReason    string
	TerminationDetail    string
	SuccessorSessionID   string
	TerminatedAt         time.Time
	RotationOperationID  string
}

// InMemoryRegistry is the process-local implementation used when no persistent
// runtime store is selected.
type InMemoryRegistry struct {
	mu                  sync.Mutex
	byKey               map[string]*Record
	history             map[string][]*Record
	lifecycle           map[string]inMemoryLifecycleProjection
	lifecycleOperations map[string]inMemoryLifecycleOperation
	lockTTL             time.Duration
}

type inMemoryLifecycleProjection struct {
	token runtimeeffects.LifecycleToken
	phase string
}

type inMemoryLifecycleOperation struct {
	requestHash string
	outcome     LifecycleMutationOutcome
}

func NewInMemoryRegistry(lockTTL time.Duration) *InMemoryRegistry {
	if lockTTL <= 0 {
		lockTTL = 120 * time.Second
	}
	return &InMemoryRegistry{
		byKey:               make(map[string]*Record),
		history:             make(map[string][]*Record),
		lifecycle:           make(map[string]inMemoryLifecycleProjection),
		lifecycleOperations: make(map[string]inMemoryLifecycleOperation),
		lockTTL:             lockTTL,
	}
}

var (
	ErrSessionLeased    = errors.New("session currently leased by another worker")
	ErrSessionSuspended = errors.New("session exists in suspended state and cannot own live execution")
)

func NewRegistry(lockTTL time.Duration) Registry {
	return NewInMemoryRegistry(lockTTL)
}

func registryKey(identity agentmemory.Identity) string {
	return identity.Key()
}

func (sr *InMemoryRegistry) Acquire(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*Lease, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(lockOwner) == "" {
		return nil, errors.New("lockOwner is required")
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()
	if err := sr.requireCurrentLifecycleLocked(ctx, identity.AgentID, "acquire"); err != nil {
		return nil, err
	}

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
	rec.LockExpiresAt = now.Add(sr.lockTTL)
	rec.LastUsedAt = now

	return &Lease{
		SessionID:            rec.SessionID,
		ProviderSessionID:    rec.ProviderSessionID,
		Identity:             rec.Identity,
		RetryReason:          rec.RetryReason,
		RetriesFromSessionID: rec.RetriesFromSessionID,
		LockOwner:            rec.LockOwner,
		ExpiresAt:            rec.LockExpiresAt,
	}, nil
}

func (sr *InMemoryRegistry) Release(_ context.Context, lease *Lease) error {
	if lease == nil {
		return errors.New("nil lease")
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()

	key := registryKey(lease.Identity)
	rec, ok := sr.byKey[key]
	if !ok {
		return fmt.Errorf("session for agent %s not found", lease.Identity.AgentID)
	}
	if rec.LockOwner != lease.LockOwner {
		return fmt.Errorf("lease owner mismatch: have=%s want=%s", rec.LockOwner, lease.LockOwner)
	}

	rec.LockOwner = ""
	rec.LockExpiresAt = time.Time{}
	rec.LastUsedAt = time.Now()
	return nil
}

func (sr *InMemoryRegistry) Rotate(ctx context.Context, identity agentmemory.Identity, lockOwner string, rotation RotationMetadata) (*Lease, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return nil, err
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()
	if err := sr.requireCurrentLifecycleLocked(ctx, identity.AgentID, "rotate"); err != nil {
		return nil, err
	}

	key := registryKey(identity)
	rec, ok := sr.byKey[key]
	if !ok {
		return nil, fmt.Errorf("session for agent %s not found", identity.AgentID)
	}
	operationID := strings.TrimSpace(rotation.OperationID)
	if operationID != "" && rec.RotationOperationID == operationID && rec.Status == "active" {
		return &Lease{
			SessionID: rec.SessionID, ProviderSessionID: rec.ProviderSessionID, Identity: rec.Identity, RetryReason: rec.RetryReason,
			RetriesFromSessionID: rec.RetriesFromSessionID, LockOwner: rec.LockOwner,
			ExpiresAt: rec.LockExpiresAt,
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
		Identity:             rec.Identity,
		RetryReason:          rec.RetryReason,
		RetriesFromSessionID: rec.RetriesFromSessionID,
		LockOwner:            rec.LockOwner,
		ExpiresAt:            rec.LockExpiresAt,
	}, nil
}

func (sr *InMemoryRegistry) IncrementTurn(ctx context.Context, identity agentmemory.Identity, sessionID string) error {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return err
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()
	if err := sr.requireCurrentLifecycleLocked(ctx, identity.AgentID, "increment_turn"); err != nil {
		return err
	}
	key := registryKey(identity)
	if rec, ok := sr.byKey[key]; ok {
		if rec.SessionID != sessionID {
			return fmt.Errorf("session mismatch: have=%s want=%s", rec.SessionID, sessionID)
		}
		rec.TurnCount++
		rec.LastUsedAt = time.Now()
		return nil
	}
	return fmt.Errorf("session for agent %s not found", identity.AgentID)
}

func (sr *InMemoryRegistry) requireCurrentLifecycleLocked(ctx context.Context, agentID, operation string) error {
	projection, managed := sr.lifecycle[strings.TrimSpace(agentID)]
	if !managed {
		return nil
	}
	if _, ok := runtimeeffects.DifferentOwnerFromContext(ctx); ok {
		return nil
	}
	token, ok := runtimeeffects.LifecycleTokenFromContext(ctx)
	if ok && token == projection.token && projection.phase == "running" {
		return nil
	}
	return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_generation_not_current", "in-memory-live-session-store", operation, map[string]any{
		"agent_id": strings.TrimSpace(agentID), "current_epoch": projection.token.RuntimeEpoch,
		"current_generation": projection.token.Generation, "current_phase": projection.phase,
	})
}

func (sr *InMemoryRegistry) ApplyLifecycleProjection(_ context.Context, req LifecycleProjectionRequest) (LifecycleMutationOutcome, bool, error) {
	if sr == nil {
		return LifecycleMutationOutcome{}, false, fmt.Errorf("in-memory session registry is required")
	}
	plan, err := req.Plan.Normalize()
	if err != nil {
		return LifecycleMutationOutcome{}, false, err
	}
	if strings.TrimSpace(req.OperationID) == "" || strings.TrimSpace(req.RequestHash) == "" || strings.TrimSpace(req.AgentID) == "" || !req.Target.Valid() || strings.TrimSpace(req.TargetPhase) == "" || req.Now.IsZero() {
		return LifecycleMutationOutcome{}, false, fmt.Errorf("complete in-memory lifecycle projection request is required")
	}
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if prior, ok := sr.lifecycleOperations[req.OperationID]; ok {
		if prior.requestHash != req.RequestHash {
			return LifecycleMutationOutcome{}, true, runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "lifecycle_operation_request_conflict", "in-memory-live-session-store", "lifecycle_projection", map[string]any{"operation_id": req.OperationID})
		}
		return prior.outcome, true, nil
	}
	current, exists := sr.lifecycle[req.AgentID]
	if req.Expected.Valid() {
		if !exists || current.token != req.Expected {
			return LifecycleMutationOutcome{}, false, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "in-memory-live-session-store", "lifecycle_projection", map[string]any{"agent_id": req.AgentID})
		}
	} else if exists {
		return LifecycleMutationOutcome{}, false, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "in-memory-live-session-store", "lifecycle_projection", map[string]any{"agent_id": req.AgentID})
	}
	outcome := LifecycleMutationOutcome{Action: plan.Action}
	if plan.Action != LifecycleMutationNone {
		keys := make([]string, 0, len(sr.byKey))
		for key := range sr.byKey {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			rec := sr.byKey[key]
			if rec == nil || rec.Identity.AgentID != req.AgentID || (rec.Status != "active" && rec.Status != "suspended") {
				continue
			}
			previous := *rec
			mutation := LifecycleSessionMutation{
				PreviousSessionID: previous.SessionID, RunID: previous.Identity.RunID,
				FlowInstance: previous.Identity.FlowInstance, PreviousStatus: previous.Status,
			}
			terminated := previous
			terminated.Status = "terminated"
			terminated.LockOwner = ""
			terminated.LockExpiresAt = time.Time{}
			terminated.TerminationReason = plan.TerminationReason.String()
			terminated.TerminationDetail = plan.TerminationDetail
			terminated.TerminatedAt = req.Now
			terminated.LastUsedAt = req.Now
			if plan.Action == LifecycleMutationRotateCurrentSet {
				mutation.SuccessorSessionID = LifecycleSuccessorSessionID(req.OperationID, previous.SessionID)
				mutation.SuccessorStatus = previous.Status
				terminated.SuccessorSessionID = mutation.SuccessorSessionID
				successor := &Record{
					SessionID: mutation.SuccessorSessionID, Identity: previous.Identity, Status: previous.Status,
					CheckpointSummary: plan.CheckpointSummary, RetriesFromSessionID: previous.SessionID,
					LastUsedAt: req.Now, RotationOperationID: req.OperationID,
				}
				sr.byKey[key] = successor
			} else {
				delete(sr.byKey, key)
			}
			sr.history[key] = append(sr.history[key], &terminated)
			outcome.Sessions = append(outcome.Sessions, mutation)
		}
	}
	sr.lifecycle[req.AgentID] = inMemoryLifecycleProjection{token: req.Target, phase: strings.TrimSpace(req.TargetPhase)}
	sr.lifecycleOperations[req.OperationID] = inMemoryLifecycleOperation{requestHash: req.RequestHash, outcome: outcome}
	return outcome, false, nil
}

func (sr *InMemoryRegistry) AdoptSessionID(ctx context.Context, identity agentmemory.Identity, lockOwner, newSessionID string) error {
	identity = identity.Normalize()
	lockOwner = strings.TrimSpace(lockOwner)
	newSessionID = strings.TrimSpace(newSessionID)
	if err := identity.Validate(); err != nil {
		return err
	}
	if lockOwner == "" || newSessionID == "" {
		return errors.New("lockOwner and newSessionID are required")
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()
	if err := sr.requireCurrentLifecycleLocked(ctx, identity.AgentID, "adopt_provider_session"); err != nil {
		return err
	}

	var (
		rec *Record
		ok  bool
	)
	for _, candidate := range sr.byKey {
		if candidate == nil {
			continue
		}
		if candidate.Identity != identity {
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
		return fmt.Errorf("session for agent %s not found", identity.AgentID)
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
		if rec == nil || rec.Identity.AgentID != agentID {
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
			if rec == nil || rec.Identity.AgentID != agentID {
				continue
			}
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
		terminated.LockExpiresAt = time.Time{}
		terminated.SuccessorSessionID = ""
		sr.history[key] = append(sr.history[key], &terminated)
		summary.OrphanedSessions = append(summary.OrphanedSessions, ResetDisposition{
			SessionID: terminated.SessionID, AgentID: terminated.Identity.AgentID,
			RunID: terminated.Identity.RunID, FlowInstance: terminated.Identity.FlowInstance,
			PreviousStatus: rec.Status, TerminationReason: terminated.TerminationReason,
			TerminationDetail: terminated.TerminationDetail,
		})
		delete(sr.byKey, key)
	}
	return summary, nil
}
