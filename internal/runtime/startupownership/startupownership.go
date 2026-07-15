package startupownership

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type State string

const (
	StateActive       State = "active"
	StatePrepared     State = "prepared"
	StateProbeSettled State = "probe_settled"
	StateAdmitted     State = "admitted"
	StateCommitted    State = "committed"
	StateRolledBack   State = "rolled_back"
	StateFinalized    State = "finalized"
	StateReleased     State = "released"
)

type AcquireRequest struct {
	OwnerID           string
	BootID            string
	BundleFingerprint string
}

type Authority struct {
	AuthorityID        string    `json:"authority_id"`
	LeaseAuthorityID   string    `json:"lease_authority_id"`
	TransitionOrdinal  uint64    `json:"transition_ordinal"`
	Generation         uint64    `json:"generation"`
	StateVersion       uint64    `json:"state_version"`
	State              State     `json:"state"`
	OwnerID            string    `json:"owner_id"`
	BootID             string    `json:"boot_id"`
	BundleFingerprint  string    `json:"bundle_fingerprint"`
	Backend            string    `json:"backend"`
	HandoffID          string    `json:"handoff_id,omitempty"`
	PredecessorOwnerID string    `json:"predecessor_owner_id,omitempty"`
	PredecessorBootID  string    `json:"predecessor_boot_id,omitempty"`
	PredecessorBundle  string    `json:"predecessor_bundle_fingerprint,omitempty"`
	CandidateOwnerID   string    `json:"candidate_owner_id,omitempty"`
	CandidateBootID    string    `json:"candidate_boot_id,omitempty"`
	ProbeSurfaceIDs    []string  `json:"probe_surface_ids,omitempty"`
	RecordedAt         time.Time `json:"recorded_at"`
}

func (a Authority) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(a.AuthorityID)); err != nil {
		return fmt.Errorf("startup authority id is invalid: %w", err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(a.LeaseAuthorityID)); err != nil {
		return fmt.Errorf("startup lease authority id is invalid: %w", err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(a.BootID)); err != nil {
		return fmt.Errorf("startup boot id is invalid: %w", err)
	}
	if a.TransitionOrdinal == 0 || a.Generation == 0 || a.StateVersion == 0 || strings.TrimSpace(a.OwnerID) == "" || strings.TrimSpace(a.BundleFingerprint) == "" || strings.TrimSpace(a.Backend) == "" || a.RecordedAt.IsZero() {
		return fmt.Errorf("startup authority identity is incomplete")
	}
	switch a.State {
	case StateActive, StatePrepared, StateProbeSettled, StateAdmitted, StateCommitted, StateRolledBack, StateFinalized, StateReleased:
	default:
		return fmt.Errorf("startup authority state %q is invalid", a.State)
	}
	if a.HandoffID != "" {
		if _, err := uuid.Parse(a.HandoffID); err != nil {
			return fmt.Errorf("startup handoff id is invalid: %w", err)
		}
		if strings.TrimSpace(a.PredecessorOwnerID) == "" || strings.TrimSpace(a.PredecessorBootID) == "" || strings.TrimSpace(a.PredecessorBundle) == "" || strings.TrimSpace(a.CandidateOwnerID) == "" || strings.TrimSpace(a.CandidateBootID) == "" {
			return fmt.Errorf("startup handoff authority identity is incomplete")
		}
	}
	return nil
}

func NewColdAuthority(req AcquireRequest, backend string) (Authority, error) {
	authorityID := uuid.NewString()
	a := Authority{
		AuthorityID: authorityID, LeaseAuthorityID: authorityID, TransitionOrdinal: 1, Generation: 1, StateVersion: 1, State: StateActive,
		OwnerID: strings.TrimSpace(req.OwnerID), BootID: strings.TrimSpace(req.BootID), BundleFingerprint: strings.TrimSpace(req.BundleFingerprint),
		Backend: strings.TrimSpace(backend), RecordedAt: time.Now().UTC(),
	}
	return a, a.Validate()
}

type HandoffRequest struct {
	CandidateOwnerID           string
	CandidateBootID            string
	CandidateBundleFingerprint string
}

type Recorder interface {
	RecordRuntimeStartupAuthorityTransition(context.Context, *Authority, ...Authority) error
}

type Lease interface {
	Authority() (Authority, error)
	MarkProbesSettled(context.Context, []string) (Authority, error)
	AdmitExecution(context.Context) (Authority, error)
	PrepareHandoff(context.Context, HandoffRequest) (Handoff, error)
	Release(context.Context) error
}

type Handoff interface {
	Authority() (Authority, error)
	MarkProbesSettled(context.Context, []string) (Authority, error)
	Commit(context.Context) (Authority, error)
	Rollback(context.Context) (Authority, error)
	Finalize(context.Context) (Authority, error)
}

type releaseFunc func(context.Context) error

type controlledLease struct {
	mu        sync.Mutex
	authority Authority
	recorder  Recorder
	release   releaseFunc
	handoff   *controlledHandoff
}

type controlledHandoff struct {
	lease       *controlledLease
	authority   Authority
	predecessor Authority
}

func NewLease(authority Authority, recorder Recorder, release func(context.Context) error) (Lease, error) {
	if err := authority.Validate(); err != nil {
		return nil, err
	}
	return &controlledLease{authority: authority, recorder: recorder, release: release}, nil
}

func (l *controlledLease) Authority() (Authority, error) {
	if l == nil {
		return Authority{}, fmt.Errorf("startup ownership lease is missing")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.authority.Validate(); err != nil {
		return Authority{}, err
	}
	return l.authority, nil
}

func (l *controlledLease) PrepareHandoff(ctx context.Context, req HandoffRequest) (Handoff, error) {
	if l == nil {
		return nil, fmt.Errorf("startup ownership lease is missing")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if (l.authority.State != StateActive && l.authority.State != StateAdmitted && l.authority.State != StateFinalized) || l.handoff != nil {
		return nil, fmt.Errorf("startup ownership lease cannot prepare another handoff")
	}
	if _, err := uuid.Parse(strings.TrimSpace(req.CandidateBootID)); err != nil || strings.TrimSpace(req.CandidateOwnerID) == "" || strings.TrimSpace(req.CandidateBundleFingerprint) == "" {
		return nil, fmt.Errorf("startup ownership handoff candidate identity is invalid")
	}
	handoffID := uuid.NewString()
	a := Authority{
		AuthorityID: handoffID, LeaseAuthorityID: l.authority.LeaseAuthorityID, Generation: l.authority.Generation,
		TransitionOrdinal: l.authority.TransitionOrdinal + 1, StateVersion: 1, State: StatePrepared, OwnerID: strings.TrimSpace(req.CandidateOwnerID), BootID: strings.TrimSpace(req.CandidateBootID),
		BundleFingerprint: strings.TrimSpace(req.CandidateBundleFingerprint), Backend: l.authority.Backend, HandoffID: handoffID,
		PredecessorOwnerID: l.authority.OwnerID, PredecessorBootID: l.authority.BootID, PredecessorBundle: l.authority.BundleFingerprint,
		CandidateOwnerID: strings.TrimSpace(req.CandidateOwnerID), CandidateBootID: strings.TrimSpace(req.CandidateBootID), RecordedAt: nextRecordedAt(l.authority),
	}
	if err := l.recordTransition(ctx, &l.authority, a); err != nil {
		return nil, err
	}
	h := &controlledHandoff{lease: l, authority: a, predecessor: l.authority}
	l.handoff = h
	return h, nil
}

func (l *controlledLease) MarkProbesSettled(ctx context.Context, surfaceIDs []string) (Authority, error) {
	if l == nil {
		return Authority{}, fmt.Errorf("startup ownership lease is missing")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.authority.State != StateActive || l.handoff != nil {
		return Authority{}, fmt.Errorf("startup ownership lease cannot settle probes from %q", l.authority.State)
	}
	next := l.authority
	next.State = StateProbeSettled
	next.TransitionOrdinal++
	next.StateVersion++
	next.ProbeSurfaceIDs = normalizeIDs(surfaceIDs)
	next.RecordedAt = nextRecordedAt(l.authority)
	if err := l.recordTransition(ctx, &l.authority, next); err != nil {
		return Authority{}, err
	}
	l.authority = next
	return next, nil
}

func (l *controlledLease) AdmitExecution(ctx context.Context) (Authority, error) {
	if l == nil {
		return Authority{}, fmt.Errorf("startup ownership lease is missing")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.authority.State != StateProbeSettled || l.handoff != nil {
		return Authority{}, fmt.Errorf("startup ownership lease must settle probes before execution admission")
	}
	next := l.authority
	next.State = StateAdmitted
	next.TransitionOrdinal++
	next.StateVersion++
	next.RecordedAt = nextRecordedAt(l.authority)
	if err := l.recordTransition(ctx, &l.authority, next); err != nil {
		return Authority{}, err
	}
	l.authority = next
	return next, nil
}

func (l *controlledLease) Release(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	if l.authority.State == StateReleased {
		l.mu.Unlock()
		return nil
	}
	next := l.authority
	next.State = StateReleased
	next.TransitionOrdinal++
	next.StateVersion++
	next.RecordedAt = nextRecordedAt(l.authority)
	if err := l.recordTransition(ctx, &l.authority, next); err != nil {
		l.mu.Unlock()
		return err
	}
	l.authority = next
	release := l.release
	l.release = nil
	l.mu.Unlock()
	if release != nil {
		return release(ctx)
	}
	return nil
}

func (h *controlledHandoff) Authority() (Authority, error) {
	if h == nil || h.lease == nil {
		return Authority{}, fmt.Errorf("startup ownership handoff is missing")
	}
	h.lease.mu.Lock()
	defer h.lease.mu.Unlock()
	return h.authority, h.authority.Validate()
}

func (h *controlledHandoff) MarkProbesSettled(ctx context.Context, surfaceIDs []string) (Authority, error) {
	return h.transition(ctx, StatePrepared, StateProbeSettled, func(next *Authority) {
		next.ProbeSurfaceIDs = normalizeIDs(surfaceIDs)
	})
}

func (h *controlledHandoff) Commit(ctx context.Context) (Authority, error) {
	if h == nil || h.lease == nil {
		return Authority{}, fmt.Errorf("startup ownership handoff is missing")
	}
	h.lease.mu.Lock()
	defer h.lease.mu.Unlock()
	if h.authority.State != StateProbeSettled || h.lease.handoff != h {
		return Authority{}, fmt.Errorf("startup ownership handoff must settle probes before commit")
	}
	next := h.authority
	next.AuthorityID = uuid.NewString()
	next.TransitionOrdinal++
	next.Generation++
	next.StateVersion++
	next.State = StateCommitted
	next.RecordedAt = nextRecordedAt(h.authority)
	if err := h.lease.recordTransition(ctx, &h.authority, next); err != nil {
		return Authority{}, err
	}
	h.authority = next
	h.lease.authority = next
	return next, nil
}

func (h *controlledHandoff) Rollback(ctx context.Context) (Authority, error) {
	if h == nil || h.lease == nil {
		return Authority{}, fmt.Errorf("startup ownership handoff is missing")
	}
	h.lease.mu.Lock()
	defer h.lease.mu.Unlock()
	if h.lease.handoff != h || (h.authority.State != StatePrepared && h.authority.State != StateProbeSettled && h.authority.State != StateCommitted) {
		return Authority{}, fmt.Errorf("startup ownership handoff cannot roll back from %q", h.authority.State)
	}
	terminal := h.authority
	terminal.State = StateRolledBack
	terminal.TransitionOrdinal++
	terminal.StateVersion++
	terminal.RecordedAt = nextRecordedAt(h.authority)
	restored := h.predecessor
	restored.AuthorityID = uuid.NewString()
	restored.TransitionOrdinal = terminal.TransitionOrdinal + 1
	restored.Generation = terminal.Generation + 1
	restored.StateVersion = terminal.StateVersion + 1
	restored.State = StateActive
	restored.OwnerID = terminal.PredecessorOwnerID
	restored.BootID = terminal.PredecessorBootID
	restored.HandoffID = ""
	restored.PredecessorOwnerID = ""
	restored.PredecessorBootID = ""
	restored.PredecessorBundle = ""
	restored.CandidateOwnerID = ""
	restored.CandidateBootID = ""
	restored.ProbeSurfaceIDs = nil
	restored.RecordedAt = nextRecordedAt(terminal)
	if err := h.lease.recordTransition(ctx, &h.authority, terminal, restored); err != nil {
		return Authority{}, err
	}
	h.authority = terminal
	h.lease.authority = restored
	h.lease.handoff = nil
	return restored, nil
}

func (h *controlledHandoff) Finalize(ctx context.Context) (Authority, error) {
	if h == nil || h.lease == nil {
		return Authority{}, fmt.Errorf("startup ownership handoff is missing")
	}
	h.lease.mu.Lock()
	defer h.lease.mu.Unlock()
	if h.lease.handoff != h || h.authority.State != StateCommitted {
		return Authority{}, fmt.Errorf("startup ownership handoff cannot finalize from %q", h.authority.State)
	}
	next := h.authority
	next.State = StateFinalized
	next.TransitionOrdinal++
	next.StateVersion++
	next.RecordedAt = nextRecordedAt(h.authority)
	if err := h.lease.recordTransition(ctx, &h.authority, next); err != nil {
		return Authority{}, err
	}
	h.authority = next
	h.lease.authority = next
	h.lease.handoff = nil
	return next, nil
}

func (h *controlledHandoff) transition(ctx context.Context, from, to State, mutate func(*Authority)) (Authority, error) {
	if h == nil || h.lease == nil {
		return Authority{}, fmt.Errorf("startup ownership handoff is missing")
	}
	h.lease.mu.Lock()
	defer h.lease.mu.Unlock()
	if h.lease.handoff != h || h.authority.State != from {
		return Authority{}, fmt.Errorf("startup ownership handoff transition %s -> %s rejected from %s", from, to, h.authority.State)
	}
	next := h.authority
	next.State = to
	next.TransitionOrdinal++
	next.StateVersion++
	next.RecordedAt = nextRecordedAt(h.authority)
	mutate(&next)
	if err := h.lease.recordTransition(ctx, &h.authority, next); err != nil {
		return Authority{}, err
	}
	h.authority = next
	return next, nil
}

func (l *controlledLease) recordTransition(ctx context.Context, previous *Authority, next ...Authority) error {
	if err := ValidateTransitionChain(previous, next...); err != nil {
		return err
	}
	if l.recorder == nil {
		return nil
	}
	return l.recorder.RecordRuntimeStartupAuthorityTransition(ctx, previous, next...)
}

// ValidateTransitionChain is the backend-neutral startup authority state
// machine. Persistence adapters call it again before their compare-and-set so
// the in-memory lease cannot be the only semantic owner.
func ValidateTransitionChain(previous *Authority, next ...Authority) error {
	if previous == nil {
		if len(next) != 1 {
			return fmt.Errorf("initial startup authority transition requires exactly one fact")
		}
		initial := next[0]
		if err := initial.Validate(); err != nil {
			return err
		}
		if initial.State != StateActive || initial.TransitionOrdinal != 1 || initial.Generation != 1 || initial.StateVersion != 1 || initial.AuthorityID != initial.LeaseAuthorityID || initial.HandoffID != "" {
			return fmt.Errorf("initial startup authority fact is malformed")
		}
		return nil
	}
	if err := previous.Validate(); err != nil {
		return fmt.Errorf("previous startup authority is invalid: %w", err)
	}
	if len(next) == 2 {
		return validateRollbackChain(*previous, next[0], next[1])
	}
	if len(next) != 1 {
		return fmt.Errorf("startup authority transition requires one fact, or two facts for rollback")
	}
	candidate := next[0]
	if err := candidate.Validate(); err != nil {
		return err
	}
	if candidate.LeaseAuthorityID != previous.LeaseAuthorityID || candidate.Backend != previous.Backend || candidate.TransitionOrdinal != previous.TransitionOrdinal+1 || !candidate.RecordedAt.After(previous.RecordedAt) {
		return fmt.Errorf("startup authority transition identity or ordering is invalid")
	}
	switch {
	case previous.State == StateActive && candidate.State == StateProbeSettled:
		return validateSameAuthorityTransition(*previous, candidate, true)
	case previous.State == StateProbeSettled && previous.HandoffID == "" && candidate.State == StateAdmitted:
		return validateSameAuthorityTransition(*previous, candidate, false)
	case (previous.State == StateActive || previous.State == StateAdmitted || previous.State == StateFinalized) && candidate.State == StatePrepared:
		return validatePreparedTransition(*previous, candidate)
	case previous.State == StatePrepared && candidate.State == StateProbeSettled:
		return validateSameAuthorityTransition(*previous, candidate, true)
	case previous.State == StateProbeSettled && previous.HandoffID != "" && candidate.State == StateCommitted:
		return validateCommittedTransition(*previous, candidate)
	case previous.State == StateCommitted && candidate.State == StateFinalized:
		return validateSameAuthorityTransition(*previous, candidate, false)
	case (previous.State == StateActive || previous.State == StateAdmitted || previous.State == StateFinalized || (previous.State == StateProbeSettled && previous.HandoffID == "")) && candidate.State == StateReleased:
		return validateSameAuthorityTransition(*previous, candidate, false)
	default:
		return fmt.Errorf("startup authority transition %s -> %s is invalid", previous.State, candidate.State)
	}
}

func validateSameAuthorityTransition(previous, next Authority, allowProbeChange bool) error {
	if next.AuthorityID != previous.AuthorityID || next.Generation != previous.Generation || next.StateVersion != previous.StateVersion+1 ||
		next.OwnerID != previous.OwnerID || next.BootID != previous.BootID || next.BundleFingerprint != previous.BundleFingerprint ||
		next.HandoffID != previous.HandoffID || next.PredecessorOwnerID != previous.PredecessorOwnerID || next.PredecessorBootID != previous.PredecessorBootID ||
		next.PredecessorBundle != previous.PredecessorBundle || next.CandidateOwnerID != previous.CandidateOwnerID || next.CandidateBootID != previous.CandidateBootID {
		return fmt.Errorf("startup authority same-coordinate transition changed immutable identity")
	}
	if !allowProbeChange && !slices.Equal(next.ProbeSurfaceIDs, previous.ProbeSurfaceIDs) {
		return fmt.Errorf("startup authority transition changed settled probe identities")
	}
	if allowProbeChange && len(previous.ProbeSurfaceIDs) != 0 {
		return fmt.Errorf("startup authority probe settlement attempted to replace prior evidence")
	}
	return nil
}

func validatePreparedTransition(previous, next Authority) error {
	if next.Generation != previous.Generation || next.StateVersion != 1 || next.AuthorityID == previous.AuthorityID || next.AuthorityID != next.HandoffID ||
		next.PredecessorOwnerID != previous.OwnerID || next.PredecessorBootID != previous.BootID || next.PredecessorBundle != previous.BundleFingerprint ||
		next.OwnerID != next.CandidateOwnerID || next.BootID != next.CandidateBootID || len(next.ProbeSurfaceIDs) != 0 {
		return fmt.Errorf("prepared startup handoff identity is invalid")
	}
	return nil
}

func validateCommittedTransition(previous, next Authority) error {
	if next.AuthorityID == previous.AuthorityID || next.Generation != previous.Generation+1 || next.StateVersion != previous.StateVersion+1 ||
		next.OwnerID != previous.OwnerID || next.BootID != previous.BootID || next.BundleFingerprint != previous.BundleFingerprint ||
		next.HandoffID != previous.HandoffID || next.PredecessorOwnerID != previous.PredecessorOwnerID || next.PredecessorBootID != previous.PredecessorBootID ||
		next.PredecessorBundle != previous.PredecessorBundle || next.CandidateOwnerID != previous.CandidateOwnerID || next.CandidateBootID != previous.CandidateBootID ||
		!slices.Equal(next.ProbeSurfaceIDs, previous.ProbeSurfaceIDs) {
		return fmt.Errorf("committed startup handoff identity is invalid")
	}
	return nil
}

func validateRollbackChain(previous, terminal, restored Authority) error {
	if previous.State != StatePrepared && previous.State != StateProbeSettled && previous.State != StateCommitted {
		return fmt.Errorf("startup authority cannot roll back from %s", previous.State)
	}
	if terminal.State != StateRolledBack || terminal.LeaseAuthorityID != previous.LeaseAuthorityID || terminal.Backend != previous.Backend ||
		terminal.TransitionOrdinal != previous.TransitionOrdinal+1 || !terminal.RecordedAt.After(previous.RecordedAt) {
		return fmt.Errorf("startup authority rollback terminal fact is invalid")
	}
	if err := validateSameAuthorityTransition(previous, terminal, false); err != nil {
		return err
	}
	if err := restored.Validate(); err != nil {
		return err
	}
	if restored.State != StateActive || restored.LeaseAuthorityID != terminal.LeaseAuthorityID || restored.Backend != terminal.Backend ||
		restored.TransitionOrdinal != terminal.TransitionOrdinal+1 || restored.Generation != terminal.Generation+1 || restored.StateVersion != terminal.StateVersion+1 ||
		restored.AuthorityID == terminal.AuthorityID || restored.OwnerID != terminal.PredecessorOwnerID || restored.BootID != terminal.PredecessorBootID ||
		restored.BundleFingerprint != terminal.PredecessorBundle || restored.HandoffID != "" || len(restored.ProbeSurfaceIDs) != 0 || !restored.RecordedAt.After(terminal.RecordedAt) {
		return fmt.Errorf("startup authority rollback restoration fact is invalid")
	}
	return nil
}

func nextRecordedAt(previous Authority) time.Time {
	now := time.Now().UTC()
	if !now.After(previous.RecordedAt) {
		return previous.RecordedAt.Add(time.Nanosecond)
	}
	return now
}

func normalizeIDs(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			out = append(out, value)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

type Store interface {
	AcquireRuntimeStartupOwnership(context.Context, AcquireRequest) (Lease, error)
}
