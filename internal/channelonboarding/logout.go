package channelonboarding

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

// SessionLogoutTarget is frozen responsibility evidence, not an SDK execution
// permit. Only the retained private connection can admit its occurrence.
type SessionLogoutTarget struct {
	OperationID       string                                  `json:"operation_id"`
	OperationRevision int64                                   `json:"operation_revision"`
	OccurrenceID      string                                  `json:"occurrence_id"`
	TargetSelector    string                                  `json:"target_selector"`
	Account           operatorchannel.SessionAccountAdmission `json:"account"`
	Coordinate        ChannelRuntimeContextCoordinate         `json:"coordinate"`
}

func (t SessionLogoutTarget) Validate() error {
	if uuid.Validate(t.OperationID) != nil || uuid.Validate(t.OccurrenceID) != nil || t.OperationRevision < 1 || strings.TrimSpace(t.TargetSelector) == "" ||
		t.Account.Provider != "whatsapp" || t.Account.Validate() != nil || t.Coordinate.ValidateContext() != nil {
		return fmt.Errorf("%w: logout requires exact paired session, operation revision, source and SDK occurrence", ErrInvalidRequest)
	}
	return nil
}

func (t SessionLogoutTarget) MatchesOperation(op Operation) bool {
	return t.Validate() == nil && op.Posture == ActivationSessionConnection &&
		op.Provider == t.Account.Provider && op.OperationID == t.OperationID && op.Revision == t.OperationRevision &&
		op.SessionConnectionID == t.Account.ConnectionID && op.SessionAccount == t.Account && op.Coordinate == t.Coordinate && op.TargetSelector == t.TargetSelector
}

type SessionLogoutLifecycle interface {
	PrepareSessionLogout(context.Context, Operation) (SessionLogoutTarget, error)
	DispatchSessionLogout(context.Context, TeardownOperation) error
}

// Pending logout retains the original transport while refusing business use.
// Ordinary inactive-session cleanup must not disconnect before unlink settles.
func RetainedSessionLogoutPending(ctx context.Context, store Store, retained Operation) (bool, error) {
	if ctx == nil || store == nil || retained.Posture != ActivationSessionConnection || retained.ValidateSessionAccount() != nil {
		return false, ErrInvalidRequest
	}
	rows, err := store.ListChannelTeardowns(ctx)
	if err != nil {
		return false, err
	}
	matched, pending := false, false
	for _, op := range rows {
		if op.Kind != TeardownLogout || op.Logout == nil || op.Logout.Account.ConnectionID != retained.SessionConnectionID {
			continue
		}
		if matched || op.Logout.Validate() != nil || op.PrincipalID != retained.PrincipalID || op.Logout.Account != retained.SessionAccount {
			return false, ErrConflict
		}
		matched = true
		if op.Phase == TeardownAuthorityRetired {
			pending = true
		}
	}
	return pending, ctx.Err()
}

type SessionLogoutReadback struct {
	OperationID       string            `json:"operation_id"`
	ExpectedRevision  int64             `json:"expected_revision"`
	EffectOperationID string            `json:"effect_operation_id"`
	Teardown          TeardownOperation `json:"teardown"`
}

func (r SessionLogoutReadback) Validate() error {
	effectID, err := effects.ChannelLogoutOperationID(r.Teardown.TeardownID)
	if err != nil || effectID != r.EffectOperationID || uuid.Validate(r.OperationID) != nil || r.ExpectedRevision < 1 ||
		r.Teardown.Kind != TeardownLogout || uuid.Validate(r.Teardown.PrincipalID) != nil || r.Teardown.Revision < 1 ||
		!r.Teardown.Phase.Valid() || r.Teardown.Scope.Validate(TeardownLogout) != nil || r.Teardown.Scope.BundleHash != "" ||
		r.Teardown.Scope.ContextPublicationGeneration != 0 || r.Teardown.RequestedAt.IsZero() || r.Teardown.UpdatedAt.IsZero() ||
		r.Teardown.Phase.Terminal() != !r.Teardown.CompletedAt.IsZero() {
		return fmt.Errorf("%w: logout readback contradicts its durable responsibility", ErrConflict)
	}
	return nil
}

func NewSessionLogoutReadback(op TeardownOperation) (SessionLogoutReadback, error) {
	if op.Kind != TeardownLogout || op.Logout == nil || op.Logout.Validate() != nil {
		return SessionLogoutReadback{}, fmt.Errorf("%w: logout readback has no exact retained target", ErrConflict)
	}
	effectID, err := effects.ChannelLogoutOperationID(op.TeardownID)
	if err != nil {
		return SessionLogoutReadback{}, err
	}
	readback := SessionLogoutReadback{OperationID: op.Logout.OperationID, ExpectedRevision: op.Logout.OperationRevision,
		EffectOperationID: effectID, Teardown: op}
	return readback, readback.Validate()
}

func (s *Service) sessionLogoutReadback(ctx context.Context, op Operation) (*SessionLogoutReadback, error) {
	teardowns, err := s.store.ListChannelTeardowns(ctx)
	if err != nil {
		return nil, err
	}
	var result *SessionLogoutReadback
	for _, teardown := range teardowns {
		if teardown.Kind != TeardownLogout || teardown.Logout == nil || teardown.Logout.OperationID != op.OperationID {
			continue
		}
		if result != nil || teardown.PrincipalID != op.PrincipalID || teardown.Logout.Account != op.SessionAccount {
			return nil, fmt.Errorf("%w: contradictory retained logout responsibility", ErrConflict)
		}
		logout, err := NewSessionLogoutReadback(teardown)
		if err != nil {
			return nil, err
		}
		result = &logout
	}
	return result, ctx.Err()
}

func (s *DestructiveService) Logout(ctx context.Context, operationID string, expectedRevision int64, requestKey, requestHash string) (SessionLogoutReadback, error) {
	if ctx == nil {
		return SessionLogoutReadback{}, ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return SessionLogoutReadback{}, err
	}
	if uuid.Validate(operationID) != nil || expectedRevision < 1 || strings.TrimSpace(requestKey) == "" || strings.TrimSpace(requestHash) == "" {
		return SessionLogoutReadback{}, ErrInvalidRequest
	}
	principal, err := s.identities.Principal()
	if err != nil {
		return SessionLogoutReadback{}, err
	}
	previous, err := s.store.ListChannelTeardowns(ctx)
	if err != nil {
		return SessionLogoutReadback{}, err
	}
	for _, op := range previous {
		if op.RequestKeyHash != requestKey {
			continue
		}
		if op.Kind != TeardownLogout || op.PrincipalID != principal.ID || op.RequestHash != requestHash || op.Logout == nil ||
			op.Logout.OperationID != operationID || op.Logout.OperationRevision != expectedRevision {
			return SessionLogoutReadback{}, ErrConflict
		}
		return s.driveSessionLogout(ctx, op)
	}
	retained, target, err := s.prepareSessionLogout(ctx, principal.ID, operationID, expectedRevision)
	if err != nil {
		return SessionLogoutReadback{}, err
	}
	op, err := s.store.ReserveChannelTeardown(ctx, ReserveTeardownRequest{
		TeardownID: uuid.NewString(), RequestKeyHash: requestKey, RequestHash: requestHash,
		Kind: TeardownLogout, PrincipalID: principal.ID, Scope: TeardownScope{Interface: retained.Interface},
		Logout: &target, RequestedAt: s.now().UTC(),
	})
	if err != nil {
		return SessionLogoutReadback{}, err
	}
	return s.driveSessionLogout(ctx, op)
}

func (s *DestructiveService) prepareSessionLogout(ctx context.Context, principalID, operationID string, expectedRevision int64) (Operation, SessionLogoutTarget, error) {
	operations, err := s.store.ListChannelOnboardingOperations(ctx)
	if err != nil {
		return Operation{}, SessionLogoutTarget{}, err
	}
	var retained *Operation
	for _, op := range operations {
		if op.OperationID == operationID {
			if retained != nil {
				return Operation{}, SessionLogoutTarget{}, ErrConflict
			}
			retained = &op
		}
	}
	if retained == nil {
		return Operation{}, SessionLogoutTarget{}, ErrNotFound
	}
	if retained.PrincipalID != principalID || retained.Phase == PhaseFailed || retained.Phase == PhaseRetired ||
		retained.Posture != ActivationSessionConnection || retained.SessionAccount.Validate() != nil || s.logout == nil {
		return Operation{}, SessionLogoutTarget{}, ErrConflict
	}
	if retained.Revision != expectedRevision {
		return Operation{}, SessionLogoutTarget{}, ErrRevisionConflict
	}
	target, err := s.logout.PrepareSessionLogout(ctx, *retained)
	if err != nil {
		return Operation{}, SessionLogoutTarget{}, err
	}
	if !target.MatchesOperation(*retained) {
		return Operation{}, SessionLogoutTarget{}, ErrConflict
	}
	return *retained, target, nil
}

func (s *DestructiveService) driveSessionLogout(ctx context.Context, op TeardownOperation) (SessionLogoutReadback, error) {
	readback, err := NewSessionLogoutReadback(op)
	if err != nil || op.Phase.Terminal() {
		return readback, err
	}
	if s.logout == nil {
		return readback, fmt.Errorf("%w: retained session logout owner is unavailable", ErrConflict)
	}
	if err := s.logout.DispatchSessionLogout(ctx, op); err != nil {
		return readback, err
	}
	current, err := s.store.GetChannelTeardown(ctx, op.TeardownID)
	if err != nil {
		return readback, err
	}
	return NewSessionLogoutReadback(current)
}
