package channelonboarding

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/google/uuid"
)

type TeardownKind string

const (
	TeardownUnbind              TeardownKind = "unbind"
	TeardownProofRevoke         TeardownKind = "proof_revoke"
	TeardownInterfaceRetirement TeardownKind = "interface_retirement"
	TeardownContextRetirement   TeardownKind = "context_retirement"
)

func (k TeardownKind) Valid() bool {
	switch k {
	case TeardownUnbind, TeardownProofRevoke, TeardownInterfaceRetirement, TeardownContextRetirement:
		return true
	default:
		return false
	}
}

type TeardownPhase string

const (
	TeardownReserved         TeardownPhase = "reserved"
	TeardownAuthorityRetired TeardownPhase = "authority_retired"
	TeardownSucceeded        TeardownPhase = "succeeded"
	TeardownFailed           TeardownPhase = "failed"
)

func (p TeardownPhase) Terminal() bool { return p == TeardownSucceeded || p == TeardownFailed }

type TeardownScope struct {
	Interface                    operatorchannel.InterfaceIdentity `json:"interface"`
	BundleHash                   string                            `json:"bundle_hash,omitempty"`
	ContextPublicationGeneration uint64                            `json:"context_publication_generation,omitempty"`
}

func (s TeardownScope) normalized() TeardownScope {
	s.Interface = s.Interface.Normalized()
	s.BundleHash = strings.TrimSpace(s.BundleHash)
	return s
}

func (s TeardownScope) Validate(kind TeardownKind) error {
	s = s.normalized()
	if kind == TeardownContextRetirement {
		if s.BundleHash == "" || s.ContextPublicationGeneration == 0 {
			return fmt.Errorf("%w: context retirement requires exact source artifact and publication identity", ErrInvalidRequest)
		}
		return nil
	}
	if err := s.Interface.Validate(); err != nil {
		return fmt.Errorf("%w: teardown interface: %v", ErrInvalidRequest, err)
	}
	if (s.BundleHash == "") != (s.ContextPublicationGeneration == 0) {
		return fmt.Errorf("%w: teardown bundle hash and publication generation must be supplied together", ErrInvalidRequest)
	}
	return nil
}

type ReserveTeardownRequest struct {
	TeardownID              string
	RequestKeyHash          string
	RequestHash             string
	Kind                    TeardownKind
	PrincipalID             string
	Scope                   TeardownScope
	ExpectedBindingRevision int64
	ExpectedProofRevision   int64
	RequestedAt             time.Time
}

func (r ReserveTeardownRequest) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(r.TeardownID)); err != nil || strings.TrimSpace(r.RequestKeyHash) == "" || strings.TrimSpace(r.RequestHash) == "" || strings.TrimSpace(r.PrincipalID) == "" || !r.Kind.Valid() || r.RequestedAt.IsZero() {
		return fmt.Errorf("%w: teardown identity, request identity, principal, kind, and time are required", ErrInvalidRequest)
	}
	if err := r.Scope.Validate(r.Kind); err != nil {
		return err
	}
	switch r.Kind {
	case TeardownUnbind:
		if r.ExpectedBindingRevision < 1 || r.ExpectedProofRevision != 0 {
			return fmt.Errorf("%w: unbind requires exact binding revision only", ErrInvalidRequest)
		}
	case TeardownProofRevoke:
		if r.ExpectedProofRevision < 1 {
			return fmt.Errorf("%w: proof revoke requires exact proof revision", ErrInvalidRequest)
		}
	case TeardownInterfaceRetirement, TeardownContextRetirement:
		if r.ExpectedBindingRevision != 0 || r.ExpectedProofRevision != 0 {
			return fmt.Errorf("%w: source retirement does not accept identity revisions", ErrInvalidRequest)
		}
	}
	return nil
}

type TeardownOperation struct {
	TeardownID              string        `json:"teardown_id"`
	RequestKeyHash          string        `json:"-"`
	RequestHash             string        `json:"-"`
	Kind                    TeardownKind  `json:"kind"`
	PrincipalID             string        `json:"principal_id"`
	Scope                   TeardownScope `json:"scope"`
	ExpectedBindingRevision int64         `json:"expected_binding_revision,omitempty"`
	ExpectedProofRevision   int64         `json:"expected_proof_revision,omitempty"`
	Phase                   TeardownPhase `json:"phase"`
	Revision                int64         `json:"revision"`
	RetiredOperations       int           `json:"retired_operations"`
	RetiredActivations      int           `json:"retired_activations"`
	FailureCode             string        `json:"failure_code,omitempty"`
	FailureMessage          string        `json:"failure_message,omitempty"`
	RequestedAt             time.Time     `json:"requested_at"`
	UpdatedAt               time.Time     `json:"updated_at"`
	CompletedAt             time.Time     `json:"completed_at,omitzero"`
}

type RetireTeardownAuthorityRequest struct {
	TeardownID       string
	ExpectedRevision int64
	Reason           string
	Now              time.Time
}

type CompleteTeardownRequest struct {
	TeardownID       string
	ExpectedRevision int64
	Succeeded        bool
	FailureCode      string
	FailureMessage   string
	Now              time.Time
}

type TeardownStore interface {
	ReserveChannelTeardown(context.Context, ReserveTeardownRequest) (TeardownOperation, error)
	GetChannelTeardown(context.Context, string) (TeardownOperation, error)
	ListChannelTeardowns(context.Context) ([]TeardownOperation, error)
	RetireChannelTeardownAuthority(context.Context, RetireTeardownAuthorityRequest) (TeardownOperation, error)
	CompleteChannelTeardown(context.Context, CompleteTeardownRequest) (TeardownOperation, error)
}

type DestructiveStore interface {
	TeardownStore
	ListChannelOnboardingOperations(context.Context) ([]Operation, error)
}

type OperationCredentialReleaser interface {
	ReleaseOperation(context.Context, Operation) error
}

type DestructiveIdentityLifecycle interface {
	Principal() (operatorchannel.Principal, error)
	ResolveRetainedInterface(context.Context, string) (operatorchannel.InterfaceIdentity, error)
	CurrentBinding(context.Context, operatorchannel.InterfaceIdentity) (operatorchannel.Binding, error)
	CurrentProof(context.Context, string) (operatorchannel.VerifiedProof, error)
	Unbind(context.Context, string, int64, string, string, time.Time) (operatorchannel.Operation, operatorchannel.Binding, error)
	RevokeProof(context.Context, string, int64, time.Time) (operatorchannel.VerifiedProof, error)
}

type DestructiveService struct {
	store       DestructiveStore
	identities  DestructiveIdentityLifecycle
	credentials OperationCredentialReleaser
	activations ActivationAuthorityRefresher
	now         func() time.Time
	testBarrier TestLifecycleBarrier
}

func NewDestructiveService(store DestructiveStore, identities DestructiveIdentityLifecycle, credentials OperationCredentialReleaser, activations ActivationAuthorityRefresher, now func() time.Time, testBarrier TestLifecycleBarrier) (*DestructiveService, error) {
	if store == nil || identities == nil || credentials == nil || activations == nil {
		return nil, fmt.Errorf("channel destructive lifecycle requires teardown, identity, credential, and activation owners")
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &DestructiveService{store: store, identities: identities, credentials: credentials, activations: activations, now: now, testBarrier: testBarrier}, nil
}

func (s *DestructiveService) Unbind(ctx context.Context, selector string, expectedRevision int64, requestKey, requestHash string) (operatorchannel.Operation, operatorchannel.Binding, error) {
	principal, err := s.identities.Principal()
	if err != nil {
		return operatorchannel.Operation{}, operatorchannel.Binding{}, err
	}
	identity, err := s.identities.ResolveRetainedInterface(ctx, selector)
	if err != nil {
		return operatorchannel.Operation{}, operatorchannel.Binding{}, err
	}
	binding, bindingErr := s.identities.CurrentBinding(ctx, identity)
	if bindingErr == nil && binding.Revision != expectedRevision {
		return operatorchannel.Operation{}, operatorchannel.Binding{}, operatorchannel.ErrRevisionConflict
	}
	if bindingErr != nil {
		replay, replayErr := s.exactTeardownReplay(ctx, TeardownUnbind, requestKey, requestHash)
		if replayErr != nil {
			return operatorchannel.Operation{}, operatorchannel.Binding{}, replayErr
		}
		if !replay {
			return operatorchannel.Operation{}, operatorchannel.Binding{}, bindingErr
		}
	}
	teardown, err := s.store.ReserveChannelTeardown(ctx, ReserveTeardownRequest{
		TeardownID: uuid.NewString(), RequestKeyHash: requestKey, RequestHash: requestHash, Kind: TeardownUnbind,
		PrincipalID: principal.ID, Scope: TeardownScope{Interface: identity}, ExpectedBindingRevision: expectedRevision, RequestedAt: s.now().UTC(),
	})
	if err != nil {
		return operatorchannel.Operation{}, operatorchannel.Binding{}, err
	}
	if teardown.Phase == TeardownFailed {
		return operatorchannel.Operation{}, operatorchannel.Binding{}, fmt.Errorf("%w: teardown %s failed: %s", ErrConflict, teardown.TeardownID, teardown.FailureMessage)
	}
	driveCtx := context.WithoutCancel(ctx)
	if _, err := s.retireAuthority(driveCtx, teardown, "unbind"); err != nil {
		return operatorchannel.Operation{}, operatorchannel.Binding{}, err
	}
	op, successor, err := s.identities.Unbind(driveCtx, selector, expectedRevision, requestKey, requestHash, s.now().UTC())
	if err != nil {
		return op, successor, err
	}
	if _, err := s.complete(driveCtx, teardown.TeardownID, true, "", ""); err != nil {
		return op, successor, err
	}
	return op, successor, nil
}

func (s *DestructiveService) RevokeProof(ctx context.Context, selector string, expectedRevision int64, requestKey, requestHash string) (operatorchannel.VerifiedProof, error) {
	principal, err := s.identities.Principal()
	if err != nil {
		return operatorchannel.VerifiedProof{}, err
	}
	identity, err := s.identities.ResolveRetainedInterface(ctx, selector)
	if err != nil {
		return operatorchannel.VerifiedProof{}, err
	}
	proof, proofErr := s.identities.CurrentProof(ctx, selector)
	if proofErr == nil && (proof.Status != operatorchannel.ProofActive || proof.Revision != expectedRevision || proof.Interface.Normalized() != identity.Normalized()) {
		return operatorchannel.VerifiedProof{}, operatorchannel.ErrProofUnavailable
	}
	if proofErr != nil {
		replay, replayErr := s.exactTeardownReplay(ctx, TeardownProofRevoke, requestKey, requestHash)
		if replayErr != nil {
			return operatorchannel.VerifiedProof{}, replayErr
		}
		if !replay {
			if !errors.Is(proofErr, operatorchannel.ErrNotFound) && !errors.Is(proofErr, operatorchannel.ErrProofUnavailable) {
				return operatorchannel.VerifiedProof{}, proofErr
			}
			return operatorchannel.VerifiedProof{}, operatorchannel.ErrProofUnavailable
		}
	}
	teardown, err := s.store.ReserveChannelTeardown(ctx, ReserveTeardownRequest{
		TeardownID: uuid.NewString(), RequestKeyHash: requestKey, RequestHash: requestHash, Kind: TeardownProofRevoke,
		PrincipalID: principal.ID, Scope: TeardownScope{Interface: identity}, ExpectedProofRevision: expectedRevision, RequestedAt: s.now().UTC(),
	})
	if err != nil {
		return operatorchannel.VerifiedProof{}, err
	}
	if teardown.Phase == TeardownFailed {
		return operatorchannel.VerifiedProof{}, operatorchannel.ErrProofUnavailable
	}
	if teardown.Phase == TeardownReserved && (teardown.Scope.Interface.Validate() != nil || teardown.Scope.Interface.Normalized() != identity.Normalized()) {
		return operatorchannel.VerifiedProof{}, fmt.Errorf("%w: teardown proof identity changed before authority retirement", ErrRevisionConflict)
	}
	driveCtx := context.WithoutCancel(ctx)
	if _, err := s.retireAuthority(driveCtx, teardown, "proof_revoked"); err != nil {
		return operatorchannel.VerifiedProof{}, err
	}
	revoked, err := s.identities.RevokeProof(driveCtx, selector, expectedRevision, s.now().UTC())
	if err != nil {
		return operatorchannel.VerifiedProof{}, err
	}
	if _, err := s.complete(driveCtx, teardown.TeardownID, true, "", ""); err != nil {
		return operatorchannel.VerifiedProof{}, err
	}
	return revoked, nil
}

func (s *DestructiveService) exactTeardownReplay(ctx context.Context, kind TeardownKind, requestKey, requestHash string) (bool, error) {
	operations, err := s.store.ListChannelTeardowns(ctx)
	if err != nil {
		return false, err
	}
	requestKey = strings.TrimSpace(requestKey)
	requestHash = strings.TrimSpace(requestHash)
	for _, operation := range operations {
		if operation.Kind != kind || operation.RequestKeyHash != requestKey {
			continue
		}
		if operation.RequestHash != requestHash {
			return false, fmt.Errorf("%w: teardown idempotency key was already used with different semantic input", ErrConflict)
		}
		return true, nil
	}
	return false, nil
}

func (s *DestructiveService) RetireContext(ctx context.Context, bundleHash string, publicationGeneration uint64, requestKey, requestHash, reason string) (TeardownOperation, error) {
	principal, err := s.identities.Principal()
	if err != nil {
		return TeardownOperation{}, err
	}
	op, err := s.store.ReserveChannelTeardown(ctx, ReserveTeardownRequest{
		TeardownID: uuid.NewString(), RequestKeyHash: requestKey, RequestHash: requestHash, Kind: TeardownContextRetirement,
		PrincipalID: principal.ID, Scope: TeardownScope{BundleHash: bundleHash, ContextPublicationGeneration: publicationGeneration}, RequestedAt: s.now().UTC(),
	})
	if err != nil {
		return TeardownOperation{}, err
	}
	driveCtx := context.WithoutCancel(ctx)
	if op, err = s.retireAuthority(driveCtx, op, reason); err != nil {
		return op, err
	}
	return s.complete(driveCtx, op.TeardownID, true, "", "")
}

func (s *DestructiveService) RetireInterface(ctx context.Context, identity operatorchannel.InterfaceIdentity, requestKey, requestHash, reason string) (TeardownOperation, error) {
	principal, err := s.identities.Principal()
	if err != nil {
		return TeardownOperation{}, err
	}
	op, err := s.store.ReserveChannelTeardown(ctx, ReserveTeardownRequest{
		TeardownID: uuid.NewString(), RequestKeyHash: requestKey, RequestHash: requestHash, Kind: TeardownInterfaceRetirement,
		PrincipalID: principal.ID, Scope: TeardownScope{Interface: identity}, RequestedAt: s.now().UTC(),
	})
	if err != nil {
		return TeardownOperation{}, err
	}
	driveCtx := context.WithoutCancel(ctx)
	if op, err = s.retireAuthority(driveCtx, op, reason); err != nil {
		return op, err
	}
	return s.complete(driveCtx, op.TeardownID, true, "", "")
}

func (s *DestructiveService) Recover(ctx context.Context) error {
	operations, err := s.store.ListChannelTeardowns(ctx)
	if err != nil {
		return err
	}
	for _, op := range operations {
		if op.Phase.Terminal() {
			continue
		}
		resumeCtx := context.WithoutCancel(ctx)
		switch op.Kind {
		case TeardownUnbind:
			_, _, err = s.Unbind(resumeCtx, op.Scope.Interface.Selector, op.ExpectedBindingRevision, op.RequestKeyHash, op.RequestHash)
		case TeardownProofRevoke:
			_, err = s.RevokeProof(resumeCtx, op.Scope.Interface.Selector, op.ExpectedProofRevision, op.RequestKeyHash, op.RequestHash)
		case TeardownInterfaceRetirement:
			_, err = s.RetireInterface(resumeCtx, op.Scope.Interface, op.RequestKeyHash, op.RequestHash, "interface_retired")
		case TeardownContextRetirement:
			_, err = s.RetireContext(resumeCtx, op.Scope.BundleHash, op.Scope.ContextPublicationGeneration, op.RequestKeyHash, op.RequestHash, "runtime_context_retired")
		default:
			err = fmt.Errorf("%w: unsupported teardown kind %q", ErrConflict, op.Kind)
		}
		if err != nil {
			return fmt.Errorf("recover channel teardown %s: %w", op.TeardownID, err)
		}
	}
	return nil
}

func (s *DestructiveService) retireAuthority(ctx context.Context, op TeardownOperation, reason string) (TeardownOperation, error) {
	if op.Phase == TeardownReserved {
		var err error
		op, err = s.store.RetireChannelTeardownAuthority(context.WithoutCancel(ctx), RetireTeardownAuthorityRequest{
			TeardownID: op.TeardownID, ExpectedRevision: op.Revision, Reason: reason, Now: s.now().UTC(),
		})
		if err != nil {
			return op, err
		}
		if s.testBarrier != nil {
			if err := s.testBarrier(TestAfterAuthorityRetirementBeforeCleanup, op.TeardownID); err != nil {
				return op, err
			}
		}
	}
	if op.Phase != TeardownAuthorityRetired && op.Phase != TeardownSucceeded {
		return op, fmt.Errorf("%w: teardown %s is %s", ErrConflict, op.TeardownID, op.Phase)
	}
	if err := s.activations.RefreshChannelActivations(context.WithoutCancel(ctx)); err != nil {
		return op, err
	}
	if err := s.releaseScopedCredentials(context.WithoutCancel(ctx), op.Scope); err != nil {
		return op, err
	}
	return op, nil
}

func (s *DestructiveService) releaseScopedCredentials(ctx context.Context, scope TeardownScope) error {
	operations, err := s.store.ListChannelOnboardingOperations(ctx)
	if err != nil {
		return err
	}
	for _, operation := range operations {
		if !teardownScopeMatchesOperation(scope, operation) {
			continue
		}
		if err := s.credentials.ReleaseOperation(ctx, operation); err != nil {
			return fmt.Errorf("release channel onboarding operation %s credentials: %w", operation.OperationID, err)
		}
	}
	return nil
}

func teardownScopeMatchesOperation(scope TeardownScope, operation Operation) bool {
	scope = scope.normalized()
	if scope.Interface.Validate() == nil {
		return scope.Interface.Normalized() == operation.Interface.Normalized()
	}
	return scope.BundleHash == operation.Coordinate.BundleHash &&
		scope.ContextPublicationGeneration == operation.Coordinate.ContextPublicationGeneration
}

func (s *DestructiveService) complete(ctx context.Context, teardownID string, succeeded bool, code, message string) (TeardownOperation, error) {
	op, err := s.store.GetChannelTeardown(ctx, teardownID)
	if err != nil || op.Phase.Terminal() {
		return op, err
	}
	return s.store.CompleteChannelTeardown(context.WithoutCancel(ctx), CompleteTeardownRequest{
		TeardownID: teardownID, ExpectedRevision: op.Revision, Succeeded: succeeded,
		FailureCode: code, FailureMessage: message, Now: s.now().UTC(),
	})
}
