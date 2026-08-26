package channelonboarding

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/google/uuid"
)

type IdentityLifecycle interface {
	Principal() (operatorchannel.Principal, error)
	Begin(context.Context, string, operatorchannel.OperationKind, int64, string, string, bool, time.Time) (operatorchannel.Operation, error)
	GetOperation(context.Context, string) (operatorchannel.Operation, error)
	ExpireOperation(context.Context, string, int64, time.Time) (operatorchannel.Operation, error)
	CurrentBinding(context.Context, operatorchannel.InterfaceIdentity) (operatorchannel.Binding, error)
	Readback(context.Context) ([]operatorchannel.Readback, error)
}

type ActivationRefresher interface {
	RefreshChannelActivations(context.Context) error
}

type TerminalActivationError struct {
	Code string
	Err  error
}

func (e *TerminalActivationError) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *TerminalActivationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func NewTerminalActivationError(code string, err error) error {
	code = strings.TrimSpace(code)
	if code == "" || err == nil {
		return fmt.Errorf("terminal channel activation failure requires a code and cause")
	}
	return &TerminalActivationError{Code: code, Err: err}
}

func AsTerminalActivationError(err error) (*TerminalActivationError, bool) {
	var terminal *TerminalActivationError
	if !errors.As(err, &terminal) {
		return nil, false
	}
	return terminal, true
}

type ConfirmationRequest struct {
	Operation  Operation
	Activation ConnectedChannelActivation
	Candidate  Candidate
	Binding    operatorchannel.Binding
}

type ConfirmationResult struct {
	OperationID     string
	TerminalSuccess bool
}

type ConfirmationDispatcher interface {
	DispatchChannelConfirmation(context.Context, ConfirmationRequest) (ConfirmationResult, error)
}

type ReadinessProjector interface {
	ProjectConnectedChannelReadiness(context.Context, Operation, Candidate) (ConnectedChannelReadiness, bool, error)
}

type CredentialRequiredError struct {
	Role     string
	StoreKey string
}

func (e *CredentialRequiredError) Error() string {
	return fmt.Sprintf("credential role %q requires hidden input or an existing admitted credential at %q", e.Role, e.StoreKey)
}

type CatalogProvider func() (*CandidateCatalog, error)

type ServiceOptions struct {
	Store        Store
	Identities   IdentityLifecycle
	Credentials  *CredentialWriter
	Catalog      CatalogProvider
	Activations  ActivationRefresher
	Confirmation ConfirmationDispatcher
	Readiness    ReadinessProjector
	Now          func() time.Time
	Secret       func() (string, error)
}

type Service struct {
	store        Store
	identities   IdentityLifecycle
	credentials  *CredentialWriter
	catalog      CatalogProvider
	activations  ActivationRefresher
	confirmation ConfirmationDispatcher
	readiness    ReadinessProjector
	now          func() time.Time
	secret       func() (string, error)
}

type StartInput struct {
	Verb               Verb
	Selection          CandidateSelection
	IdempotencyKey     string
	ProviderCredential string
	SaveProof          bool
}

type RetryInput struct {
	OperationID        string
	ProviderCredential string
}

type Result struct {
	Operation         Operation                  `json:"operation"`
	Candidate         Candidate                  `json:"candidate"`
	IdentityOperation *operatorchannel.Operation `json:"identity_operation,omitempty"`
	Binding           *operatorchannel.Binding   `json:"binding,omitempty"`
	Readiness         *ConnectedChannelReadiness `json:"readiness,omitempty"`
}

func NewService(opts ServiceOptions) (*Service, error) {
	if opts.Store == nil || opts.Identities == nil || opts.Credentials == nil || opts.Catalog == nil || opts.Activations == nil || opts.Confirmation == nil || opts.Readiness == nil {
		return nil, fmt.Errorf("channel onboarding service requires store, identity, credential, catalog, activation, confirmation, and readiness owners")
	}
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}
	if opts.Secret == nil {
		opts.Secret = randomSigningSecret
	}
	return &Service{
		store: opts.Store, identities: opts.Identities, credentials: opts.Credentials, catalog: opts.Catalog,
		activations: opts.Activations, confirmation: opts.Confirmation, readiness: opts.Readiness, now: opts.Now, secret: opts.Secret,
	}, nil
}

func (s *Service) Start(ctx context.Context, input StartInput) (Result, error) {
	if s == nil || !input.Verb.Valid() {
		return Result{}, fmt.Errorf("%w: an explicit connect, reconnect, or rebind verb is required", ErrInvalidRequest)
	}
	principal, err := s.identities.Principal()
	if err != nil {
		return Result{}, err
	}
	catalog, err := s.catalog()
	if err != nil {
		return Result{}, err
	}
	candidate, err := catalog.Resolve(input.Selection)
	if err != nil {
		return Result{}, err
	}
	reservations := credentialReservations(candidate)
	requestHash := operatorchannel.Hash(
		"channel-onboarding-request-v1", principal.ID, string(input.Verb), candidate.Provider,
		candidate.Coordinate.BundleHash, candidate.Coordinate.BundleSource, candidate.Coordinate.BundleIdentity,
		candidate.Coordinate.PackInventoryGeneration, fmt.Sprint(candidate.Coordinate.ContextPublicationGeneration),
		candidate.Coordinate.PlanGeneration.Diagnostic(), fmt.Sprint(candidate.Coordinate.TargetGeneration),
		candidate.Interface.Key(), candidate.Target.Selector, string(candidate.Posture), string(candidate.Ceremony),
		candidate.ProviderCredentialRole, candidate.SigningCredentialRole, candidate.ConfirmationOperation,
		candidate.ConnectionHealth, fmt.Sprint(input.SaveProof),
	)
	requestKey := operatorchannel.Hash("channel-onboarding-key-v1", principal.ID, strings.TrimSpace(input.IdempotencyKey))
	if strings.TrimSpace(input.IdempotencyKey) == "" {
		requestKey = operatorchannel.Hash("channel-onboarding-key-v1", principal.ID, uuid.NewString())
	}
	start := StartRequest{
		OperationID: uuid.NewString(), RequestKeyHash: requestKey, RequestHash: requestHash, PrincipalID: principal.ID,
		Verb: input.Verb, Provider: candidate.Provider, Interface: candidate.Interface, Coordinate: candidate.Coordinate,
		TargetSelector: candidate.Target.Selector, Posture: candidate.Posture, Ceremony: candidate.Ceremony,
		SaveProof: input.SaveProof, CredentialReservations: reservations, RequestedAt: s.now().UTC(),
	}
	if existing, found, err := findOperationByRequestKey(ctx, s.store, requestKey); err != nil {
		return Result{}, err
	} else if found {
		if existing.RequestHash != requestHash {
			return Result{}, fmt.Errorf("%w: onboarding idempotency key was already used with different semantic input", ErrConflict)
		}
		return s.drive(context.WithoutCancel(ctx), existing, candidate, input.ProviderCredential)
	}
	state, err := s.slotState(ctx, start)
	if err != nil {
		return Result{}, err
	}
	decision := AdmitVerb(input.Verb, state)
	if decision != AdmissionStart && decision != AdmissionStartPreservingIdentity && decision != AdmissionStartReplacingIdentity {
		return Result{}, admissionError(input.Verb, state, decision)
	}
	op, err := s.store.ReserveChannelOnboarding(ctx, start)
	if err != nil {
		return Result{}, err
	}
	return s.drive(context.WithoutCancel(ctx), op, candidate, input.ProviderCredential)
}

func (s *Service) Get(ctx context.Context, operationID string) (Result, error) {
	op, err := s.store.GetChannelOnboarding(ctx, strings.TrimSpace(operationID))
	if err != nil {
		return Result{}, err
	}
	candidate, err := s.currentCandidate(op)
	if err != nil {
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
			return s.result(ctx, op, historicalCandidate(op))
		}
		return Result{Operation: op}, err
	}
	return s.result(ctx, op, candidate)
}

// ReadbackConnectedChannels composes retained identity state with the operation
// that owns each current activation, falling back to the latest operation only
// when no current activation exists. It is the only list/status projection.
func (s *Service) ReadbackConnectedChannels(ctx context.Context) ([]ConnectedChannelReadback, error) {
	if s == nil {
		return nil, fmt.Errorf("channel onboarding service is required")
	}
	identities, err := s.identities.Readback(ctx)
	if err != nil {
		return nil, err
	}
	identityByKey := make(map[string]operatorchannel.Readback, len(identities))
	for _, identity := range identities {
		identityByKey[identity.Interface.Normalized().Key()] = identity
	}
	operations, err := s.store.ListChannelOnboardingOperations(ctx)
	if err != nil {
		return nil, err
	}
	latestBySlot := make(map[string]Operation, len(operations))
	operationByID := make(map[string]Operation, len(operations))
	for _, operation := range operations {
		operationByID[operation.OperationID] = operation
		current, found := latestBySlot[operation.SlotKey]
		if !found || operation.UpdatedAt.After(current.UpdatedAt) || (operation.UpdatedAt.Equal(current.UpdatedAt) && operation.Revision > current.Revision) {
			latestBySlot[operation.SlotKey] = operation
		}
	}
	slots := make([]string, 0, len(latestBySlot))
	for slot := range latestBySlot {
		slots = append(slots, slot)
	}
	sort.Strings(slots)
	rows := make([]ConnectedChannelReadback, 0, len(slots)+len(identities))
	representedIdentity := map[string]struct{}{}
	for _, slot := range slots {
		operation := latestBySlot[slot]
		activation, activationErr := s.store.GetConnectedChannelActivation(ctx, slot)
		if activationErr == nil {
			owner, found := operationByID[activation.OperationID]
			if !found || owner.SlotKey != slot {
				return nil, fmt.Errorf("%w: current activation %s has no exact owning onboarding operation", ErrConflict, activation.ActivationID)
			}
			operation = owner
		} else if !errors.Is(activationErr, ErrNotFound) {
			return nil, activationErr
		}
		identityKey := operation.Interface.Normalized().Key()
		identity, found := identityByKey[identityKey]
		if !found {
			return nil, fmt.Errorf("%w: onboarding operation %s has no retained channel identity", ErrConflict, operation.OperationID)
		}
		result, err := s.Get(ctx, operation.OperationID)
		if err != nil {
			return nil, err
		}
		row := ConnectedChannelReadback{Identity: identity, Operation: &result.Operation, Readiness: result.Readiness}
		if activationErr == nil {
			row.Activation = &activation
		}
		rows = append(rows, row)
		representedIdentity[identityKey] = struct{}{}
	}
	for _, identity := range identities {
		if _, represented := representedIdentity[identity.Interface.Normalized().Key()]; represented {
			continue
		}
		row := ConnectedChannelReadback{Identity: identity}
		if identity.Status == operatorchannel.BindingCurrent {
			recovery, err := s.activationRecovery(identity.Interface)
			if err != nil {
				return nil, err
			}
			row.Recovery = recovery
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (s *Service) activationRecovery(identity operatorchannel.InterfaceIdentity) (*ConnectedChannelRecovery, error) {
	catalog, err := s.catalog()
	if err != nil {
		return nil, err
	}
	identity = identity.Normalized()
	matches := []Candidate{}
	provider := ""
	for _, candidate := range catalog.Candidates() {
		if candidate.Interface.Normalized() != identity {
			continue
		}
		if provider != "" && provider != candidate.Provider {
			return nil, fmt.Errorf("%w: retained channel identity %q maps to multiple providers", ErrConflict, identity.Selector)
		}
		provider = candidate.Provider
		matches = append(matches, candidate)
	}
	if len(matches) == 0 {
		return nil, nil
	}
	commands := make([]string, 0, len(matches))
	if len(matches) == 1 {
		commands = append(commands, "swarm channel reconnect "+provider)
	} else {
		for _, candidate := range matches {
			commands = append(commands, fmt.Sprintf(
				"swarm channel reconnect %s --bundle %s --interface %s --target %s",
				provider, candidate.Coordinate.BundleHash, candidate.Interface.Selector, candidate.Target.Selector,
			))
		}
	}
	return &ConnectedChannelRecovery{Reason: ReadinessActivationUnavailable, Provider: provider, Commands: commands}, nil
}

func (s *Service) Retry(ctx context.Context, input RetryInput) (Result, error) {
	op, err := s.store.GetChannelOnboarding(ctx, strings.TrimSpace(input.OperationID))
	if err != nil {
		return Result{}, err
	}
	candidate, err := s.currentCandidate(op)
	if err != nil {
		return Result{Operation: op}, err
	}
	return s.drive(context.WithoutCancel(ctx), op, candidate, input.ProviderCredential)
}

func (s *Service) Recover(ctx context.Context) error {
	operations, err := s.store.ListChannelOnboardingOperations(ctx)
	if err != nil {
		return err
	}
	for _, op := range operations {
		if op.Phase.Terminal() {
			continue
		}
		candidate, err := s.currentCandidate(op)
		if err != nil {
			return err
		}
		if _, err := s.drive(context.WithoutCancel(ctx), op, candidate, ""); err != nil {
			var credentialRequired *CredentialRequiredError
			if errors.As(err, &credentialRequired) {
				continue
			}
			return fmt.Errorf("recover channel onboarding %s: %w", op.OperationID, err)
		}
	}
	return nil
}

func (s *Service) drive(ctx context.Context, op Operation, candidate Candidate, providerCredential string) (Result, error) {
	for {
		switch op.Phase {
		case PhasePreparing:
			admissions, err := s.admitCredentials(ctx, op, candidate, providerCredential)
			if err != nil {
				return s.blockedResult(ctx, op, candidate, err)
			}
			op, err = s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseActivatingProvider,
				CredentialAdmissions: admissions, Now: s.now().UTC(),
			})
			if err != nil {
				return Result{}, err
			}
		case PhaseActivatingProvider:
			if err := s.activations.RefreshChannelActivations(ctx); err != nil {
				if terminal, ok := AsTerminalActivationError(err); ok {
					failed, failErr := s.failOperation(ctx, op, terminal.Code, terminal.Error())
					if failErr != nil {
						return Result{}, errors.Join(err, failErr)
					}
					return s.result(ctx, failed, candidate)
				}
				return s.blockedResult(ctx, op, candidate, err)
			}
			var err error
			op, err = s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseAwaitingExternalIdentity, Now: s.now().UTC()})
			if err != nil {
				return Result{}, err
			}
		case PhaseAwaitingExternalIdentity:
			next, blocked, err := s.advanceIdentity(ctx, op, candidate)
			if err != nil {
				return Result{}, err
			}
			op = next
			if blocked {
				return s.result(ctx, op, candidate)
			}
		case PhaseAwaitingOperatorConfirmation:
			if op.IdentityOperationID != "" {
				identityOp, identityErr := s.currentIdentityOperation(ctx, op.IdentityOperationID)
				if identityErr != nil {
					return Result{}, identityErr
				}
				if identityOp.State.Terminal() && identityOp.State != operatorchannel.StateBound {
					failed, failErr := s.failOperation(ctx, op, "identity_"+string(identityOp.State), fmt.Sprintf("identity operation %s ended in %s", identityOp.OperationID, identityOp.State))
					if failErr != nil {
						return Result{}, failErr
					}
					return s.result(ctx, failed, candidate)
				}
			}
			binding, blocked, err := s.confirmedBinding(ctx, op)
			if err != nil {
				return Result{}, err
			}
			if blocked {
				return s.result(ctx, op, candidate)
			}
			op, err = s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhasePublishingActivation,
				BindingRevision: binding.Revision, Now: s.now().UTC(),
			})
			if err != nil {
				return Result{}, err
			}
		case PhasePublishingActivation:
			binding, _, err := s.confirmedBinding(ctx, op)
			if err != nil {
				return Result{}, err
			}
			op, _, err = s.store.PublishConnectedChannelActivation(ctx, PublishActivationRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, ActivationID: uuid.NewString(),
				BindingRevision: binding.Revision, ProofID: binding.ProofID, ProofRevision: binding.ProofRevision, Now: s.now().UTC(),
			})
			if err != nil {
				return Result{}, err
			}
			if err := s.activations.RefreshChannelActivations(ctx); err != nil {
				return s.blockedResult(ctx, op, candidate, err)
			}
		case PhaseDeliveringConfirmation:
			if strings.TrimSpace(op.ConfirmationOperationID) == "" {
				confirmationOperationID, err := ConfirmationOperationID(op.OperationID, op.ActivationRevision)
				if err != nil {
					return Result{}, err
				}
				op, err = s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
					OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseDeliveringConfirmation,
					ConfirmationOperationID: confirmationOperationID, Now: s.now().UTC(),
				})
				if err != nil {
					return Result{}, err
				}
				continue
			}
			activation, err := s.store.GetConnectedChannelActivation(ctx, op.SlotKey)
			if err != nil {
				return Result{}, err
			}
			binding, err := s.identities.CurrentBinding(ctx, op.Interface)
			if err != nil {
				return Result{}, err
			}
			confirmation, err := s.confirmation.DispatchChannelConfirmation(ctx, ConfirmationRequest{Operation: op, Activation: activation, Candidate: candidate, Binding: binding})
			if err != nil {
				return s.blockedResult(ctx, op, candidate, err)
			}
			if confirmation.OperationID != op.ConfirmationOperationID {
				return Result{}, fmt.Errorf("%w: confirmation dispatcher returned operation %q for exact operation %q", ErrConflict, confirmation.OperationID, op.ConfirmationOperationID)
			}
			if !confirmation.TerminalSuccess {
				return s.result(ctx, op, candidate)
			}
			op, err = s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseSucceeded,
				Now: s.now().UTC(),
			})
			if err != nil {
				return Result{}, err
			}
		case PhaseSucceeded, PhaseFailed, PhaseRetired:
			return s.result(ctx, op, candidate)
		default:
			return Result{}, fmt.Errorf("%w: unsupported onboarding phase %q", ErrConflict, op.Phase)
		}
	}
}

func (s *Service) admitCredentials(ctx context.Context, op Operation, candidate Candidate, providerCredential string) ([]CredentialAdmission, error) {
	admissions := make([]CredentialAdmission, 0, len(op.CredentialReservations))
	for _, reservation := range op.CredentialReservations {
		value := ""
		switch reservation.Role {
		case candidate.ProviderCredentialRole:
			value = providerCredential
		case candidate.SigningCredentialRole:
			if observed, err := s.credentials.Observe(ctx, reservation.StoreKey); err == nil {
				admissions = append(admissions, observedCredentialAdmission(op.OperationID, reservation, observed))
				continue
			}
			var err error
			value, err = s.secret()
			if err != nil {
				return nil, err
			}
		}
		if strings.TrimSpace(value) == "" {
			observed, err := s.credentials.Observe(ctx, reservation.StoreKey)
			if err != nil {
				return nil, errors.Join(&CredentialRequiredError{Role: reservation.Role, StoreKey: reservation.StoreKey}, err)
			}
			admissions = append(admissions, observedCredentialAdmission(op.OperationID, reservation, observed))
			continue
		}
		written, err := s.credentials.Admit(ctx, CredentialWriteRequest{StoreKey: reservation.StoreKey, Value: value, Receipt: credentialReceipt(op.OperationID, reservation.Role)})
		if err != nil {
			return nil, err
		}
		admissions = append(admissions, CredentialAdmission{Role: reservation.Role, StoreKey: written.StoreKey, Kind: CredentialAdmissionWritten, Receipt: written.Receipt, Epoch: written.Epoch})
	}
	return admissions, nil
}

func (s *Service) advanceIdentity(ctx context.Context, op Operation, candidate Candidate) (Operation, bool, error) {
	if op.Verb == VerbReconnect {
		binding, err := s.identities.CurrentBinding(ctx, op.Interface)
		if err != nil {
			return op, false, err
		}
		next, err := s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
			OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseAwaitingOperatorConfirmation,
			BindingRevision: binding.Revision, Now: s.now().UTC(),
		})
		return next, false, err
	}
	if op.IdentityOperationID == "" {
		expectedRevision := int64(0)
		kind := operatorchannel.OperationConnect
		if op.Verb == VerbRebind {
			kind = operatorchannel.OperationRebind
			binding, err := s.identities.CurrentBinding(ctx, op.Interface)
			if err != nil {
				return op, false, err
			}
			expectedRevision = binding.Revision
		}
		identityOp, err := s.identities.Begin(ctx, candidate.Interface.Selector, kind, expectedRevision,
			operatorchannel.Hash("channel-onboarding-identity-key-v1", op.OperationID),
			operatorchannel.Hash("channel-onboarding-identity-request-v1", op.OperationID, string(op.Verb), candidate.Interface.Key()),
			op.SaveProof, s.now().UTC())
		if err != nil {
			return op, false, err
		}
		next, err := s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
			OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseAwaitingExternalIdentity,
			IdentityOperationID: identityOp.OperationID, Now: s.now().UTC(),
		})
		return next, true, err
	}
	identityOp, err := s.currentIdentityOperation(ctx, op.IdentityOperationID)
	if err != nil {
		return op, false, err
	}
	switch identityOp.State {
	case operatorchannel.StateAwaitingClaim:
		return op, true, nil
	case operatorchannel.StateAwaitingConfirmation, operatorchannel.StateBound:
		next, err := s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
			OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseAwaitingOperatorConfirmation,
			IdentityOperationID: identityOp.OperationID, BindingRevision: identityOp.BindingRevision, Now: s.now().UTC(),
		})
		return next, identityOp.State != operatorchannel.StateBound, err
	default:
		failed, err := s.failOperation(ctx, op, "identity_"+string(identityOp.State), fmt.Sprintf("identity operation %s ended in %s", identityOp.OperationID, identityOp.State))
		return failed, false, err
	}
}

func (s *Service) currentIdentityOperation(ctx context.Context, operationID string) (operatorchannel.Operation, error) {
	identityOp, err := s.identities.GetOperation(ctx, operationID)
	if err != nil {
		return operatorchannel.Operation{}, err
	}
	now := s.now().UTC()
	if !identityOp.State.Terminal() && !identityOp.ExpiresAt.IsZero() && !identityOp.ExpiresAt.After(now) {
		return s.identities.ExpireOperation(context.WithoutCancel(ctx), identityOp.OperationID, identityOp.Revision, now)
	}
	return identityOp, nil
}

func (s *Service) failOperation(ctx context.Context, op Operation, code, message string) (Operation, error) {
	for _, admission := range op.CredentialAdmissions {
		if _, err := s.credentials.Release(context.WithoutCancel(ctx), admission); err != nil {
			return op, fmt.Errorf("release failed onboarding credential %q: %w", admission.StoreKey, err)
		}
	}
	failed, err := s.store.AdvanceChannelOnboarding(context.WithoutCancel(ctx), AdvanceRequest{
		OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseFailed,
		FailureCode: strings.TrimSpace(code), FailureMessage: strings.TrimSpace(message), Now: s.now().UTC(),
	})
	if err != nil {
		return op, err
	}
	if err := s.activations.RefreshChannelActivations(context.WithoutCancel(ctx)); err != nil {
		return failed, fmt.Errorf("refresh channel activations after terminal onboarding failure: %w", err)
	}
	return failed, nil
}

func (s *Service) confirmedBinding(ctx context.Context, op Operation) (operatorchannel.Binding, bool, error) {
	if op.IdentityOperationID != "" {
		identityOp, err := s.currentIdentityOperation(ctx, op.IdentityOperationID)
		if err != nil {
			return operatorchannel.Binding{}, false, err
		}
		if identityOp.State != operatorchannel.StateBound {
			return operatorchannel.Binding{}, true, nil
		}
	}
	binding, err := s.identities.CurrentBinding(ctx, op.Interface)
	if err != nil {
		return operatorchannel.Binding{}, false, err
	}
	if op.BindingRevision > 0 && binding.Revision != op.BindingRevision {
		return operatorchannel.Binding{}, false, fmt.Errorf("%w: current identity binding revision changed during onboarding", ErrRevisionConflict)
	}
	return binding, false, nil
}

func (s *Service) slotState(ctx context.Context, req StartRequest) (SlotState, error) {
	operations, err := s.store.ListChannelOnboardingOperations(ctx)
	if err != nil {
		return "", err
	}
	for _, op := range operations {
		if op.SlotKey == req.SlotKey() && !op.Phase.Terminal() {
			return SlotOperationPending, nil
		}
	}
	binding, bindingErr := s.identities.CurrentBinding(ctx, req.Interface)
	bindingCurrent := bindingErr == nil && binding.Status == operatorchannel.BindingCurrent
	if bindingErr != nil && !errors.Is(bindingErr, operatorchannel.ErrNotFound) {
		return "", bindingErr
	}
	activation, activationErr := s.store.GetConnectedChannelActivation(ctx, req.SlotKey())
	activationCurrent := activationErr == nil && activation.Status == ActivationCurrent
	if activationErr != nil && !errors.Is(activationErr, ErrNotFound) {
		return "", activationErr
	}
	if !bindingCurrent && !activationCurrent {
		return SlotAbsent, nil
	}
	if bindingCurrent && !activationCurrent {
		return SlotIdentityCurrentActivationAbsent, nil
	}
	if !bindingCurrent {
		return SlotUncertain, nil
	}
	if !activation.Coordinate.Matches(req.Coordinate) || activation.BindingRevision != binding.Revision {
		return SlotActivationStale, nil
	}
	return SlotReady, nil
}

func (s *Service) currentCandidate(op Operation) (Candidate, error) {
	catalog, err := s.catalog()
	if err != nil {
		return Candidate{}, err
	}
	candidate, current := catalog.FindExact(op.Provider, op.Interface, op.Coordinate, op.TargetSelector)
	if !current {
		return Candidate{}, fmt.Errorf("%w: onboarding runtime context was replaced; explicit retry against a new operation is required", ErrConflict)
	}
	return candidate, nil
}

func historicalCandidate(op Operation) Candidate {
	return Candidate{
		Provider: op.Provider, Interface: op.Interface, Coordinate: op.Coordinate,
		Target:  CandidateTarget{Selector: op.TargetSelector, Provider: op.Provider, Generation: op.Coordinate.TargetGeneration},
		Posture: op.Posture, Ceremony: op.Ceremony,
	}
}

func (s *Service) result(ctx context.Context, op Operation, candidate Candidate) (Result, error) {
	result := Result{Operation: op, Candidate: candidate}
	if op.IdentityOperationID != "" {
		identityOp, err := s.identities.GetOperation(ctx, op.IdentityOperationID)
		if err != nil {
			return result, err
		}
		identityOp = operatorchannel.ProjectOperationReadback(identityOp)
		result.IdentityOperation = &identityOp
	}
	if binding, err := s.identities.CurrentBinding(ctx, op.Interface); err == nil {
		result.Binding = &binding
	}
	readiness, found, err := s.readiness.ProjectConnectedChannelReadiness(ctx, op, candidate)
	if err != nil {
		return result, err
	}
	if found {
		result.Readiness = &readiness
	}
	return result, nil
}

func (s *Service) blockedResult(ctx context.Context, op Operation, candidate Candidate, cause error) (Result, error) {
	result, err := s.result(ctx, op, candidate)
	return result, errors.Join(cause, err)
}

func findOperationByRequestKey(ctx context.Context, store Store, key string) (Operation, bool, error) {
	operations, err := store.ListChannelOnboardingOperations(ctx)
	if err != nil {
		return Operation{}, false, err
	}
	for _, op := range operations {
		if op.RequestKeyHash == key {
			return op, true, nil
		}
	}
	return Operation{}, false, nil
}

func credentialReservations(candidate Candidate) []CredentialReservation {
	roles := []string{candidate.ProviderCredentialRole}
	if candidate.SigningCredentialRole != "" {
		roles = append(roles, candidate.SigningCredentialRole)
	}
	reservations := make([]CredentialReservation, 0, len(roles))
	for _, role := range roles {
		reservations = append(reservations, CredentialReservation{
			Role:     role,
			StoreKey: "channel." + candidate.Provider + "." + operatorchannel.Hash("channel-credential-slot-v1", candidate.Coordinate.BundleHash, candidate.Interface.Key(), candidate.Target.Selector) + "." + role,
		})
	}
	return reservations
}

func credentialReceipt(operationID, role string) string {
	return operatorchannel.Hash("channel-onboarding-credential-receipt-v1", operationID, role)
}

func observedCredentialAdmission(operationID string, reservation CredentialReservation, observed CredentialWriteResult) CredentialAdmission {
	return CredentialAdmission{
		Role: reservation.Role, StoreKey: reservation.StoreKey, Kind: CredentialAdmissionObserved,
		Receipt: operatorchannel.Hash("channel-onboarding-credential-observation-v1", operationID, reservation.Role, observed.Epoch),
		Epoch:   observed.Epoch,
	}
}

func ConfirmationOperationID(onboardingOperationID string, activationRevision int64) (string, error) {
	if _, err := uuid.Parse(strings.TrimSpace(onboardingOperationID)); err != nil || activationRevision < 1 {
		return "", fmt.Errorf("%w: confirmation requires an exact onboarding operation and activation revision", ErrInvalidRequest)
	}
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("channel-confirmation-v1:%s:%d", onboardingOperationID, activationRevision))).String(), nil
}

func admissionError(verb Verb, state SlotState, decision AdmissionDecision) error {
	return fmt.Errorf("%w: channel %s is not valid while the selected slot is %s (%s)", ErrConflict, verb, state, decision)
}

func randomSigningSecret() (string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
