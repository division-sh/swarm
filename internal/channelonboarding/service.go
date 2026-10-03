package channelonboarding

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

var errOnboardingRuntimeContextRetired = errors.New("onboarding runtime context retired")

type IdentityLifecycle interface {
	Principal() (operatorchannel.Principal, error)
	Begin(context.Context, string, operatorchannel.OperationKind, int64, string, string, string, runtimecredentials.ValueEvidence, bool, time.Time) (operatorchannel.Operation, error)
	Confirm(context.Context, string, int64, bool, time.Time) (operatorchannel.Operation, operatorchannel.Binding, error)
	GetOperation(context.Context, string) (operatorchannel.Operation, error)
	ExpireOperation(context.Context, string, int64, time.Time) (operatorchannel.Operation, error)
	CurrentBinding(context.Context, operatorchannel.InterfaceIdentity) (operatorchannel.Binding, error)
	CurrentBindingReadiness(context.Context, operatorchannel.InterfaceIdentity) (operatorchannel.Binding, bool, error)
	Readback(context.Context) ([]operatorchannel.Readback, error)
}

type ActivationAuthorityRefresher interface {
	RefreshChannelActivations(context.Context) error
}

type ActivationRefresher interface {
	ActivationAuthorityRefresher
	AdmitChannelTarget(context.Context, Operation, Candidate) (Candidate, error)
	PreflightChannelActivation(context.Context, Operation, Candidate) error
	RefreshChannelActivationCandidates(context.Context) error
	PublishChannelActivation(context.Context, Operation, ConnectedChannelActivation) error
	PromoteChannelRegistration(context.Context, Operation, ConnectedChannelActivation) error
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

type EffectRebindDisposition struct {
	RetryAllowed                bool
	RemintConfirmationOperation bool
	BlockingEffectOperationID   string
	BlockingEffectState         string
}

type EffectRebindReconciler interface {
	ReconcileChannelEffectsBeforeRebind(context.Context, Operation) (EffectRebindDisposition, error)
}

type EffectRetryBlockedError struct {
	OperationID string
	State       string
}

func (e *EffectRetryBlockedError) Error() string {
	return fmt.Sprintf("channel effect %s is %s; refusing blind retry", strings.TrimSpace(e.OperationID), strings.TrimSpace(e.State))
}

type ReadinessProjector interface {
	ProjectConnectedChannelReadiness(context.Context, Operation, Candidate) (ConnectedChannelReadiness, bool, error)
}

type CredentialRequiredError struct {
	OperationID string
	Role        string
	StoreKey    string
}

func (e *CredentialRequiredError) Error() string {
	return fmt.Sprintf("credential role %q requires hidden input or an existing admitted credential at %q", e.Role, e.StoreKey)
}

func (e *CredentialRequiredError) ResumeCommand() string {
	return "swarm channel resume " + strings.TrimSpace(e.OperationID) + " --credential-stdin"
}

type CatalogProvider func() (*CandidateCatalog, error)

type TestLifecycleBoundary string

const (
	TestAfterCredentialWriteBeforeCheckpoint   TestLifecycleBoundary = "credential_write_before_checkpoint"
	TestAfterStandingTargetReconciliation      TestLifecycleBoundary = "standing_reconciliation_before_target_publication"
	TestAfterStandingTargetPublication         TestLifecycleBoundary = "standing_target_publication_before_registration"
	TestAfterIdentityBeginBeforeCheckpoint     TestLifecycleBoundary = "identity_begin_before_checkpoint"
	TestAfterBindingCheckpointBeforeActivation TestLifecycleBoundary = "binding_checkpoint_before_activation"
	TestAfterStaleIdentitySettlement           TestLifecycleBoundary = "stale_identity_before_parent_reset"
	TestAfterPendingResetCleanup               TestLifecycleBoundary = "pending_reset_cleanup_before_commit"
	TestAfterPendingResetCommit                TestLifecycleBoundary = "pending_reset_commit_before_response"
	TestAfterActivationCommitBeforePublication TestLifecycleBoundary = "activation_commit_before_process_publication"
	TestAfterProcessPublicationBeforePromotion TestLifecycleBoundary = "process_publication_before_promotion"
	TestAfterAuthorityRetirementBeforeCleanup  TestLifecycleBoundary = "authority_retirement_before_cleanup"
)

type TestLifecycleBarrier func(TestLifecycleBoundary, string) error

type ServiceOptions struct {
	SourceArtifacts sourceartifact.Reader
	Store           Store
	Identities      IdentityLifecycle
	Credentials     *CredentialWriter
	Catalog         CatalogProvider
	Activations     ActivationRefresher
	Confirmation    ConfirmationDispatcher
	Readiness       ReadinessProjector
	Now             func() time.Time
	Secret          func() (string, error)
	TestBarrier     TestLifecycleBarrier
}

type Service struct {
	sourceArtifacts sourceartifact.Reader
	store           Store
	identities      IdentityLifecycle
	credentials     *CredentialWriter
	catalog         CatalogProvider
	activations     ActivationRefresher
	confirmation    ConfirmationDispatcher
	effects         EffectRebindReconciler
	readiness       ReadinessProjector
	now             func() time.Time
	secret          func() (string, error)
	driveMu         sync.Mutex
	driveLocks      map[string]*operationDriveLock
	testBarrier     TestLifecycleBarrier
}

type operationDriveLock struct {
	mu   sync.Mutex
	refs int
}

type StartInput struct {
	Verb               Verb
	Selection          CandidateSelection
	IdempotencyKey     string
	ProviderCredential string
	SaveProof          bool
	ClientLanguage     string
}

type RetryInput struct {
	OperationID            string
	ProviderCredential     string
	ClientLanguage         string
	ExpectedLocaleRevision int64
}

type Result struct {
	Operation         Operation                  `json:"operation"`
	Candidate         *Candidate                 `json:"candidate,omitempty"`
	IdentityOperation *operatorchannel.Operation `json:"identity_operation,omitempty"`
	Binding           *operatorchannel.Binding   `json:"binding,omitempty"`
	Readiness         *ConnectedChannelReadiness `json:"readiness,omitempty"`
}

func NewService(opts ServiceOptions) (*Service, error) {
	if opts.Store == nil || opts.Identities == nil || opts.Credentials == nil || opts.Catalog == nil || opts.Activations == nil || opts.Confirmation == nil || opts.Readiness == nil {
		return nil, fmt.Errorf("channel onboarding service requires store, identity, credential, catalog, activation, confirmation, and readiness owners")
	}
	effects, ok := opts.Confirmation.(EffectRebindReconciler)
	if !ok {
		return nil, fmt.Errorf("channel onboarding confirmation owner must reconcile durable effects before retry or rebind")
	}
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}
	if opts.Secret == nil {
		opts.Secret = randomSigningSecret
	}
	return &Service{
		sourceArtifacts: opts.SourceArtifacts,
		store:           opts.Store, identities: opts.Identities, credentials: opts.Credentials, catalog: opts.Catalog,
		activations: opts.Activations, confirmation: opts.Confirmation, effects: effects, readiness: opts.Readiness, now: opts.Now, secret: opts.Secret,
		testBarrier: opts.TestBarrier,
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
	if input.ClientLanguage != "" {
		profile, err := candidate.Plan.NativeInboxProfile()
		if err != nil {
			return Result{}, err
		}
		if err := profile.ValidateLanguage(input.ClientLanguage); err != nil {
			return Result{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}
	}
	reservations := credentialReservations(candidate)
	durable := candidate.Coordinate.DurableIdentity()
	requestHash := operatorchannel.Hash(
		"channel-onboarding-request-v2", principal.ID, string(input.Verb), candidate.Provider,
		durable.BundleHash, durable.BundleIdentity,
		durable.PackInventoryGeneration, durable.PlanGeneration.Diagnostic(),
		candidate.Interface.Key(), candidate.Target.Selector, string(candidate.Posture), string(candidate.Ceremony),
		candidate.ProviderCredentialRole, candidate.SigningCredentialRole, candidate.ConfirmationOperation,
		candidate.ConnectionHealth, fmt.Sprint(input.SaveProof), input.ClientLanguage,
	)
	requestKey := operatorchannel.Hash("channel-onboarding-key-v1", principal.ID, strings.TrimSpace(input.IdempotencyKey))
	if strings.TrimSpace(input.IdempotencyKey) == "" {
		requestKey = operatorchannel.Hash("channel-onboarding-key-v1", principal.ID, uuid.NewString())
	}
	start := StartRequest{
		OperationID: uuid.NewString(), RequestKeyHash: requestKey, RequestHash: requestHash, PrincipalID: principal.ID,
		Verb: input.Verb, Provider: candidate.Provider, Interface: candidate.Interface, Coordinate: candidate.Coordinate,
		TargetSelector: candidate.Target.Selector, Posture: candidate.Posture, Ceremony: candidate.Ceremony,
		SaveProof: input.SaveProof, ClientLanguage: input.ClientLanguage, CredentialReservations: reservations, RequestedAt: s.now().UTC(),
	}
	if existing, found, err := findOperationByRequestKey(ctx, s.store, requestKey); err != nil {
		return Result{}, err
	} else if found {
		if existing.RequestHash != requestHash {
			return Result{}, fmt.Errorf("%w: onboarding idempotency key was already used with different semantic input", ErrConflict)
		}
		if existing.Phase == PhaseFailed || existing.Phase == PhaseRetired {
			return s.Get(ctx, existing.OperationID)
		}
		existing, candidate, err = s.bindCurrentCandidate(context.WithoutCancel(ctx), existing)
		if err != nil {
			return Result{Operation: existing}, err
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
			return s.result(ctx, op, nil)
		}
		return Result{Operation: op}, err
	}
	return s.result(ctx, op, &candidate)
}

// ReadbackConnectedChannels composes retained identity state with every exact
// current activation and active onboarding responsibility. It is the only
// list/status projection.
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
	operationByID := make(map[string]Operation, len(operations))
	for _, operation := range operations {
		if _, duplicate := operationByID[operation.OperationID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate onboarding operation %s in canonical readback", ErrConflict, operation.OperationID)
		}
		operationByID[operation.OperationID] = operation
	}
	sort.Slice(operations, func(i, j int) bool {
		if operations[i].SlotKey != operations[j].SlotKey {
			return operations[i].SlotKey < operations[j].SlotKey
		}
		return operations[i].OperationID < operations[j].OperationID
	})
	activations, err := s.store.ListCurrentConnectedChannelActivations(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(activations, func(i, j int) bool { return activations[i].SlotKey < activations[j].SlotKey })
	rows := make([]ConnectedChannelReadback, 0, len(activations)+len(identities))
	representedIdentity := map[string]struct{}{}
	representedOperation := map[string]struct{}{}
	for index := range activations {
		activation := activations[index]
		operation, found := operationByID[activation.OperationID]
		if !found || operation.SlotKey != activation.SlotKey {
			return nil, fmt.Errorf("%w: current activation %s has no exact owning onboarding operation", ErrConflict, activation.ActivationID)
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
		row := ConnectedChannelReadback{Identity: identity, Operation: &result.Operation, Activation: &activation, Readiness: result.Readiness}
		rows = append(rows, row)
		representedIdentity[identityKey] = struct{}{}
		representedOperation[operation.OperationID] = struct{}{}
	}
	for index := range operations {
		operation := operations[index]
		if operation.Phase.Terminal() {
			continue
		}
		if _, represented := representedOperation[operation.OperationID]; represented {
			continue
		}
		identityKey := operation.Interface.Normalized().Key()
		identity, found := identityByKey[identityKey]
		if !found {
			continue
		}
		if identity.PrincipalID != operation.PrincipalID {
			return nil, fmt.Errorf("%w: onboarding operation %s contradicts its retained channel principal", ErrConflict, operation.OperationID)
		}
		result, err := s.Get(ctx, operation.OperationID)
		if err != nil {
			return nil, err
		}
		rows = append(rows, ConnectedChannelReadback{
			Identity: identity, Operation: &result.Operation, Readiness: result.Readiness,
			Recovery: activeOperationRecovery(result.Operation),
		})
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
	if s.sourceArtifacts != nil {
		for i := range rows {
			label, err := s.connectedChannelSourceLabel(ctx, rows[i])
			if err != nil {
				return nil, err
			}
			rows[i].SourceLabel = label
		}
	}
	return rows, nil
}

func (s *Service) connectedChannelSourceLabel(ctx context.Context, row ConnectedChannelReadback) (string, error) {
	hash := ""
	if row.Activation != nil {
		hash = row.Activation.Coordinate.BundleHash
	} else if row.Operation != nil {
		hash = row.Operation.Coordinate.BundleHash
	}
	if hash == "" {
		return "", nil
	}
	label, err := sourceartifact.LoadHumanLabel(ctx, s.sourceArtifacts, hash)
	if err != nil {
		return "", err
	}
	if row.Operation == nil {
		return label, nil
	}
	catalog, err := s.catalog()
	if err != nil {
		return "", err
	}
	op := row.Operation
	if current, present := catalog.FindDurableSuccessor(op.Provider, op.Interface, op.Coordinate, op.TargetSelector, op.Posture, op.Ceremony); present && current.SourceLabel != "" {
		return current.SourceLabel, nil
	}
	return label, nil
}

func activeOperationRecovery(operation Operation) *ConnectedChannelRecovery {
	return &ConnectedChannelRecovery{
		Reason: ReadinessActivationUnavailable, Provider: operation.Provider, Commands: []string{operation.CredentialRecoveryCommand()},
	}
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
				"swarm channel reconnect %s --source <matching-source-directory> --interface %s --target %s",
				provider, candidate.Interface.Selector, candidate.Target.Selector,
			))
		}
	}
	return &ConnectedChannelRecovery{Reason: ReadinessActivationUnavailable, Provider: provider, Commands: commands}, nil
}

func (s *Service) Retry(ctx context.Context, input RetryInput) (Result, error) {
	operationID := strings.TrimSpace(input.OperationID)
	unlock := s.lockDrive(operationID)
	defer unlock()

	op, err := s.store.GetChannelOnboarding(ctx, operationID)
	if err != nil {
		return Result{}, err
	}
	if op.Phase == PhaseFailed || op.Phase == PhaseRetired {
		if input.ClientLanguage != "" || input.ExpectedLocaleRevision != 0 {
			return Result{Operation: op}, fmt.Errorf("%w: terminal onboarding cannot acquire a client qualification", ErrConflict)
		}
		return s.Get(ctx, op.OperationID)
	}
	if input.ClientLanguage == "" && input.ExpectedLocaleRevision != 0 {
		return Result{Operation: op}, fmt.Errorf("%w: locale revision requires an explicit language", ErrInvalidRequest)
	}
	if input.ClientLanguage != "" {
		candidate, err := s.currentCandidate(op)
		if err != nil {
			return Result{Operation: op}, err
		}
		profile, err := candidate.Plan.NativeInboxProfile()
		if err != nil {
			return Result{Operation: op}, err
		}
		if err := profile.ValidateLanguage(input.ClientLanguage); err != nil {
			return Result{Operation: op}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}
		if input.ExpectedLocaleRevision < 1 {
			return Result{Operation: op}, fmt.Errorf("%w: client language requires its observed locale revision", ErrInvalidRequest)
		}
	}
	rebound, candidate, err := s.bindCurrentCandidate(context.WithoutCancel(ctx), op)
	if err != nil {
		return Result{Operation: op}, fmt.Errorf("bind onboarding retry to current candidate: %w", err)
	}
	if input.ClientLanguage != "" {
		profile, err := candidate.Plan.NativeInboxProfile()
		if err != nil {
			return Result{Operation: rebound}, err
		}
		if err := profile.ValidateLanguage(input.ClientLanguage); err != nil {
			return Result{Operation: rebound}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}
		principal, err := s.identities.Principal()
		if err != nil {
			return Result{Operation: rebound}, err
		}
		rebound, err = s.store.SetChannelClientLocale(ctx, SetClientLocaleRequest{
			OperationID: rebound.OperationID, PrincipalID: principal.ID, ExpectedRevision: input.ExpectedLocaleRevision,
			Language: input.ClientLanguage, Now: s.now().UTC(),
		})
		if err != nil {
			return Result{Operation: op}, err
		}
	}
	result, err := s.driveLocked(context.WithoutCancel(ctx), rebound, candidate, input.ProviderCredential)
	if err != nil {
		return result, fmt.Errorf("drive onboarding retry: %w", err)
	}
	return result, nil
}

// ConfirmIdentity coordinates public claimant confirmation with its durable
// onboarding parent. The identity owner classifies terminal replay first; this
// owner only resets a still-active parent after a credential-stale settlement.
func (s *Service) ConfirmIdentity(ctx context.Context, operationID string, expectedRevision int64, approve bool, now time.Time) (operatorchannel.Operation, operatorchannel.Binding, error) {
	identityBefore, err := s.identities.GetOperation(ctx, strings.TrimSpace(operationID))
	if err != nil {
		return operatorchannel.Operation{}, operatorchannel.Binding{}, err
	}
	parentID := strings.TrimSpace(identityBefore.OnboardingOperationID)
	if parentID == "" {
		return operatorchannel.Operation{}, operatorchannel.Binding{}, fmt.Errorf("%w: identity operation has no durable onboarding parent", ErrConflict)
	}
	unlocks := s.lockDrive(parentID)
	defer unlocks()
	parent, err := s.store.GetChannelOnboarding(ctx, parentID)
	if err != nil {
		return operatorchannel.Operation{}, operatorchannel.Binding{}, err
	}
	identity, binding, confirmErr := s.identities.Confirm(ctx, identityBefore.OperationID, expectedRevision, approve, now)
	if !errors.Is(confirmErr, operatorchannel.ErrCredentialStale) || identity.State != operatorchannel.StateCredentialStale {
		return identity, binding, confirmErr
	}
	if parent.Phase.Terminal() {
		return identity, binding, fmt.Errorf("%w: onboarding parent is already %s", ErrConflict, parent.Phase)
	}
	if parent.IdentityOperationID == identity.OperationID {
		if err := s.reachTestBarrier(TestAfterStaleIdentitySettlement, parent.OperationID); err != nil {
			return identity, binding, err
		}
		parent, err = s.resetCredentialStaleIdentity(context.WithoutCancel(ctx), parent)
		if err != nil {
			return identity, binding, errors.Join(confirmErr, err)
		}
	} else if parent.Phase != PhasePreparing || parent.IdentityOperationID != "" {
		return identity, binding, errors.Join(confirmErr, fmt.Errorf("%w: onboarding parent no longer owns stale identity operation", ErrRevisionConflict))
	}
	return identity, binding, credentialRequiredForStaleIdentity(parent, identity)
}

// ReconcileLocal settles durable, process-independent onboarding facts before
// serve exposes mutation or publishes executable/registration authority. It
// deliberately performs no provider preflight, registration effect, runtime
// publication, or confirmation delivery.
func (s *Service) ReconcileLocal(ctx context.Context) error {
	operations, err := s.store.ListChannelOnboardingOperations(ctx)
	if err != nil {
		return err
	}
	for _, initial := range operations {
		op := initial
		if op.Phase.Terminal() {
			if op.Phase == PhaseFailed {
				if err := s.credentials.ReleaseOperation(context.WithoutCancel(ctx), op); err != nil {
					return fmt.Errorf("reconcile failed channel onboarding %s credential cleanup: %w", op.OperationID, err)
				}
			}
			activation, activationErr := s.store.GetConnectedChannelActivation(ctx, op.SlotKey)
			if activationErr != nil || activation.OperationID != op.OperationID {
				continue
			}
		}
		op, candidate, err := s.bindCurrentCandidate(context.WithoutCancel(ctx), op)
		if err != nil {
			var blocked *EffectRetryBlockedError
			if errors.As(err, &blocked) {
				continue
			}
			if op.Phase.Terminal() {
				continue
			}
			if !errors.Is(err, errOnboardingRuntimeContextRetired) {
				return err
			}
			if _, err := s.failOperationLocal(context.WithoutCancel(ctx), op, "runtime_context_retired", fmt.Sprintf("onboarding runtime context for operation %s is no longer current: %v", op.OperationID, err)); err != nil {
				return fmt.Errorf("retire obsolete channel onboarding %s: %w", op.OperationID, err)
			}
			continue
		}
		for step := 0; step < 5; step++ {
			switch op.Phase {
			case PhasePreparing:
				admissions, err := s.admitCredentials(ctx, op, candidate, "", candidate.Target.Generation > 0)
				var required *CredentialRequiredError
				if errors.As(err, &required) {
					step = 5
					continue
				}
				if err != nil {
					return fmt.Errorf("reconcile local credentials for %s: %w", op.OperationID, err)
				}
				op, err = s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
					OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseCredentialsAdmitted,
					CredentialAdmissions: admissions, ReplaceCredentialAdmissions: true, Now: s.now().UTC(),
				})
				if err != nil {
					return err
				}
			case PhaseCredentialsAdmitted:
				if err := s.validateCredentialAdmissions(ctx, op); err != nil {
					var required *CredentialRequiredError
					if !errors.As(err, &required) {
						return fmt.Errorf("validate local credential admission for %s: %w", op.OperationID, err)
					}
					op, err = s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
						OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhasePreparing,
						ReplaceCredentialAdmissions: true, Now: s.now().UTC(),
					})
					if err != nil {
						return err
					}
					continue
				}
				step = 5
			case PhaseAwaitingExternalIdentity:
				if op.IdentityOperationID == "" {
					step = 5
					continue
				}
				identityOp, err := s.currentIdentityOperation(ctx, op.IdentityOperationID)
				if err != nil {
					return err
				}
				switch identityOp.State {
				case operatorchannel.StateAwaitingClaim:
					step = 5
				case operatorchannel.StateCredentialStale:
					op, err = s.resetCredentialStaleIdentity(context.WithoutCancel(ctx), op)
					if err != nil {
						return err
					}
					step = 5
				case operatorchannel.StateAwaitingConfirmation, operatorchannel.StateBound:
					op, err = s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
						OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseAwaitingOperatorConfirmation,
						IdentityOperationID: identityOp.OperationID, Now: s.now().UTC(),
					})
					if err != nil {
						return err
					}
				default:
					if _, err := s.failOperationLocal(ctx, op, "identity_"+string(identityOp.State), fmt.Sprintf("identity operation %s ended in %s", identityOp.OperationID, identityOp.State)); err != nil {
						return err
					}
					step = 5
				}
			case PhaseAwaitingOperatorConfirmation, PhasePublishingActivation:
				if op.IdentityOperationID != "" {
					identityOp, err := s.currentIdentityOperation(ctx, op.IdentityOperationID)
					if err != nil {
						return err
					}
					if identityOp.State == operatorchannel.StateCredentialStale {
						op, err = s.resetCredentialStaleIdentity(context.WithoutCancel(ctx), op)
						if err != nil {
							return err
						}
						step = 5
						continue
					}
					if identityOp.State.Terminal() && identityOp.State != operatorchannel.StateBound {
						if _, err := s.failOperationLocal(ctx, op, "identity_"+string(identityOp.State), fmt.Sprintf("identity operation %s ended in %s", identityOp.OperationID, identityOp.State)); err != nil {
							return err
						}
						step = 5
						continue
					}
					if identityOp.State != operatorchannel.StateBound {
						step = 5
						continue
					}
				}
				decision, err := s.reconcileConfirmedBinding(ctx, op)
				if err != nil {
					return err
				}
				op = decision.operation
				if decision.blocked || decision.credentialRequired != nil || op.Phase == PhasePublishingActivation {
					step = 5
					continue
				}
				op, err = s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
					OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhasePublishingActivation,
					BindingRevision: decision.binding.Revision, Now: s.now().UTC(),
				})
				if err != nil {
					return err
				}
			default:
				step = 5
			}
		}
	}
	return nil
}

func (s *Service) Recover(ctx context.Context) error {
	operations, err := s.store.ListChannelOnboardingOperations(ctx)
	if err != nil {
		return err
	}
	for _, op := range operations {
		if op.Phase.Terminal() {
			if op.Phase == PhaseFailed {
				if err := s.credentials.ReleaseOperation(context.WithoutCancel(ctx), op); err != nil {
					return fmt.Errorf("recover failed channel onboarding %s credential cleanup: %w", op.OperationID, err)
				}
			}
			continue
		}
		rebound, candidate, err := s.bindCurrentCandidate(context.WithoutCancel(ctx), op)
		if err != nil {
			var blocked *EffectRetryBlockedError
			if errors.As(err, &blocked) {
				continue
			}
			if errors.Is(err, errOnboardingRuntimeContextRetired) {
				_, failErr := s.failOperation(
					context.WithoutCancel(ctx),
					op,
					"runtime_context_retired",
					fmt.Sprintf("onboarding runtime context for operation %s is no longer current", op.OperationID),
				)
				if failErr != nil {
					return fmt.Errorf("retire obsolete channel onboarding %s: %w", op.OperationID, failErr)
				}
				continue
			}
			return err
		}
		op = rebound
		if candidate.Target.Generation == 0 && (op.Phase == PhasePreparing || op.Phase.RequiresExecutableTarget()) {
			// Local recovery may settle historical responsibility, but cannot
			// execute a declaration-only candidate or adopt its default keys.
			continue
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
	unlock := s.lockDrive(op.OperationID)
	defer unlock()
	return s.driveLocked(ctx, op, candidate, providerCredential)
}

func (s *Service) driveLocked(ctx context.Context, op Operation, candidate Candidate, providerCredential string) (Result, error) {
	current, err := s.store.GetChannelOnboarding(ctx, op.OperationID)
	if err != nil {
		return Result{}, err
	}
	if current.RequestHash != op.RequestHash || current.PrincipalID != op.PrincipalID {
		return Result{}, fmt.Errorf("%w: onboarding operation changed before execution", ErrRevisionConflict)
	}
	op = current
	if !op.Coordinate.MatchesDeclaration(candidate.Coordinate) || op.Phase.RequiresExecutableTarget() && candidate.Validate() != nil {
		return Result{Operation: op, Candidate: &candidate}, fmt.Errorf("%w: onboarding operation is not fenced to the exact current runtime occurrence", ErrRevisionConflict)
	}
	for {
		switch op.Phase {
		case PhasePreparing:
			admissions, err := s.admitCredentials(ctx, op, candidate, providerCredential, true)
			if err != nil {
				result, blockedErr := s.blockedResult(ctx, op, candidate, err)
				if blockedErr != nil {
					return result, fmt.Errorf("admit channel credentials: %w", blockedErr)
				}
				return result, nil
			}
			if err := s.reachTestBarrier(TestAfterCredentialWriteBeforeCheckpoint, op.OperationID); err != nil {
				return Result{}, err
			}
			next, err := s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseCredentialsAdmitted,
				CredentialAdmissions: admissions, ReplaceCredentialAdmissions: true, Now: s.now().UTC(),
			})
			if err != nil {
				return Result{}, err
			}
			op = next
		case PhaseCredentialsAdmitted:
			if err := s.validateCredentialAdmissions(ctx, op); err != nil {
				var required *CredentialRequiredError
				if !errors.As(err, &required) {
					return s.blockedResult(ctx, op, candidate, err)
				}
				releaseErr := s.releaseCandidateCredentials(context.WithoutCancel(ctx), op.CredentialAdmissions)
				reset, resetErr := s.store.AdvanceChannelOnboarding(context.WithoutCancel(ctx), AdvanceRequest{
					OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhasePreparing,
					ReplaceCredentialAdmissions: true, Now: s.now().UTC(),
				})
				if resetErr == nil {
					op = reset
				}
				return s.blockedResult(ctx, op, candidate, errors.Join(err, releaseErr, resetErr))
			}
			promoted, err := s.activations.AdmitChannelTarget(ctx, op, candidate)
			if err != nil {
				return s.blockedResult(ctx, op, candidate, fmt.Errorf("admit channel target: %w", err))
			}
			if err := promoted.Validate(); err != nil {
				return Result{}, err
			}
			if !promoted.Coordinate.MatchesDurableIdentity(candidate.Coordinate) || promoted.Target.Selector != candidate.Target.Selector || promoted.Target.AdmissionGeneration != candidate.Target.AdmissionGeneration || promoted.Interface.Normalized() != candidate.Interface.Normalized() {
				return Result{}, fmt.Errorf("%w: admitted channel target changed declaration authority", ErrRevisionConflict)
			}
			if !op.Coordinate.Matches(promoted.Coordinate) {
				next, err := s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
					OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: op.Phase,
					RebindCoordinate: &promoted.Coordinate, Now: s.now().UTC(),
				})
				if err != nil {
					return Result{}, err
				}
				op = next
			}
			candidate = promoted
			if err := s.activations.PreflightChannelActivation(ctx, op, candidate); err != nil {
				if terminal, ok := AsTerminalActivationError(err); ok {
					failed, failErr := s.failOperation(ctx, op, terminal.Code, terminal.Error())
					if failErr != nil {
						return Result{}, errors.Join(err, failErr)
					}
					return s.result(ctx, failed, &candidate)
				}
				releaseErr := s.releaseCandidateCredentials(context.WithoutCancel(ctx), op.CredentialAdmissions)
				reset, resetErr := s.store.AdvanceChannelOnboarding(context.WithoutCancel(ctx), AdvanceRequest{
					OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhasePreparing,
					ReplaceCredentialAdmissions: true, Now: s.now().UTC(),
				})
				if resetErr == nil {
					op = reset
				}
				return s.blockedResult(ctx, op, candidate, errors.Join(fmt.Errorf("preflight channel activation: %w", err), releaseErr, resetErr))
			}
			next, err := s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseActivatingProvider,
				Now: s.now().UTC(),
			})
			if err != nil {
				return Result{}, err
			}
			op = next
		case PhaseActivatingProvider:
			if err := s.activations.RefreshChannelActivationCandidates(ctx); err != nil {
				if terminal, ok := AsTerminalActivationError(err); ok {
					failed, failErr := s.failOperation(ctx, op, terminal.Code, terminal.Error())
					if failErr != nil {
						return Result{}, errors.Join(err, failErr)
					}
					return s.result(ctx, failed, &candidate)
				}
				return s.blockedResult(ctx, op, candidate, fmt.Errorf("refresh channel activation candidates: %w", err))
			}
			var err error
			op, err = s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseAwaitingExternalIdentity, Now: s.now().UTC()})
			if err != nil {
				return Result{}, err
			}
		case PhaseAwaitingExternalIdentity:
			next, blocked, err := s.advanceIdentity(ctx, op, candidate)
			if err != nil {
				var required *CredentialRequiredError
				if errors.As(err, &required) {
					op = next
					if strings.TrimSpace(providerCredential) != "" {
						continue
					}
					return s.blockedResult(ctx, op, candidate, required)
				}
				return Result{}, fmt.Errorf("advance channel identity: %w", err)
			}
			op = next
			if blocked {
				result, err := s.result(ctx, op, &candidate)
				if err != nil {
					return result, fmt.Errorf("project channel awaiting external identity: %w", err)
				}
				return result, nil
			}
		case PhaseAwaitingOperatorConfirmation:
			var identityOp operatorchannel.Operation
			if op.IdentityOperationID != "" {
				var identityErr error
				identityOp, identityErr = s.currentIdentityOperation(ctx, op.IdentityOperationID)
				if identityErr != nil {
					return Result{}, identityErr
				}
				if identityOp.State == operatorchannel.StateCredentialStale {
					op, err = s.resetCredentialStaleIdentity(context.WithoutCancel(ctx), op)
					if err != nil {
						return Result{}, err
					}
					if strings.TrimSpace(providerCredential) == "" {
						return s.blockedResult(ctx, op, candidate, credentialRequiredForStaleIdentity(op, identityOp))
					}
					continue
				}
				if identityOp.State.Terminal() && identityOp.State != operatorchannel.StateBound {
					failed, failErr := s.failOperation(ctx, op, "identity_"+string(identityOp.State), fmt.Sprintf("identity operation %s ended in %s", identityOp.OperationID, identityOp.State))
					if failErr != nil {
						return Result{}, failErr
					}
					return s.result(ctx, failed, &candidate)
				}
			}
			decision, err := s.reconcileConfirmedBinding(ctx, op)
			if err != nil {
				return Result{}, err
			}
			op = decision.operation
			if decision.credentialRequired != nil {
				if strings.TrimSpace(providerCredential) == "" {
					return s.blockedResult(ctx, op, candidate, decision.credentialRequired)
				}
				continue
			}
			if decision.blocked {
				return s.result(ctx, op, &candidate)
			}
			op, err = s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhasePublishingActivation,
				BindingRevision: decision.binding.Revision, Now: s.now().UTC(),
			})
			if err != nil {
				return Result{}, err
			}
		case PhasePublishingActivation:
			if err := s.reachTestBarrier(TestAfterBindingCheckpointBeforeActivation, op.OperationID); err != nil {
				return Result{}, err
			}
			decision, err := s.reconcileConfirmedBinding(ctx, op)
			if err != nil {
				return Result{}, err
			}
			op = decision.operation
			if decision.credentialRequired != nil {
				if strings.TrimSpace(providerCredential) == "" {
					return s.blockedResult(ctx, op, candidate, decision.credentialRequired)
				}
				continue
			}
			if decision.blocked {
				return s.result(ctx, op, &candidate)
			}
			binding := decision.binding
			op, _, err = s.store.PublishConnectedChannelActivation(ctx, PublishActivationRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, ActivationID: uuid.NewString(),
				BindingRevision: binding.Revision, ConversationRef: binding.ConversationRef,
				ProofID: binding.ProofID, ProofRevision: binding.ProofRevision, Now: s.now().UTC(),
			})
			if err != nil {
				return Result{}, err
			}
			if err := s.reachTestBarrier(TestAfterActivationCommitBeforePublication, op.OperationID); err != nil {
				return Result{}, err
			}
		case PhasePublishingProcessActivation:
			activation, err := s.currentOperationActivation(ctx, op)
			if err != nil {
				return Result{}, err
			}
			if err := s.activations.PublishChannelActivation(ctx, op, activation); err != nil {
				return s.blockedResult(ctx, op, candidate, err)
			}
			if err := s.reachTestBarrier(TestAfterProcessPublicationBeforePromotion, op.OperationID); err != nil {
				return Result{}, err
			}
			op, err = s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhasePromotingRegistration, Now: s.now().UTC(),
			})
			if err != nil {
				return Result{}, err
			}
		case PhasePromotingRegistration:
			activation, err := s.currentOperationActivation(ctx, op)
			if err != nil {
				return Result{}, err
			}
			if err := s.activations.PromoteChannelRegistration(ctx, op, activation); err != nil {
				return s.blockedResult(ctx, op, candidate, err)
			}
			op, err = s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseRetiringPredecessor, Now: s.now().UTC(),
			})
			if err != nil {
				return Result{}, err
			}
		case PhaseRetiringPredecessor:
			activation, err := s.currentOperationActivation(ctx, op)
			if err != nil {
				return Result{}, err
			}
			if err := s.releaseSupersededCredentials(ctx, op, activation); err != nil {
				return s.blockedResult(ctx, op, candidate, err)
			}
			op, err = s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseDeliveringConfirmation, Now: s.now().UTC(),
			})
			if err != nil {
				return Result{}, err
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
				return s.result(ctx, op, &candidate)
			}
			op, err = s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseSucceeded,
				Now: s.now().UTC(),
			})
			if err != nil {
				return Result{}, err
			}
		case PhaseSucceeded, PhaseFailed, PhaseRetired:
			return s.result(ctx, op, &candidate)
		default:
			return Result{}, fmt.Errorf("%w: unsupported onboarding phase %q", ErrConflict, op.Phase)
		}
	}
}

func (s *Service) lockDrive(operationID string) func() {
	s.driveMu.Lock()
	if s.driveLocks == nil {
		s.driveLocks = map[string]*operationDriveLock{}
	}
	lock := s.driveLocks[operationID]
	if lock == nil {
		lock = &operationDriveLock{}
		s.driveLocks[operationID] = lock
	}
	lock.refs++
	s.driveMu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		s.driveMu.Lock()
		lock.refs--
		if lock.refs == 0 && s.driveLocks[operationID] == lock {
			delete(s.driveLocks, operationID)
		}
		s.driveMu.Unlock()
	}
}

func (s *Service) reachTestBarrier(boundary TestLifecycleBoundary, responsibilityID string) error {
	if s == nil || s.testBarrier == nil {
		return nil
	}
	return s.testBarrier(boundary, responsibilityID)
}

func (s *Service) releaseCandidateCredentials(ctx context.Context, admissions []CredentialAdmission) error {
	var releaseErr error
	for _, admission := range admissions {
		if _, err := s.credentials.Release(ctx, admission); err != nil {
			releaseErr = errors.Join(releaseErr, fmt.Errorf("release rejected channel credential %q: %w", admission.StoreKey, err))
		}
	}
	return releaseErr
}

func (s *Service) resetCredentialStaleIdentity(ctx context.Context, op Operation) (Operation, error) {
	validated, err := s.store.ResetChannelOnboardingPendingIdentity(ctx, PendingResetRequest{
		OperationID: op.OperationID, ExpectedRevision: op.Revision, Now: s.now().UTC(),
	})
	if err != nil {
		return op, err
	}
	if err := s.releaseCandidateCredentials(ctx, validated.CredentialAdmissions); err != nil {
		return op, fmt.Errorf("release credential-stale onboarding admissions: %w", err)
	}
	if err := s.reachTestBarrier(TestAfterPendingResetCleanup, op.OperationID); err != nil {
		return op, err
	}
	reset, err := s.store.ResetChannelOnboardingPendingIdentity(ctx, PendingResetRequest{
		OperationID: validated.OperationID, ExpectedRevision: validated.Revision, Commit: true, Now: s.now().UTC(),
	})
	if err != nil {
		return op, fmt.Errorf("reset credential-stale onboarding responsibility: %w", err)
	}
	return reset, s.reachTestBarrier(TestAfterPendingResetCommit, op.OperationID)
}

func credentialRequiredForStaleIdentity(parent Operation, identity operatorchannel.Operation) *CredentialRequiredError {
	role := "provider"
	if len(parent.CredentialReservations) > 0 && strings.TrimSpace(parent.CredentialReservations[0].Role) != "" {
		role = parent.CredentialReservations[0].Role
	}
	return &CredentialRequiredError{
		OperationID: parent.OperationID,
		Role:        role,
		StoreKey:    strings.TrimSpace(identity.ProviderCredential.Key),
	}
}

func credentialRequiredForStaleParent(parent Operation, identity operatorchannel.Operation) *CredentialRequiredError {
	required := credentialRequiredForStaleIdentity(parent, identity)
	if required.StoreKey != "" {
		return required
	}
	for _, admission := range parent.CredentialAdmissions {
		if admission.Role == required.Role {
			required.StoreKey = strings.TrimSpace(admission.StoreKey)
			break
		}
	}
	return required
}

func (s *Service) admitCredentials(ctx context.Context, op Operation, candidate Candidate, providerCredential string, allowNewAdmission bool) ([]CredentialAdmission, error) {
	currentByRole := map[string]CredentialAdmission{}
	if current, err := s.store.GetConnectedChannelActivation(ctx, op.SlotKey); err == nil {
		for _, admission := range current.CredentialAdmissions {
			if err := admission.Validate(); err != nil {
				return nil, err
			}
			if _, duplicate := currentByRole[admission.Role]; duplicate {
				return nil, fmt.Errorf("%w: current activation has duplicate credential role %q", ErrConflict, admission.Role)
			}
			currentByRole[admission.Role] = admission
		}
	} else if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	admissions := make([]CredentialAdmission, len(op.CredentialReservations))
	writes := make([]CredentialWriteRequest, len(op.CredentialReservations))
	generateSigning := -1
	// Observe every required role before generating or writing any credential.
	// A missing role must not conceal another role's invalid value or read error.
	var missingErr error
	for i, reservation := range op.CredentialReservations {
		operationKey := operationCredentialStoreKey(reservation.StoreKey, op.OperationID, reservation.Role)
		operationReceipt := credentialReceipt(op.OperationID, reservation.Role)
		if written, found, err := s.credentials.ObserveWritten(ctx, operationKey, operationReceipt); err != nil {
			return nil, err
		} else if found {
			admissions[i] = CredentialAdmission{
				Role: reservation.Role, StoreKey: written.StoreKey, Kind: CredentialAdmissionWritten,
				Receipt: written.Receipt, ValueSeal: written.ValueSeal,
			}
			continue
		}
		if !allowNewAdmission {
			missingErr = errors.Join(missingErr, &CredentialRequiredError{OperationID: op.OperationID, Role: reservation.Role, StoreKey: operationKey})
			continue
		}
		value := ""
		switch reservation.Role {
		case candidate.ProviderCredentialRole:
			value = providerCredential
			if value != "" {
				if err := runtimecredentials.ValidateValue(value); err != nil {
					return nil, err
				}
			}
		case candidate.SigningCredentialRole:
			if current, ok := currentByRole[reservation.Role]; ok {
				observed, present, err := s.credentials.ObserveOptional(ctx, current.StoreKey)
				if err != nil {
					return nil, err
				}
				if !present {
					missingErr = errors.Join(missingErr, &CredentialRequiredError{OperationID: op.OperationID, Role: reservation.Role, StoreKey: current.StoreKey})
					continue
				}
				if observed.ValueSeal != current.ValueSeal {
					return nil, fmt.Errorf("%w: current signing credential %q changed", ErrConflict, current.StoreKey)
				}
				admissions[i] = observedCredentialAdmissionForKey(op.OperationID, reservation.Role, current.StoreKey, observed)
				continue
			}
			observed, present, err := s.credentials.ObserveOptional(ctx, reservation.StoreKey)
			if err != nil {
				return nil, err
			}
			if present {
				admissions[i] = observedCredentialAdmission(op.OperationID, reservation, observed)
				continue
			}
			if generateSigning != -1 {
				return nil, fmt.Errorf("%w: duplicate signing credential reservation", ErrConflict)
			}
			generateSigning = i
			writes[i] = CredentialWriteRequest{StoreKey: operationKey, Receipt: operationReceipt}
			continue
		}
		if strings.TrimSpace(value) == "" {
			storeKey := reservation.StoreKey
			if current, ok := currentByRole[reservation.Role]; ok {
				storeKey = current.StoreKey
			}
			observed, present, err := s.credentials.ObserveOptional(ctx, storeKey)
			if err != nil {
				return nil, err
			}
			if !present {
				missingErr = errors.Join(missingErr, &CredentialRequiredError{OperationID: op.OperationID, Role: reservation.Role, StoreKey: storeKey})
				continue
			}
			admissions[i] = observedCredentialAdmissionForKey(op.OperationID, reservation.Role, storeKey, observed)
			continue
		}
		writes[i] = CredentialWriteRequest{StoreKey: operationKey, Value: value, Receipt: operationReceipt}
	}
	if missingErr != nil {
		return nil, missingErr
	}
	for _, admission := range admissions {
		if admission.StoreKey == "" {
			continue
		}
		current, err := s.credentials.Current(ctx, admission)
		if err != nil || !current {
			return nil, errors.Join(fmt.Errorf("%w: credential %q changed before admission", ErrConflict, admission.StoreKey), err)
		}
	}
	if generateSigning != -1 {
		value, err := s.secret()
		if err != nil {
			return nil, err
		}
		writes[generateSigning].Value = value
	}
	for i, write := range writes {
		if write.StoreKey == "" {
			continue
		}
		written, err := s.credentials.Admit(ctx, write)
		if err != nil {
			return nil, err
		}
		admissions[i] = CredentialAdmission{Role: op.CredentialReservations[i].Role, StoreKey: written.StoreKey, Kind: CredentialAdmissionWritten, Receipt: written.Receipt, ValueSeal: written.ValueSeal}
	}
	return admissions, nil
}

func (s *Service) validateCredentialAdmissions(ctx context.Context, op Operation) error {
	if len(op.CredentialAdmissions) != len(op.CredentialReservations) {
		return fmt.Errorf("%w: onboarding operation has incomplete credential admission", ErrConflict)
	}
	byRole := make(map[string]CredentialAdmission, len(op.CredentialAdmissions))
	var staleErr error
	for _, admission := range op.CredentialAdmissions {
		if err := admission.Validate(); err != nil {
			return err
		}
		if _, duplicate := byRole[admission.Role]; duplicate {
			return fmt.Errorf("%w: onboarding operation has duplicate credential role %q", ErrConflict, admission.Role)
		}
		byRole[admission.Role] = admission
		if _, _, err := s.credentials.ObserveOptional(ctx, admission.StoreKey); err != nil {
			return err
		}
		current, err := s.credentials.Current(ctx, admission)
		if err != nil {
			return err
		}
		if !current {
			staleErr = errors.Join(staleErr, &CredentialRequiredError{OperationID: op.OperationID, Role: admission.Role, StoreKey: admission.StoreKey})
		}
		if admission.Kind == CredentialAdmissionWritten {
			written, found, err := s.credentials.ObserveWritten(ctx, admission.StoreKey, admission.Receipt)
			if err != nil {
				return err
			}
			if !found || written.ValueSeal != admission.ValueSeal {
				staleErr = errors.Join(staleErr, &CredentialRequiredError{OperationID: op.OperationID, Role: admission.Role, StoreKey: admission.StoreKey})
			}
		}
	}
	for _, reservation := range op.CredentialReservations {
		if admission, found := byRole[reservation.Role]; !found || admission.StoreKey == "" {
			return fmt.Errorf("%w: onboarding credential role %q is not admitted", ErrConflict, reservation.Role)
		}
	}
	return staleErr
}

func (s *Service) advanceIdentity(ctx context.Context, op Operation, candidate Candidate) (Operation, bool, error) {
	if op.IdentityOperationID != "" {
		identityOp, err := s.currentIdentityOperation(ctx, op.IdentityOperationID)
		if err != nil {
			return op, false, err
		}
		switch identityOp.State {
		case operatorchannel.StateAwaitingClaim:
			return op, true, nil
		case operatorchannel.StateCredentialStale:
			reset, err := s.resetCredentialStaleIdentity(context.WithoutCancel(ctx), op)
			if err != nil {
				return op, false, err
			}
			return reset, true, credentialRequiredForStaleIdentity(reset, identityOp)
		case operatorchannel.StateAwaitingConfirmation, operatorchannel.StateBound:
			next, err := s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
				OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseAwaitingOperatorConfirmation,
				IdentityOperationID: identityOp.OperationID, Now: s.now().UTC(),
			})
			return next, identityOp.State != operatorchannel.StateBound, err
		default:
			failed, err := s.failOperation(ctx, op, "identity_"+string(identityOp.State), fmt.Sprintf("identity operation %s ended in %s", identityOp.OperationID, identityOp.State))
			return failed, false, err
		}
	}
	providerCredential, err := providerIdentityEvidence(op, candidate)
	if err != nil {
		return op, false, err
	}
	identityKind := operatorchannel.OperationConnect
	expectedRevision := int64(0)
	boundCredentialRestart := op.Phase == PhaseAwaitingExternalIdentity && op.BindingRevision > 0
	if boundCredentialRestart {
		binding, _, bindingErr := s.identities.CurrentBindingReadiness(ctx, op.Interface)
		if bindingErr != nil && !errors.Is(bindingErr, operatorchannel.ErrCredentialStale) {
			return op, false, bindingErr
		}
		if binding.Revision != op.BindingRevision {
			return op, false, fmt.Errorf("%w: retained stale binding revision changed before reconnect", ErrRevisionConflict)
		}
		identityKind, expectedRevision = operatorchannel.OperationReconnect, binding.Revision
	}
	if !boundCredentialRestart && op.Verb == VerbReconnect {
		binding, proofCurrent, bindingErr := s.identities.CurrentBindingReadiness(ctx, op.Interface)
		if bindingErr == nil {
			proofPostureCurrent := binding.ProviderCredential == providerCredential &&
				((op.SaveProof && strings.TrimSpace(binding.ProofID) != "" && proofCurrent) ||
					(!op.SaveProof && strings.TrimSpace(binding.ProofID) == ""))
			if proofPostureCurrent {
				next, err := s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
					OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseAwaitingOperatorConfirmation,
					BindingRevision: binding.Revision, Now: s.now().UTC(),
				})
				return next, false, err
			}
			identityKind, expectedRevision = operatorchannel.OperationReconnect, binding.Revision
		}
		if bindingErr != nil && (!errors.Is(bindingErr, operatorchannel.ErrCredentialStale) || binding.Revision < 1) {
			return op, false, bindingErr
		}
		if bindingErr != nil {
			identityKind, expectedRevision = operatorchannel.OperationReconnect, binding.Revision
		}
	}
	if !boundCredentialRestart && op.Verb == VerbRebind {
		identityKind = operatorchannel.OperationRebind
		binding, err := s.identities.CurrentBinding(ctx, op.Interface)
		if err != nil && !errors.Is(err, operatorchannel.ErrCredentialStale) {
			return op, false, err
		}
		expectedRevision = binding.Revision
	}
	identityOp, err := s.identities.Begin(ctx, candidate.Interface.Selector, identityKind, expectedRevision,
		operatorchannel.Hash("channel-onboarding-identity-key-v2", op.OperationID, fmt.Sprint(op.Revision)),
		operatorchannel.Hash("channel-onboarding-identity-request-v2", op.OperationID, fmt.Sprint(op.Revision), string(op.Verb), candidate.Interface.Key()),
		op.OperationID,
		providerCredential, op.SaveProof, s.now().UTC())
	if err != nil {
		return op, false, err
	}
	if err := s.reachTestBarrier(TestAfterIdentityBeginBeforeCheckpoint, op.OperationID); err != nil {
		return op, false, err
	}
	next, err := s.store.AdvanceChannelOnboarding(ctx, AdvanceRequest{
		OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseAwaitingExternalIdentity,
		IdentityOperationID: identityOp.OperationID, Now: s.now().UTC(),
	})
	return next, true, err
}

func providerIdentityEvidence(op Operation, candidate Candidate) (runtimecredentials.ValueEvidence, error) {
	for _, admission := range op.CredentialAdmissions {
		if admission.Role != candidate.ProviderCredentialRole {
			continue
		}
		evidence := runtimecredentials.ValueEvidence{Key: admission.StoreKey, Seal: admission.ValueSeal}
		if err := evidence.Validate(); err != nil {
			return runtimecredentials.ValueEvidence{}, fmt.Errorf("%w: provider credential admission is invalid", ErrConflict)
		}
		return evidence, nil
	}
	return runtimecredentials.ValueEvidence{}, fmt.Errorf("%w: provider credential role %q is not admitted", ErrConflict, candidate.ProviderCredentialRole)
}

func (s *Service) currentIdentityOperation(ctx context.Context, operationID string) (operatorchannel.Operation, error) {
	for attempt := 0; attempt < 4; attempt++ {
		identityOp, err := s.identities.GetOperation(ctx, operationID)
		if err != nil {
			return operatorchannel.Operation{}, err
		}
		now := s.now().UTC()
		if identityOp.State.Terminal() || identityOp.ExpiresAt.IsZero() || identityOp.ExpiresAt.After(now) {
			return identityOp, nil
		}
		expired, err := s.identities.ExpireOperation(context.WithoutCancel(ctx), identityOp.OperationID, identityOp.Revision, now)
		if err == nil {
			return expired, nil
		}
		if !errors.Is(err, operatorchannel.ErrRevisionConflict) {
			return operatorchannel.Operation{}, err
		}
	}
	return operatorchannel.Operation{}, fmt.Errorf("%w: identity operation %s changed repeatedly while expiry was reconciled", ErrRevisionConflict, operationID)
}

func (s *Service) currentOperationActivation(ctx context.Context, op Operation) (ConnectedChannelActivation, error) {
	activation, err := s.store.GetConnectedChannelActivation(ctx, op.SlotKey)
	if err != nil {
		return ConnectedChannelActivation{}, err
	}
	if activation.Status != ActivationCurrent || activation.OperationID != op.OperationID || activation.Revision != op.ActivationRevision || activation.BindingRevision != op.BindingRevision {
		return ConnectedChannelActivation{}, fmt.Errorf("%w: current activation contradicts onboarding handoff operation", ErrRevisionConflict)
	}
	return activation, nil
}

func (s *Service) failOperation(ctx context.Context, op Operation, code, message string) (Operation, error) {
	failed, err := s.failOperationLocal(ctx, op, code, message)
	if err != nil {
		return op, err
	}
	if err := s.activations.RefreshChannelActivations(context.WithoutCancel(ctx)); err != nil {
		return failed, fmt.Errorf("refresh channel activations after terminal onboarding failure: %w", err)
	}
	return failed, nil
}

func (s *Service) failOperationLocal(ctx context.Context, op Operation, code, message string) (Operation, error) {
	failed, err := s.store.AdvanceChannelOnboarding(context.WithoutCancel(ctx), AdvanceRequest{
		OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: PhaseFailed,
		FailureCode: strings.TrimSpace(code), FailureMessage: strings.TrimSpace(message), Now: s.now().UTC(),
	})
	if err != nil {
		return op, err
	}
	if err := s.credentials.ReleaseOperation(context.WithoutCancel(ctx), failed); err != nil {
		return failed, fmt.Errorf("release failed onboarding operation credentials: %w", err)
	}
	return failed, nil
}

func (s *Service) releaseSupersededCredentials(ctx context.Context, current Operation, activation ConnectedChannelActivation) error {
	operations, err := s.store.ListChannelOnboardingOperations(ctx)
	if err != nil {
		return err
	}
	retained := make(map[string]struct{}, len(activation.CredentialAdmissions))
	for _, admission := range activation.CredentialAdmissions {
		retained[credentialAdmissionCurrentness(admission)] = struct{}{}
	}
	for _, operation := range operations {
		if operation.OperationID == current.OperationID {
			continue
		}
		sameSupersededOwner := operation.SlotKey == current.SlotKey
		if current.Verb == VerbRebind {
			sameSupersededOwner = operation.Interface.Normalized() == current.Interface.Normalized()
		}
		if !sameSupersededOwner {
			continue
		}
		for _, admission := range operation.CredentialAdmissions {
			if _, keep := retained[credentialAdmissionCurrentness(admission)]; keep {
				continue
			}
			if _, err := s.credentials.Release(context.WithoutCancel(ctx), admission); err != nil {
				return fmt.Errorf("release superseded channel credential %q: %w", admission.StoreKey, err)
			}
		}
	}
	return nil
}

// credentialCleanupReceiptIdentity makes a checkpointed admission win over a
// later recovery observation that reused the same deterministic retry receipt.
func credentialCleanupReceiptIdentity(admission CredentialAdmission) string {
	return admission.StoreKey + "\x00" + admission.Receipt
}

func credentialAdmissionCurrentness(admission CredentialAdmission) string {
	return admission.StoreKey + "\x00" + admission.ValueSeal.String()
}

type confirmedBindingDecision struct {
	operation          Operation
	binding            operatorchannel.Binding
	blocked            bool
	credentialRequired *CredentialRequiredError
}

func (s *Service) reconcileConfirmedBinding(ctx context.Context, op Operation) (confirmedBindingDecision, error) {
	decision := confirmedBindingDecision{operation: op}
	var identityOp operatorchannel.Operation
	if op.IdentityOperationID != "" {
		var err error
		identityOp, err = s.currentIdentityOperation(ctx, op.IdentityOperationID)
		if err != nil {
			return decision, err
		}
		if identityOp.State != operatorchannel.StateBound {
			decision.blocked = true
			return decision, nil
		}
	}
	binding, err := s.identities.CurrentBinding(ctx, op.Interface)
	stale := errors.Is(err, operatorchannel.ErrCredentialStale)
	if err != nil && !stale {
		return decision, err
	}
	// The store validates the exact committed child/binding under the parent's
	// revision fence even when confirmation preceded the parent checkpoint.
	op, err = s.store.ReconcileChannelOnboardingBinding(ctx, ReconcileBindingRequest{
		OperationID: op.OperationID, ExpectedRevision: op.Revision,
		ExpectedBindingRevision: binding.Revision, Now: s.now().UTC(),
	})
	if err != nil {
		return decision, err
	}
	decision.operation, decision.binding = op, binding
	admissionErr := s.validateCredentialAdmissions(ctx, op)
	var required *CredentialRequiredError
	if admissionErr != nil && !errors.As(admissionErr, &required) {
		return decision, admissionErr
	}
	stale = stale || required != nil
	if !stale {
		decision.blocked = identityOp.ProofStatus == operatorchannel.ProofPending || identityOp.ProofStatus == operatorchannel.ProofFailed
		return decision, nil
	}
	decision.credentialRequired = required
	if decision.credentialRequired == nil {
		decision.credentialRequired = credentialRequiredForStaleParent(op, identityOp)
	}
	if err := s.releaseCandidateCredentials(context.WithoutCancel(ctx), op.CredentialAdmissions); err != nil {
		return decision, fmt.Errorf("release bound credential-stale onboarding admissions: %w", err)
	}
	// Revalidate after cleanup: a concurrent retirement or replacement must win,
	// and interrupted cleanup must leave the exact admissions available to retry.
	decision.operation, err = s.store.ReconcileChannelOnboardingBinding(context.WithoutCancel(ctx), ReconcileBindingRequest{
		OperationID: op.OperationID, ExpectedRevision: op.Revision,
		ExpectedBindingRevision: binding.Revision, ResetCredentials: true, Now: s.now().UTC(),
	})
	if err != nil {
		return decision, err
	}
	return decision, nil
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
	bindingStale := errors.Is(bindingErr, operatorchannel.ErrCredentialStale) && binding.Status == operatorchannel.BindingCurrent
	if bindingErr != nil && !errors.Is(bindingErr, operatorchannel.ErrNotFound) && !bindingStale {
		return "", bindingErr
	}
	activation, activationErr := s.store.GetConnectedChannelActivation(ctx, req.SlotKey())
	activationCurrent := activationErr == nil && activation.Status == ActivationCurrent
	if activationErr != nil && !errors.Is(activationErr, ErrNotFound) {
		return "", activationErr
	}
	if !bindingCurrent && !activationCurrent {
		if bindingStale {
			return SlotActivationStale, nil
		}
		return SlotAbsent, nil
	}
	if bindingCurrent && !activationCurrent {
		return SlotIdentityCurrentActivationAbsent, nil
	}
	if !bindingCurrent {
		if bindingStale {
			return SlotActivationStale, nil
		}
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
	candidate, current := catalog.FindDurableSuccessor(op.Provider, op.Interface, op.Coordinate, op.TargetSelector, op.Posture, op.Ceremony)
	if !current {
		return Candidate{}, fmt.Errorf("%w: %w; explicit retry against a new operation is required", ErrConflict, errOnboardingRuntimeContextRetired)
	}
	return candidate, nil
}

func (s *Service) bindCurrentCandidate(ctx context.Context, op Operation) (Operation, Candidate, error) {
	candidate, err := s.currentCandidate(op)
	if err != nil {
		return op, Candidate{}, err
	}
	disposition := EffectRebindDisposition{RetryAllowed: true}
	if phaseMayOwnExternalEffect(op.Phase) {
		disposition, err = s.effects.ReconcileChannelEffectsBeforeRebind(context.WithoutCancel(ctx), op)
		if err != nil {
			return op, Candidate{}, fmt.Errorf("reconcile onboarding operation %s effects before retry or rebind: %w", op.OperationID, err)
		}
	}
	if !disposition.RetryAllowed {
		return op, candidate, &EffectRetryBlockedError{
			OperationID: disposition.BlockingEffectOperationID,
			State:       disposition.BlockingEffectState,
		}
	}
	rebound := op
	if !op.Coordinate.MatchesDeclaration(candidate.Coordinate) {
		coordinate := candidate.Coordinate
		if op.Phase.RequiresExecutableTarget() && coordinate.TargetGeneration == 0 {
			// Retain the historical target coordinate for local reconciliation.
			// Declaration discovery grants no replacement executable authority.
			coordinate.TargetGeneration = op.Coordinate.TargetGeneration
		}
		rebound, err = s.store.AdvanceChannelOnboarding(context.WithoutCancel(ctx), AdvanceRequest{
			OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: op.Phase,
			RebindCoordinate: &coordinate, ClearConfirmationOperationID: disposition.RemintConfirmationOperation,
			Now: s.now().UTC(),
		})
		if err != nil {
			return op, Candidate{}, fmt.Errorf("rebind onboarding operation %s to current runtime occurrence: %w", op.OperationID, err)
		}
	}
	return rebound, candidate, nil
}

func phaseMayOwnExternalEffect(phase Phase) bool {
	switch phase {
	case PhaseActivatingProvider,
		PhaseAwaitingExternalIdentity,
		PhaseAwaitingOperatorConfirmation,
		PhasePublishingActivation,
		PhasePublishingProcessActivation,
		PhasePromotingRegistration,
		PhaseRetiringPredecessor,
		PhaseDeliveringConfirmation:
		return true
	default:
		return false
	}
}

func historicalCandidate(op Operation) Candidate {
	return Candidate{
		Provider: op.Provider, Interface: op.Interface, Coordinate: op.Coordinate,
		Target:  CandidateTarget{Selector: op.TargetSelector, Provider: op.Provider, Generation: op.Coordinate.TargetGeneration},
		Posture: op.Posture, Ceremony: op.Ceremony,
	}
}

func (s *Service) result(ctx context.Context, op Operation, candidate *Candidate) (Result, error) {
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
	readinessCandidate := historicalCandidate(op)
	if candidate != nil {
		readinessCandidate = *candidate
	}
	readiness, found, err := s.readiness.ProjectConnectedChannelReadiness(ctx, op, readinessCandidate)
	if err != nil {
		return result, err
	}
	if found {
		result.Readiness = &readiness
	}
	return result, nil
}

func (s *Service) blockedResult(ctx context.Context, op Operation, candidate Candidate, cause error) (Result, error) {
	result, err := s.result(ctx, op, &candidate)
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

func operationCredentialStoreKey(baseKey, operationID, role string) string {
	return strings.TrimSpace(baseKey) + ".operation." + operatorchannel.Hash(
		"channel-onboarding-credential-occurrence-v1", strings.TrimSpace(operationID), strings.TrimSpace(role),
	)
}

func credentialReceipt(operationID, role string) string {
	return operatorchannel.Hash("channel-onboarding-credential-receipt-v1", operationID, role)
}

func observedCredentialAdmission(operationID string, reservation CredentialReservation, observed CredentialWriteResult) CredentialAdmission {
	return observedCredentialAdmissionForKey(operationID, reservation.Role, reservation.StoreKey, observed)
}

func observedCredentialAdmissionForKey(_ string, role, storeKey string, observed CredentialWriteResult) CredentialAdmission {
	return CredentialAdmission{
		Role: role, StoreKey: storeKey, Kind: CredentialAdmissionObserved, ValueSeal: observed.ValueSeal,
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
