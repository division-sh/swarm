package channelonboarding

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
)

var (
	ErrInvalidRequest   = errors.New("invalid channel onboarding request")
	ErrNotFound         = errors.New("channel onboarding resource not found")
	ErrConflict         = errors.New("channel onboarding conflict")
	ErrRevisionConflict = errors.New("channel onboarding revision conflict")
)

type ActivationPosture string

const (
	ActivationWebhookRegistration ActivationPosture = "webhook_registration"
	ActivationSessionConnection   ActivationPosture = "session_connection"
)

func (p ActivationPosture) Valid() bool {
	return p == ActivationWebhookRegistration || p == ActivationSessionConnection
}

type IdentityCeremony string

const (
	CeremonyAuthenticatedTextChallenge IdentityCeremony = "authenticated_text_challenge"
	CeremonyProviderPairing            IdentityCeremony = "provider_pairing"
)

func (c IdentityCeremony) Valid() bool {
	return c == CeremonyAuthenticatedTextChallenge || c == CeremonyProviderPairing
}

type Verb string

const (
	VerbConnect   Verb = "connect"
	VerbReconnect Verb = "reconnect"
	VerbRebind    Verb = "rebind"
)

func (v Verb) Valid() bool {
	return v == VerbConnect || v == VerbReconnect || v == VerbRebind
}

type Phase string

const (
	PhasePreparing                    Phase = "preparing"
	PhaseCredentialsAdmitted          Phase = "credentials_admitted"
	PhaseActivatingProvider           Phase = "activating_provider"
	PhaseAwaitingExternalIdentity     Phase = "awaiting_external_identity"
	PhaseAwaitingOperatorConfirmation Phase = "awaiting_operator_confirmation"
	PhasePublishingActivation         Phase = "publishing_activation"
	PhasePublishingProcessActivation  Phase = "publishing_process_activation"
	PhasePromotingRegistration        Phase = "promoting_registration"
	PhaseRetiringPredecessor          Phase = "retiring_predecessor"
	PhaseDeliveringConfirmation       Phase = "delivering_confirmation"
	PhaseSucceeded                    Phase = "succeeded"
	PhaseFailed                       Phase = "failed"
	PhaseRetired                      Phase = "retired"
)

var validPhases = [...]Phase{
	PhasePreparing,
	PhaseCredentialsAdmitted,
	PhaseActivatingProvider,
	PhaseAwaitingExternalIdentity,
	PhaseAwaitingOperatorConfirmation,
	PhasePublishingActivation,
	PhasePublishingProcessActivation,
	PhasePromotingRegistration,
	PhaseRetiringPredecessor,
	PhaseDeliveringConfirmation,
	PhaseSucceeded,
	PhaseFailed,
	PhaseRetired,
}

func ValidPhases() []Phase {
	return append([]Phase(nil), validPhases[:]...)
}

func (p Phase) Terminal() bool {
	return p == PhaseSucceeded || p == PhaseFailed || p == PhaseRetired
}

func (p Phase) Valid() bool {
	for _, valid := range validPhases {
		if p == valid {
			return true
		}
	}
	return false
}

// ChannelDurableContextIdentity identifies the source semantics that survive a
// process restart. It deliberately excludes process-local publication and
// target occurrences.
type ChannelDurableContextIdentity struct {
	BundleHash              string
	BundleSource            string
	BundleIdentity          string
	PackInventoryGeneration string
	PlanGeneration          plangeneration.Generation
}

func (i ChannelDurableContextIdentity) Normalized() ChannelDurableContextIdentity {
	i.BundleHash = strings.TrimSpace(i.BundleHash)
	i.BundleSource = strings.TrimSpace(i.BundleSource)
	i.BundleIdentity = strings.TrimSpace(i.BundleIdentity)
	i.PackInventoryGeneration = strings.TrimSpace(i.PackInventoryGeneration)
	return i
}

func (i ChannelDurableContextIdentity) Validate() error {
	i = i.Normalized()
	if _, err := runtimecorrelation.DecodeBundleSourceFact(i.BundleHash, i.BundleSource); err != nil {
		return fmt.Errorf("channel durable context source: %w", err)
	}
	if i.BundleIdentity == "" || i.PackInventoryGeneration == "" || !i.PlanGeneration.Valid() {
		return fmt.Errorf("channel durable context requires exact bundle identity, inventory, and plan generation")
	}
	return nil
}

func (i ChannelDurableContextIdentity) Matches(other ChannelDurableContextIdentity) bool {
	i, other = i.Normalized(), other.Normalized()
	return i.Validate() == nil && other.Validate() == nil && i == other
}

type ChannelLiveRuntimeOccurrence struct {
	RuntimeInstanceID            string
	ContextPublicationGeneration uint64
	TargetGeneration             uint64
}

func (o ChannelLiveRuntimeOccurrence) Validate() error {
	o.RuntimeInstanceID = strings.TrimSpace(o.RuntimeInstanceID)
	if o.RuntimeInstanceID == "" || o.ContextPublicationGeneration == 0 || o.TargetGeneration == 0 {
		return fmt.Errorf("channel live runtime occurrence requires runtime instance, publication, and target generations")
	}
	return nil
}

func (o ChannelLiveRuntimeOccurrence) Matches(other ChannelLiveRuntimeOccurrence) bool {
	return o.Validate() == nil && other.Validate() == nil && o == other
}

// ChannelRuntimeContextCoordinate combines restart-stable source semantics
// with the exact live runtime occurrence currently admitted for execution.
type ChannelRuntimeContextCoordinate struct {
	BundleHash                   string                    `json:"bundle_hash"`
	BundleSource                 string                    `json:"bundle_source"`
	BundleIdentity               string                    `json:"bundle_identity"`
	PackInventoryGeneration      string                    `json:"pack_inventory_generation"`
	RuntimeInstanceID            string                    `json:"runtime_instance_id"`
	ContextPublicationGeneration uint64                    `json:"context_publication_generation"`
	PlanGeneration               plangeneration.Generation `json:"plan_generation"`
	TargetGeneration             uint64                    `json:"target_generation"`
}

func (c ChannelRuntimeContextCoordinate) DurableIdentity() ChannelDurableContextIdentity {
	c = c.Normalized()
	return ChannelDurableContextIdentity{
		BundleHash: c.BundleHash, BundleSource: c.BundleSource, BundleIdentity: c.BundleIdentity,
		PackInventoryGeneration: c.PackInventoryGeneration, PlanGeneration: c.PlanGeneration,
	}
}

func (c ChannelRuntimeContextCoordinate) LiveOccurrence() ChannelLiveRuntimeOccurrence {
	return ChannelLiveRuntimeOccurrence{
		RuntimeInstanceID:            c.RuntimeInstanceID,
		ContextPublicationGeneration: c.ContextPublicationGeneration,
		TargetGeneration:             c.TargetGeneration,
	}
}

func (c ChannelRuntimeContextCoordinate) Normalized() ChannelRuntimeContextCoordinate {
	c.BundleHash = strings.TrimSpace(c.BundleHash)
	c.BundleSource = strings.TrimSpace(c.BundleSource)
	c.BundleIdentity = strings.TrimSpace(c.BundleIdentity)
	c.PackInventoryGeneration = strings.TrimSpace(c.PackInventoryGeneration)
	c.RuntimeInstanceID = strings.TrimSpace(c.RuntimeInstanceID)
	return c
}

func (c ChannelRuntimeContextCoordinate) Validate() error {
	if err := c.ValidateContext(); err != nil {
		return err
	}
	if c.TargetGeneration == 0 {
		return fmt.Errorf("channel runtime context requires an exact target generation")
	}
	return nil
}

func (c ChannelRuntimeContextCoordinate) ValidateContext() error {
	c = c.Normalized()
	if err := c.DurableIdentity().Validate(); err != nil {
		return err
	}
	if c.RuntimeInstanceID == "" || c.ContextPublicationGeneration == 0 {
		return fmt.Errorf("channel runtime context requires an exact runtime instance and publication generation")
	}
	return nil
}

func (c ChannelRuntimeContextCoordinate) MatchesDurableIdentity(other ChannelRuntimeContextCoordinate) bool {
	return c.DurableIdentity().Matches(other.DurableIdentity())
}

func (c ChannelRuntimeContextCoordinate) MatchesContextOccurrence(runtimeInstanceID string, publicationGeneration uint64) bool {
	c = c.Normalized()
	runtimeInstanceID = strings.TrimSpace(runtimeInstanceID)
	return c.ValidateContext() == nil && runtimeInstanceID != "" && publicationGeneration != 0 &&
		c.RuntimeInstanceID == runtimeInstanceID && c.ContextPublicationGeneration == publicationGeneration
}

func (c ChannelRuntimeContextCoordinate) Matches(other ChannelRuntimeContextCoordinate) bool {
	c, other = c.Normalized(), other.Normalized()
	return c.Validate() == nil && other.Validate() == nil && c == other
}

type SlotState string

const (
	SlotAbsent                          SlotState = "absent"
	SlotIdentityCurrentActivationAbsent SlotState = "identity_current_activation_absent"
	SlotOperationPending                SlotState = "operation_pending"
	SlotReady                           SlotState = "ready"
	SlotActivationStale                 SlotState = "activation_stale"
	SlotUncertain                       SlotState = "uncertain"
	SlotRetired                         SlotState = "retired"
)

type AdmissionDecision string

const (
	AdmissionStart                   AdmissionDecision = "start"
	AdmissionStartPreservingIdentity AdmissionDecision = "start_preserving_identity"
	AdmissionStartReplacingIdentity  AdmissionDecision = "start_replacing_identity"
	AdmissionTeachReconnect          AdmissionDecision = "teach_reconnect"
	AdmissionAlreadyConnected        AdmissionDecision = "already_connected"
	AdmissionNothingToReconnect      AdmissionDecision = "nothing_to_reconnect"
	AdmissionNothingToRebind         AdmissionDecision = "nothing_to_rebind"
	AdmissionConflict                AdmissionDecision = "conflict"
)

func AdmitVerb(verb Verb, state SlotState) AdmissionDecision {
	if !verb.Valid() {
		return AdmissionConflict
	}
	switch verb {
	case VerbConnect:
		switch state {
		case SlotAbsent, SlotRetired:
			return AdmissionStart
		case SlotIdentityCurrentActivationAbsent, SlotActivationStale:
			return AdmissionTeachReconnect
		case SlotReady:
			return AdmissionAlreadyConnected
		default:
			return AdmissionConflict
		}
	case VerbReconnect:
		switch state {
		case SlotIdentityCurrentActivationAbsent, SlotActivationStale, SlotReady:
			return AdmissionStartPreservingIdentity
		case SlotAbsent, SlotRetired:
			return AdmissionNothingToReconnect
		default:
			return AdmissionConflict
		}
	case VerbRebind:
		switch state {
		case SlotIdentityCurrentActivationAbsent, SlotActivationStale, SlotReady:
			return AdmissionStartReplacingIdentity
		case SlotAbsent, SlotRetired:
			return AdmissionNothingToRebind
		default:
			return AdmissionConflict
		}
	default:
		return AdmissionConflict
	}
}

type ReadinessReason string

const (
	ReadinessReady                   ReadinessReason = "ready"
	ReadinessCoordinateInvalid       ReadinessReason = "runtime_context_coordinate_invalid"
	ReadinessPlanUnavailable         ReadinessReason = "channel_plan_unavailable"
	ReadinessActivationUnavailable   ReadinessReason = "activation_not_current"
	ReadinessBindingUnavailable      ReadinessReason = "binding_not_current"
	ReadinessProofUnavailable        ReadinessReason = "proof_not_current"
	ReadinessCredentialsUnavailable  ReadinessReason = "credentials_not_current"
	ReadinessConfirmationUnavailable ReadinessReason = "confirmation_not_terminal_success"
	ReadinessTargetUnavailable       ReadinessReason = "target_not_current"
	ReadinessExposureUnavailable     ReadinessReason = "exposure_not_current"
	ReadinessRegistrationUnavailable ReadinessReason = "registration_not_current"
	ReadinessSessionUnavailable      ReadinessReason = "session_not_current"
)

type ReadinessFacts struct {
	Coordinate                       ChannelRuntimeContextCoordinate   `json:"coordinate"`
	Interface                        operatorchannel.InterfaceIdentity `json:"interface"`
	ActivationRevision               int64                             `json:"activation_revision"`
	PlanGeneration                   plangeneration.Generation         `json:"plan_generation"`
	ActivationGeneration             ChannelActivationGeneration       `json:"-"`
	RegistrationActivationGeneration ChannelActivationGeneration       `json:"-"`
	ActivationCurrent                bool                              `json:"activation_current"`
	BindingRevision                  int64                             `json:"binding_revision"`
	ExpectedBindingRevision          int64                             `json:"expected_binding_revision"`
	ProofID                          string                            `json:"proof_id,omitempty"`
	ProofRevision                    int64                             `json:"proof_revision,omitempty"`
	ExpectedProofRevision            int64                             `json:"expected_proof_revision,omitempty"`
	ProofCurrent                     bool                              `json:"proof_current"`
	CredentialsCurrent               bool                              `json:"credentials_current"`
	ConfirmationActivationRevision   int64                             `json:"confirmation_activation_revision"`
	ConfirmationBindingRevision      int64                             `json:"confirmation_binding_revision"`
	ConfirmationTerminalSuccess      bool                              `json:"confirmation_terminal_success"`
	Posture                          ActivationPosture                 `json:"activation_posture"`
	TargetGeneration                 uint64                            `json:"target_generation,omitempty"`
	ExpectedTargetGeneration         uint64                            `json:"expected_target_generation,omitempty"`
	ExposureGeneration               string                            `json:"exposure_generation,omitempty"`
	ExpectedExposureGeneration       string                            `json:"expected_exposure_generation,omitempty"`
	RegistrationCurrent              bool                              `json:"registration_current,omitempty"`
	ServiceFulfillmentGeneration     string                            `json:"service_fulfillment_generation,omitempty"`
	ExpectedServiceGeneration        string                            `json:"expected_service_fulfillment_generation,omitempty"`
	SessionCurrent                   bool                              `json:"session_current,omitempty"`
	ObservedAt                       time.Time                         `json:"observed_at"`
}

type ConnectedChannelReadiness struct {
	Ready                bool                            `json:"ready"`
	Reason               ReadinessReason                 `json:"reason"`
	Coordinate           ChannelRuntimeContextCoordinate `json:"coordinate"`
	ActivationRevision   int64                           `json:"activation_revision"`
	BindingRevision      int64                           `json:"binding_revision"`
	ActivationGeneration string                          `json:"activation_generation,omitempty"`
	ObservedAt           time.Time                       `json:"observed_at"`
}

// ConnectedChannelReadback is the canonical presentation projection for one
// retained identity and, when present, its exact onboarding activation.
type ConnectedChannelReadback struct {
	Identity   operatorchannel.Readback    `json:"identity"`
	Operation  *Operation                  `json:"operation,omitempty"`
	Activation *ConnectedChannelActivation `json:"activation,omitempty"`
	Readiness  *ConnectedChannelReadiness  `json:"readiness,omitempty"`
	Recovery   *ConnectedChannelRecovery   `json:"recovery,omitempty"`
}

type ConnectedChannelRecovery struct {
	Reason   ReadinessReason `json:"reason"`
	Provider string          `json:"provider"`
	Commands []string        `json:"commands"`
}

func ProjectReadiness(f ReadinessFacts) ConnectedChannelReadiness {
	result := ConnectedChannelReadiness{Coordinate: f.Coordinate, ActivationRevision: f.ActivationRevision, BindingRevision: f.BindingRevision, ObservedAt: f.ObservedAt.UTC()}
	if f.ActivationGeneration.Valid() {
		result.ActivationGeneration = f.ActivationGeneration.Diagnostic()
	}
	fail := func(reason ReadinessReason) ConnectedChannelReadiness { result.Reason = reason; return result }
	if f.Coordinate.Validate() != nil || f.Interface.Validate() != nil || !f.Posture.Valid() {
		return fail(ReadinessCoordinateInvalid)
	}
	if !f.PlanGeneration.Equal(f.Coordinate.PlanGeneration) || !f.ActivationGeneration.Valid() {
		return fail(ReadinessPlanUnavailable)
	}
	if !f.ActivationCurrent || f.ActivationRevision < 1 {
		return fail(ReadinessActivationUnavailable)
	}
	if f.BindingRevision < 1 || f.BindingRevision != f.ExpectedBindingRevision {
		return fail(ReadinessBindingUnavailable)
	}
	if strings.TrimSpace(f.ProofID) != "" && (!f.ProofCurrent || f.ProofRevision < 1 || f.ProofRevision != f.ExpectedProofRevision) {
		return fail(ReadinessProofUnavailable)
	}
	if !f.CredentialsCurrent {
		return fail(ReadinessCredentialsUnavailable)
	}
	if !f.ConfirmationTerminalSuccess || f.ConfirmationActivationRevision != f.ActivationRevision || f.ConfirmationBindingRevision != f.BindingRevision {
		return fail(ReadinessConfirmationUnavailable)
	}
	switch f.Posture {
	case ActivationWebhookRegistration:
		if f.TargetGeneration == 0 || f.TargetGeneration != f.ExpectedTargetGeneration || f.TargetGeneration != f.Coordinate.TargetGeneration {
			return fail(ReadinessTargetUnavailable)
		}
		if strings.TrimSpace(f.ExposureGeneration) == "" || f.ExposureGeneration != f.ExpectedExposureGeneration {
			return fail(ReadinessExposureUnavailable)
		}
		if !f.RegistrationCurrent || !f.RegistrationActivationGeneration.Equal(f.ActivationGeneration) {
			return fail(ReadinessRegistrationUnavailable)
		}
	case ActivationSessionConnection:
		if strings.TrimSpace(f.ServiceFulfillmentGeneration) == "" || f.ServiceFulfillmentGeneration != f.ExpectedServiceGeneration || !f.SessionCurrent {
			return fail(ReadinessSessionUnavailable)
		}
	}
	result.Ready, result.Reason = true, ReadinessReady
	return result
}

type CredentialAdmissionKind string

const (
	CredentialAdmissionWritten  CredentialAdmissionKind = "written"
	CredentialAdmissionObserved CredentialAdmissionKind = "observed"
)

func (k CredentialAdmissionKind) Valid() bool {
	return k == CredentialAdmissionWritten || k == CredentialAdmissionObserved
}

type CredentialAdmission struct {
	Role     string                  `json:"role"`
	StoreKey string                  `json:"store_key"`
	Kind     CredentialAdmissionKind `json:"kind"`
	Receipt  string                  `json:"receipt"`
	Epoch    string                  `json:"epoch"`
}

func (a CredentialAdmission) Validate() error {
	if strings.TrimSpace(a.Role) == "" || strings.TrimSpace(a.StoreKey) == "" || !a.Kind.Valid() || strings.TrimSpace(a.Receipt) == "" || strings.TrimSpace(a.Epoch) == "" {
		return fmt.Errorf("%w: credential admission requires role, key, kind, receipt, and epoch", ErrInvalidRequest)
	}
	return nil
}

type CredentialReservation struct {
	Role     string `json:"role"`
	StoreKey string `json:"store_key"`
}

func (r CredentialReservation) Validate() error {
	if strings.TrimSpace(r.Role) == "" || strings.TrimSpace(r.StoreKey) == "" {
		return fmt.Errorf("%w: credential reservation requires role and key", ErrInvalidRequest)
	}
	return nil
}

type StartRequest struct {
	OperationID            string                            `json:"operation_id"`
	RequestKeyHash         string                            `json:"request_key_hash"`
	RequestHash            string                            `json:"request_hash"`
	PrincipalID            string                            `json:"principal_id"`
	Verb                   Verb                              `json:"verb"`
	Provider               string                            `json:"provider"`
	Interface              operatorchannel.InterfaceIdentity `json:"interface"`
	Coordinate             ChannelRuntimeContextCoordinate   `json:"coordinate"`
	TargetSelector         string                            `json:"target_selector"`
	Posture                ActivationPosture                 `json:"activation_posture"`
	Ceremony               IdentityCeremony                  `json:"identity_ceremony"`
	SaveProof              bool                              `json:"save_proof"`
	CredentialReservations []CredentialReservation           `json:"credential_reservations"`
	RequestedAt            time.Time                         `json:"requested_at"`
}

func (r StartRequest) SlotKey() string {
	return operatorchannel.Hash("channel-onboarding-slot-v1", r.Coordinate.BundleHash, r.Coordinate.BundleSource, r.Interface.Key(), r.TargetSelector, r.Provider)
}

func (r StartRequest) Validate() error {
	if strings.TrimSpace(r.OperationID) == "" || strings.TrimSpace(r.RequestKeyHash) == "" || strings.TrimSpace(r.RequestHash) == "" || strings.TrimSpace(r.PrincipalID) == "" || !r.Verb.Valid() || strings.TrimSpace(r.Provider) == "" || strings.TrimSpace(r.TargetSelector) == "" || r.RequestedAt.IsZero() {
		return fmt.Errorf("%w: operation identity, semantic request, principal, verb, provider, target, and time are required", ErrInvalidRequest)
	}
	if err := r.Interface.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if err := r.Coordinate.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if !r.Posture.Valid() || !r.Ceremony.Valid() {
		return fmt.Errorf("%w: activation posture and identity ceremony are required", ErrInvalidRequest)
	}
	if len(r.CredentialReservations) == 0 {
		return fmt.Errorf("%w: at least one credential reservation is required", ErrInvalidRequest)
	}
	roles := map[string]struct{}{}
	for _, reservation := range r.CredentialReservations {
		if err := reservation.Validate(); err != nil {
			return err
		}
		if _, duplicate := roles[reservation.Role]; duplicate {
			return fmt.Errorf("%w: duplicate credential reservation role %q", ErrInvalidRequest, reservation.Role)
		}
		roles[reservation.Role] = struct{}{}
	}
	return nil
}

type Operation struct {
	OperationID             string                            `json:"operation_id"`
	RequestKeyHash          string                            `json:"-"`
	RequestHash             string                            `json:"-"`
	SlotKey                 string                            `json:"-"`
	PrincipalID             string                            `json:"principal_id"`
	Verb                    Verb                              `json:"verb"`
	Provider                string                            `json:"provider"`
	Interface               operatorchannel.InterfaceIdentity `json:"interface"`
	Coordinate              ChannelRuntimeContextCoordinate   `json:"coordinate"`
	TargetSelector          string                            `json:"target_selector"`
	Posture                 ActivationPosture                 `json:"activation_posture"`
	Ceremony                IdentityCeremony                  `json:"identity_ceremony"`
	Phase                   Phase                             `json:"phase"`
	Revision                int64                             `json:"revision"`
	SaveProof               bool                              `json:"save_proof"`
	CredentialReservations  []CredentialReservation           `json:"credential_reservations"`
	CredentialAdmissions    []CredentialAdmission             `json:"credential_admissions,omitempty"`
	IdentityOperationID     string                            `json:"identity_operation_id,omitempty"`
	BindingRevision         int64                             `json:"binding_revision,omitempty"`
	ActivationRevision      int64                             `json:"activation_revision,omitempty"`
	ConfirmationOperationID string                            `json:"confirmation_operation_id,omitempty"`
	FailureCode             string                            `json:"failure_code,omitempty"`
	FailureMessage          string                            `json:"failure_message,omitempty"`
	RequestedAt             time.Time                         `json:"requested_at"`
	UpdatedAt               time.Time                         `json:"updated_at"`
	CompletedAt             time.Time                         `json:"completed_at,omitzero"`
}

type AdvanceRequest struct {
	OperationID                 string
	ExpectedRevision            int64
	Phase                       Phase
	RebindCoordinate            *ChannelRuntimeContextCoordinate
	CredentialAdmissions        []CredentialAdmission
	ReplaceCredentialAdmissions bool
	IdentityOperationID         string
	BindingRevision             int64
	ConfirmationOperationID     string
	FailureCode                 string
	FailureMessage              string
	Now                         time.Time
}

type ActivationStatus string

const (
	ActivationCurrent ActivationStatus = "current"
	ActivationRetired ActivationStatus = "retired"
)

type ConnectedChannelActivation struct {
	ActivationID         string                            `json:"activation_id"`
	SlotKey              string                            `json:"-"`
	OperationID          string                            `json:"operation_id"`
	OperationRevision    int64                             `json:"operation_revision"`
	PrincipalID          string                            `json:"principal_id"`
	Provider             string                            `json:"provider"`
	Interface            operatorchannel.InterfaceIdentity `json:"interface"`
	Coordinate           ChannelRuntimeContextCoordinate   `json:"coordinate"`
	TargetSelector       string                            `json:"target_selector"`
	Posture              ActivationPosture                 `json:"activation_posture"`
	BindingRevision      int64                             `json:"binding_revision"`
	ConversationRef      string                            `json:"conversation_reference"`
	ProofID              string                            `json:"proof_id,omitempty"`
	ProofRevision        int64                             `json:"proof_revision,omitempty"`
	CredentialAdmissions []CredentialAdmission             `json:"credential_admissions"`
	Revision             int64                             `json:"revision"`
	Status               ActivationStatus                  `json:"status"`
	CreatedAt            time.Time                         `json:"created_at"`
	UpdatedAt            time.Time                         `json:"updated_at"`
	RetiredAt            time.Time                         `json:"retired_at,omitzero"`
	RetirementReason     string                            `json:"retirement_reason,omitempty"`
}

type PublishActivationRequest struct {
	OperationID      string
	ExpectedRevision int64
	ActivationID     string
	BindingRevision  int64
	ConversationRef  string
	ProofID          string
	ProofRevision    int64
	Now              time.Time
}

type RetireActivationRequest struct {
	SlotKey                    string
	ExpectedActivationRevision int64
	Reason                     string
	Now                        time.Time
}

type Store interface {
	TeardownStore
	ReserveChannelOnboarding(context.Context, StartRequest) (Operation, error)
	GetChannelOnboarding(context.Context, string) (Operation, error)
	ListChannelOnboardingOperations(context.Context) ([]Operation, error)
	AdvanceChannelOnboarding(context.Context, AdvanceRequest) (Operation, error)
	PublishConnectedChannelActivation(context.Context, PublishActivationRequest) (Operation, ConnectedChannelActivation, error)
	GetConnectedChannelActivation(context.Context, string) (ConnectedChannelActivation, error)
	ListCurrentConnectedChannelActivations(context.Context) ([]ConnectedChannelActivation, error)
	RetireConnectedChannelActivation(context.Context, RetireActivationRequest) (ConnectedChannelActivation, error)
}
