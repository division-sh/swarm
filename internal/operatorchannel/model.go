package operatorchannel

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/google/uuid"
)

const (
	ProofFormat                = "swarm-verified-account-proof-v2"
	ChallengePrefix            = "SWARM-"
	DefaultChallengeTTL        = 10 * time.Minute
	DefaultConnectWait         = 2 * time.Minute
	InterfaceHITLChannelV2     = "swarm.hitl-channel/v2"
	DispositionConsumedBinding = "consumed_by_binding"
	DispositionRejectedClaim   = "rejected_binding_claim"
)

var (
	ErrInvalidRequest     = errors.New("invalid operator channel request")
	ErrNotFound           = errors.New("operator channel resource not found")
	ErrConflict           = errors.New("operator channel conflict")
	ErrRevisionConflict   = errors.New("operator channel revision conflict")
	ErrOperationTerminal  = errors.New("operator channel operation is terminal")
	ErrProofUnavailable   = errors.New("verified account proof is unavailable")
	ErrBindingUnavailable = errors.New("operator channel binding is unavailable")
	ErrCredentialStale    = errors.New("operator channel provider credential is stale")
)

var proofNamespace = uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm-verified-account-proof"))

type ConversationScope string

const (
	ConversationScopeDirect ConversationScope = "direct"
	ConversationScopeShared ConversationScope = "shared"
)

func (s ConversationScope) Valid() bool {
	return s == ConversationScopeDirect || s == ConversationScopeShared
}

type OperationKind string

const (
	OperationConnect   OperationKind = "connect"
	OperationReconnect OperationKind = "reconnect"
	OperationRebind    OperationKind = "rebind"
	OperationUnbind    OperationKind = "unbind"
)

func (k OperationKind) Valid() bool {
	switch k {
	case OperationConnect, OperationReconnect, OperationRebind, OperationUnbind:
		return true
	default:
		return false
	}
}

type OperationState string

const (
	StateAwaitingClaim        OperationState = "awaiting_claim"
	StateAwaitingConfirmation OperationState = "awaiting_confirmation"
	StateBound                OperationState = "bound"
	StateRejected             OperationState = "rejected"
	StateExpired              OperationState = "expired"
	StateUnbound              OperationState = "unbound"
)

func (s OperationState) Terminal() bool {
	switch s {
	case StateBound, StateRejected, StateExpired, StateUnbound:
		return true
	default:
		return false
	}
}

type BindingStatus string

const (
	BindingCurrent BindingStatus = "current"
	BindingStale   BindingStatus = "stale"
	BindingUnbound BindingStatus = "unbound"
	BindingRevoked BindingStatus = "revoked"
)

type ProofStatus string

const (
	ProofPending ProofStatus = "pending"
	ProofActive  ProofStatus = "active"
	ProofRevoked ProofStatus = "revoked"
	ProofFailed  ProofStatus = "failed"
	ProofSkipped ProofStatus = "skipped"
)

type BindingSource string

const (
	BindingSourceLiveVerification BindingSource = "live_verification"
	BindingSourceLocalProof       BindingSource = "local_proof"
)

type ConsentScope string

const (
	ConsentNotify ConsentScope = "notify"
	ConsentDecide ConsentScope = "decide"
)

type Principal struct {
	ID        string    `json:"principal_id"`
	CreatedAt time.Time `json:"created_at"`
}

func (p Principal) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(p.ID)); err != nil {
		return fmt.Errorf("%w: principal_id must be a UUID", ErrInvalidRequest)
	}
	if p.CreatedAt.IsZero() {
		return fmt.Errorf("%w: principal created_at is required", ErrInvalidRequest)
	}
	return nil
}

type InterfaceIdentity struct {
	InterfaceRef        string `json:"interface_ref"`
	ChannelPackID       string `json:"channel_pack_id"`
	ChannelPackVersion  string `json:"channel_pack_version"`
	ChannelManifestHash string `json:"channel_manifest_hash"`
	SemanticGeneration  string `json:"semantic_generation"`
	Selector            string `json:"selector"`
}

func (i InterfaceIdentity) Normalized() InterfaceIdentity {
	i.InterfaceRef = strings.TrimSpace(i.InterfaceRef)
	i.ChannelPackID = strings.TrimSpace(i.ChannelPackID)
	i.ChannelPackVersion = strings.TrimSpace(i.ChannelPackVersion)
	i.ChannelManifestHash = strings.TrimSpace(i.ChannelManifestHash)
	i.SemanticGeneration = strings.TrimSpace(i.SemanticGeneration)
	i.Selector = strings.TrimSpace(i.Selector)
	if i.Selector == "" {
		i.Selector = i.CanonicalSelector()
	}
	return i
}

func (i InterfaceIdentity) Validate() error {
	i = i.Normalized()
	if i.InterfaceRef == "" || i.ChannelPackID == "" || i.ChannelPackVersion == "" || i.ChannelManifestHash == "" || i.SemanticGeneration == "" {
		return fmt.Errorf("%w: complete pack-qualified interface identity is required", ErrInvalidRequest)
	}
	if i.Selector != i.CanonicalSelector() {
		return fmt.Errorf("%w: interface selector does not match the pack-qualified semantic identity", ErrInvalidRequest)
	}
	return nil
}

func (i InterfaceIdentity) Key() string {
	i.InterfaceRef = strings.TrimSpace(i.InterfaceRef)
	i.ChannelPackID = strings.TrimSpace(i.ChannelPackID)
	i.ChannelPackVersion = strings.TrimSpace(i.ChannelPackVersion)
	i.ChannelManifestHash = strings.TrimSpace(i.ChannelManifestHash)
	i.SemanticGeneration = strings.TrimSpace(i.SemanticGeneration)
	parts := []string{i.InterfaceRef, i.ChannelPackID, i.ChannelPackVersion, i.ChannelManifestHash, i.SemanticGeneration}
	var key strings.Builder
	for _, part := range parts {
		fmt.Fprintf(&key, "%d:%s", len(part), part)
	}
	return key.String()
}

func (i InterfaceIdentity) CanonicalSelector() string {
	packID := strings.TrimSpace(i.ChannelPackID)
	if packID == "" {
		return ""
	}
	return packID + "@" + Hash("operator-channel-interface-selector-v1", i.Key())
}

type TextFact struct {
	Interface           InterfaceIdentity `json:"interface"`
	ExternalAccountRef  string            `json:"external_account_reference"`
	ConversationRef     string            `json:"conversation_reference"`
	ConversationScope   ConversationScope `json:"conversation_scope"`
	Text                string            `json:"text"`
	AccountPresentation string            `json:"account_presentation,omitempty"`
}

func (f TextFact) Validate() error {
	if err := f.Interface.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(f.ExternalAccountRef) == "" || strings.TrimSpace(f.ConversationRef) == "" || strings.TrimSpace(f.Text) == "" {
		return fmt.Errorf("%w: text claim requires account, conversation, and text", ErrInvalidRequest)
	}
	if !f.ConversationScope.Valid() {
		return fmt.Errorf("%w: conversation_scope must be direct or shared", ErrInvalidRequest)
	}
	return nil
}

type InboundClaim struct {
	TextFact
	Provider              string `json:"provider"`
	ProviderEventID       string `json:"provider_event_id"`
	PublicationID         string `json:"publication_id"`
	ProviderAuthorization string `json:"provider_authorization"`
	Challenge             string `json:"challenge"`
}

func (c InboundClaim) Validate() error {
	if err := c.TextFact.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(c.Provider) == "" || strings.TrimSpace(c.ProviderEventID) == "" || strings.TrimSpace(c.PublicationID) == "" || strings.TrimSpace(c.ProviderAuthorization) == "" {
		return fmt.Errorf("%w: inbound claim provenance is required", ErrInvalidRequest)
	}
	if !ValidChallenge(c.Challenge) || strings.TrimSpace(c.Text) != strings.TrimSpace(c.Challenge) {
		return fmt.Errorf("%w: exact canonical challenge text is required", ErrInvalidRequest)
	}
	return nil
}

type BeginRequest struct {
	OperationID          string                           `json:"operation_id"`
	Kind                 OperationKind                    `json:"kind"`
	PrincipalID          string                           `json:"principal_id"`
	Interface            InterfaceIdentity                `json:"interface"`
	ExpectedRevision     int64                            `json:"expected_revision"`
	RequestKeyHash       string                           `json:"request_key_hash"`
	RequestHash          string                           `json:"request_hash"`
	SaveProof            bool                             `json:"save_proof"`
	PlannedProofID       string                           `json:"planned_proof_id,omitempty"`
	PlannedProofRevision int64                            `json:"planned_proof_revision,omitempty"`
	ProviderCredential   runtimecredentials.ValueEvidence `json:"-"`
	RequestedAt          time.Time                        `json:"requested_at"`
	ExpiresAt            time.Time                        `json:"expires_at"`
}

type ConfirmRequest struct {
	OperationID      string    `json:"operation_id"`
	PrincipalID      string    `json:"principal_id"`
	ExpectedRevision int64     `json:"expected_revision"`
	Approve          bool      `json:"approve"`
	ConfirmedAt      time.Time `json:"confirmed_at"`
}

type ExpireRequest struct {
	OperationID      string    `json:"operation_id"`
	PrincipalID      string    `json:"principal_id"`
	ExpectedRevision int64     `json:"expected_revision"`
	ExpiredAt        time.Time `json:"expired_at"`
}

type UnbindRequest struct {
	OperationID      string            `json:"operation_id"`
	PrincipalID      string            `json:"principal_id"`
	Interface        InterfaceIdentity `json:"interface"`
	ExpectedRevision int64             `json:"expected_revision"`
	RequestKeyHash   string            `json:"request_key_hash"`
	RequestHash      string            `json:"request_hash"`
	RequestedAt      time.Time         `json:"requested_at"`
}

type BootBindRequest struct {
	PrincipalID string            `json:"principal_id"`
	Interface   InterfaceIdentity `json:"interface"`
	Proof       VerifiedProof     `json:"proof"`
	RequestedAt time.Time         `json:"requested_at"`
}

type Operation struct {
	OperationID             string                           `json:"operation_id"`
	Kind                    OperationKind                    `json:"kind"`
	PrincipalID             string                           `json:"principal_id"`
	Interface               InterfaceIdentity                `json:"interface"`
	Challenge               string                           `json:"challenge,omitempty"`
	State                   OperationState                   `json:"state"`
	Revision                int64                            `json:"revision"`
	BindingRevision         int64                            `json:"binding_revision,omitempty"`
	ExternalAccountRef      string                           `json:"external_account_reference,omitempty"`
	ConversationRef         string                           `json:"conversation_reference,omitempty"`
	ConversationScope       ConversationScope                `json:"conversation_scope,omitempty"`
	AccountPresentation     string                           `json:"account_presentation,omitempty"`
	SaveProof               bool                             `json:"save_proof"`
	ProofID                 string                           `json:"proof_id,omitempty"`
	ProofRevision           int64                            `json:"proof_revision,omitempty"`
	ProofStatus             ProofStatus                      `json:"proof_status"`
	ClaimDisposition        string                           `json:"claim_disposition,omitempty"`
	RequestedAt             time.Time                        `json:"requested_at"`
	ExpiresAt               time.Time                        `json:"expires_at,omitzero"`
	ClaimedAt               time.Time                        `json:"claimed_at,omitzero"`
	CompletedAt             time.Time                        `json:"completed_at,omitzero"`
	RequestHash             string                           `json:"-"`
	ExpectedBindingRevision int64                            `json:"-"`
	PlannedProofID          string                           `json:"-"`
	PlannedProofRevision    int64                            `json:"-"`
	ProviderCredential      runtimecredentials.ValueEvidence `json:"-"`
}

type Binding struct {
	PrincipalID         string                           `json:"principal_id"`
	Interface           InterfaceIdentity                `json:"interface"`
	ExternalAccountRef  string                           `json:"external_account_reference,omitempty"`
	ConversationRef     string                           `json:"conversation_reference,omitempty"`
	ConversationScope   ConversationScope                `json:"conversation_scope,omitempty"`
	AccountPresentation string                           `json:"account_presentation,omitempty"`
	Revision            int64                            `json:"revision"`
	Status              BindingStatus                    `json:"status"`
	Source              BindingSource                    `json:"source,omitempty"`
	ProofID             string                           `json:"proof_id,omitempty"`
	ProofRevision       int64                            `json:"proof_revision,omitempty"`
	OperationID         string                           `json:"operation_id"`
	UpdatedAt           time.Time                        `json:"updated_at"`
	ProviderCredential  runtimecredentials.ValueEvidence `json:"-"`
}

type VerifiedProof struct {
	Format              string                           `json:"format"`
	ProofID             string                           `json:"proof_id"`
	Revision            int64                            `json:"revision"`
	Status              ProofStatus                      `json:"status"`
	Interface           InterfaceIdentity                `json:"interface"`
	ExternalAccountRef  string                           `json:"external_account_reference"`
	ConversationRef     string                           `json:"conversation_reference"`
	ConversationScope   ConversationScope                `json:"conversation_scope"`
	AccountPresentation string                           `json:"account_presentation,omitempty"`
	Method              string                           `json:"method"`
	Challenge           string                           `json:"challenge"`
	OriginalOperationID string                           `json:"original_operation_id"`
	MintingStoreID      string                           `json:"minting_store_id"`
	MintingDeploymentID string                           `json:"minting_deployment_id"`
	VerifiedAt          time.Time                        `json:"verified_at"`
	OperatorConfirmed   bool                             `json:"operator_confirmed"`
	ConsentScopes       []ConsentScope                   `json:"consent_scopes"`
	ProviderCredential  runtimecredentials.ValueEvidence `json:"-"`
}

func (p VerifiedProof) Validate() error {
	if p.Format != ProofFormat || p.Status != ProofActive {
		return fmt.Errorf("%w: active %s record is required", ErrProofUnavailable, ProofFormat)
	}
	if _, err := uuid.Parse(strings.TrimSpace(p.ProofID)); err != nil || p.Revision < 1 || p.VerifiedAt.IsZero() || !p.OperatorConfirmed {
		return fmt.Errorf("%w: proof identity, revision, verification, and confirmation are required", ErrProofUnavailable)
	}
	if err := p.Interface.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrProofUnavailable, err)
	}
	if strings.TrimSpace(p.ExternalAccountRef) == "" || strings.TrimSpace(p.ConversationRef) == "" || !p.ConversationScope.Valid() {
		return fmt.Errorf("%w: proof account and conversation identity are required", ErrProofUnavailable)
	}
	if strings.TrimSpace(p.Method) == "" || !ValidChallenge(p.Challenge) || strings.TrimSpace(p.OriginalOperationID) == "" || strings.TrimSpace(p.MintingStoreID) == "" || strings.TrimSpace(p.MintingDeploymentID) == "" {
		return fmt.Errorf("%w: proof provenance is incomplete", ErrProofUnavailable)
	}
	if len(p.ConsentScopes) == 0 {
		return fmt.Errorf("%w: proof consent scope is required", ErrProofUnavailable)
	}
	if err := p.ProviderCredential.Validate(); err != nil {
		return fmt.Errorf("%w: provider credential evidence is invalid", ErrProofUnavailable)
	}
	for _, scope := range p.ConsentScopes {
		if scope != ConsentNotify && scope != ConsentDecide {
			return fmt.Errorf("%w: unsupported consent scope %q", ErrProofUnavailable, scope)
		}
	}
	return nil
}

type Readback struct {
	PrincipalID         string            `json:"principal_id"`
	Interface           InterfaceIdentity `json:"interface"`
	Status              BindingStatus     `json:"status"`
	Reason              string            `json:"reason,omitempty"`
	BindingRevision     int64             `json:"binding_revision,omitempty"`
	ExternalAccountRef  string            `json:"external_account_reference,omitempty"`
	ConversationRef     string            `json:"conversation_reference,omitempty"`
	ConversationScope   ConversationScope `json:"conversation_scope,omitempty"`
	AccountPresentation string            `json:"account_presentation,omitempty"`
	Source              BindingSource     `json:"source,omitempty"`
	ProofID             string            `json:"proof_id,omitempty"`
	ProofRevision       int64             `json:"proof_revision,omitempty"`
	ProofStatus         ProofStatus       `json:"proof_status,omitempty"`
	ConsentScopes       []ConsentScope    `json:"consent_scopes,omitempty"`
	PendingOperation    *Operation        `json:"pending_operation,omitempty"`
}

type ClaimSettlement struct {
	Consumed    bool      `json:"consumed"`
	Disposition string    `json:"disposition"`
	Operation   Operation `json:"operation"`
}

type ProofResponsibility struct {
	Operation Operation     `json:"operation"`
	Binding   Binding       `json:"binding"`
	Proof     VerifiedProof `json:"proof"`
}

type Store interface {
	EnsureOperatorPrincipal(context.Context, time.Time) (Principal, error)
	BeginChannelBinding(context.Context, BeginRequest) (Operation, error)
	ConfirmChannelBinding(context.Context, ConfirmRequest) (Operation, Binding, error)
	ExpireChannelBinding(context.Context, ExpireRequest) (Operation, error)
	UnbindOperatorChannel(context.Context, UnbindRequest) (Operation, Binding, error)
	BindOperatorChannelFromProof(context.Context, BootBindRequest) (Binding, error)
	ListOperatorChannelOperations(context.Context, string) ([]Operation, error)
	ListOperatorChannelBindings(context.Context, string) ([]Binding, error)
	ListPendingProofResponsibilities(context.Context) ([]ProofResponsibility, error)
	CompleteProofResponsibility(context.Context, string, string, int64, ProofStatus, string, time.Time) error
}

type ProofStore interface {
	List(context.Context) ([]VerifiedProof, error)
	Get(context.Context, InterfaceIdentity) (VerifiedProof, bool, error)
	Put(context.Context, VerifiedProof) error
	Revoke(context.Context, InterfaceIdentity, int64, time.Time) (VerifiedProof, error)
}

func NewOperationID() string { return uuid.NewString() }

func ProofIDForInterface(identity InterfaceIdentity) string {
	return uuid.NewSHA1(proofNamespace, []byte(identity.Key())).String()
}

func NewChallenge() (string, error) {
	var raw [10]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate operator channel challenge: %w", err)
	}
	return ChallengePrefix + base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw[:]), nil
}

func ValidChallenge(raw string) bool {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, ChallengePrefix) || len(raw) != len(ChallengePrefix)+16 {
		return false
	}
	_, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(strings.TrimPrefix(raw, ChallengePrefix))
	return err == nil
}

func ChallengeFromText(text string) (string, bool) {
	text = strings.TrimSpace(text)
	return text, ValidChallenge(text)
}

func Hash(parts ...string) string {
	normalized := make([]string, len(parts))
	for i := range parts {
		normalized[i] = strings.TrimSpace(parts[i])
	}
	sum := sha256.Sum256([]byte(strings.Join(normalized, "\x00")))
	return hex.EncodeToString(sum[:])
}

func MaskPresentation(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.HasPrefix(value, "@") {
		if len(value) <= 4 {
			return "@***"
		}
		return value[:2] + strings.Repeat("*", len(value)-3) + value[len(value)-1:]
	}
	if len(value) <= 4 {
		return strings.Repeat("*", len(value))
	}
	return value[:2] + strings.Repeat("*", len(value)-4) + value[len(value)-2:]
}

func MaskClaimantPresentation(presentation, externalAccountRef string) string {
	if masked := MaskPresentation(presentation); masked != "" {
		return masked
	}
	return MaskPresentation(externalAccountRef)
}

func ProjectOperationReadback(operation Operation) Operation {
	operation.AccountPresentation = MaskClaimantPresentation(operation.AccountPresentation, operation.ExternalAccountRef)
	operation.ExternalAccountRef = MaskPresentation(operation.ExternalAccountRef)
	operation.ConversationRef = MaskPresentation(operation.ConversationRef)
	return operation
}
