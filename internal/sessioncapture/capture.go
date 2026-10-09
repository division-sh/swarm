package sessioncapture

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/google/uuid"
)

const MaxCaptureEventBytes = 1 << 20
const MaxPendingCaptureCount = 1024
const MaxPendingCaptureBytes = 16 << 20

var ErrCaptureConflict = errors.New("WhatsApp event identity already carries different capture evidence")
var ErrCaptureCapacity = errors.New("WhatsApp incoming capture capacity exhausted")
var ErrCaptureScopeChanged = errors.New("WhatsApp capture belongs to a different original admission, principal or source")

// Scope freezes existing admitted facts. It does not grant execution or
// reconstruct a binding from today's source/principal during recovery.
type Scope struct {
	Kind                channelonboarding.SessionInputScope
	Session             operatorchannel.SessionAccountAdmission
	PublicationBinding  runtimeinbound.BindingGeneration
	Source              channelonboarding.ChannelDurableContextIdentity
	OnboardingOperation string
	OperationRevision   int64
	ActivationID        string
	ActivationRevision  int64
	TargetSelector      string
	PrincipalID         string
	BindingRevision     int64
}

func (s Scope) Validate() error {
	if err := s.Session.Validate(); err != nil {
		return err
	}
	if err := s.Source.Validate(); err != nil {
		return err
	}
	switch s.Kind {
	case channelonboarding.SessionInputOnboarding:
		if s.PublicationBinding != (runtimeinbound.BindingGeneration{}) || s.ActivationRevision != 0 || s.ActivationID != "" {
			return fmt.Errorf("WhatsApp onboarding capture cannot carry business publication authority")
		}
	case channelonboarding.SessionInputBusiness:
		if err := s.PublicationBinding.Validate(); err != nil {
			return err
		}
		if s.BindingRevision < 1 || s.ActivationRevision < 1 || uuid.Validate(s.ActivationID) != nil {
			return fmt.Errorf("WhatsApp business capture requires its confirmed binding")
		}
	default:
		return fmt.Errorf("WhatsApp capture requires explicit onboarding or business scope")
	}
	if s.Session.Provider != "whatsapp" || uuid.Validate(s.OnboardingOperation) != nil ||
		uuid.Validate(s.PrincipalID) != nil || s.BindingRevision < 0 || s.OperationRevision < 1 || s.TargetSelector == "" {
		return fmt.Errorf("WhatsApp capture requires its exact existing onboarding/principal/source scope")
	}
	return nil
}

type Event struct {
	Scope        Scope
	OccurrenceID string
	Conversation string
	EventID      string
	Kind         string
	Body         []byte
	ReceivedAt   time.Time
	Source       Source
}

type Source struct {
	Coordinate        channelonboarding.ChannelRuntimeContextCoordinate
	CatalogGeneration triggergeneration.Generation
}

func (e Event) Validate() error {
	if err := e.Scope.Validate(); err != nil {
		return err
	}
	if e.Source.Coordinate.ValidateContext() != nil || !e.Source.CatalogGeneration.Valid() ||
		!e.Scope.Source.Matches(e.Source.Coordinate.DurableIdentity()) {
		return fmt.Errorf("WhatsApp capture requires its frozen exact source and catalog")
	}
	if e.Scope.Kind == channelonboarding.SessionInputBusiness &&
		(e.Source.Coordinate.Validate() != nil || e.Source.Coordinate.TargetGeneration != uint64(e.Scope.PublicationBinding.Generation)) {
		return fmt.Errorf("WhatsApp business capture contradicts its original target generation")
	}
	if uuid.Validate(e.OccurrenceID) != nil || e.Conversation == "" || e.EventID == "" ||
		(e.Kind != "message" && e.Kind != "edit" && e.Kind != "revoke") {
		return fmt.Errorf("WhatsApp capture requires exact occurrence/conversation/event and supported kind")
	}
	if len(e.Body) == 0 || len(e.Body) > MaxCaptureEventBytes {
		return ErrCaptureCapacity
	}
	if e.ReceivedAt.IsZero() || !e.ReceivedAt.Equal(e.ReceivedAt.Truncate(time.Microsecond)) {
		return fmt.Errorf("WhatsApp capture requires its original microsecond receipt time")
	}
	_, err := canonicaljson.Decode(e.Body)
	return err
}

func (e Event) RequireOriginalScope(current Scope) error {
	if err := current.Validate(); err != nil {
		return err
	}
	if e.Scope != current {
		return ErrCaptureScopeChanged
	}
	return nil
}

func DecodeCapture(raw, digest []byte) (Event, error) {
	var event Event
	if got := sha256.Sum256(raw); !bytes.Equal(got[:], digest) {
		return event, fmt.Errorf("WhatsApp incoming capture digest mismatch")
	}
	if _, err := canonicaljson.Decode(raw); err != nil {
		return event, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil {
		return event, err
	}
	return event, event.Validate()
}

var ErrCaptureMissing = errors.New("WhatsApp original capture is not pending")
var ErrCapturePublicationPending = errors.New("WhatsApp capture has no verified committed publication")

func (e Event) SameCapture(other Event) bool {
	return e.Scope == other.Scope && e.OccurrenceID == other.OccurrenceID && e.Conversation == other.Conversation &&
		e.EventID == other.EventID && e.Kind == other.Kind && e.ReceivedAt.Equal(other.ReceivedAt) &&
		e.Source.Coordinate.MatchesDeclaration(other.Source.Coordinate) && e.Source.CatalogGeneration.Equal(other.Source.CatalogGeneration) && bytes.Equal(e.Body, other.Body)
}

func (e Event) PublicationFingerprint() (string, error) {
	if err := e.Validate(); err != nil {
		return "", err
	}
	return runtimeinbound.SemanticFingerprint(struct {
		Scope                       publicationAuthority
		Conversation, EventID, Kind string
		Body                        []byte
		CatalogGeneration           string
	}{e.Scope.publicationAuthority(), e.Conversation, e.EventID, e.Kind, e.Body, e.Source.CatalogGeneration.Diagnostic()})
}

// Coordinate rebinding advances process revisions, not the stable activation.
// Original revisions remain in capture/provenance and gate unfinished execution.
type publicationAuthority struct {
	Kind                channelonboarding.SessionInputScope
	Session             operatorchannel.SessionAccountAdmission
	PublicationBinding  runtimeinbound.BindingGeneration
	Source              channelonboarding.ChannelDurableContextIdentity
	OnboardingOperation string
	ActivationID        string
	TargetSelector      string
	PrincipalID         string
	BindingRevision     int64
	OperationRevision   int64
	ActivationRevision  int64
}

func (s Scope) publicationAuthority() publicationAuthority {
	result := publicationAuthority{Kind: s.Kind, Session: s.Session, PublicationBinding: s.PublicationBinding,
		Source: s.Source, OnboardingOperation: s.OnboardingOperation, ActivationID: s.ActivationID,
		TargetSelector: s.TargetSelector, PrincipalID: s.PrincipalID, BindingRevision: s.BindingRevision}
	if s.Kind != channelonboarding.SessionInputBusiness {
		result.OperationRevision, result.ActivationRevision = s.OperationRevision, s.ActivationRevision
	}
	return result
}

func (e Event) SameDelivery(other Event) bool {
	return e.Scope.publicationAuthority() == other.Scope.publicationAuthority() && e.Conversation == other.Conversation &&
		e.EventID == other.EventID && e.Kind == other.Kind && e.Source.CatalogGeneration.Equal(other.Source.CatalogGeneration) && bytes.Equal(e.Body, other.Body)
}

const CaptureProvenanceKey = "whatsapp_capture"

// The selected-store request retains original occurrence provenance; only the
// retry-stable content fingerprint excludes that transport occurrence.
func WithCaptureProvenance(event Event, request runtimeinbound.Request) (runtimeinbound.Request, error) {
	if err := event.Validate(); err != nil {
		return runtimeinbound.Request{}, err
	}
	if request.Identity().BindingGeneration() != event.Scope.PublicationBinding {
		return runtimeinbound.Request{}, runtimeinbound.ErrRequestIdentityConflict
	}
	metadata := request.Normalized().OriginalTransportMetadata
	if _, err := canonicaljson.Decode(metadata); err != nil {
		return runtimeinbound.Request{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(metadata, &fields); err != nil || fields == nil {
		return runtimeinbound.Request{}, fmt.Errorf("WhatsApp publication requires object transport metadata")
	}
	if _, exists := fields[CaptureProvenanceKey]; exists {
		return runtimeinbound.Request{}, runtimeinbound.ErrRequestIdentityConflict
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return runtimeinbound.Request{}, err
	}
	fields[CaptureProvenanceKey] = raw
	request.OriginalTransportMetadata, err = json.Marshal(fields)
	return request, err
}

func PublicationCaptureProvenance(request runtimeinbound.Request) (Event, error) {
	var event Event
	if _, err := canonicaljson.Decode(request.OriginalTransportMetadata); err != nil {
		return event, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(request.OriginalTransportMetadata, &fields); err != nil {
		return event, err
	}
	raw, ok := fields[CaptureProvenanceKey]
	if !ok {
		return event, fmt.Errorf("WhatsApp publication has no original capture evidence")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil {
		return event, err
	}
	return event, event.Validate()
}

func PublicationRequestBytes(request runtimeinbound.Request) ([]byte, error) {
	request = request.Normalized()
	if err := request.Validate(); err != nil {
		return nil, err
	}
	// Both selected stores retain timestamps at microsecond precision. Refuse an
	// unrepresentable input instead of silently changing its frozen evidence.
	if !request.OriginalReceivedAt.Equal(request.OriginalReceivedAt.Truncate(time.Microsecond)) {
		return nil, fmt.Errorf("WhatsApp publication receipt time must have microsecond precision")
	}
	return request.CanonicalBytes()
}

func ValidateCapturePublication(event Event, request runtimeinbound.Request) error {
	identity, err := event.PublicationIdentity()
	if err != nil {
		return err
	}
	fingerprint, err := event.PublicationFingerprint()
	if err != nil {
		return err
	}
	publicationID, markerID, err := runtimeinbound.DeterministicIDs(identity)
	if err != nil {
		return err
	}
	if request.Identity() != identity || request.RequestFingerprint != fingerprint ||
		request.PublicationID != publicationID || request.MarkerEventID != markerID || !request.OriginalReceivedAt.Equal(event.ReceivedAt) {
		return runtimeinbound.ErrRequestIdentityConflict
	}
	original, err := PublicationCaptureProvenance(request)
	if err != nil {
		return fmt.Errorf("%w: %w", runtimeinbound.ErrRequestIdentityConflict, err)
	}
	if !original.SameCapture(event) {
		return runtimeinbound.ErrRequestIdentityConflict
	}
	return nil
}

func DecodePublicationRequest(event Event, raw, digest []byte) (runtimeinbound.Request, error) {
	var request runtimeinbound.Request
	if got := sha256.Sum256(raw); len(raw) == 0 || !bytes.Equal(got[:], digest) {
		return request, fmt.Errorf("WhatsApp capture publication digest mismatch")
	}
	if _, err := canonicaljson.Decode(raw); err != nil {
		return request, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return request, err
	}
	canonical, err := PublicationRequestBytes(request)
	if err != nil {
		return request, err
	}
	if !bytes.Equal(canonical, raw) {
		return request, fmt.Errorf("WhatsApp capture publication request is not canonical")
	}
	return request, ValidateCapturePublication(event, request)
}

// Staging freezes the request selected by the existing runtime admission owner;
// it does not grant account, target, standing or normalized-output authority.
func VerifyHistoricalCapture(event Event, record runtimeinbound.Record) ([]byte, error) {
	if err := event.Validate(); err != nil {
		return nil, err
	}
	if record.State != "committed" || record.CommittedAt.IsZero() {
		return nil, ErrCapturePublicationPending
	}
	original, err := PublicationCaptureProvenance(record.Request)
	if err != nil {
		return nil, err
	}
	if err := ValidateCapturePublication(original, record.Request); err != nil {
		return nil, err
	}
	if !original.SameDelivery(event) {
		return nil, runtimeinbound.ErrRequestIdentityConflict
	}
	return PublicationRequestBytes(record.Request)
}

// Match the existing capture key dimensions without introducing another hash
// or UUID owner. Runtime's canonical publication identity owner consumes this.
func (event Event) PublicationProviderEventID() (string, error) {
	if err := event.Validate(); err != nil {
		return "", err
	}
	key, err := json.Marshal([]string{event.Scope.Session.ConnectionID, event.Scope.Session.AccountRef,
		event.Conversation, event.EventID, event.Kind})
	return string(key), err
}

func (event Event) PublicationIdentity() (runtimeinbound.Identity, error) {
	key, err := event.PublicationProviderEventID()
	if err != nil {
		return runtimeinbound.Identity{}, err
	}
	identity := event.Scope.PublicationBinding.Identity("whatsapp", key)
	return identity, identity.Validate()
}

type CallbackFailure struct{ ConnectionID, OccurrenceID, Reason string }

type PendingCapture struct {
	Sequence     int64
	Event        Event
	Request      *runtimeinbound.Request
	RequestBytes []byte
}
