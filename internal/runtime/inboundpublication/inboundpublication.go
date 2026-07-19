package inboundpublication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/google/uuid"
)

const RequestSemanticProjectionVersion = "inbound-request-semantic-v1"

var ErrRequestIdentityConflict = errors.New("inbound request identity conflict")

type AcknowledgementMode string

const (
	AcknowledgementAfterPublish          AcknowledgementMode = "after_publish"
	AcknowledgementDurableBeforeDispatch AcknowledgementMode = "durable_before_dispatch"
)

var publicationNamespace = uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm-inbound-publication"))

type Request struct {
	PublicationID               string
	Provider                    string
	EntityID                    string
	ProviderEventID             string
	RequestFingerprint          string
	RequestProjectionVersion    string
	StableServiceID             string
	PackageKey                  string
	FlowID                      string
	InstanceID                  string
	TargetAlias                 string
	TargetFlowInstance          string
	ExpectedPublicationSequence int64
	ResolvedRunID               string
	MarkerEventID               string
	AcknowledgementMode         AcknowledgementMode
	OriginalReceivedAt          time.Time
	OriginalUserAgent           string
	OriginalTransportMetadata   json.RawMessage
}

func (r Request) Normalized() Request {
	r.PublicationID = strings.TrimSpace(r.PublicationID)
	r.Provider = strings.ToLower(strings.TrimSpace(r.Provider))
	r.EntityID = strings.TrimSpace(r.EntityID)
	r.ProviderEventID = strings.TrimSpace(r.ProviderEventID)
	r.RequestFingerprint = strings.ToLower(strings.TrimSpace(r.RequestFingerprint))
	r.RequestProjectionVersion = strings.TrimSpace(r.RequestProjectionVersion)
	if r.RequestProjectionVersion == "" {
		r.RequestProjectionVersion = RequestSemanticProjectionVersion
	}
	r.StableServiceID = strings.TrimSpace(r.StableServiceID)
	r.PackageKey = strings.TrimSpace(r.PackageKey)
	r.FlowID = strings.TrimSpace(r.FlowID)
	r.InstanceID = strings.TrimSpace(r.InstanceID)
	r.TargetAlias = strings.Trim(strings.TrimSpace(r.TargetAlias), "/")
	r.TargetFlowInstance = strings.Trim(strings.TrimSpace(r.TargetFlowInstance), "/")
	r.ResolvedRunID = strings.TrimSpace(r.ResolvedRunID)
	r.MarkerEventID = strings.TrimSpace(r.MarkerEventID)
	r.AcknowledgementMode = AcknowledgementMode(strings.TrimSpace(string(r.AcknowledgementMode)))
	r.OriginalReceivedAt = r.OriginalReceivedAt.UTC()
	r.OriginalUserAgent = strings.TrimSpace(r.OriginalUserAgent)
	if len(r.OriginalTransportMetadata) == 0 {
		r.OriginalTransportMetadata = json.RawMessage(`{}`)
	}
	return r
}

func (r Request) Validate() error {
	r = r.Normalized()
	required := map[string]string{
		"publication_id":             r.PublicationID,
		"provider":                   r.Provider,
		"entity_id":                  r.EntityID,
		"provider_event_id":          r.ProviderEventID,
		"request_fingerprint":        r.RequestFingerprint,
		"stable_service_id":          r.StableServiceID,
		"package_key":                r.PackageKey,
		"flow_id":                    r.FlowID,
		"instance_id":                r.InstanceID,
		"target_alias":               r.TargetAlias,
		"target_flow_instance":       r.TargetFlowInstance,
		"resolved_run_id":            r.ResolvedRunID,
		"marker_event_id":            r.MarkerEventID,
		"request_projection_version": r.RequestProjectionVersion,
	}
	for field, value := range required {
		if value == "" {
			return fmt.Errorf("%s is required", field)
		}
	}
	for field, value := range map[string]string{
		"publication_id":    r.PublicationID,
		"entity_id":         r.EntityID,
		"stable_service_id": r.StableServiceID,
		"resolved_run_id":   r.ResolvedRunID,
		"marker_event_id":   r.MarkerEventID,
	} {
		if _, err := uuid.Parse(value); err != nil {
			return fmt.Errorf("%s must be a UUID: %w", field, err)
		}
	}
	if r.RequestProjectionVersion != RequestSemanticProjectionVersion {
		return fmt.Errorf("request_projection_version must be %s", RequestSemanticProjectionVersion)
	}
	if err := validateSHA256("request_fingerprint", r.RequestFingerprint); err != nil {
		return err
	}
	if r.ExpectedPublicationSequence < 0 {
		return fmt.Errorf("expected_publication_sequence must be nonnegative")
	}
	switch r.AcknowledgementMode {
	case AcknowledgementAfterPublish, AcknowledgementDurableBeforeDispatch:
	default:
		return fmt.Errorf("unsupported acknowledgement_mode %q", r.AcknowledgementMode)
	}
	if r.OriginalReceivedAt.IsZero() {
		return fmt.Errorf("original_received_at is required")
	}
	var metadata map[string]any
	if err := json.Unmarshal(r.OriginalTransportMetadata, &metadata); err != nil {
		return fmt.Errorf("original_transport_metadata must be a JSON object: %w", err)
	}
	if metadata == nil {
		return fmt.Errorf("original_transport_metadata must be a JSON object")
	}
	return nil
}

type EventFinalization struct {
	Ordinal           int
	Event             events.Event
	Kind              runtimeprovideroutput.Kind
	Authorization     runtimeprovideroutput.Authorization
	RecipientManifest json.RawMessage
}

type Finalization struct {
	EvidenceEvent events.Event
	Events        []EventFinalization
}

type Mutation interface {
	Context() context.Context
	FinalizeInboundPublication(ctx context.Context, finalization Finalization) error
}

type EventRecord struct {
	Ordinal                      int
	EventID                      string
	EventName                    string
	Kind                         runtimeprovideroutput.Kind
	Authorization                runtimeprovideroutput.Authorization
	EventIntegrityFingerprint    string
	RecipientManifestFingerprint string
	RecipientCount               int
	Event                        events.Event
}

type Record struct {
	Request
	State       string
	OutputCount int
	CreatedAt   time.Time
	CommittedAt time.Time
	Events      []EventRecord
	Created     bool
}

type EvidencePayload struct {
	PublicationID   string   `json:"publication_id"`
	Provider        string   `json:"provider"`
	ProviderEventID string   `json:"provider_event_id"`
	EntityID        string   `json:"entity_id"`
	EventIDs        []string `json:"event_ids"`
	EventNames      []string `json:"event_names"`
	OutputCount     int      `json:"output_count"`
}

func (r Record) EventIDs() []string {
	ids := make([]string, len(r.Events))
	for i := range r.Events {
		ids[i] = strings.TrimSpace(r.Events[i].EventID)
	}
	return ids
}

func (r Record) EventNames() []string {
	names := make([]string, len(r.Events))
	for i := range r.Events {
		names[i] = strings.TrimSpace(r.Events[i].EventName)
	}
	return names
}

func BuildEvidencePayload(request Request, eventIDs, eventNames []string) (json.RawMessage, error) {
	request = request.Normalized()
	if len(eventIDs) < 1 || len(eventIDs) > 2 || len(eventNames) != len(eventIDs) {
		return nil, fmt.Errorf("inbound evidence requires one or two ordered event ids and names")
	}
	ids := make([]string, len(eventIDs))
	names := make([]string, len(eventNames))
	for index := range eventIDs {
		ids[index] = strings.TrimSpace(eventIDs[index])
		names[index] = strings.TrimSpace(eventNames[index])
		expectedID, err := DeterministicEventID(request.PublicationID, index)
		if err != nil {
			return nil, err
		}
		if ids[index] != expectedID {
			return nil, fmt.Errorf("inbound evidence event ordinal %d does not use its reserved event_id", index)
		}
		if names[index] == "" {
			return nil, fmt.Errorf("inbound evidence event ordinal %d name is required", index)
		}
	}
	payload, err := json.Marshal(EvidencePayload{
		PublicationID: request.PublicationID, Provider: request.Provider,
		ProviderEventID: request.ProviderEventID, EntityID: request.EntityID,
		EventIDs: ids, EventNames: names, OutputCount: len(ids),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal inbound evidence payload: %w", err)
	}
	return json.RawMessage(payload), nil
}

func ValidateEvidenceEvent(request Request, evidence events.Event, eventIDs, eventNames []string) error {
	request = request.Normalized()
	expectedPayload, err := BuildEvidencePayload(request, eventIDs, eventNames)
	if err != nil {
		return err
	}
	if strings.TrimSpace(evidence.ID()) != request.MarkerEventID {
		return fmt.Errorf("inbound evidence event_id does not match reserved marker_event_id")
	}
	if strings.TrimSpace(evidence.RunID()) != request.ResolvedRunID {
		return fmt.Errorf("inbound evidence event must use the admitted resolved_run_id")
	}
	if evidence.Type() != events.EventTypePlatformInboundRecord {
		return fmt.Errorf("inbound evidence event must be platform.inbound_recorded")
	}
	if strings.TrimSpace(evidence.Envelope().EntityID) != request.EntityID {
		return fmt.Errorf("inbound evidence event must use the admitted entity_id")
	}

	var expected, actual EvidencePayload
	if err := json.Unmarshal(expectedPayload, &expected); err != nil {
		return fmt.Errorf("decode canonical inbound evidence payload: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(evidence.Payload()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&actual); err != nil {
		return fmt.Errorf("decode inbound evidence payload: %w", err)
	}
	if err := ensureEvidencePayloadEOF(decoder); err != nil {
		return err
	}
	if actual.PublicationID != expected.PublicationID || actual.Provider != expected.Provider ||
		actual.ProviderEventID != expected.ProviderEventID || actual.EntityID != expected.EntityID ||
		actual.OutputCount != expected.OutputCount || !slices.Equal(actual.EventIDs, expected.EventIDs) ||
		!slices.Equal(actual.EventNames, expected.EventNames) {
		return fmt.Errorf("inbound evidence payload does not match the committed ordered event batch")
	}
	return nil
}

func ensureEvidencePayloadEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("inbound evidence payload contains trailing JSON")
		}
		return fmt.Errorf("decode inbound evidence payload trailing JSON: %w", err)
	}
	return nil
}

type Runner interface {
	RunInboundPublicationMutation(ctx context.Context, request Request, fn func(Mutation) error) (Record, error)
	LoadInboundPublicationByIdentity(ctx context.Context, provider, entityID, providerEventID string) (Record, bool, error)
	ValidateInboundPublicationIntegrity(ctx context.Context) error
}

func DeterministicIDs(provider, entityID, providerEventID string) (publicationID, markerEventID string) {
	identity := strings.Join([]string{
		strings.ToLower(strings.TrimSpace(provider)),
		strings.TrimSpace(entityID),
		strings.TrimSpace(providerEventID),
	}, "\x00")
	publication := uuid.NewSHA1(publicationNamespace, []byte(identity))
	return publication.String(), uuid.NewSHA1(publication, []byte("evidence")).String()
}

func DeterministicEventID(publicationID string, ordinal int) (string, error) {
	publication, err := uuid.Parse(strings.TrimSpace(publicationID))
	if err != nil {
		return "", fmt.Errorf("publication_id must be a UUID: %w", err)
	}
	if ordinal < 0 {
		return "", fmt.Errorf("event ordinal must be nonnegative")
	}
	return uuid.NewSHA1(publication, []byte(fmt.Sprintf("event:%d", ordinal))).String(), nil
}

func SemanticFingerprint(projection any) (string, error) {
	encoded, err := json.Marshal(projection)
	if err != nil {
		return "", fmt.Errorf("marshal inbound semantic projection: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func EventIntegrityFingerprint(evt events.Event, kind runtimeprovideroutput.Kind, authorization runtimeprovideroutput.Authorization) (string, error) {
	var payload any
	if len(evt.Payload()) > 0 {
		decoder := json.NewDecoder(strings.NewReader(string(evt.Payload())))
		decoder.UseNumber()
		if err := decoder.Decode(&payload); err != nil {
			return "", fmt.Errorf("decode event payload for integrity: %w", err)
		}
	}
	envelope := evt.Envelope()
	projection := struct {
		ID            string `json:"id"`
		Type          string `json:"type"`
		SourceAgent   string `json:"source_agent"`
		TaskID        string `json:"task_id"`
		Payload       any    `json:"payload"`
		ChainDepth    int    `json:"chain_depth"`
		RunID         string `json:"run_id"`
		ParentEventID string `json:"parent_event_id"`
		Envelope      struct {
			EntityID     string                 `json:"entity_id"`
			FlowInstance string                 `json:"flow_instance"`
			Scope        events.EventScope      `json:"scope"`
			Source       events.RouteIdentity   `json:"source"`
			Target       events.RouteIdentity   `json:"target"`
			TargetSet    []events.RouteIdentity `json:"target_set"`
		} `json:"envelope"`
		Kind          runtimeprovideroutput.Kind          `json:"kind"`
		Authorization runtimeprovideroutput.Authorization `json:"authorization"`
	}{
		ID: evt.ID(), Type: string(evt.Type()), SourceAgent: evt.SourceAgent(), TaskID: evt.TaskID(),
		Payload: payload, ChainDepth: evt.ChainDepth(), RunID: evt.RunID(), ParentEventID: evt.ParentEventID(),
		Kind: kind, Authorization: authorization.Normalized(),
	}
	projection.Envelope.EntityID = envelope.EntityID
	projection.Envelope.FlowInstance = envelope.FlowInstance
	projection.Envelope.Scope = envelope.Scope
	projection.Envelope.Source = envelope.Source
	projection.Envelope.Target = envelope.Target
	projection.Envelope.TargetSet = envelope.TargetSet
	return SemanticFingerprint(projection)
}

func CanonicalRecipientManifest(routes []events.DeliveryRoute) (json.RawMessage, string, int, error) {
	routes = events.NormalizeDeliveryRoutes(routes)
	if routes == nil {
		routes = []events.DeliveryRoute{}
	}
	slices.SortFunc(routes, func(left, right events.DeliveryRoute) int {
		left = left.Normalized()
		right = right.Normalized()
		leftKey := strings.Join([]string{
			left.SubscriberType,
			left.SubscriberID,
			left.Target.FlowID,
			left.Target.FlowInstance,
			left.Target.EntityID,
			left.Context.ReplyContextID(),
		}, "\x00")
		rightKey := strings.Join([]string{
			right.SubscriberType,
			right.SubscriberID,
			right.Target.FlowID,
			right.Target.FlowInstance,
			right.Target.EntityID,
			right.Context.ReplyContextID(),
		}, "\x00")
		return strings.Compare(leftKey, rightKey)
	})
	encoded, err := json.Marshal(routes)
	if err != nil {
		return nil, "", 0, fmt.Errorf("marshal inbound recipient manifest: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return json.RawMessage(encoded), hex.EncodeToString(sum[:]), len(routes), nil
}

func validateSHA256(field, value string) error {
	if len(value) != sha256.Size*2 {
		return fmt.Errorf("%s must be a SHA-256 hex digest", field)
	}
	if _, err := hex.DecodeString(value); err != nil {
		return fmt.Errorf("%s must be hex: %w", field, err)
	}
	return nil
}
