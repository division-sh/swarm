package inboundpublication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/google/uuid"
)

const SemanticProjectionVersion = "inbound-semantic-v1"

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
	SemanticFingerprint         string
	SemanticProjectionVersion   string
	StableServiceID             string
	PackageKey                  string
	FlowID                      string
	InstanceID                  string
	TargetAlias                 string
	TargetFlowInstance          string
	ExpectedPublicationSequence int64
	ResolvedRunID               string
	MarkerEventID               string
	PublicationEventID          string
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
	r.SemanticFingerprint = strings.ToLower(strings.TrimSpace(r.SemanticFingerprint))
	r.SemanticProjectionVersion = strings.TrimSpace(r.SemanticProjectionVersion)
	if r.SemanticProjectionVersion == "" {
		r.SemanticProjectionVersion = SemanticProjectionVersion
	}
	r.StableServiceID = strings.TrimSpace(r.StableServiceID)
	r.PackageKey = strings.TrimSpace(r.PackageKey)
	r.FlowID = strings.TrimSpace(r.FlowID)
	r.InstanceID = strings.TrimSpace(r.InstanceID)
	r.TargetAlias = strings.Trim(strings.TrimSpace(r.TargetAlias), "/")
	r.TargetFlowInstance = strings.Trim(strings.TrimSpace(r.TargetFlowInstance), "/")
	r.ResolvedRunID = strings.TrimSpace(r.ResolvedRunID)
	r.MarkerEventID = strings.TrimSpace(r.MarkerEventID)
	r.PublicationEventID = strings.TrimSpace(r.PublicationEventID)
	rAcknowledgementMode := AcknowledgementMode(strings.TrimSpace(string(r.AcknowledgementMode)))
	r.AcknowledgementMode = rAcknowledgementMode
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
		"publication_id":       r.PublicationID,
		"provider":             r.Provider,
		"entity_id":            r.EntityID,
		"provider_event_id":    r.ProviderEventID,
		"semantic_fingerprint": r.SemanticFingerprint,
		"stable_service_id":    r.StableServiceID,
		"package_key":          r.PackageKey,
		"flow_id":              r.FlowID,
		"instance_id":          r.InstanceID,
		"target_alias":         r.TargetAlias,
		"target_flow_instance": r.TargetFlowInstance,
		"resolved_run_id":      r.ResolvedRunID,
		"marker_event_id":      r.MarkerEventID,
		"publication_event_id": r.PublicationEventID,
	}
	for field, value := range required {
		if value == "" {
			return fmt.Errorf("%s is required", field)
		}
	}
	for field, value := range map[string]string{
		"publication_id":       r.PublicationID,
		"entity_id":            r.EntityID,
		"stable_service_id":    r.StableServiceID,
		"resolved_run_id":      r.ResolvedRunID,
		"marker_event_id":      r.MarkerEventID,
		"publication_event_id": r.PublicationEventID,
	} {
		if _, err := uuid.Parse(value); err != nil {
			return fmt.Errorf("%s must be a UUID: %w", field, err)
		}
	}
	if r.SemanticProjectionVersion != SemanticProjectionVersion {
		return fmt.Errorf("semantic_projection_version must be %s", SemanticProjectionVersion)
	}
	if len(r.SemanticFingerprint) != sha256.Size*2 {
		return fmt.Errorf("semantic_fingerprint must be a SHA-256 hex digest")
	}
	if _, err := hex.DecodeString(r.SemanticFingerprint); err != nil {
		return fmt.Errorf("semantic_fingerprint must be hex: %w", err)
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
	return nil
}

type Finalization struct {
	EvidenceEvent     events.Event
	PublicationEvent  events.Event
	RecipientManifest json.RawMessage
}

type Mutation interface {
	runtimebus.EventMutation
	FinalizeInboundPublication(ctx context.Context, finalization Finalization) error
}

type Record struct {
	Request
	State                string
	RecipientManifest    json.RawMessage
	RecipientFingerprint string
	RecipientCount       int
	CreatedAt            time.Time
	CommittedAt          time.Time
	PublicationEvent     events.Event
	Created              bool
}

type Runner interface {
	RunInboundPublicationMutation(ctx context.Context, request Request, fn func(Mutation) error) (Record, error)
	LoadInboundPublicationByIdentity(ctx context.Context, provider, entityID, providerEventID string) (Record, bool, error)
	ValidateInboundPublicationIntegrity(ctx context.Context) error
}

func DeterministicIDs(provider, entityID, providerEventID string) (publicationID, markerEventID, publicationEventID string) {
	identity := strings.Join([]string{
		strings.ToLower(strings.TrimSpace(provider)),
		strings.TrimSpace(entityID),
		strings.TrimSpace(providerEventID),
	}, "\x00")
	publication := uuid.NewSHA1(publicationNamespace, []byte(identity))
	return publication.String(),
		uuid.NewSHA1(publication, []byte("evidence")).String(),
		uuid.NewSHA1(publication, []byte("publication")).String()
}

func SemanticFingerprint(projection any) (string, error) {
	encoded, err := json.Marshal(projection)
	if err != nil {
		return "", fmt.Errorf("marshal inbound semantic projection: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func CanonicalRecipientManifest(routes []events.DeliveryRoute) (json.RawMessage, string, int, error) {
	routes = events.NormalizeDeliveryRoutes(routes)
	if routes == nil {
		routes = []events.DeliveryRoute{}
	}
	encoded, err := json.Marshal(routes)
	if err != nil {
		return nil, "", 0, fmt.Errorf("marshal inbound recipient manifest: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return json.RawMessage(encoded), hex.EncodeToString(sum[:]), len(routes), nil
}
