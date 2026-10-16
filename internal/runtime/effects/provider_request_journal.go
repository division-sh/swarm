package effects

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

const ProviderRequestJournalVersion = "provider-call-resume.v1"

// ProviderRequestJournal records inputs, not execution authority. The native
// admission owner must bind it to the exact attempt, origin, frame and surface.
type ProviderRequestJournal struct {
	Version                  string            `json:"version"`
	Adapter                  string            `json:"adapter"`
	FrameID                  string            `json:"frame_id"`
	FrameContentHash         string            `json:"frame_content_hash"`
	SurfaceID                string            `json:"surface_id"`
	SurfaceFingerprint       string            `json:"surface_fingerprint"`
	SessionID                string            `json:"session_id"`
	TurnOrdinal              int               `json:"turn_ordinal"`
	LogicalOperationIdentity string            `json:"logical_operation_identity"`
	Memory                   *agentmemory.Plan `json:"memory"`
	Method                   string            `json:"method"`
	Destination              string            `json:"destination"`
	SemanticHeaders          map[string]string `json:"semantic_headers"`
	// Binary encoding preserves rendering evidence through JSONB normalization.
	Rendering          []byte `json:"rendering"`
	BodyFingerprint    string `json:"body_fingerprint"`
	RequestFingerprint string `json:"request_fingerprint"`
}

func NewProviderRequestJournal(input ProviderRequestJournal, body []byte) (ProviderRequestJournal, error) {
	if input.BodyFingerprint != "" || input.RequestFingerprint != "" {
		return ProviderRequestJournal{}, fmt.Errorf("provider request journal fingerprints must be derived from the physical request")
	}
	input.Rendering = bytes.Clone(input.Rendering)
	input.SemanticHeaders = maps.Clone(input.SemanticHeaders)
	if input.Memory != nil {
		memory := *input.Memory
		input.Memory = &memory
	}
	input.BodyFingerprint = Fingerprint(body)
	if err := input.validate(); err != nil {
		return ProviderRequestJournal{}, err
	}
	fingerprint, err := input.physicalRequestFingerprint()
	if err != nil {
		return ProviderRequestJournal{}, err
	}
	input.RequestFingerprint = fingerprint
	return input, nil
}

// ValidateRequest compares an independently rendered body with both immutable
// pins. It must not be used to replace the original operation fingerprint.
func (j ProviderRequestJournal) ValidateRequest(body []byte, operationFingerprint string) error {
	if err := j.validate(); err != nil {
		return err
	}
	if operationFingerprint == "" || j.BodyFingerprint != operationFingerprint || Fingerprint(body) != operationFingerprint {
		return fmt.Errorf("provider request journal body differs from the admitted operation")
	}
	fingerprint, err := j.physicalRequestFingerprint()
	if err != nil {
		return err
	}
	if j.RequestFingerprint != fingerprint {
		return fmt.Errorf("provider request journal physical request differs from its admitted fingerprint")
	}
	return nil
}

func EncodeProviderRequestJournal(journal ProviderRequestJournal) ([]byte, error) {
	if err := journal.validateSealed(); err != nil {
		return nil, err
	}
	return json.Marshal(journal)
}

func DecodeProviderRequestJournal(raw []byte) (ProviderRequestJournal, error) {
	if _, err := canonicaljson.Decode(raw); err != nil {
		return ProviderRequestJournal{}, fmt.Errorf("invalid provider request journal JSON: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var journal ProviderRequestJournal
	if err := decoder.Decode(&journal); err != nil {
		return ProviderRequestJournal{}, fmt.Errorf("decode provider request journal: %w", err)
	}
	if err := journal.validateSealed(); err != nil {
		return ProviderRequestJournal{}, err
	}
	return journal, nil
}

func (j ProviderRequestJournal) validateSealed() error {
	if err := j.validate(); err != nil {
		return err
	}
	fingerprint, err := j.physicalRequestFingerprint()
	if err != nil {
		return err
	}
	if j.RequestFingerprint != fingerprint {
		return fmt.Errorf("provider request journal physical fingerprint is missing or contradictory")
	}
	return nil
}

func (j ProviderRequestJournal) validate() error {
	if j.Version != ProviderRequestJournalVersion {
		return fmt.Errorf("unsupported provider request journal version")
	}
	for _, value := range []string{j.FrameID, j.FrameContentHash, j.SurfaceID, j.SurfaceFingerprint, j.SessionID, j.LogicalOperationIdentity} {
		if value == "" || value != strings.TrimSpace(value) {
			return fmt.Errorf("provider request journal requires exact frame, surface, session and logical operation coordinates")
		}
	}
	if j.TurnOrdinal < 1 || j.Method != http.MethodPost || j.Memory == nil {
		return fmt.Errorf("provider request journal requires a positive turn, memory plan and POST request")
	}
	// These adapters have no credential-free query contract. Never persist a
	// URL component that could carry credentials or silently strip it on replay.
	if !ProviderRequestJournalSupportsDestination(j.Destination) {
		return fmt.Errorf("provider request journal requires an exact credential-free HTTP destination")
	}
	if j.SemanticHeaders["content-type"] != "application/json" {
		return fmt.Errorf("provider request journal requires the JSON content type")
	}
	wantHeaders := 1
	switch j.Adapter {
	case "anthropic_api":
		wantHeaders = 2
		if j.SemanticHeaders["anthropic-version"] != "2023-06-01" {
			return fmt.Errorf("provider request journal requires the admitted Anthropic version header")
		}
	case "openai_compatible", "openai_responses":
	default:
		return fmt.Errorf("provider request journal adapter is not an HTTP provider")
	}
	if len(j.SemanticHeaders) != wantHeaders {
		return fmt.Errorf("provider request journal contains unsupported or credential-bearing headers")
	}
	if _, err := canonicaljson.Decode(j.Rendering); err != nil {
		return fmt.Errorf("provider request journal rendering inputs are missing or malformed: %w", err)
	}
	decoded, err := hex.DecodeString(j.BodyFingerprint)
	if err != nil || len(decoded) != 32 || j.BodyFingerprint != strings.ToLower(j.BodyFingerprint) {
		return fmt.Errorf("provider request journal requires the original body fingerprint")
	}
	return nil
}

// ProviderRequestJournalSupportsDestination describes the credential-free
// journal format, not whether the original provider request may be dispatched.
func ProviderRequestJournalSupportsDestination(raw string) bool {
	destination, err := url.Parse(raw)
	return err == nil && (destination.Scheme == "http" || destination.Scheme == "https") && destination.Host != "" &&
		destination.User == nil && destination.RawQuery == "" && !destination.ForceQuery && destination.Fragment == "" &&
		destination.Opaque == "" && raw == strings.TrimSpace(raw)
}

func (j ProviderRequestJournal) physicalRequestFingerprint() (string, error) {
	raw, err := json.Marshal(struct {
		Version         string            `json:"version"`
		Method          string            `json:"method"`
		Destination     string            `json:"destination"`
		SemanticHeaders map[string]string `json:"semantic_headers"`
		BodyFingerprint string            `json:"body_fingerprint"`
	}{j.Version, j.Method, j.Destination, j.SemanticHeaders, j.BodyFingerprint})
	if err != nil {
		return "", err
	}
	return Fingerprint(raw), nil
}

// ValidateProviderRequestJournalAdmission binds a journal to this original
// admission. The store must independently render and persist it before ACK.
func ValidateProviderRequestJournalAdmission(ctx context.Context, authority Authority, request AuthorizeRequest) error {
	if request.ProviderRequestJournal == nil {
		return nil
	}
	journal := request.ProviderRequestJournal
	if err := journal.validateSealed(); err != nil {
		return err
	}
	registration, registered := RegistrationFor(request.Adapter)
	if !registered || registration.Kind != KindProviderTurn || registration.Transport != "http" ||
		request.Kind != registration.Kind || request.Class != registration.Class || request.Transport != registration.Transport ||
		authority.Target.Kind != UsageTargetAgentTurn || request.AgentFrame == nil || request.CapabilitySurface == nil {
		return fmt.Errorf("provider request journal requires its managed HTTP provider admission")
	}
	if authority.Kind != AuthorityNormalAgent && authority.Kind != AuthoritySelectedContractFork {
		return fmt.Errorf("provider request journal has a different execution owner")
	}
	fingerprint, err := request.CapabilitySurface.ContinuationFingerprint()
	if err != nil {
		return err
	}
	if journal.Adapter != request.Adapter || journal.FrameID != request.AgentFrame.FrameID ||
		journal.FrameContentHash != request.AgentFrame.ContentHash || journal.SurfaceID != request.CapabilitySurface.ID ||
		journal.SurfaceFingerprint != fingerprint || journal.SessionID != authority.Target.SessionID ||
		journal.TurnOrdinal != request.CapabilitySurface.Authority.TurnOrdinal || *journal.Memory != authority.Target.Memory ||
		journal.BodyFingerprint != request.RequestFingerprint || journal.LogicalOperationIdentity != logicalOperationIdentity(ctx) {
		return fmt.Errorf("provider request journal differs from its exact admitted request or execution coordinates")
	}
	operationID, err := canonicalOperationID(ctx, authority, request.Adapter, request.Lineage)
	if err != nil {
		return err
	}
	if operationID != request.OperationID {
		return fmt.Errorf("provider request journal belongs to a different logical operation")
	}
	return nil
}
