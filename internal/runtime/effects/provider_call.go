package effects

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

const ProviderCallJournalVersion = "provider-call/v1"

// ProviderCall freezes adapter input, not permission to execute it. The native
// owner binds it to the physical attempt before the provider can be invoked.
type ProviderCall struct {
	SessionID            string                     `json:"session_id"`
	TurnCount            int                        `json:"turn_count"`
	Memory               agentmemory.Plan           `json:"memory"`
	Messages             []byte                     `json:"messages"`
	ProviderContract     []byte                     `json:"provider_contract"`
	Request              ProviderCallRequest        `json:"request"`
	TurnTimeout          *timeridentity.TurnTimeout `json:"turn_timeout,omitempty"`
	PredecessorAttemptID string                     `json:"predecessor_attempt_id,omitempty"`
}

type ProviderCallRequest struct {
	Method      string            `json:"method"`
	Destination string            `json:"destination"`
	Headers     map[string]string `json:"headers"`
	Body        []byte            `json:"body"`
}

type ProviderCallJournal struct {
	Version       string          `json:"version"`
	OperationID   string          `json:"operation_id"`
	AttemptID     string          `json:"attempt_id"`
	Adapter       string          `json:"adapter"`
	TargetKind    UsageTargetKind `json:"target_kind"`
	TargetID      string          `json:"target_id"`
	TargetOrdinal int             `json:"target_ordinal"`
	SourceHash    string          `json:"source_hash"`
	FrameID       string          `json:"frame_id"`
	SurfaceID     string          `json:"surface_id"`
	SurfaceHash   string          `json:"surface_hash"`
	Call          ProviderCall    `json:"call"`
}

// ProviderCallRecovery carries old input and provenance only. A replacement
// requires a fresh frame, original grant and acknowledged native authorization.
type ProviderCallRecovery struct {
	Journal ProviderCallJournal
	Frame   agentframe.Frame
	Parent  *CompletionContinuationSnapshot
}

type ProviderCallResumeStore interface {
	LoadProviderCallResume(context.Context, CompletionContinuationRequest) (ProviderCallRecovery, bool, error)
}

func (c *Controller) LoadProviderCallResume(ctx context.Context, sessionID string, memory agentmemory.Plan) (ProviderCallRecovery, bool, error) {
	claim, ok := deliverylifecycle.ClaimFromContext(ctx)
	if !ok || claim.Version() <= 1 {
		return ProviderCallRecovery{}, false, nil
	}
	authority, ok := completionAuthorityFromContext(ctx)
	if !ok || c == nil {
		return ProviderCallRecovery{}, false, fmt.Errorf("provider resume requires current completion authority")
	}
	store, ok := c.completionStore.(ProviderCallResumeStore)
	if !ok {
		return ProviderCallRecovery{}, false, fmt.Errorf("provider resume owner is not bound")
	}
	admission, ok := managedexecution.FromContext(ctx)
	if !ok {
		return ProviderCallRecovery{}, false, fmt.Errorf("provider resume requires current managed execution")
	}
	origin, err := DeliveryCompletionOrigin(claim)
	if err != nil {
		return ProviderCallRecovery{}, false, err
	}
	return store.LoadProviderCallResume(ctx, CompletionContinuationRequest{Authority: authority, Origin: origin,
		ExecutionAuthorityID: admission.ExecutionAuthorityID, SessionID: sessionID, Memory: memory})
}

func BeginManagedProviderCall(ctx context.Context, adapter string, frame agentframe.Frame, call ProviderCall) (*Handle, error) {
	call.TurnTimeout = turnTimeoutFromContext(ctx)
	return beginCompletion(ctx, adapter, call.Request.Body, &frame, nil, &call)
}

func (r ProviderCallRequest) Fingerprint() (string, error) {
	if err := r.validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	return ProviderCallJournalVersion + ":" + Fingerprint(raw), nil
}

func (r ProviderCallRequest) validate() error {
	u, err := url.Parse(r.Destination)
	if err != nil || u == nil || u.User != nil || u.Host == "" || u.Fragment != "" ||
		(u.Scheme != "https" && u.Scheme != "http") || r.Method != http.MethodPost {
		return fmt.Errorf("provider call requires an exact credential-free HTTP destination")
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return fmt.Errorf("provider call destination query is invalid")
	}
	for name := range query {
		lower := strings.ToLower(name)
		if strings.Contains(lower, "token") || strings.Contains(lower, "key") || strings.Contains(lower, "secret") || strings.Contains(lower, "auth") {
			return fmt.Errorf("provider call destination cannot contain credentials")
		}
	}
	for name, value := range r.Headers {
		switch name {
		case "content-type", "anthropic-version", "anthropic-beta", "openai-beta", "accept":
		default:
			return fmt.Errorf("provider call contains an unsupported semantic header")
		}
		if strings.TrimSpace(value) == "" || strings.ContainsAny(value, "\r\n") {
			return fmt.Errorf("provider call semantic header is invalid")
		}
	}
	if r.Headers["content-type"] != "application/json" || !json.Valid(r.Body) {
		return fmt.Errorf("provider call requires a complete JSON request")
	}
	return nil
}

func (c ProviderCall) Validate(authority Authority, req AuthorizeRequest) error {
	if req.Kind != KindProviderTurn || req.AgentFrame == nil || req.CapabilitySurface == nil ||
		authority.Target.Kind != UsageTargetAgentTurn || c.SessionID != authority.Target.SessionID ||
		c.Memory != authority.Target.Memory || c.TurnCount < 0 {
		return fmt.Errorf("provider call does not match the exact managed session")
	}
	if err := ValidateManagedAgentFrame(*req.AgentFrame, authority, *req.CapabilitySurface); err != nil {
		return err
	}
	if req.CapabilitySurface.Authority.TurnOrdinal != c.TurnCount+1 {
		return fmt.Errorf("provider call cursor does not match the admitted turn")
	}
	var messages []json.RawMessage
	if err := json.Unmarshal(c.Messages, &messages); err != nil || len(messages) == 0 {
		return fmt.Errorf("provider call requires the ordered conversation input")
	}
	if (c.TurnTimeout == nil) != (req.TurnTimeout == nil) ||
		(c.TurnTimeout != nil && (*c.TurnTimeout != *req.TurnTimeout || c.TurnTimeout.Validate() != nil)) {
		return fmt.Errorf("provider call contradicts the logical turn bound")
	}
	fingerprint, err := c.Request.Fingerprint()
	if err != nil {
		return err
	}
	if req.RequestFingerprint != fingerprint {
		return fmt.Errorf("provider call request fingerprint does not match")
	}
	if Fingerprint(c.ProviderContract) != req.CapabilitySurface.ProviderContract {
		return fmt.Errorf("provider call contract does not match its frozen surface")
	}
	if c.PredecessorAttemptID != "" {
		id, err := uuid.Parse(c.PredecessorAttemptID)
		if err != nil || id == uuid.Nil || id.String() != c.PredecessorAttemptID || c.PredecessorAttemptID == req.AttemptID {
			return fmt.Errorf("provider call predecessor identity is invalid")
		}
	}
	return nil
}

func (j ProviderCallJournal) ValidateRequestBinding(operationID, attemptID, fingerprint string) error {
	bound, err := j.Call.Request.Fingerprint()
	if err != nil {
		return err
	}
	if j.Version != ProviderCallJournalVersion || j.OperationID != operationID || j.AttemptID != attemptID || bound != fingerprint {
		return fmt.Errorf("provider call journal contradicts its physical request")
	}
	return nil
}

func (j ProviderCallJournal) Validate(authority Authority, req AuthorizeRequest) error {
	if err := j.ValidateRequestBinding(req.OperationID, req.AttemptID, req.RequestFingerprint); err != nil {
		return err
	}
	if j.Adapter != req.Adapter || j.TargetKind != authority.Target.Kind || j.TargetID != authority.Target.ID ||
		j.TargetOrdinal != authority.Target.Ordinal || req.AgentFrame == nil || req.CapabilitySurface == nil ||
		j.SourceHash != req.AgentFrame.Session.SourceArtifact.Hash || j.FrameID != req.AgentFrame.FrameID ||
		j.SurfaceID != req.CapabilitySurface.ID || j.SurfaceHash != req.CapabilitySurface.IntegrityHash {
		return fmt.Errorf("provider call journal contradicts its physical attempt")
	}
	return j.Call.Validate(authority, req)
}

// ReadOnly checks the recorded contract and surface, never today's provider
// configuration or whether a response happened to contain a tool call.
func (j ProviderCallJournal) ReadOnly(surface managedcapabilities.Surface) bool {
	if surface.ValidateEffective() != nil || j.SurfaceID != surface.ID || j.SurfaceHash != surface.IntegrityHash {
		return false
	}
	var contract struct {
		RuntimeMode, Provider, Transport string
		NativeTools                      struct {
			Capabilities struct{ Bash, WebSearch, FileIO bool }
		}
	}
	if json.Unmarshal(j.Call.ProviderContract, &contract) != nil || contract.Transport != "api" ||
		contract.Provider != surface.Provider || contract.RuntimeMode != surface.RuntimeMode ||
		surface.Transport != "api" || Fingerprint(j.Call.ProviderContract) != surface.ProviderContract ||
		contract.NativeTools.Capabilities.Bash || contract.NativeTools.Capabilities.WebSearch || contract.NativeTools.Capabilities.FileIO {
		return false
	}
	switch j.Adapter {
	case "anthropic_api", "openai_compatible", "openai_responses":
	default:
		return false
	}
	for _, tool := range surface.Tools {
		if tool.Capability.AuthorizationClass == "provider_native" {
			return false
		}
		for _, binding := range tool.Bindings {
			if binding.Kind == managedcapabilities.BindingProviderBuiltin || binding.Kind == managedcapabilities.BindingMCPProvider {
				return false
			}
		}
	}
	return true
}

func newProviderCallJournal(authority Authority, req AuthorizeRequest) (ProviderCallJournal, error) {
	operation, err := uuid.Parse(req.OperationID)
	wantAttempt, attemptErr := AttemptID(req.OperationID, 1)
	if err != nil || operation == uuid.Nil || operation.String() != req.OperationID || attemptErr != nil || wantAttempt != req.AttemptID {
		return ProviderCallJournal{}, fmt.Errorf("provider call journal requires native initial-attempt identities")
	}
	j := ProviderCallJournal{Version: ProviderCallJournalVersion, OperationID: req.OperationID, AttemptID: req.AttemptID,
		Adapter: req.Adapter, TargetKind: authority.Target.Kind, TargetID: authority.Target.ID, TargetOrdinal: authority.Target.Ordinal,
		Call: *req.ProviderCall}
	if req.AgentFrame != nil && req.CapabilitySurface != nil {
		j.SourceHash, j.FrameID = req.AgentFrame.Session.SourceArtifact.Hash, req.AgentFrame.FrameID
		j.SurfaceID, j.SurfaceHash = req.CapabilitySurface.ID, req.CapabilitySurface.IntegrityHash
	}
	return j, j.Validate(authority, req)
}

// EncodeAttemptAuthorityEvidence leaves operation authority unchanged. Only
// this initial attempt insert adds the immutable request journal.
func EncodeAttemptAuthorityEvidence(authority Authority, req AuthorizeRequest) ([]byte, error) {
	evidence := authority.Evidence()
	if req.ProviderCall != nil {
		journal, err := newProviderCallJournal(authority, req)
		if err != nil {
			return nil, err
		}
		evidence["provider_call_journal"] = journal
	}
	return json.Marshal(evidence)
}
