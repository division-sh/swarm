package agentframe

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

const (
	Version            = "agent-execution-frame.v1"
	frameIDPrefix      = "agent-frame:v1:"
	contentHashPrefix  = "agent-frame-content:v1:sha256:"
	criteriaHashPrefix = "agent-criteria:v1:sha256:"
)

type TurnKind string

const (
	TurnInitial          TurnKind = "initial"
	TurnToolContinuation TurnKind = "tool_continuation"
	TurnBoardDirective   TurnKind = "board_directive"
	TurnRemediation      TurnKind = "remediation"
)

func (k TurnKind) Valid() bool {
	switch k {
	case TurnInitial, TurnToolContinuation, TurnBoardDirective, TurnRemediation:
		return true
	default:
		return false
	}
}

type SessionSeed struct {
	AgentIdentity  agentidentity.Identity
	Role           string
	FlowID         string
	Intent         agentintent.Resolved
	Criteria       []string
	ProviderPrompt agentintent.ProviderPrompt
	RuntimeMode    string
	Provider       string
	Transport      string
	ModelAlias     string
	Model          string
}

func (s SessionSeed) Validate() error {
	if err := s.AgentIdentity.Validate(); err != nil {
		return fmt.Errorf("agent identity: %w", err)
	}
	if err := s.Intent.Validate(); err != nil {
		return fmt.Errorf("resolved intent: %w", err)
	}
	if !slices.Equal(s.Criteria, canonicalStrings(s.Criteria)) {
		return fmt.Errorf("criteria references are not canonical")
	}
	if err := s.ProviderPrompt.Validate(s.Intent, s.Criteria); err != nil {
		return fmt.Errorf("provider prompt: %w", err)
	}
	if strings.TrimSpace(s.RuntimeMode) == "" || strings.TrimSpace(s.Provider) == "" || strings.TrimSpace(s.Transport) == "" {
		return fmt.Errorf("provider runtime mode, provider, and transport are required")
	}
	if err := validateProviderModelSelection(s.ModelAlias, s.Model); err != nil {
		return err
	}
	return nil
}

type BundleSource struct {
	Hash   string `json:"hash"`
	Source string `json:"source"`
}

type Intent struct {
	Kind        string `json:"kind"`
	Coordinate  string `json:"coordinate"`
	Provenance  string `json:"provenance"`
	Content     string `json:"content"`
	ContentHash string `json:"content_hash"`
	Identity    string `json:"identity"`
}

type Criteria struct {
	References []string `json:"references"`
	Identity   string   `json:"identity"`
}

type Provider struct {
	RuntimeMode string `json:"runtime_mode"`
	Provider    string `json:"provider"`
	Transport   string `json:"transport"`
	ModelAlias  string `json:"model_alias,omitempty"`
	Model       string `json:"model,omitempty"`
}

type SessionContract struct {
	Bundle         BundleSource           `json:"bundle"`
	AgentIdentity  agentidentity.Identity `json:"agent_identity"`
	Role           string                 `json:"role,omitempty"`
	FlowID         string                 `json:"flow_id,omitempty"`
	Intent         Intent                 `json:"intent"`
	Criteria       Criteria               `json:"criteria"`
	Provider       Provider               `json:"provider"`
	ProviderPrompt string                 `json:"provider_prompt"`
}

type Route struct {
	FlowInstance string `json:"flow_instance,omitempty"`
	EntityID     string `json:"entity_id,omitempty"`
	FlowID       string `json:"flow_id,omitempty"`
}

type RoutingSource struct {
	Kind      string `json:"kind"`
	Route     Route  `json:"route"`
	Authority string `json:"authority,omitempty"`
}

type Event struct {
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	ProducerType  string          `json:"producer_type"`
	ProducerID    string          `json:"producer_id"`
	TaskID        string          `json:"task_id,omitempty"`
	RunID         string          `json:"run_id"`
	ParentEventID string          `json:"parent_event_id,omitempty"`
	ChainDepth    int             `json:"chain_depth"`
	ExecutionMode string          `json:"execution_mode"`
	Payload       json.RawMessage `json:"payload"`
	// PayloadBytesBase64 binds the exact admitted bytes even when an outer JSON
	// encoder compacts the payload value for provider transport.
	PayloadBytesBase64 string        `json:"payload_bytes_base64"`
	EntityID           string        `json:"entity_id,omitempty"`
	FlowInstance       string        `json:"flow_instance,omitempty"`
	Scope              string        `json:"scope,omitempty"`
	Source             Route         `json:"source"`
	Target             Route         `json:"target"`
	TargetSet          []Route       `json:"target_set"`
	RoutingSource      RoutingSource `json:"routing_source"`
}

type CapabilityBinding struct {
	Kind                 string `json:"kind"`
	ExactName            string `json:"exact_name"`
	RequiredEvidenceKind string `json:"required_evidence_kind"`
}

type Capability struct {
	Name               string              `json:"name"`
	DefinitionHash     string              `json:"definition_hash"`
	Kind               string              `json:"kind,omitempty"`
	ContextRequirement string              `json:"context_requirement,omitempty"`
	AuthorizationClass string              `json:"authorization_class,omitempty"`
	Bindings           []CapabilityBinding `json:"bindings"`
}

type CapabilityPlan struct {
	SurfaceID       string       `json:"surface_id"`
	PlanFingerprint string       `json:"plan_fingerprint"`
	Candidates      []Capability `json:"authorized_candidates"`
}

type Directive struct {
	Identity string `json:"identity"`
	Text     string `json:"text"`
	Source   string `json:"source,omitempty"`
	Operator string `json:"operator,omitempty"`
}

type Remediation struct {
	Reason string `json:"reason"`
}

type ProviderInput struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ToolResultEntry struct {
	CallID  string
	Payload json.RawMessage
	OK      bool
}

type UnresolvedFact struct {
	Status string `json:"status"`
	Value  string `json:"value,omitempty"`
}

func Unresolved() UnresolvedFact { return UnresolvedFact{Status: "unresolved"} }

func Resolved(value string) UnresolvedFact {
	value = strings.TrimSpace(value)
	if value == "" {
		return Unresolved()
	}
	return UnresolvedFact{Status: "resolved", Value: value}
}

type Lifecycle struct {
	Stage        UnresolvedFact `json:"stage"`
	LoopRevision UnresolvedFact `json:"loop_revision"`
}

type TurnContext struct {
	Kind           TurnKind        `json:"kind"`
	ParentFrameID  string          `json:"parent_frame_id,omitempty"`
	Event          Event           `json:"event"`
	Capability     CapabilityPlan  `json:"capability"`
	Directive      *Directive      `json:"directive,omitempty"`
	Remediation    *Remediation    `json:"remediation,omitempty"`
	ToolResult     json.RawMessage `json:"tool_result,omitempty"`
	Lifecycle      Lifecycle       `json:"lifecycle"`
	PackProvenance UnresolvedFact  `json:"pack_provenance"`
}

type Frame struct {
	Version     string          `json:"version"`
	FrameID     string          `json:"frame_id"`
	ContentHash string          `json:"content_hash"`
	Session     SessionContract `json:"session"`
	Turn        TurnContext     `json:"turn"`
}

type TurnDraft struct {
	Kind          TurnKind
	Event         events.Event
	ParentFrameID string
	InputRole     string
	InputContent  string
	Directive     *Directive
	Remediation   *Remediation
}

type Completion struct {
	BundleHash   string
	BundleSource string
	Surface      managedcapabilities.Surface
}

func Complete(seed SessionSeed, draft TurnDraft, completion Completion) (Frame, error) {
	if err := seed.Validate(); err != nil {
		return Frame{}, err
	}
	session, err := completeSession(seed, completion.BundleHash, completion.BundleSource)
	if err != nil {
		return Frame{}, err
	}
	turn, err := completeTurn(seed, draft, completion.Surface)
	if err != nil {
		return Frame{}, err
	}
	frame := Frame{
		Version: Version,
		FrameID: frameIDPrefix + completion.Surface.Authority.ID,
		Session: session,
		Turn:    turn,
	}
	canonical, err := frame.canonicalContent()
	if err != nil {
		return Frame{}, err
	}
	sum := sha256.Sum256(canonical)
	frame.ContentHash = contentHashPrefix + hex.EncodeToString(sum[:])
	return frame, frame.Validate()
}

func completeSession(seed SessionSeed, bundleHash, bundleSource string) (SessionContract, error) {
	if err := bundleidentity.ValidateCanonicalHash(strings.TrimSpace(bundleHash)); err != nil {
		return SessionContract{}, fmt.Errorf("bundle hash: %w", err)
	}
	bundleSource = strings.TrimSpace(bundleSource)
	if bundleSource != "persisted" && bundleSource != "ephemeral" {
		return SessionContract{}, fmt.Errorf("bundle source must be persisted or ephemeral")
	}
	prompt, err := seed.ProviderPrompt.Text()
	if err != nil {
		return SessionContract{}, err
	}
	criteriaIdentity, err := CriteriaIdentity(bundleHash, seed.Criteria)
	if err != nil {
		return SessionContract{}, err
	}
	return SessionContract{
		Bundle:        BundleSource{Hash: strings.TrimSpace(bundleHash), Source: bundleSource},
		AgentIdentity: seed.AgentIdentity.Normalize(),
		Role:          strings.TrimSpace(seed.Role),
		FlowID:        strings.Trim(strings.TrimSpace(seed.FlowID), "/"),
		Intent: Intent{
			Kind: string(seed.Intent.Kind), Coordinate: seed.Intent.Coordinate, Provenance: seed.Intent.Provenance,
			Content: seed.Intent.Content, ContentHash: seed.Intent.ContentHash, Identity: seed.Intent.Identity,
		},
		Criteria: Criteria{References: append([]string(nil), seed.Criteria...), Identity: criteriaIdentity},
		Provider: Provider{
			RuntimeMode: strings.TrimSpace(seed.RuntimeMode), Provider: strings.TrimSpace(seed.Provider),
			Transport: strings.TrimSpace(seed.Transport), ModelAlias: seed.ModelAlias, Model: seed.Model,
		},
		ProviderPrompt: prompt,
	}, nil
}

func completeTurn(seed SessionSeed, draft TurnDraft, surface managedcapabilities.Surface) (TurnContext, error) {
	if !draft.Kind.Valid() {
		return TurnContext{}, fmt.Errorf("turn kind %q is invalid", draft.Kind)
	}
	if err := surface.Validate(); err != nil {
		return TurnContext{}, fmt.Errorf("capability surface: %w", err)
	}
	if surface.Authority.Kind != managedcapabilities.AuthorityProviderTurn {
		return TurnContext{}, fmt.Errorf("execution frame requires provider-turn capability authority")
	}
	if !surface.MatchesActor(seed.AgentIdentity) {
		return TurnContext{}, fmt.Errorf("capability surface actor does not match session contract")
	}
	if surface.Provider != strings.TrimSpace(seed.Provider) || surface.Transport != strings.TrimSpace(seed.Transport) || surface.RuntimeMode != strings.TrimSpace(seed.RuntimeMode) {
		return TurnContext{}, fmt.Errorf("capability surface provider contract does not match session contract")
	}
	event, err := projectEvent(draft.Event)
	if err != nil {
		return TurnContext{}, err
	}
	if strings.TrimSpace(surface.Authority.RunID) != event.RunID {
		return TurnContext{}, fmt.Errorf("capability surface run does not match admitted event")
	}
	plan, err := projectCapabilityPlan(surface)
	if err != nil {
		return TurnContext{}, err
	}
	turn := TurnContext{
		Kind: draft.Kind, ParentFrameID: strings.TrimSpace(draft.ParentFrameID), Event: event,
		Capability: plan, Lifecycle: Lifecycle{Stage: Unresolved(), LoopRevision: Unresolved()}, PackProvenance: Unresolved(),
	}
	if draft.Directive != nil {
		directive := *draft.Directive
		turn.Directive = &directive
	}
	if draft.Remediation != nil {
		remediation := *draft.Remediation
		turn.Remediation = &remediation
	}
	if draft.Kind == TurnToolContinuation {
		if strings.TrimSpace(draft.InputRole) != "tool" || strings.TrimSpace(draft.InputContent) == "" {
			return TurnContext{}, fmt.Errorf("tool continuation requires exact tool input")
		}
		result, err := canonicalPayload(json.RawMessage(draft.InputContent))
		if err != nil {
			return TurnContext{}, fmt.Errorf("tool continuation input must be JSON: %w", err)
		}
		turn.ToolResult = result
	} else if strings.TrimSpace(draft.InputRole) != "" || strings.TrimSpace(draft.InputContent) != "" {
		return TurnContext{}, fmt.Errorf("non-continuation turn cannot carry raw provider input")
	}
	if err := validateTurnShape(turn); err != nil {
		return TurnContext{}, err
	}
	return turn, nil
}

func validateTurnShape(turn TurnContext) error {
	switch turn.Kind {
	case TurnInitial:
		if turn.ParentFrameID != "" || turn.Directive != nil || turn.Remediation != nil || len(turn.ToolResult) != 0 {
			return fmt.Errorf("initial turn carries unsupported parent, directive, remediation, or tool result")
		}
	case TurnToolContinuation:
		if turn.ParentFrameID == "" || turn.Directive != nil || turn.Remediation != nil || len(turn.ToolResult) == 0 {
			return fmt.Errorf("tool continuation requires parent frame and exact tool input")
		}
		if err := validateFrameID(turn.ParentFrameID); err != nil {
			return fmt.Errorf("tool continuation parent frame: %w", err)
		}
	case TurnBoardDirective:
		if turn.ParentFrameID != "" || turn.Directive == nil || turn.Remediation != nil || len(turn.ToolResult) != 0 {
			return fmt.Errorf("board directive turn is malformed")
		}
		if strings.TrimSpace(turn.Directive.Identity) == "" || strings.TrimSpace(turn.Directive.Text) == "" {
			return fmt.Errorf("board directive identity and text are required")
		}
	case TurnRemediation:
		if turn.ParentFrameID == "" || turn.Directive == nil || turn.Remediation == nil || len(turn.ToolResult) != 0 {
			return fmt.Errorf("remediation turn requires parent frame, directive, and reason")
		}
		if strings.TrimSpace(turn.Directive.Identity) == "" || strings.TrimSpace(turn.Directive.Text) == "" || strings.TrimSpace(turn.Remediation.Reason) == "" {
			return fmt.Errorf("remediation directive identity, text, and reason are required")
		}
		if err := validateFrameID(turn.ParentFrameID); err != nil {
			return fmt.Errorf("remediation parent frame: %w", err)
		}
	}
	return nil
}

func renderProviderInput(turn TurnContext) (ProviderInput, error) {
	role := "user"
	payload := any(struct {
		Kind           TurnKind       `json:"kind"`
		Parent         string         `json:"parent_frame_id,omitempty"`
		Event          Event          `json:"event"`
		Directive      *Directive     `json:"directive,omitempty"`
		Remediation    *Remediation   `json:"remediation,omitempty"`
		Lifecycle      Lifecycle      `json:"lifecycle"`
		PackProvenance UnresolvedFact `json:"pack_provenance"`
	}{turn.Kind, turn.ParentFrameID, turn.Event, turn.Directive, turn.Remediation, turn.Lifecycle, turn.PackProvenance})
	if turn.Kind == TurnToolContinuation {
		role = "tool"
		payload = struct {
			Kind           TurnKind        `json:"kind"`
			Parent         string          `json:"parent_frame_id"`
			Event          Event           `json:"event"`
			ToolResult     json.RawMessage `json:"tool_result"`
			Lifecycle      Lifecycle       `json:"lifecycle"`
			PackProvenance UnresolvedFact  `json:"pack_provenance"`
		}{turn.Kind, turn.ParentFrameID, turn.Event, turn.ToolResult, turn.Lifecycle, turn.PackProvenance}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return ProviderInput{}, fmt.Errorf("encode provider turn input: %w", err)
	}
	return ProviderInput{Role: role, Content: string(raw)}, nil
}

func projectEvent(event events.Event) (Event, error) {
	if strings.TrimSpace(event.ID()) == "" || strings.TrimSpace(string(event.Type())) == "" || strings.TrimSpace(event.RunID()) == "" {
		return Event{}, fmt.Errorf("execution frame requires admitted event id, type, and run id")
	}
	producer := event.Producer()
	if err := producer.Validate(); err != nil {
		return Event{}, fmt.Errorf("event producer: %w", err)
	}
	if !event.ExecutionMode().Valid() {
		return Event{}, fmt.Errorf("execution frame requires admitted event execution mode")
	}
	payload := event.Payload()
	if !json.Valid(payload) {
		return Event{}, fmt.Errorf("event payload is not valid JSON")
	}
	payload = append(json.RawMessage(nil), payload...)
	envelope := event.NormalizedEnvelope()
	targets := make([]Route, 0, len(envelope.TargetSet))
	for _, target := range envelope.TargetSet {
		targets = append(targets, projectRoute(target))
	}
	source := event.RoutingSource()
	return Event{
		ID: event.ID(), Type: string(event.Type()), ProducerType: string(producer.Type()), ProducerID: producer.ID(),
		TaskID: event.TaskID(), RunID: event.RunID(), ParentEventID: event.ParentEventID(), ChainDepth: event.ChainDepth(),
		ExecutionMode: string(event.ExecutionMode()), Payload: payload, PayloadBytesBase64: base64.StdEncoding.EncodeToString(payload), EntityID: event.EntityID(), FlowInstance: event.FlowInstance(),
		Scope: string(event.Scope()), Source: projectRoute(envelope.Source), Target: projectRoute(envelope.Target), TargetSet: targets,
		RoutingSource: RoutingSource{Kind: source.Kind().StorageCode(), Route: projectRoute(source.Route()), Authority: source.Authority().StorageCode()},
	}, nil
}

func projectRoute(route events.RouteIdentity) Route {
	route = route.Normalized()
	return Route{FlowInstance: route.FlowInstance, EntityID: route.EntityID, FlowID: route.FlowID}
}

func canonicalPayload(raw json.RawMessage) (json.RawMessage, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return canonical, nil
}

func projectCapabilityPlan(surface managedcapabilities.Surface) (CapabilityPlan, error) {
	fingerprint, err := surface.PlanFingerprint()
	if err != nil {
		return CapabilityPlan{}, err
	}
	candidates := make([]Capability, 0, len(surface.Tools))
	for _, tool := range surface.Tools {
		if !tool.Capability.Visible || !tool.Capability.Callable {
			continue
		}
		bindings := make([]CapabilityBinding, 0, len(tool.Bindings))
		for _, binding := range tool.Bindings {
			bindings = append(bindings, CapabilityBinding{
				Kind: string(binding.Kind), ExactName: binding.ExactName, RequiredEvidenceKind: binding.RequiredEvidenceKind,
			})
		}
		candidates = append(candidates, Capability{
			Name: tool.Name, DefinitionHash: tool.DefinitionHash, Kind: string(tool.Capability.Kind),
			ContextRequirement: string(tool.Capability.ContextRequirement), AuthorizationClass: tool.Capability.AuthorizationClass,
			Bindings: bindings,
		})
	}
	return CapabilityPlan{SurfaceID: surface.ID, PlanFingerprint: fingerprint, Candidates: candidates}, nil
}

func CriteriaIdentity(bundleHash string, references []string) (string, error) {
	if err := bundleidentity.ValidateCanonicalHash(strings.TrimSpace(bundleHash)); err != nil {
		return "", err
	}
	if !slices.Equal(references, canonicalStrings(references)) {
		return "", fmt.Errorf("criteria references are not canonical")
	}
	raw, err := json.Marshal(struct {
		Domain     string   `json:"domain"`
		Version    int      `json:"version"`
		BundleHash string   `json:"bundle_hash"`
		References []string `json:"references"`
	}{"division-sh.swarm.agent-criteria", 1, strings.TrimSpace(bundleHash), references})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return criteriaHashPrefix + hex.EncodeToString(sum[:]), nil
}

func canonicalStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	slices.Sort(out)
	return out
}

func (f Frame) canonicalContent() ([]byte, error) {
	return json.Marshal(struct {
		Version string          `json:"version"`
		Session SessionContract `json:"session"`
		Turn    TurnContext     `json:"turn"`
	}{f.Version, f.Session, f.Turn})
}

func (f Frame) Validate() error {
	if f.Version != Version {
		return fmt.Errorf("execution frame version %q is invalid", f.Version)
	}
	if err := validateFrameID(f.FrameID); err != nil {
		return err
	}
	if err := f.Session.AgentIdentity.Validate(); err != nil {
		return fmt.Errorf("execution frame agent identity: %w", err)
	}
	if strings.TrimSpace(f.Session.Provider.RuntimeMode) == "" || strings.TrimSpace(f.Session.Provider.Provider) == "" || strings.TrimSpace(f.Session.Provider.Transport) == "" {
		return fmt.Errorf("execution frame provider contract is incomplete")
	}
	if err := validateProviderModelSelection(f.Session.Provider.ModelAlias, f.Session.Provider.Model); err != nil {
		return fmt.Errorf("execution frame provider selection: %w", err)
	}
	if !f.Turn.Kind.Valid() || f.Turn.Capability.SurfaceID == "" || f.Turn.Capability.PlanFingerprint == "" {
		return fmt.Errorf("execution frame turn is incomplete")
	}
	if strings.TrimSpace(f.Turn.Event.ID) == "" || strings.TrimSpace(f.Turn.Event.Type) == "" || strings.TrimSpace(f.Turn.Event.RunID) == "" || !executionmode.Mode(f.Turn.Event.ExecutionMode).Valid() {
		return fmt.Errorf("execution frame event identity or causal execution mode is incomplete")
	}
	if !json.Valid(f.Turn.Event.Payload) {
		return fmt.Errorf("execution frame event payload is not valid JSON")
	}
	exactPayload, err := base64.StdEncoding.DecodeString(f.Turn.Event.PayloadBytesBase64)
	if err != nil || !bytes.Equal(exactPayload, f.Turn.Event.Payload) {
		return fmt.Errorf("execution frame event payload bytes do not match exact admitted payload evidence")
	}
	if err := validatePresenceFact("lifecycle stage", f.Turn.Lifecycle.Stage, true); err != nil {
		return err
	}
	if err := validatePresenceFact("lifecycle loop revision", f.Turn.Lifecycle.LoopRevision, true); err != nil {
		return err
	}
	if err := validatePresenceFact("pack provenance", f.Turn.PackProvenance, false); err != nil {
		return err
	}
	if err := validateTurnShape(f.Turn); err != nil {
		return err
	}
	if _, err := renderProviderInput(f.Turn); err != nil {
		return fmt.Errorf("execution frame provider input rendering: %w", err)
	}
	canonical, err := f.canonicalContent()
	if err != nil {
		return err
	}
	sum := sha256.Sum256(canonical)
	if f.ContentHash != contentHashPrefix+hex.EncodeToString(sum[:]) {
		return fmt.Errorf("execution frame content hash does not match canonical content")
	}
	return nil
}

func validateProviderModelSelection(alias, model string) error {
	if alias != strings.TrimSpace(alias) || model != strings.TrimSpace(model) {
		return fmt.Errorf("provider model alias and concrete model must be exact")
	}
	if (alias == "") != (model == "") {
		return fmt.Errorf("provider model alias and concrete model must be present together")
	}
	return nil
}

func validatePresenceFact(name string, fact UnresolvedFact, allowResolved bool) error {
	switch fact.Status {
	case "unresolved":
		if strings.TrimSpace(fact.Value) != "" {
			return fmt.Errorf("%s unresolved fact carries a value", name)
		}
	case "resolved":
		if !allowResolved {
			return fmt.Errorf("%s is deferred and must remain unresolved", name)
		}
		if strings.TrimSpace(fact.Value) == "" {
			return fmt.Errorf("%s resolved fact requires a value", name)
		}
	default:
		return fmt.Errorf("%s presence status %q is invalid", name, fact.Status)
	}
	return nil
}

func validateFrameID(frameID string) error {
	authorityID := strings.TrimPrefix(frameID, frameIDPrefix)
	if authorityID == frameID {
		return fmt.Errorf("execution frame id is invalid")
	}
	if _, err := uuid.Parse(authorityID); err != nil {
		return fmt.Errorf("execution frame id authority coordinate: %w", err)
	}
	return nil
}

func (f Frame) ProviderTurnAuthorityID() (string, error) {
	if err := validateFrameID(f.FrameID); err != nil {
		return "", err
	}
	return strings.TrimPrefix(f.FrameID, frameIDPrefix), nil
}

func (f Frame) MatchesSurface(surface managedcapabilities.Surface) bool {
	if f.Validate() != nil || surface.Validate() != nil || surface.Authority.Kind != managedcapabilities.AuthorityProviderTurn {
		return false
	}
	fingerprint, err := surface.PlanFingerprint()
	return err == nil && f.FrameID == frameIDPrefix+surface.Authority.ID && f.Turn.Capability.SurfaceID == surface.ID &&
		f.Turn.Capability.PlanFingerprint == fingerprint && surface.MatchesActor(f.Session.AgentIdentity)
}

func (f Frame) ProviderInput() (role, content string, err error) {
	if err := f.Validate(); err != nil {
		return "", "", err
	}
	input, err := renderProviderInput(f.Turn)
	if err != nil {
		return "", "", err
	}
	return input.Role, input.Content, nil
}

// DecodeProviderToolResults is the canonical decoder for the transport-neutral
// tool-continuation projection emitted by ProviderInput.
func DecodeProviderToolResults(content string) ([]ToolResultEntry, error) {
	var projection struct {
		Kind       TurnKind        `json:"kind"`
		ToolResult json.RawMessage `json:"tool_result"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(content)), &projection); err != nil {
		return nil, err
	}
	if projection.Kind != TurnToolContinuation || len(projection.ToolResult) == 0 {
		return nil, fmt.Errorf("provider input is not a tool continuation")
	}
	var entries []map[string]any
	if err := json.Unmarshal(projection.ToolResult, &entries); err != nil {
		return nil, fmt.Errorf("decode provider tool results: %w", err)
	}
	results := make([]ToolResultEntry, 0, len(entries))
	for _, entry := range entries {
		raw, err := json.Marshal(entry)
		if err != nil {
			return nil, err
		}
		callID, _ := entry["tool_call_id"].(string)
		ok, _ := entry["ok"].(bool)
		results = append(results, ToolResultEntry{CallID: strings.TrimSpace(callID), Payload: raw, OK: ok})
	}
	return results, nil
}

func CapabilityNames(frame Frame) []string {
	names := make([]string, 0, len(frame.Turn.Capability.Candidates))
	for _, candidate := range frame.Turn.Capability.Candidates {
		names = append(names, candidate.Name)
	}
	return names
}
