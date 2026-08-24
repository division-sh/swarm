package effects

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type EffectClass string

const (
	EffectReadOnly       EffectClass = "read_only"
	EffectWriteOrUnknown EffectClass = "write_or_unknown"
)

type Kind string

const (
	KindProviderTurn          Kind = "provider_turn"
	KindProviderStartupProbe  Kind = "provider_startup_probe"
	KindHTTPToolTarget        Kind = "http_tool_target"
	KindManagedCredential     Kind = "managed_credential_request"
	KindNativeWebSearchHTTP   Kind = "native_web_search_http"
	KindMCPHTTPRequest        Kind = "mcp_http_request"
	KindMCPStdioRequest       Kind = "mcp_stdio_request"
	KindNativeCommand         Kind = "native_command"
	KindNativeFileWrite       Kind = "native_file_write"
	KindToolResultRelay       Kind = "tool_result_relay"
	KindClaudeToolResultRelay Kind = "claude_tool_result_relay"
	KindServeRegistration     Kind = "serve_registration"
)

type LifecycleToken struct {
	RuntimeEpoch int64                         `json:"runtime_epoch"`
	Identity     runtimeagentidentity.Identity `json:"identity"`
	AgentID      string                        `json:"agent_id"`
	Generation   uint64                        `json:"generation"`
}

func (t LifecycleToken) Valid() bool {
	identity := t.Identity.Normalize()
	return t.RuntimeEpoch > 0 &&
		identity.Validate() == nil &&
		identity.AgentID() == strings.TrimSpace(t.AgentID) &&
		t.Generation > 0
}

type lifecycleTokenKey struct{}

type DifferentOwner string

const (
	OwnerRuntimeDependency       DifferentOwner = "runtime_dependency"
	OwnerCredentialLifecycle     DifferentOwner = "credential_lifecycle"
	OwnerOperatorInfrastructure  DifferentOwner = "operator_infrastructure"
	OwnerPipelineActivity        DifferentOwner = "pipeline_activity"
	OwnerBuildTestInfrastructure DifferentOwner = "build_test_infrastructure"
)

type differentOwnerKey struct{}
type logicalOperationIdentityKey struct{}

func WithLifecycleToken(ctx context.Context, token LifecycleToken) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, lifecycleTokenKey{}, token)
}

func LifecycleTokenFromContext(ctx context.Context) (LifecycleToken, bool) {
	if ctx == nil {
		return LifecycleToken{}, false
	}
	token, ok := ctx.Value(lifecycleTokenKey{}).(LifecycleToken)
	return token, ok && token.Valid()
}

// WithDifferentOwner explicitly classifies a context whose external effects
// are not managed agent attempts. Absence of lifecycle context is never enough
// to infer this distinction.
func WithDifferentOwner(ctx context.Context, owner DifferentOwner) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, differentOwnerKey{}, DifferentOwner(strings.TrimSpace(string(owner))))
}

func DifferentOwnerFromContext(ctx context.Context) (DifferentOwner, bool) {
	if ctx == nil {
		return "", false
	}
	owner, ok := ctx.Value(differentOwnerKey{}).(DifferentOwner)
	return owner, ok && owner.valid()
}

func (o DifferentOwner) valid() bool {
	switch o {
	case OwnerRuntimeDependency, OwnerCredentialLifecycle, OwnerOperatorInfrastructure, OwnerPipelineActivity, OwnerBuildTestInfrastructure:
		return true
	default:
		return false
	}
}

// WithLogicalOperationIdentity supplies canonical identity when an effect is
// not rooted in an inbound event (for example, an explicit directive step).
func WithLogicalOperationIdentity(ctx context.Context, identity string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, logicalOperationIdentityKey{}, strings.TrimSpace(identity))
}

func LogicalOperationIdentityFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	identity, ok := ctx.Value(logicalOperationIdentityKey{}).(string)
	identity = strings.TrimSpace(identity)
	return identity, ok && identity != ""
}

// WithLogicalOperationIdentitySegment refines the current logical work with a
// deterministic child coordinate, such as a provider turn or tool-call ID.
func WithLogicalOperationIdentitySegment(ctx context.Context, segment string) context.Context {
	segment = strings.TrimSpace(segment)
	if segment == "" {
		return ctx
	}
	identity := logicalOperationIdentity(ctx)
	if identity == "" {
		return WithLogicalOperationIdentity(ctx, segment)
	}
	return WithLogicalOperationIdentity(ctx, identity+"\x00"+segment)
}

type State string

const (
	StatePrepared         State = "prepared"
	StateAuthorized       State = "authorized"
	StateLaunched         State = "launched"
	StateResponseObserved State = "response_observed"
	StateSettled          State = "settled"
	StateTerminalFailure  State = "terminal_failure"
	StateOutcomeUncertain State = "outcome_uncertain"
)

type Registration struct {
	Kind               Kind
	Class              EffectClass
	Adapter            string
	Transport          string
	LaunchSite         string
	LaunchObserved     string
	OutcomeMapping     string
	CanonicalEvidence  string
	SettlementRecovery string
	Proof              string
	PrimitiveKeys      []string
	PrelaunchFailure   State
	PostlaunchFailure  State
}

var registrations = []Registration{
	registration(KindProviderStartupProbe, EffectWriteOrUnknown, "claude_cli_startup_probe", "process", "internal/runtime/llm/cli_runtime_startup_probe.go", []string{"internal/runtime/llm/cli_runtime_startup_probe.go:runUntilCLIStartupInit:process_launch:1"}, "TestClaudeStartupProbeManagedCapabilityAuthority"),
	registration(KindProviderTurn, EffectWriteOrUnknown, "anthropic_api", "http", "internal/runtime/llm/api_runtime.go", []string{"internal/runtime/llm/api_runtime.go:sendRequest:http_do:1"}, "TestManagedProviderEffectOutcomes/anthropic_api"),
	registration(KindProviderTurn, EffectWriteOrUnknown, "openai_compatible", "http", "internal/runtime/llm/openai_compatible_runtime.go", []string{"internal/runtime/llm/openai_compatible_runtime.go:sendRequest:http_do:1"}, "TestManagedProviderEffectOutcomes/openai_compatible"),
	registration(KindProviderTurn, EffectWriteOrUnknown, "openai_responses", "http", "internal/runtime/llm/openai_responses_runtime.go", []string{"internal/runtime/llm/openai_responses_runtime.go:sendRequest:http_do:1"}, "TestManagedProviderEffectOutcomes/openai_responses"),
	registration(KindProviderTurn, EffectWriteOrUnknown, "claude_cli", "process", "internal/runtime/llm/cli_runtime_process.go", []string{"internal/runtime/llm/cli_runtime_process.go:runWithPreparedInput:process_launch:1", "internal/runtime/llm/cli_runtime_process.go:runStreamingPrepared:process_launch:1"}, "TestManagedClaudeCLIEffectOutcomes"),
	registration(KindProviderTurn, EffectReadOnly, "mock_python", "in_process", "internal/runtime/llm/mock_runtime.go", nil, "TestMockRuntimeEndToEnd"),
	registration(KindHTTPToolTarget, EffectWriteOrUnknown, "authored_http_tool", "http", "internal/runtime/tools/executor_http.go", []string{"internal/runtime/tools/executor_http.go:execHTTPRequestOnce:http_do:1"}, "TestManagedToolEffectOutcomes/authored_http_tool"),
	registration(KindServeRegistration, EffectWriteOrUnknown, "provider_registration", "http", "internal/runtime/registration/provider_http.go", []string{"internal/runtime/registration/provider_http.go:executeProviderApply:http_do:1"}, "TestProviderRegistrationApplyEffectOutcomes"),
	registration(KindManagedCredential, EffectWriteOrUnknown, "managed_credential", "http", "internal/runtime/managedcredentials/store.go", []string{"internal/runtime/managedcredentials/store.go:exchange:http_do:1", "internal/runtime/managedcredentials/store.go:exchangeGitHubAppInstallation:http_do:1"}, "TestManagedCredentialEffectOutcomes"),
	registration(KindNativeWebSearchHTTP, EffectWriteOrUnknown, "native_web_search", "http", "internal/runtime/tools/executor_native.go", []string{"internal/runtime/tools/executor_native.go:doNormalizedSearch:http_do:1"}, "TestManagedToolEffectOutcomes/native_web_search"),
	registration(KindMCPHTTPRequest, EffectWriteOrUnknown, "mcp_tools_call_http", "http", "internal/runtime/mcp/client.go", []string{"internal/runtime/mcp/client.go:callHTTPServerWithCredentialKeyResolver:http_do:1"}, "TestManagedMCPEffectOutcomes/http"),
	registration(KindMCPStdioRequest, EffectWriteOrUnknown, "mcp_tools_call_stdio", "stdio", "internal/runtime/mcp/client.go", []string{"internal/runtime/mcp/client.go:Call:stdio_write:1"}, "TestManagedMCPEffectOutcomes/stdio"),
	registration(KindNativeCommand, EffectWriteOrUnknown, "native_bash", "process", "internal/runtime/tools/executor_native.go", []string{"internal/runtime/tools/executor_native.go:runWorkspaceCommand:process_launch:1"}, "TestManagedNativeEffectOutcomes/bash"),
	registration(KindNativeCommand, EffectReadOnly, "native_read_file", "process", "internal/runtime/tools/executor_native.go", []string{"internal/runtime/tools/executor_native.go:runWorkspaceCommand:process_launch:1"}, "TestManagedNativeEffectOutcomes/read_file"),
	registration(KindNativeFileWrite, EffectWriteOrUnknown, "native_write_file", "filesystem", "internal/runtime/tools/executor_native.go", []string{"internal/runtime/tools/executor_native.go:execNativeHostWriteFile:filesystem_write:1", "internal/runtime/tools/executor_native.go:execNativeHostWriteFile:filesystem_write:2", "internal/runtime/tools/executor_native.go:execNativeHostWriteFile:filesystem_write:3", "internal/runtime/tools/executor_native.go:execNativeHostWriteFile:filesystem_write:4", "internal/runtime/tools/executor_native.go:execNativeHostWriteFile:filesystem_write:5", "internal/runtime/tools/executor_native.go:execNativeHostWriteFile:filesystem_write:6", "internal/runtime/tools/executor_native.go:runWorkspaceCommand:process_launch:1"}, "TestManagedNativeEffectOutcomes/write_file"),
	registration(KindToolResultRelay, EffectWriteOrUnknown, "tool_result_relay", "filesystem", "internal/runtime/tools/tool_result_relay.go", []string{"internal/runtime/tools/tool_result_relay.go:writeToolResultRelayFile:filesystem_write:1", "internal/runtime/tools/tool_result_relay.go:writeToolResultRelayFile:filesystem_write:2", "internal/runtime/tools/tool_result_relay.go:writeToolResultRelayFile:filesystem_write:3", "internal/runtime/tools/tool_result_relay.go:writeToolResultRelayFile:filesystem_write:4", "internal/runtime/tools/tool_result_relay.go:writeToolResultRelayFile:filesystem_write:5", "internal/runtime/tools/tool_result_relay.go:writeToolResultRelayFile:filesystem_write:6", "internal/runtime/tools/executor_native.go:runWorkspaceCommand:process_launch:1"}, "TestManagedRelayEffectOutcomes/tool_result_relay"),
	registration(KindClaudeToolResultRelay, EffectWriteOrUnknown, "claude_tool_result_relay", "process", "internal/runtime/llm/cli_tool_result_relay.go", []string{"internal/runtime/llm/cli_tool_result_relay.go:runWorkspaceCommand:process_launch:1"}, "TestManagedRelayEffectOutcomes/claude_tool_result_relay"),
}

func registration(kind Kind, class EffectClass, adapter, transport, launchSite string, primitiveKeys []string, proof string) Registration {
	postlaunch := StateOutcomeUncertain
	if class == EffectReadOnly {
		postlaunch = StateTerminalFailure
	}
	registration := Registration{
		Kind: kind, Class: class, Adapter: adapter, Transport: transport, LaunchSite: launchSite,
		LaunchObserved:     "state=launched must commit before: " + strings.Join(primitiveKeys, ","),
		OutcomeMapping:     fmt.Sprintf("success=%s; proven_launch_rejection=%s; postlaunch_failure=%s", StateSettled, StateTerminalFailure, postlaunch),
		CanonicalEvidence:  "operation_id, attempt_id, lifecycle token, request fingerprint, launch timestamp, settlement evidence",
		SettlementRecovery: fmt.Sprintf("authorized=%s; launched/response_observed=%s; replay=no_redispatch", StateTerminalFailure, StateOutcomeUncertain),
		Proof:              proof,
		PrimitiveKeys:      append([]string(nil), primitiveKeys...),
		PrelaunchFailure:   StateTerminalFailure,
		PostlaunchFailure:  postlaunch,
	}
	if len(primitiveKeys) == 0 {
		registration.LaunchObserved = "state=launched must commit before deterministic in-process execution"
	}
	if adapter == "claude_cli" {
		registration.SettlementRecovery = fmt.Sprintf("authorized=%s; exact zero-process-launch terminal failure may authorize next ordinal; launched/response_observed=%s; postlaunch replay=no_redispatch", StateTerminalFailure, StateOutcomeUncertain)
	}
	return registration
}

func Registrations() []Registration {
	return append([]Registration(nil), registrations...)
}

func RegistrationFor(adapter string) (Registration, bool) {
	adapter = strings.TrimSpace(adapter)
	for _, registration := range registrations {
		if registration.Adapter == adapter {
			return registration, true
		}
	}
	return Registration{}, false
}

type AuthorizeRequest struct {
	OperationID        string
	AttemptID          string
	Kind               Kind
	Class              EffectClass
	Adapter            string
	Transport          string
	RequestFingerprint string
	CapabilitySurface  *managedcapabilities.Surface
	AgentFrame         *agentframe.Frame
	Origin             CompletionOrigin
	Lineage            map[string]string
	Now                time.Time
}

type Attempt struct {
	OperationID       string
	AttemptID         string
	Token             LifecycleToken
	Authority         Authority
	Kind              Kind
	Class             EffectClass
	Adapter           string
	Transport         string
	Ordinal           int
	AuthorizedAt      time.Time
	Origin            CompletionOrigin
	completionRequest []byte
	completionPayload json.RawMessage
	completionSurface *managedcapabilities.Surface
	completionPhase   CompletionProjectionPhase
}

type CompletionProjectionPhase string

const (
	CompletionProjectionResponseSettled       CompletionProjectionPhase = "response_settled"
	CompletionProjectionConversationProjected CompletionProjectionPhase = "conversation_projected"
	CompletionProjectionResponseConsumed      CompletionProjectionPhase = "response_consumed"
)

func (p CompletionProjectionPhase) Valid() bool {
	return p == CompletionProjectionResponseSettled ||
		p == CompletionProjectionConversationProjected ||
		p == CompletionProjectionResponseConsumed
}

type completionContinuationEvidence struct {
	Version string          `json:"version"`
	Request []byte          `json:"request"`
	Payload json.RawMessage `json:"payload"`
}

const (
	completionContinuationEvidenceKey     = "completion_continuation_v1"
	completionContinuationEvidenceVersion = "completion-continuation.v1"
)

// AttachCompletionContinuationEvidence records the exact provider request and
// its adapter-neutral settled response projection in immutable evidence.
func AttachCompletionContinuationEvidence(evidence map[string]any, request []byte, payload json.RawMessage) error {
	if evidence == nil {
		return fmt.Errorf("completion continuation evidence map is required")
	}
	request = bytes.TrimSpace(request)
	payload = bytes.TrimSpace(payload)
	if len(request) == 0 || len(payload) == 0 || !json.Valid(payload) {
		return fmt.Errorf("completion continuation requires exact request bytes and valid payload JSON")
	}
	evidence[completionContinuationEvidenceKey] = completionContinuationEvidence{
		Version: completionContinuationEvidenceVersion,
		Request: append([]byte(nil), request...),
		Payload: append(json.RawMessage(nil), payload...),
	}
	return nil
}

// AdmitCompletionContinuation projects immutable settled evidence onto the
// current fenced delivery authority. Only the selected store may call this
// after proving the operation, request, plan, source bundle, and delivery.
func AdmitCompletionContinuation(attempt Attempt, evidence json.RawMessage, requestFingerprint string, surface managedcapabilities.Surface, phase CompletionProjectionPhase) (Attempt, error) {
	if attempt.Kind != KindProviderTurn || attempt.Authority.Kind != AuthorityNormalAgent ||
		attempt.Origin.Kind != CompletionOriginDelivery || !phase.Valid() {
		return Attempt{}, fmt.Errorf("completion continuation requires a normal provider delivery attempt and phase")
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(evidence, &document); err != nil {
		return Attempt{}, fmt.Errorf("decode completion continuation evidence: %w", err)
	}
	var continuation completionContinuationEvidence
	if err := json.Unmarshal(document[completionContinuationEvidenceKey], &continuation); err != nil {
		return Attempt{}, fmt.Errorf("decode settled completion continuation: %w", err)
	}
	continuation.Request = bytes.TrimSpace(continuation.Request)
	continuation.Payload = bytes.TrimSpace(continuation.Payload)
	if continuation.Version != completionContinuationEvidenceVersion || len(continuation.Request) == 0 ||
		len(continuation.Payload) == 0 || !json.Valid(continuation.Payload) ||
		Fingerprint(continuation.Request) != strings.TrimSpace(requestFingerprint) {
		return Attempt{}, fmt.Errorf("settled completion continuation evidence is missing, invalid, or request-mismatched")
	}
	if err := surface.Validate(); err != nil {
		return Attempt{}, fmt.Errorf("settled completion continuation capability surface: %w", err)
	}
	attempt.completionRequest = append([]byte(nil), continuation.Request...)
	attempt.completionPayload = append(json.RawMessage(nil), continuation.Payload...)
	cloned := surface.Clone()
	attempt.completionSurface = &cloned
	attempt.completionPhase = phase
	return attempt, nil
}

type CompletionContinuationSnapshot struct {
	Request []byte
	Payload json.RawMessage
	Surface managedcapabilities.Surface
	Phase   CompletionProjectionPhase
}

func (a Attempt) CompletionContinuation() (CompletionContinuationSnapshot, bool) {
	if len(a.completionPayload) == 0 || a.completionSurface == nil || !a.completionPhase.Valid() {
		return CompletionContinuationSnapshot{}, false
	}
	return CompletionContinuationSnapshot{
		Request: append([]byte(nil), a.completionRequest...),
		Payload: append(json.RawMessage(nil), a.completionPayload...),
		Surface: a.completionSurface.Clone(),
		Phase:   a.completionPhase,
	}, true
}

type Settlement struct {
	OperationID               string
	AttemptID                 string
	Authority                 Authority
	State                     State
	Failure                   *runtimefailures.Envelope
	Evidence                  map[string]any
	CompletionProjectionPhase CompletionProjectionPhase
	Now                       time.Time
}

type Store interface {
	IsExternalEffectAuthorityCurrent(context.Context, Authority) (bool, error)
	AuthorizeExternalAttempt(context.Context, Authority, AuthorizeRequest) (Attempt, error)
	MarkExternalAttemptLaunched(context.Context, Attempt, time.Time) error
	MarkExternalAttemptResponseObserved(context.Context, Attempt, map[string]any, time.Time) error
	SettleExternalAttempt(context.Context, Settlement) error
}

type CompletionStore interface {
	SettleCompletion(context.Context, Attempt, CompletionSettlement) (CompletionSettlementResult, error)
}

type CompletionContinuationRequest struct {
	Authority            Authority
	Origin               CompletionOrigin
	ExecutionAuthorityID string
	SessionID            string
	Memory               agentmemory.Plan
}

type CompletionConversationProjection struct {
	Payload           json.RawMessage
	SessionID         string
	Identity          agentmemory.Identity
	Memory            agentmemory.Plan
	ExpectedTurnCount int
	TurnCount         int
	Messages          json.RawMessage
}

type CompletionContinuationStore interface {
	RecoverCompletionContinuation(context.Context, CompletionContinuationRequest) (Attempt, bool, error)
	ProjectCompletionConversation(context.Context, Attempt, CompletionConversationProjection) error
	ConsumeCompletionResponse(context.Context, Attempt) error
}

type CompletionHeartbeatStore interface {
	HeartbeatCompletionAttempt(context.Context, Attempt, time.Time, time.Duration) error
}

type RecoverySummary struct {
	PrelaunchTerminal int
	OutcomeUncertain  int
}

type RecoveryRequest struct {
	now              time.Time
	executionPosture executionposture.Posture
}

func NewRecoveryRequest(now time.Time, posture executionposture.Posture) RecoveryRequest {
	return RecoveryRequest{now: now.UTC(), executionPosture: posture}
}

func (r RecoveryRequest) Validate() error {
	if r.now.IsZero() {
		return errors.New("external effect recovery time is required")
	}
	if !r.executionPosture.Valid() {
		return errors.New("external effect recovery execution posture is required")
	}
	return nil
}

func (r RecoveryRequest) Now() time.Time { return r.now }

func (r RecoveryRequest) Admit(mode ExecutionMode) error {
	return r.executionPosture.Admit(mode, "external effect startup recovery")
}

type RecoveryStore interface {
	ReconcileExternalEffectAttempts(context.Context, RecoveryRequest) (RecoverySummary, error)
}

type Controller struct {
	store                       Store
	completionStore             CompletionStore
	completionContinuationStore CompletionContinuationStore
	completionHeartbeatStore    CompletionHeartbeatStore
	completionSpendProjector    CompletionSpendProjector
	executionPosture            executionposture.Posture
}

type controllerContextKey struct{}

func NewController(store Store) *Controller {
	return &Controller{store: store}
}

func NewCompletionController(store Store, completionStore CompletionStore, heartbeatStore CompletionHeartbeatStore, projector CompletionSpendProjector) *Controller {
	continuationStore, _ := completionStore.(CompletionContinuationStore)
	return &Controller{
		store:                       store,
		completionStore:             completionStore,
		completionContinuationStore: continuationStore,
		completionHeartbeatStore:    heartbeatStore,
		completionSpendProjector:    projector,
	}
}

// WithExecutionPosture binds the immutable process ceiling before the
// controller is published to runtime execution contexts.
func (c *Controller) WithExecutionPosture(posture executionposture.Posture) *Controller {
	if c != nil {
		c.executionPosture = posture
	}
	return c
}

func WithController(ctx context.Context, controller *Controller) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, controllerContextKey{}, controller)
}

func ControllerFromContext(ctx context.Context) (*Controller, bool) {
	if ctx == nil {
		return nil, false
	}
	controller, ok := ctx.Value(controllerContextKey{}).(*Controller)
	return controller, ok && controller != nil && controller.Enabled()
}

func (c *Controller) Enabled() bool { return c != nil && c.store != nil }

func (c *Controller) CompletionEnabled() bool {
	if !c.Enabled() {
		return false
	}
	return c.completionHeartbeatStore != nil && c.completionStore != nil && c.completionSpendProjector != nil
}

func (c *Controller) RecoverCompletionContinuation(ctx context.Context, sessionID string, memory agentmemory.Plan) (*Handle, bool, error) {
	if c == nil || c.completionContinuationStore == nil {
		return nil, false, runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "completion_continuation_store_missing", "external-effects", "recover_completion", nil)
	}
	authority, ok := completionAuthorityFromContext(ctx)
	if !ok || authority.Kind != AuthorityNormalAgent {
		return nil, false, nil
	}
	claim, ok := runtimedelivery.ClaimFromContext(ctx)
	if !ok {
		return nil, false, nil
	}
	admission, ok := managedexecution.FromContext(ctx)
	if !ok || !admission.AuthorizesNormal() {
		return nil, false, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_continuation_execution_authority_missing", "external-effects", "recover_completion", nil)
	}
	origin, err := DeliveryCompletionOrigin(claim)
	if err != nil {
		return nil, false, err
	}
	attempt, found, err := c.completionContinuationStore.RecoverCompletionContinuation(context.WithoutCancel(ctx), CompletionContinuationRequest{
		Authority: authority, Origin: origin, ExecutionAuthorityID: admission.ExecutionAuthorityID,
		SessionID: strings.TrimSpace(sessionID), Memory: memory,
	})
	if err != nil || !found {
		return nil, found, err
	}
	return &Handle{controller: c, attempt: attempt}, true, nil
}

func (c *Controller) ExecutionPosture() executionposture.Posture {
	if c == nil {
		return ""
	}
	return c.executionPosture
}

func (c *Controller) IsCurrent(ctx context.Context, token LifecycleToken) (bool, error) {
	if c == nil || c.store == nil {
		return false, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_controller_missing", "external-effects", "check_generation", nil)
	}
	authority := NormalAgentAuthority(token, fmt.Sprintf("agent:%s:%d:%d", token.AgentID, token.RuntimeEpoch, token.Generation), time.Now().UTC().Add(5*time.Minute))
	if mode, found := ExecutionModeFromContext(ctx); found {
		authority.ExecutionMode = mode
	}
	return c.store.IsExternalEffectAuthorityCurrent(ctx, authority)
}

// ProjectionCurrent authorizes successor-facing mutable projections after an
// effect response. Immutable attempt, turn, and spend evidence does not use it.
func ProjectionCurrent(ctx context.Context) (bool, error) {
	if authority, ok := AuthorityFromContext(ctx); ok {
		controller, hasController := ControllerFromContext(ctx)
		if !hasController {
			return false, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_controller_missing", "external-effects", "check_generation", map[string]any{"authority_kind": authority.Kind, "authority_id": authority.ID})
		}
		return controller.store.IsExternalEffectAuthorityCurrent(context.WithoutCancel(ctx), authority)
	}
	token, hasToken := LifecycleTokenFromContext(ctx)
	if !hasToken {
		if _, differentOwner := DifferentOwnerFromContext(ctx); differentOwner {
			return true, nil
		}
		return false, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_token_missing", "external-effects", "check_generation", nil)
	}
	controller, ok := ControllerFromContext(ctx)
	if !ok {
		return false, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_controller_missing", "external-effects", "check_generation", map[string]any{"agent_id": token.AgentID})
	}
	return controller.IsCurrent(context.WithoutCancel(ctx), token)
}

func Fingerprint(raw []byte) string {
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum[:])
}

type Handle struct {
	controller     *Controller
	attempt        Attempt
	differentOwner DifferentOwner
}

func Begin(ctx context.Context, adapter string, request []byte, lineage map[string]string) (*Handle, error) {
	if err := admitExecutionMode(ctx, adapter); err != nil {
		return nil, err
	}
	controller, hasController := ControllerFromContext(ctx)
	existingAuthority, hasExistingAuthority := AuthorityFromContext(ctx)
	token, hasToken := LifecycleTokenFromContext(ctx)
	differentOwner, hasDifferentOwner := DifferentOwnerFromContext(ctx)
	if hasExistingAuthority {
		if hasDifferentOwner {
			return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "external_effect_owner_conflict", "external-effects", "authorize_attempt", map[string]any{"adapter": strings.TrimSpace(adapter), "different_owner": differentOwner})
		}
		if !hasController {
			return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_controller_missing", "external-effects", "authorize_attempt", map[string]any{"adapter": strings.TrimSpace(adapter), "authority_kind": existingAuthority.Kind})
		}
		if existingAuthority.Kind != AuthorityNormalAgent && existingAuthority.Kind != AuthoritySelectedContractFork {
			return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "external_effect_authority_kind_rejected", "external-effects", "authorize_attempt", map[string]any{"adapter": strings.TrimSpace(adapter), "authority_kind": existingAuthority.Kind})
		}
		ctx = WithAuthority(ctx, existingAuthority)
		operationID, err := canonicalOperationID(ctx, existingAuthority, strings.TrimSpace(adapter), lineage)
		if err != nil {
			return nil, err
		}
		surface, err := managedEffectCapabilitySurface(ctx, existingAuthority)
		if err != nil {
			return nil, err
		}
		attempt, err := controller.Authorize(ctx, AuthorizeRequest{OperationID: operationID, Adapter: adapter, RequestFingerprint: Fingerprint(request), CapabilitySurface: &surface, Lineage: lineage})
		if err != nil {
			return nil, err
		}
		return &Handle{controller: controller, attempt: attempt}, nil
	}
	if !hasToken {
		if hasDifferentOwner {
			return &Handle{differentOwner: differentOwner}, nil
		}
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_token_missing", "external-effects", "authorize_attempt", map[string]any{"adapter": strings.TrimSpace(adapter)})
	}
	if hasDifferentOwner {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "external_effect_owner_conflict", "external-effects", "authorize_attempt", map[string]any{"adapter": strings.TrimSpace(adapter), "different_owner": differentOwner})
	}
	if !hasController {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_controller_missing", "external-effects", "authorize_attempt", map[string]any{"adapter": strings.TrimSpace(adapter)})
	}
	authority := NormalAgentAuthority(token, fmt.Sprintf("agent:%s:%d:%d", token.AgentID, token.RuntimeEpoch, token.Generation), time.Now().UTC().Add(5*time.Minute))
	ctx = WithAuthority(ctx, authority)
	surface, err := managedEffectCapabilitySurface(ctx, authority)
	if err != nil {
		return nil, err
	}
	fingerprint := Fingerprint(request)
	operationID, err := canonicalOperationID(ctx, authority, strings.TrimSpace(adapter), lineage)
	if err != nil {
		return nil, err
	}
	attempt, err := controller.Authorize(ctx, AuthorizeRequest{
		OperationID: operationID, Adapter: adapter, RequestFingerprint: fingerprint, CapabilitySurface: &surface, Lineage: lineage,
	})
	if err != nil {
		return nil, err
	}
	return &Handle{controller: controller, attempt: attempt}, nil
}

// BeginServeRegistration authorizes one provider callback-registration write.
// It is deliberately separate from agent capability surfaces and DifferentOwner.
func BeginServeRegistration(ctx context.Context, request []byte, lineage map[string]string) (*Handle, error) {
	const adapter = "provider_registration"
	if err := admitExecutionMode(ctx, adapter); err != nil {
		return nil, err
	}
	if _, differentOwner := DifferentOwnerFromContext(ctx); differentOwner {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "external_effect_owner_conflict", "external-effects", "authorize_serve_registration", nil)
	}
	controller, ok := ControllerFromContext(ctx)
	if !ok {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_controller_missing", "external-effects", "authorize_serve_registration", nil)
	}
	authority, ok := AuthorityFromContext(ctx)
	if !ok || authority.Kind != AuthorityServeRegistration {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "serve_registration_authority_missing", "external-effects", "authorize_serve_registration", nil)
	}
	ctx = WithLogicalOperationIdentitySegment(ctx, "serve-registration:"+authority.ServeRegistration.IntentID)
	operationID, err := canonicalOperationID(ctx, authority, adapter, lineage)
	if err != nil {
		return nil, err
	}
	attempt, err := controller.Authorize(ctx, AuthorizeRequest{
		OperationID: operationID, Adapter: adapter, RequestFingerprint: Fingerprint(request), Lineage: lineage,
	})
	if err != nil {
		return nil, err
	}
	return &Handle{controller: controller, attempt: attempt}, nil
}

func managedEffectCapabilitySurface(ctx context.Context, authority Authority) (managedcapabilities.Surface, error) {
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok || surface.Authority.Kind != managedcapabilities.AuthorityProviderTurn || surface.HasMismatch() {
		return managedcapabilities.Surface{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "managed_effect_capability_surface_missing", "external-effects", "authorize_attempt", map[string]any{"authority_kind": authority.Kind})
	}
	admission, ok := managedexecution.FromContext(ctx)
	if !ok {
		return managedcapabilities.Surface{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "managed_execution_admission_missing", "external-effects", "authorize_attempt", map[string]any{"authority_kind": authority.Kind})
	}
	switch authority.Kind {
	case AuthorityNormalAgent:
		sameSurfaceActor, surfaceActorErr := runtimeagentidentity.Equal(authority.Normal.Identity, surface.ActorIdentity)
		sameTargetActor, targetActorErr := runtimeagentidentity.Equal(authority.Normal.Identity, authority.Target.AgentIdentity)
		if !admission.AuthorizesNormal() || surface.Authority.ExecutionKind != managedcapabilities.ExecutionNormalAgent ||
			surface.Authority.ExecutionAuthorityID != admission.ExecutionAuthorityID ||
			surfaceActorErr != nil || !sameSurfaceActor {
			return managedcapabilities.Surface{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "managed_effect_execution_authority_mismatch", "external-effects", "authorize_attempt", map[string]any{"authority_kind": authority.Kind, "surface_id": surface.ID})
		}
		if targetActorErr != nil || !sameTargetActor {
			return managedcapabilities.Surface{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "managed_effect_turn_identity_mismatch", "external-effects", "authorize_attempt", map[string]any{"authority_kind": authority.Kind, "surface_id": surface.ID})
		}
	case AuthoritySelectedContractFork:
		if !admission.AuthorizesSelected(authority.SelectedFork.ExecutionID, authority.SelectedFork.ForkRunID, authority.SelectedFork.Generation) ||
			surface.Authority.ExecutionKind != managedcapabilities.ExecutionSelectedContractFork ||
			surface.Authority.ExecutionAuthorityID != authority.SelectedFork.ExecutionID ||
			surface.Authority.RunID != authority.SelectedFork.ForkRunID {
			return managedcapabilities.Surface{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "managed_effect_execution_authority_mismatch", "external-effects", "authorize_attempt", map[string]any{"authority_kind": authority.Kind, "surface_id": surface.ID})
		}
	default:
		return managedcapabilities.Surface{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "managed_effect_authority_kind_rejected", "external-effects", "authorize_attempt", map[string]any{"authority_kind": authority.Kind})
	}
	if !ProviderTurnTargetMatchesCapabilitySurface(authority.Target, surface) {
		return managedcapabilities.Surface{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "managed_effect_turn_identity_mismatch", "external-effects", "authorize_attempt", map[string]any{"authority_kind": authority.Kind, "surface_id": surface.ID})
	}
	return surface.Clone(), nil
}

func BeginCompletion(ctx context.Context, adapter string, request []byte, lineage map[string]string) (*Handle, error) {
	return beginCompletion(ctx, adapter, request, nil, lineage)
}

func BeginManagedCompletion(ctx context.Context, adapter string, request []byte, frame agentframe.Frame, lineage map[string]string) (*Handle, error) {
	return beginCompletion(ctx, adapter, request, &frame, lineage)
}

func beginCompletion(ctx context.Context, adapter string, request []byte, frame *agentframe.Frame, lineage map[string]string) (*Handle, error) {
	if err := admitExecutionMode(ctx, adapter); err != nil {
		return nil, err
	}
	if _, differentOwner := DifferentOwnerFromContext(ctx); differentOwner {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "external_effect_owner_conflict", "external-effects", "authorize_attempt", map[string]any{"adapter": strings.TrimSpace(adapter)})
	}
	controller, ok := ControllerFromContext(ctx)
	if !ok {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_controller_missing", "external-effects", "authorize_attempt", map[string]any{"adapter": strings.TrimSpace(adapter)})
	}
	if controller.completionHeartbeatStore == nil {
		return nil, runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "completion_heartbeat_store_missing", "external-effects", "authorize_attempt", map[string]any{"adapter": strings.TrimSpace(adapter)})
	}
	authority, ok := completionAuthorityFromContext(ctx)
	if !ok {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_execution_authority_missing", "external-effects", "authorize_attempt", map[string]any{"adapter": strings.TrimSpace(adapter)})
	}
	if err := authority.ValidateCompletionAdapter(adapter); err != nil {
		return nil, runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "completion_execution_authority_invalid", "external-effects", "authorize_attempt", map[string]any{"adapter": strings.TrimSpace(adapter)}, err)
	}
	ctx = WithAuthority(ctx, authority)
	var capabilitySurface *managedcapabilities.Surface
	var origin CompletionOrigin
	if authority.Target.Kind == UsageTargetAgentTurn {
		surface, err := managedEffectCapabilitySurface(ctx, authority)
		if err != nil {
			return nil, err
		}
		capabilitySurface = &surface
		if authority.Kind == AuthorityNormalAgent {
			claim, hasDelivery := runtimedelivery.ClaimFromContext(ctx)
			directive, hasDirective := directiveCompletionOriginFromContext(ctx)
			if hasDelivery == hasDirective {
				return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_origin_missing_or_ambiguous", "external-effects", "authorize_attempt", map[string]any{"adapter": strings.TrimSpace(adapter)})
			}
			if hasDelivery {
				if claim.SubscriberClass() != runtimedelivery.SubscriberAgent ||
					claim.SubscriberID() != authority.Normal.AgentID || claim.RunID() != authority.Target.RunID {
					return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_origin_delivery_claim_mismatch", "external-effects", "authorize_attempt", map[string]any{"adapter": strings.TrimSpace(adapter), "delivery_id": claim.DeliveryID()})
				}
				origin, err = DeliveryCompletionOrigin(claim)
			} else {
				origin, err = DirectiveCompletionOrigin(directive)
			}
			if err != nil {
				return nil, runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "completion_origin_invalid", "external-effects", "authorize_attempt", map[string]any{"adapter": strings.TrimSpace(adapter)}, err)
			}
		}
	}
	operationID, err := canonicalOperationID(ctx, authority, strings.TrimSpace(adapter), lineage)
	if err != nil {
		return nil, err
	}
	attempt, err := controller.Authorize(ctx, AuthorizeRequest{
		OperationID: operationID, Adapter: adapter, RequestFingerprint: Fingerprint(request),
		CapabilitySurface: capabilitySurface, AgentFrame: frame, Origin: origin, Lineage: lineage,
	})
	if err != nil {
		return nil, err
	}
	return &Handle{controller: controller, attempt: attempt}, nil
}

func BeginStartupProbe(ctx context.Context, adapter string, request []byte, lineage map[string]string) (*Handle, error) {
	controller, ok := ControllerFromContext(ctx)
	if !ok {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_controller_missing", "external-effects", "authorize_startup_probe", map[string]any{"adapter": strings.TrimSpace(adapter)})
	}
	authority, ok := AuthorityFromContext(ctx)
	if !ok || (authority.Kind != AuthorityStartupProbe && authority.Kind != AuthoritySelectedContractFork) {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "startup_probe_authority_missing", "external-effects", "authorize_startup_probe", map[string]any{"adapter": strings.TrimSpace(adapter)})
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok || !startupProbeSurfaceMatchesAuthority(surface, authority) {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "startup_probe_capability_surface_mismatch", "external-effects", "authorize_startup_probe", map[string]any{"adapter": strings.TrimSpace(adapter)})
	}
	ctx = WithLogicalOperationIdentitySegment(ctx, "startup-probe:"+surface.Authority.ID)
	operationID, err := canonicalOperationID(ctx, authority, strings.TrimSpace(adapter), lineage)
	if err != nil {
		return nil, err
	}
	cloned := surface.Clone()
	attempt, err := controller.Authorize(ctx, AuthorizeRequest{
		OperationID: operationID, Adapter: adapter, RequestFingerprint: Fingerprint(request),
		CapabilitySurface: &cloned, Lineage: lineage,
	})
	if err != nil {
		return nil, err
	}
	return &Handle{controller: controller, attempt: attempt}, nil
}

func admitExecutionMode(ctx context.Context, adapter string) error {
	mode, found := ExecutionModeFromContext(ctx)
	if controller, ok := ControllerFromContext(ctx); ok {
		if !controller.executionPosture.Valid() {
			return runtimefailures.New(runtimefailures.ClassAuthorizationDenied, "process_execution_posture_missing", "external-effects", "authorize_attempt", map[string]any{
				"action": "execute_external_effect", "adapter": strings.TrimSpace(adapter),
			})
		}
		if !found {
			return runtimefailures.New(runtimefailures.ClassAuthorizationDenied, "execution_mode_missing", "external-effects", "authorize_attempt", map[string]any{
				"action": "execute_external_effect", "adapter": strings.TrimSpace(adapter), "execution_posture": controller.executionPosture,
			})
		}
		if err := controller.executionPosture.Admit(mode, "external effect authorization"); err != nil {
			return runtimefailures.Wrap(runtimefailures.ClassAuthorizationDenied, "process_execution_posture_rejected", "external-effects", "authorize_attempt", map[string]any{
				"adapter": strings.TrimSpace(adapter), "execution_mode": mode, "execution_posture": controller.executionPosture,
			}, err)
		}
	}
	if !found || mode != ExecutionModeMock || strings.TrimSpace(adapter) == "mock_python" {
		return nil
	}
	registration, registered := RegistrationFor(adapter)
	attributes := map[string]any{"action": "execute_external_effect", "adapter": strings.TrimSpace(adapter), "execution_mode": mode}
	if registered {
		attributes["effect_kind"] = registration.Kind
		attributes["transport"] = registration.Transport
	}
	return runtimefailures.New(runtimefailures.ClassAuthorizationDenied, "mock_external_effect_forbidden", "external-effects", "authorize_attempt", attributes)
}

func canonicalOperationID(ctx context.Context, authority Authority, adapter string, lineage map[string]string) (string, error) {
	identity := logicalOperationIdentity(ctx)
	if identity == "" {
		return "", runtimefailures.New(runtimefailures.ClassLifecycleConflict, "external_effect_logical_identity_missing", "external-effects", "authorize_attempt", map[string]any{"adapter": adapter, "authority_kind": authority.Kind, "authority_id": authority.ID})
	}
	lineageJSON, err := json.Marshal(lineage)
	if err != nil {
		return "", fmt.Errorf("marshal external effect lineage identity: %w", err)
	}
	authorityPrincipal := strings.TrimSpace(authority.ID)
	operationIdentityVersion := "runtime-effect-v2"
	if authority.Kind == AuthorityNormalAgent {
		authorityPrincipal, err = authority.Normal.Identity.Fingerprint()
		if err != nil {
			return "", runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "external_effect_authority_identity_invalid", "external-effects", "authorize_attempt", map[string]any{"adapter": adapter, "authority_kind": authority.Kind}, err)
		}
		operationIdentityVersion = "runtime-effect-v3"
	}
	seed := strings.Join([]string{
		operationIdentityVersion, string(authority.Kind), authorityPrincipal, identity, adapter, string(lineageJSON),
	}, "\x00")
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(seed)).String(), nil
}

func logicalOperationIdentity(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	identity, _ := LogicalOperationIdentityFromContext(ctx)
	if identity == "" {
		if evt, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
			identity = strings.TrimSpace(evt.ID())
		}
	}
	if identity == "" {
		if runtimeLineage, ok := runtimecorrelation.RuntimeLineageFromContext(ctx); ok {
			identity = strings.TrimSpace(runtimeLineage.SubjectEventID)
		}
	}
	return identity
}

func (h *Handle) Attempt() Attempt {
	if h == nil {
		return Attempt{}
	}
	return h.attempt
}

func (h *Handle) CompletionContinuation() (CompletionContinuationSnapshot, bool) {
	if h == nil {
		return CompletionContinuationSnapshot{}, false
	}
	return h.attempt.CompletionContinuation()
}

func (h *Handle) MarkLaunched(ctx context.Context) error {
	if h == nil {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_handle_missing", "external-effects", "launch_attempt", nil)
	}
	if h.differentOwner != "" {
		return nil
	}
	if _, continuation := h.attempt.CompletionContinuation(); continuation {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_continuation_dispatch_forbidden", "external-effects", "launch_attempt", map[string]any{"attempt_id": h.attempt.AttemptID})
	}
	return h.controller.MarkLaunched(ctx, h.attempt)
}

func (h *Handle) Heartbeat(ctx context.Context, lease time.Duration) error {
	if h == nil || h.controller == nil || h.controller.store == nil || h.attempt.AttemptID == "" {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_effect_handle_missing", "llm-completion-authority", "heartbeat_attempt", nil)
	}
	if lease <= 0 {
		return runtimefailures.New(runtimefailures.ClassSchemaInvalid, "completion_heartbeat_lease_invalid", "llm-completion-authority", "heartbeat_attempt", map[string]any{"attempt_id": h.attempt.AttemptID})
	}
	if _, continuation := h.attempt.CompletionContinuation(); continuation {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_continuation_heartbeat_forbidden", "external-effects", "heartbeat_attempt", map[string]any{"attempt_id": h.attempt.AttemptID})
	}
	store := h.controller.completionHeartbeatStore
	if store == nil {
		return runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "completion_heartbeat_store_missing", "llm-completion-authority", "heartbeat_attempt", map[string]any{"attempt_id": h.attempt.AttemptID})
	}
	return store.HeartbeatCompletionAttempt(ctx, h.attempt, time.Now().UTC(), lease)
}

func (h *Handle) MarkResponseObserved(ctx context.Context, evidence map[string]any) error {
	if h == nil || h.controller == nil || h.controller.store == nil {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_handle_missing", "external-effects", "observe_response", nil)
	}
	if h.differentOwner != "" {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_execution_authority_missing", "external-effects", "observe_response", nil)
	}
	if _, continuation := h.attempt.CompletionContinuation(); continuation {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_continuation_observation_forbidden", "external-effects", "observe_response", map[string]any{"attempt_id": h.attempt.AttemptID})
	}
	return h.controller.MarkResponseObserved(ctx, h.attempt, evidence)
}

func (h *Handle) Settle(ctx context.Context, state State, failure *runtimefailures.Envelope, evidence map[string]any) error {
	if h == nil {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_handle_missing", "external-effects", "settle_attempt", nil)
	}
	if h.differentOwner != "" {
		return nil
	}
	if _, continuation := h.attempt.CompletionContinuation(); continuation {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_continuation_settlement_forbidden", "external-effects", "settle_attempt", map[string]any{"attempt_id": h.attempt.AttemptID})
	}
	return h.controller.Settle(ctx, Settlement{
		OperationID: h.attempt.OperationID, AttemptID: h.attempt.AttemptID,
		Authority: h.attempt.Authority,
		State:     state, Failure: failure, Evidence: evidence,
	})
}

func (h *Handle) Succeed(ctx context.Context, evidence map[string]any) error {
	return h.Settle(ctx, StateSettled, nil, evidence)
}

func (h *Handle) SettleCompletion(ctx context.Context, settlement CompletionSettlement) (CompletionSettlementResult, error) {
	if h == nil || h.controller == nil || h.controller.store == nil {
		return CompletionSettlementResult{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_effect_handle_missing", "llm-completion-authority", "settle_completion", nil)
	}
	store := h.controller.completionStore
	if store == nil {
		return CompletionSettlementResult{}, runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "completion_settlement_store_missing", "llm-completion-authority", "settle_completion", nil)
	}
	if _, continuation := h.attempt.CompletionContinuation(); continuation {
		return CompletionSettlementResult{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_continuation_resettlement_forbidden", "llm-completion-authority", "settle_completion", map[string]any{"attempt_id": h.attempt.AttemptID})
	}
	settlement.Settlement.OperationID = h.attempt.OperationID
	settlement.Settlement.AttemptID = h.attempt.AttemptID
	settlement.Settlement.Authority = h.attempt.Authority
	if settlement.Now.IsZero() {
		settlement.Now = time.Now().UTC()
	}
	settlement.Settlement.Now = settlement.Now
	if settlement.Settlement.State == StateSettled && h.attempt.Authority.Kind == AuthorityNormalAgent &&
		h.attempt.Origin.Kind == CompletionOriginDelivery {
		if _, ok := settlement.Settlement.Evidence[completionContinuationEvidenceKey]; ok {
			settlement.Settlement.CompletionProjectionPhase = CompletionProjectionResponseSettled
		}
	}
	if err := settlement.Validate(h.attempt); err != nil {
		return CompletionSettlementResult{}, runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "completion_settlement_invalid", "llm-completion-authority", "settle_completion", map[string]any{"validation_error": err.Error()}, err)
	}
	result, err := store.SettleCompletion(context.WithoutCancel(ctx), h.attempt, settlement)
	if result.Committed && !result.Disposition.Valid() {
		return CompletionSettlementResult{}, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "completion_settlement_disposition_invalid", "llm-completion-authority", "settle_completion", map[string]any{"attempt_id": h.attempt.AttemptID, "disposition": result.Disposition})
	}
	if result.Committed {
		recordCompletionSettlementObservation(ctx, CompletionSettlementObservation{
			AttemptID: result.AttemptID, Disposition: result.Disposition, Origin: result.Origin,
			OriginSettled: result.OriginSettled, Finalization: result.Finalization,
		})
	}
	if result.Committed && result.SpendRecorded && h.controller.completionSpendProjector != nil {
		h.controller.completionSpendProjector.ProjectCommittedCompletionSpend(context.WithoutCancel(ctx), CompletionSpendProjection{
			AttemptID: result.AttemptID,
			EntityID:  result.EntityID,
		})
	}
	if result.Committed && result.continuation != nil {
		h.attempt = *result.continuation
	}
	return result, err
}

func (h *Handle) ProjectCompletionConversation(ctx context.Context, projection CompletionConversationProjection) error {
	if h == nil || h.controller == nil || h.controller.completionContinuationStore == nil {
		return runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "completion_continuation_store_missing", "external-effects", "project_completion", nil)
	}
	return h.controller.completionContinuationStore.ProjectCompletionConversation(context.WithoutCancel(ctx), h.attempt, projection)
}

func (h *Handle) ConsumeCompletionResponse(ctx context.Context) error {
	if h == nil || h.controller == nil || h.controller.completionContinuationStore == nil {
		return runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "completion_continuation_store_missing", "external-effects", "consume_completion", nil)
	}
	return h.controller.completionContinuationStore.ConsumeCompletionResponse(context.WithoutCancel(ctx), h.attempt)
}

func (h *Handle) Fail(ctx context.Context, state State, class runtimefailures.Class, code, component, operation string, attributes map[string]any, cause error) error {
	if h == nil {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_handle_missing", "external-effects", "settle_attempt", nil)
	}
	if h.differentOwner != "" {
		return cause
	}
	var failureErr error
	var failure *runtimefailures.Envelope
	if cause == nil {
		failureErr = runtimefailures.New(class, code, component, operation, attributes)
		envelope, _ := runtimefailures.EnvelopeFromError(failureErr)
		failure = &envelope
	} else {
		failureErr = runtimefailures.Wrap(class, code, component, operation, attributes, cause)
		envelope, _ := runtimefailures.EnvelopeFromError(failureErr)
		failure = &envelope
	}
	if err := h.Settle(ctx, state, failure, attributes); err != nil {
		return errors.Join(failureErr, err)
	}
	return failureErr
}

func (c *Controller) Authorize(ctx context.Context, req AuthorizeRequest) (Attempt, error) {
	if c == nil || c.store == nil {
		return Attempt{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_controller_missing", "external-effects", "authorize_attempt", nil)
	}
	registration, ok := RegistrationFor(req.Adapter)
	if !ok {
		return Attempt{}, fmt.Errorf("external effect adapter %q is not registered", strings.TrimSpace(req.Adapter))
	}
	if req.Kind == "" {
		req.Kind = registration.Kind
	}
	if req.Class == "" {
		req.Class = registration.Class
	}
	if req.Transport == "" {
		req.Transport = registration.Transport
	}
	if req.Kind != registration.Kind || req.Class != registration.Class || req.Transport != registration.Transport {
		return Attempt{}, fmt.Errorf("external effect adapter %q registration mismatch", req.Adapter)
	}
	if strings.TrimSpace(req.RequestFingerprint) == "" {
		return Attempt{}, fmt.Errorf("external effect request fingerprint is required")
	}
	authority, ok := completionAuthorityFromContext(ctx)
	if !ok {
		return Attempt{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "external_effect_authority_missing", "external-effects", "authorize_attempt", map[string]any{"adapter": req.Adapter})
	}
	if !c.executionPosture.Valid() {
		return Attempt{}, runtimefailures.New(runtimefailures.ClassAuthorizationDenied, "process_execution_posture_missing", "external-effects", "authorize_attempt", map[string]any{
			"action": "execute_external_effect", "adapter": strings.TrimSpace(req.Adapter),
		})
	}
	if err := c.executionPosture.Admit(authority.ExecutionMode, "external effect authorization"); err != nil {
		return Attempt{}, runtimefailures.Wrap(runtimefailures.ClassAuthorizationDenied, "process_execution_posture_rejected", "external-effects", "authorize_attempt", map[string]any{
			"adapter": strings.TrimSpace(req.Adapter), "execution_mode": authority.ExecutionMode, "execution_posture": c.executionPosture,
		}, err)
	}
	if registration.Kind == KindProviderTurn {
		if err := authority.ValidateCompletionAdapter(req.Adapter); err != nil {
			return Attempt{}, runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "completion_execution_authority_invalid", "external-effects", "authorize_attempt", map[string]any{"adapter": req.Adapter}, err)
		}
		if authority.Target.Kind == UsageTargetAgentTurn {
			if req.CapabilitySurface == nil {
				return Attempt{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "managed_capability_surface_missing", "external-effects", "authorize_attempt", map[string]any{"adapter": req.Adapter})
			}
			validated, err := managedEffectCapabilitySurface(managedcapabilities.WithContext(ctx, *req.CapabilitySurface), authority)
			if err != nil {
				return Attempt{}, err
			}
			req.CapabilitySurface = &validated
			if req.AgentFrame == nil {
				return Attempt{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "agent_execution_frame_missing_or_mismatched", "external-effects", "authorize_attempt", map[string]any{"adapter": req.Adapter})
			}
			if err := validateManagedAgentFramePrelaunch(ctx, *req.AgentFrame, authority, validated); err != nil {
				return Attempt{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "agent_execution_frame_authority_mismatch", "external-effects", "authorize_attempt", map[string]any{
					"adapter": req.Adapter, "validation_error": err.Error(),
				})
			}
			if authority.Kind == AuthorityNormalAgent {
				if err := req.Origin.Validate(); err != nil {
					return Attempt{}, runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "completion_origin_missing_or_ambiguous", "external-effects", "authorize_attempt", map[string]any{"adapter": req.Adapter}, err)
				}
				if req.Origin.Kind == CompletionOriginDelivery && (req.Origin.Delivery.SubscriberClass() != runtimedelivery.SubscriberAgent ||
					req.Origin.Delivery.SubscriberID() != authority.Normal.AgentID || req.Origin.Delivery.RunID() != authority.Target.RunID) {
					return Attempt{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_origin_delivery_claim_mismatch", "external-effects", "authorize_attempt", map[string]any{"adapter": req.Adapter, "delivery_id": req.Origin.Delivery.DeliveryID()})
				}
			} else if req.Origin.Validate() == nil {
				return Attempt{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_origin_delivery_claim_owner_mismatch", "external-effects", "authorize_attempt", map[string]any{"adapter": req.Adapter, "authority_kind": authority.Kind})
			}
		} else if req.CapabilitySurface != nil || req.AgentFrame != nil {
			return Attempt{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "managed_capability_surface_owner_mismatch", "external-effects", "authorize_attempt", map[string]any{"adapter": req.Adapter, "target_kind": authority.Target.Kind})
		}
	} else if registration.Kind == KindProviderStartupProbe {
		if req.AgentFrame != nil || req.CapabilitySurface == nil || !startupProbeSurfaceMatchesAuthority(*req.CapabilitySurface, authority) {
			return Attempt{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "startup_probe_authority_invalid", "external-effects", "authorize_attempt", map[string]any{"adapter": req.Adapter})
		}
	} else if registration.Kind == KindServeRegistration {
		if authority.Kind != AuthorityServeRegistration || req.CapabilitySurface != nil || req.AgentFrame != nil {
			return Attempt{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "serve_registration_authority_invalid", "external-effects", "authorize_attempt", map[string]any{"adapter": req.Adapter})
		}
	} else {
		if req.AgentFrame != nil {
			return Attempt{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "agent_execution_frame_owner_mismatch", "external-effects", "authorize_attempt", map[string]any{"adapter": req.Adapter})
		}
		if req.CapabilitySurface == nil {
			return Attempt{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "managed_effect_capability_surface_missing", "external-effects", "authorize_attempt", map[string]any{"adapter": req.Adapter, "authority_kind": authority.Kind})
		}
		if _, err := managedEffectCapabilitySurface(managedcapabilities.WithContext(ctx, *req.CapabilitySurface), authority); err != nil {
			return Attempt{}, err
		}
	}
	if req.OperationID == "" {
		return Attempt{}, fmt.Errorf("external effect logical operation id is required")
	}
	if req.AttemptID == "" {
		var err error
		req.AttemptID, err = AttemptID(req.OperationID, 1)
		if err != nil {
			return Attempt{}, err
		}
	}
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	}
	return c.store.AuthorizeExternalAttempt(ctx, authority, req)
}

func startupProbeSurfaceMatchesAuthority(surface managedcapabilities.Surface, authority Authority) bool {
	if surface.Validate() != nil || surface.Authority.Kind != managedcapabilities.AuthorityStartupProbe {
		return false
	}
	switch authority.Kind {
	case AuthorityStartupProbe:
		return surface.Authority.ID == authority.StartupProbe.ProbeID &&
			surface.Authority.ExecutionKind == managedcapabilities.ExecutionKind(authority.StartupProbe.ExecutionKind) &&
			surface.Authority.ExecutionAuthorityID == authority.StartupProbe.ExecutionAuthorityID
	case AuthoritySelectedContractFork:
		return surface.Authority.ExecutionKind == managedcapabilities.ExecutionSelectedContractFork &&
			surface.Authority.ExecutionAuthorityID == authority.SelectedFork.ExecutionID &&
			surface.Authority.RunID == authority.SelectedFork.ForkRunID &&
			surface.Authority.StartupOwnerID == authority.ExecutionOwner &&
			surface.Authority.StartupGeneration == authority.SelectedFork.Generation
	default:
		return false
	}
}

func AttemptID(operationID string, ordinal int) (string, error) {
	if ordinal <= 0 {
		return "", fmt.Errorf("external effect attempt ordinal must be positive")
	}
	operationUUID, err := uuid.Parse(operationID)
	if err != nil {
		return "", fmt.Errorf("parse external effect logical operation id: %w", err)
	}
	return uuid.NewSHA1(operationUUID, []byte(fmt.Sprintf("attempt:%d", ordinal))).String(), nil
}

func (c *Controller) MarkLaunched(ctx context.Context, attempt Attempt) error {
	if c == nil || c.store == nil || attempt.AttemptID == "" {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_controller_missing", "external-effects", "launch_attempt", nil)
	}
	return c.store.MarkExternalAttemptLaunched(context.WithoutCancel(ctx), attempt, time.Now().UTC())
}

func (c *Controller) MarkResponseObserved(ctx context.Context, attempt Attempt, evidence map[string]any) error {
	if c == nil || c.store == nil || attempt.AttemptID == "" {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_controller_missing", "external-effects", "observe_response", nil)
	}
	return c.store.MarkExternalAttemptResponseObserved(context.WithoutCancel(ctx), attempt, evidence, time.Now().UTC())
}

func (c *Controller) Settle(ctx context.Context, settlement Settlement) error {
	if c == nil || c.store == nil || settlement.AttemptID == "" {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_controller_missing", "external-effects", "settle_attempt", nil)
	}
	if settlement.Now.IsZero() {
		settlement.Now = time.Now().UTC()
	}
	if settlement.State != StateSettled && settlement.State != StateTerminalFailure && settlement.State != StateOutcomeUncertain {
		return fmt.Errorf("unsupported external effect settlement state %q", settlement.State)
	}
	return c.store.SettleExternalAttempt(context.WithoutCancel(ctx), settlement)
}

func EvidenceJSON(evidence map[string]any) json.RawMessage {
	if len(evidence) == 0 {
		return json.RawMessage(`{}`)
	}
	raw, err := json.Marshal(evidence)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}
