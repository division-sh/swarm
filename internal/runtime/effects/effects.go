package effects

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
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
	KindHTTPToolTarget        Kind = "http_tool_target"
	KindManagedCredential     Kind = "managed_credential_request"
	KindNativeWebSearchHTTP   Kind = "native_web_search_http"
	KindMCPHTTPRequest        Kind = "mcp_http_request"
	KindMCPStdioRequest       Kind = "mcp_stdio_request"
	KindNativeCommand         Kind = "native_command"
	KindNativeFileWrite       Kind = "native_file_write"
	KindToolResultRelay       Kind = "tool_result_relay"
	KindClaudeToolResultRelay Kind = "claude_tool_result_relay"
)

type LifecycleToken struct {
	RuntimeEpoch int64  `json:"runtime_epoch"`
	AgentID      string `json:"agent_id"`
	Generation   uint64 `json:"generation"`
}

func (t LifecycleToken) Valid() bool {
	return t.RuntimeEpoch > 0 && strings.TrimSpace(t.AgentID) != "" && t.Generation > 0
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
	registration(KindProviderTurn, EffectWriteOrUnknown, "anthropic_api", "http", "internal/runtime/llm/api_runtime.go", []string{"internal/runtime/llm/api_runtime.go:sendRequest:http_do:1"}, "TestManagedProviderEffectOutcomes/anthropic_api"),
	registration(KindProviderTurn, EffectWriteOrUnknown, "openai_compatible", "http", "internal/runtime/llm/openai_compatible_runtime.go", []string{"internal/runtime/llm/openai_compatible_runtime.go:sendRequest:http_do:1"}, "TestManagedProviderEffectOutcomes/openai_compatible"),
	registration(KindProviderTurn, EffectWriteOrUnknown, "openai_responses", "http", "internal/runtime/llm/openai_responses_runtime.go", []string{"internal/runtime/llm/openai_responses_runtime.go:sendRequest:http_do:1"}, "TestManagedProviderEffectOutcomes/openai_responses"),
	registration(KindProviderTurn, EffectWriteOrUnknown, "claude_cli", "process", "internal/runtime/llm/cli_runtime_process.go", []string{"internal/runtime/llm/cli_runtime_process.go:runWithPreparedInput:process_launch:1", "internal/runtime/llm/cli_runtime_process.go:runStreamingPrepared:process_launch:1"}, "TestManagedClaudeCLIEffectOutcomes"),
	registration(KindHTTPToolTarget, EffectWriteOrUnknown, "authored_http_tool", "http", "internal/runtime/tools/executor_http.go", []string{"internal/runtime/tools/executor_http.go:execHTTPRequestOnce:http_do:1"}, "TestManagedToolEffectOutcomes/authored_http_tool"),
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
	Lineage            map[string]string
	Now                time.Time
}

type Attempt struct {
	OperationID  string
	AttemptID    string
	Token        LifecycleToken
	Authority    Authority
	Kind         Kind
	Class        EffectClass
	Adapter      string
	Transport    string
	Ordinal      int
	AuthorizedAt time.Time
}

type Settlement struct {
	OperationID string
	AttemptID   string
	Authority   Authority
	State       State
	Failure     *runtimefailures.Envelope
	Evidence    map[string]any
	Now         time.Time
}

type Store interface {
	IsExternalEffectAuthorityCurrent(context.Context, Authority) (bool, error)
	AuthorizeExternalAttempt(context.Context, Authority, AuthorizeRequest) (Attempt, error)
	MarkExternalAttemptLaunched(context.Context, Attempt, time.Time) error
	MarkExternalAttemptResponseObserved(context.Context, Attempt, map[string]any, time.Time) error
	SettleExternalAttempt(context.Context, Settlement) error
}

type CompletionStore interface {
	SettleCompletion(context.Context, Attempt, CompletionSettlement) error
}

type CompletionHeartbeatStore interface {
	HeartbeatCompletionAttempt(context.Context, Attempt, time.Time, time.Duration) error
}

type RecoverySummary struct {
	PrelaunchTerminal int
	OutcomeUncertain  int
}

type RecoveryStore interface {
	ReconcileExternalEffectAttempts(context.Context, time.Time) (RecoverySummary, error)
}

type Controller struct {
	store Store
}

type controllerContextKey struct{}

func NewController(store Store) *Controller {
	return &Controller{store: store}
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
	_, canHeartbeat := c.store.(CompletionHeartbeatStore)
	_, canSettle := c.store.(CompletionStore)
	return canHeartbeat && canSettle
}

func (c *Controller) IsCurrent(ctx context.Context, token LifecycleToken) (bool, error) {
	if c == nil || c.store == nil {
		return false, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_controller_missing", "external-effects", "check_generation", nil)
	}
	authority := NormalAgentAuthority(token, fmt.Sprintf("agent:%s:%d:%d", token.AgentID, token.RuntimeEpoch, token.Generation), time.Now().UTC().Add(5*time.Minute))
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
	controller, hasController := ControllerFromContext(ctx)
	token, hasToken := LifecycleTokenFromContext(ctx)
	differentOwner, hasDifferentOwner := DifferentOwnerFromContext(ctx)
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
	fingerprint := Fingerprint(request)
	operationID, err := canonicalOperationID(ctx, authority, strings.TrimSpace(adapter), lineage)
	if err != nil {
		return nil, err
	}
	attempt, err := controller.Authorize(ctx, AuthorizeRequest{
		OperationID: operationID, Adapter: adapter, RequestFingerprint: fingerprint, Lineage: lineage,
	})
	if err != nil {
		return nil, err
	}
	return &Handle{controller: controller, attempt: attempt}, nil
}

func BeginCompletion(ctx context.Context, adapter string, request []byte, lineage map[string]string) (*Handle, error) {
	if _, differentOwner := DifferentOwnerFromContext(ctx); differentOwner {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "external_effect_owner_conflict", "external-effects", "authorize_attempt", map[string]any{"adapter": strings.TrimSpace(adapter)})
	}
	controller, ok := ControllerFromContext(ctx)
	if !ok {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_controller_missing", "external-effects", "authorize_attempt", map[string]any{"adapter": strings.TrimSpace(adapter)})
	}
	if _, ok := controller.store.(CompletionHeartbeatStore); !ok {
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
	operationID, err := canonicalOperationID(ctx, authority, strings.TrimSpace(adapter), lineage)
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

func canonicalOperationID(ctx context.Context, authority Authority, adapter string, lineage map[string]string) (string, error) {
	identity := logicalOperationIdentity(ctx)
	if identity == "" {
		return "", runtimefailures.New(runtimefailures.ClassLifecycleConflict, "external_effect_logical_identity_missing", "external-effects", "authorize_attempt", map[string]any{"adapter": adapter, "authority_kind": authority.Kind, "authority_id": authority.ID})
	}
	lineageJSON, err := json.Marshal(lineage)
	if err != nil {
		return "", fmt.Errorf("marshal external effect lineage identity: %w", err)
	}
	seed := strings.Join([]string{
		"runtime-effect-v2", string(authority.Kind), authority.ID, identity, adapter, string(lineageJSON),
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

func (h *Handle) MarkLaunched(ctx context.Context) error {
	if h == nil {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_handle_missing", "external-effects", "launch_attempt", nil)
	}
	if h.differentOwner != "" {
		return nil
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
	store, ok := h.controller.store.(CompletionHeartbeatStore)
	if !ok {
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
	return h.controller.MarkResponseObserved(ctx, h.attempt, evidence)
}

func (h *Handle) Settle(ctx context.Context, state State, failure *runtimefailures.Envelope, evidence map[string]any) error {
	if h == nil {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_handle_missing", "external-effects", "settle_attempt", nil)
	}
	if h.differentOwner != "" {
		return nil
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

func (h *Handle) SettleCompletion(ctx context.Context, settlement CompletionSettlement) error {
	if h == nil || h.controller == nil || h.controller.store == nil {
		return runtimefailures.New(runtimefailures.ClassLifecycleConflict, "completion_effect_handle_missing", "llm-completion-authority", "settle_completion", nil)
	}
	store, ok := h.controller.store.(CompletionStore)
	if !ok {
		return runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "completion_settlement_store_missing", "llm-completion-authority", "settle_completion", nil)
	}
	settlement.Settlement.OperationID = h.attempt.OperationID
	settlement.Settlement.AttemptID = h.attempt.AttemptID
	settlement.Settlement.Authority = h.attempt.Authority
	if settlement.Now.IsZero() {
		settlement.Now = time.Now().UTC()
	}
	settlement.Settlement.Now = settlement.Now
	if err := settlement.Validate(h.attempt); err != nil {
		return runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "completion_settlement_invalid", "llm-completion-authority", "settle_completion", map[string]any{"validation_error": err.Error()}, err)
	}
	return store.SettleCompletion(context.WithoutCancel(ctx), h.attempt, settlement)
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
	if registration.Kind == KindProviderTurn {
		if err := authority.ValidateCompletionAdapter(req.Adapter); err != nil {
			return Attempt{}, runtimefailures.Wrap(runtimefailures.ClassLifecycleConflict, "completion_execution_authority_invalid", "external-effects", "authorize_attempt", map[string]any{"adapter": req.Adapter}, err)
		}
	} else if authority.Kind != AuthorityNormalAgent {
		return Attempt{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "external_effect_authority_kind_rejected", "external-effects", "authorize_attempt", map[string]any{
			"adapter": req.Adapter, "authority_kind": authority.Kind,
		})
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
