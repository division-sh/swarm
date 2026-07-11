package effects

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"

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
}

var registrations = []Registration{
	registration(KindProviderTurn, EffectWriteOrUnknown, "anthropic_api", "http", "internal/runtime/llm/api_runtime.go"),
	registration(KindProviderTurn, EffectWriteOrUnknown, "openai_compatible", "http", "internal/runtime/llm/openai_compatible_runtime.go"),
	registration(KindProviderTurn, EffectWriteOrUnknown, "openai_responses", "http", "internal/runtime/llm/openai_responses_runtime.go"),
	registration(KindProviderTurn, EffectWriteOrUnknown, "claude_cli", "process", "internal/runtime/llm/cli_runtime_process.go"),
	registration(KindHTTPToolTarget, EffectWriteOrUnknown, "authored_http_tool", "http", "internal/runtime/tools/executor_http.go"),
	registration(KindManagedCredential, EffectWriteOrUnknown, "managed_credential", "http", "internal/runtime/managedcredentials/store.go"),
	registration(KindNativeWebSearchHTTP, EffectReadOnly, "native_web_search", "http", "internal/runtime/tools/executor_native.go"),
	registration(KindMCPHTTPRequest, EffectWriteOrUnknown, "mcp_tools_call_http", "http", "internal/runtime/mcp/client.go"),
	registration(KindMCPStdioRequest, EffectWriteOrUnknown, "mcp_tools_call_stdio", "stdio", "internal/runtime/mcp/client.go"),
	registration(KindNativeCommand, EffectWriteOrUnknown, "native_bash", "process", "internal/runtime/tools/executor_native.go"),
	registration(KindNativeCommand, EffectReadOnly, "native_read_file", "process", "internal/runtime/tools/executor_native.go"),
	registration(KindNativeFileWrite, EffectWriteOrUnknown, "native_write_file", "filesystem", "internal/runtime/tools/executor_native.go"),
	registration(KindToolResultRelay, EffectWriteOrUnknown, "tool_result_relay", "filesystem", "internal/runtime/tools/tool_result_relay.go"),
	registration(KindClaudeToolResultRelay, EffectWriteOrUnknown, "claude_tool_result_relay", "process", "internal/runtime/llm/cli_tool_result_relay.go"),
}

func registration(kind Kind, class EffectClass, adapter, transport, launchSite string) Registration {
	return Registration{
		Kind: kind, Class: class, Adapter: adapter, Transport: transport, LaunchSite: launchSite,
		LaunchObserved:     "adapter marks the durable attempt immediately before the primitive launch",
		OutcomeMapping:     "adapter maps every post-launch result through its effect-aware outcome table",
		CanonicalEvidence:  "durable operation/attempt evidence keyed by lifecycle generation",
		SettlementRecovery: "same attempt settles once; recovery never redispatches",
		Proof:              "adapter-specific stale-before-launch and post-launch outcome tests",
	}
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
	State       State
	Failure     *runtimefailures.Envelope
	Evidence    map[string]any
	Now         time.Time
}

type Store interface {
	IsLifecycleTokenCurrent(context.Context, LifecycleToken) (bool, error)
	AuthorizeExternalAttempt(context.Context, LifecycleToken, AuthorizeRequest) (Attempt, error)
	MarkExternalAttemptLaunched(context.Context, Attempt, time.Time) error
	SettleExternalAttempt(context.Context, Settlement) error
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

func (c *Controller) IsCurrent(ctx context.Context, token LifecycleToken) (bool, error) {
	if c == nil || c.store == nil {
		return false, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_controller_missing", "external-effects", "check_generation", nil)
	}
	return c.store.IsLifecycleTokenCurrent(ctx, token)
}

// ProjectionCurrent authorizes successor-facing mutable projections after an
// effect response. Immutable attempt, turn, and spend evidence does not use it.
func ProjectionCurrent(ctx context.Context) (bool, error) {
	token, hasToken := LifecycleTokenFromContext(ctx)
	if !hasToken {
		return true, nil
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
	controller *Controller
	attempt    Attempt
}

func Begin(ctx context.Context, adapter string, request []byte, lineage map[string]string) (*Handle, error) {
	controller, hasController := ControllerFromContext(ctx)
	_, hasToken := LifecycleTokenFromContext(ctx)
	if !hasToken {
		return nil, nil
	}
	if !hasController {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_controller_missing", "external-effects", "authorize_attempt", map[string]any{"adapter": strings.TrimSpace(adapter)})
	}
	attempt, err := controller.Authorize(ctx, AuthorizeRequest{
		Adapter: adapter, RequestFingerprint: Fingerprint(request), Lineage: lineage,
	})
	if err != nil {
		return nil, err
	}
	return &Handle{controller: controller, attempt: attempt}, nil
}

func (h *Handle) Attempt() Attempt {
	if h == nil {
		return Attempt{}
	}
	return h.attempt
}

func (h *Handle) MarkLaunched(ctx context.Context) error {
	if h == nil {
		return nil
	}
	return h.controller.MarkLaunched(ctx, h.attempt)
}

func (h *Handle) Settle(ctx context.Context, state State, failure *runtimefailures.Envelope, evidence map[string]any) error {
	if h == nil {
		return nil
	}
	return h.controller.Settle(ctx, Settlement{
		OperationID: h.attempt.OperationID, AttemptID: h.attempt.AttemptID,
		State: state, Failure: failure, Evidence: evidence,
	})
}

func (h *Handle) Succeed(ctx context.Context, evidence map[string]any) error {
	return h.Settle(ctx, StateSettled, nil, evidence)
}

func (h *Handle) Fail(ctx context.Context, state State, class runtimefailures.Class, code, component, operation string, attributes map[string]any, cause error) error {
	if h == nil {
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
		return err
	}
	return failureErr
}

func (c *Controller) Authorize(ctx context.Context, req AuthorizeRequest) (Attempt, error) {
	if c == nil || c.store == nil {
		return Attempt{}, nil
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
	token, ok := LifecycleTokenFromContext(ctx)
	if !ok {
		return Attempt{}, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_token_missing", "external-effects", "authorize_attempt", map[string]any{"adapter": req.Adapter})
	}
	if req.OperationID == "" {
		req.OperationID = uuid.NewString()
	}
	if req.AttemptID == "" {
		req.AttemptID = uuid.NewString()
	}
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	}
	return c.store.AuthorizeExternalAttempt(ctx, token, req)
}

func (c *Controller) MarkLaunched(ctx context.Context, attempt Attempt) error {
	if c == nil || c.store == nil || attempt.AttemptID == "" {
		return nil
	}
	return c.store.MarkExternalAttemptLaunched(context.WithoutCancel(ctx), attempt, time.Now().UTC())
}

func (c *Controller) Settle(ctx context.Context, settlement Settlement) error {
	if c == nil || c.store == nil || settlement.AttemptID == "" {
		return nil
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
