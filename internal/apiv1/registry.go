package apiv1

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"swarm/internal/apispec"
)

const (
	MethodUnavailableCode                = "METHOD_UNAVAILABLE"
	BundleMismatchCode                   = "BUNDLE_MISMATCH"
	UnsupportedBundleRefCode             = "UNSUPPORTED_BUNDLE_REF"
	EventNotDeclaredCode                 = "EVENT_NOT_DECLARED"
	EventNotFoundCode                    = "EVENT_NOT_FOUND"
	EventPublishFailedCode               = "EVENT_PUBLISH_FAILED"
	EventReplayNoDeliveryHistoryCode     = "EVENT_REPLAY_NO_DELIVERY_HISTORY"
	EventReplaySubscriberNotOriginalCode = "EVENT_REPLAY_SUBSCRIBER_NOT_ORIGINAL"
	EventReplaySubscriberUnavailableCode = "EVENT_REPLAY_SUBSCRIBER_UNAVAILABLE"
	EventReplayNotEligibleCode           = "EVENT_REPLAY_NOT_ELIGIBLE"
	EntityNotFoundCode                   = "ENTITY_NOT_FOUND"
	AgentNotFoundCode                    = "AGENT_NOT_FOUND"
	AgentNotRunningCode                  = "AGENT_NOT_RUNNING"
	SessionNotFoundCode                  = "SESSION_NOT_FOUND"
	TurnNotFoundCode                     = "TURN_NOT_FOUND"
	ForkNotFoundCode                     = "FORK_NOT_FOUND"
	PayloadValidationFailedCode          = "PAYLOAD_VALIDATION_FAILED"
	RunNotFoundCode                      = "RUN_NOT_FOUND"
	RunAlreadyTerminalCode               = "RUN_ALREADY_TERMINAL"
	AmbiguousRunTargetCode               = "AMBIGUOUS_RUN_TARGET"
	RunNotPausedCode                     = "RUN_NOT_PAUSED"
	RunAlreadyPausedCode                 = "RUN_ALREADY_PAUSED"
	MailboxNotFoundCode                  = "MAILBOX_NOT_FOUND"
	MailboxAlreadyDecidedCode            = "MAILBOX_ALREADY_DECIDED"
	MailboxApprovalEventUnconfiguredCode = "MAILBOX_APPROVAL_EVENT_UNCONFIGURED"
	InvalidDeferUntilCode                = "INVALID_DEFER_UNTIL"
	IdempotencyConflictCode              = "IDEMPOTENCY_CONFLICT"
	RuntimeAlreadyPausedCode             = "RUNTIME_ALREADY_PAUSED"
	RuntimeNotPausedCode                 = "RUNTIME_NOT_PAUSED"
	RuntimeNukeInProgressCode            = "RUNTIME_NUKE_IN_PROGRESS"
)

type Registry struct {
	api        *apispec.APISpecification
	methods    map[string]apispec.Method
	errorCodes map[string]int
}

func LoadRegistry(platformSpecPath string) (*Registry, error) {
	api, err := apispec.LoadPlatformSpec(strings.TrimSpace(platformSpecPath))
	if err != nil {
		return nil, err
	}
	if _, err := apispec.Validate(api); err != nil {
		return nil, err
	}
	return NewRegistry(api)
}

func NewRegistry(api *apispec.APISpecification) (*Registry, error) {
	if _, err := apispec.Validate(api); err != nil {
		return nil, err
	}
	methods := make(map[string]apispec.Method, len(api.MethodCatalog))
	for name, method := range api.MethodCatalog {
		methods[strings.TrimSpace(name)] = method
	}
	errorCodes := apispec.ApplicationErrorCodes(api.Components.Errors)
	for _, required := range []string{MethodUnavailableCode, RunNotFoundCode} {
		if _, ok := errorCodes[required]; !ok {
			return nil, fmt.Errorf("api specification missing components.errors.%s", required)
		}
	}
	return &Registry{
		api:        api,
		methods:    methods,
		errorCodes: errorCodes,
	}, nil
}

func (r *Registry) Method(name string) (apispec.Method, bool) {
	if r == nil {
		return apispec.Method{}, false
	}
	method, ok := r.methods[strings.TrimSpace(name)]
	return method, ok
}

func (r *Registry) MethodNames() []string {
	if r == nil {
		return nil
	}
	names := make([]string, 0, len(r.methods))
	for name := range r.methods {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r *Registry) ApplicationErrorCode(code string) (int, bool) {
	if r == nil {
		return 0, false
	}
	numeric, ok := r.errorCodes[strings.TrimSpace(code)]
	return numeric, ok
}

func (r *Registry) MailboxApprovalEventRoutes() map[string]string {
	out := map[string]string{}
	if r == nil || r.api == nil {
		return out
	}
	for _, route := range r.api.Conventions.Mailbox.ApprovalEventRoutes {
		itemType := strings.TrimSpace(route.ItemType)
		eventName := strings.TrimSpace(route.EventName)
		if itemType == "" || eventName == "" {
			continue
		}
		out[itemType] = eventName
	}
	return out
}

func OpenRPCMethodNames(path string) ([]string, error) {
	raw, err := os.ReadFile(strings.TrimSpace(path))
	if err != nil {
		return nil, err
	}
	var doc apispec.OpenRPCDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse openrpc: %w", err)
	}
	names := make([]string, 0, len(doc.Methods))
	for _, method := range doc.Methods {
		names = append(names, strings.TrimSpace(method.Name))
	}
	sort.Strings(names)
	return names, nil
}

func MailboxApprovalRoutesFromSpec(path string) (map[string]string, error) {
	registry, err := LoadRegistry(path)
	if err != nil {
		return nil, err
	}
	return registry.MailboxApprovalEventRoutes(), nil
}
