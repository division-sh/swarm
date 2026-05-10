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
	RunNotFoundCode                      = "RUN_NOT_FOUND"
	MailboxNotFoundCode                  = "MAILBOX_NOT_FOUND"
	MailboxAlreadyDecidedCode            = "MAILBOX_ALREADY_DECIDED"
	MailboxApprovalEventUnconfiguredCode = "MAILBOX_APPROVAL_EVENT_UNCONFIGURED"
	InvalidDeferUntilCode                = "INVALID_DEFER_UNTIL"
	IdempotencyConflictCode              = "IDEMPOTENCY_CONFLICT"
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
