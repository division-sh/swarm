package apispec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	OpenRPCVersion                     = "1.2.6"
	OpenRPCApplicationErrorCodeStart   = -32000
	OpenRPCApplicationErrorCodeMinimum = -32099

	ExamplesPolicyStatusDeferred             = "deferred"
	ExamplesPolicyOwner                      = "api_specification.examples_policy"
	ExamplesPolicyAppliesToAllGenerated      = "all_generated_methods"
	ExamplesPolicyOpenRPCExamplesOmitted     = "omitted_until_explicitly_authored"
	ExamplesPolicyRuntimeFixturesNotExamples = "not_examples_source"

	ServiceDiscoveryPolicyStatusNotPublished            = "not_published"
	ServiceDiscoveryPolicyOwner                         = "api_specification.service_discovery_policy"
	ServiceDiscoveryPolicyAppliesToGeneratedCatalog     = "generated_v1_method_catalog"
	ServiceDiscoveryPolicyRPCDiscoverOmitted            = "omitted"
	ServiceDiscoveryPolicyPublicationArtifactOpenRPC    = "generated_openrpc_json"
	ServiceDiscoveryPolicyRuntimeBehaviorMethodNotFound = "out_of_catalog_method_not_found"
)

type PlatformSpec struct {
	APISpecification APISpecification `yaml:"api_specification"`
}

type APISpecification struct {
	Description            string                 `yaml:"description" json:"description,omitempty"`
	Components             Components             `yaml:"components" json:"components"`
	ExamplesPolicy         ExamplesPolicy         `yaml:"examples_policy" json:"examples_policy,omitempty"`
	ServiceDiscoveryPolicy ServiceDiscoveryPolicy `yaml:"service_discovery_policy" json:"service_discovery_policy,omitempty"`
	MethodCatalogMetadata  map[string]any         `yaml:"method_catalog_metadata" json:"method_catalog_metadata,omitempty"`
	MethodCatalog          map[string]Method      `yaml:"method_catalog" json:"method_catalog"`
	Conventions            Conventions            `yaml:"conventions" json:"conventions"`
}

type ExamplesPolicy struct {
	Status                    string   `yaml:"status" json:"status,omitempty"`
	Owner                     string   `yaml:"owner" json:"owner,omitempty"`
	AppliesTo                 string   `yaml:"applies_to" json:"applies_to,omitempty"`
	OpenRPCMethodExamples     string   `yaml:"openrpc_method_examples" json:"openrpc_method_examples,omitempty"`
	RuntimeProbeFixtures      string   `yaml:"runtime_probe_fixtures" json:"runtime_probe_fixtures,omitempty"`
	Reason                    string   `yaml:"reason" json:"reason,omitempty"`
	Requirements              []string `yaml:"requirements" json:"requirements,omitempty"`
	FutureSourceModelRequired bool     `yaml:"future_source_model_required" json:"future_source_model_required,omitempty"`
}

type ServiceDiscoveryPolicy struct {
	Status              string   `yaml:"status" json:"status,omitempty"`
	Owner               string   `yaml:"owner" json:"owner,omitempty"`
	AppliesTo           string   `yaml:"applies_to" json:"applies_to,omitempty"`
	RPCDiscover         string   `yaml:"rpc_discover" json:"rpc_discover,omitempty"`
	PublicationArtifact string   `yaml:"publication_artifact" json:"publication_artifact,omitempty"`
	RuntimeBehavior     string   `yaml:"runtime_behavior" json:"runtime_behavior,omitempty"`
	Reason              string   `yaml:"reason" json:"reason,omitempty"`
	Requirements        []string `yaml:"requirements" json:"requirements,omitempty"`
}

type Components struct {
	Schemas              map[string]any `yaml:"schemas" json:"schemas"`
	ErrorCatalogMetadata map[string]any `yaml:"error_catalog_metadata" json:"error_catalog_metadata,omitempty"`
	Errors               map[string]any `yaml:"errors" json:"errors"`
}

type Conventions struct {
	Idempotency struct {
		MutatingMethods []string `yaml:"mutating_methods" json:"mutating_methods"`
	} `yaml:"idempotency" json:"idempotency"`
	Scopes struct {
		Catalog []string `yaml:"catalog" json:"catalog"`
	} `yaml:"scopes" json:"scopes"`
	Mailbox struct {
		StatusStorageModel  string                      `yaml:"status_storage_model" json:"status_storage_model,omitempty"`
		DecisionEventRoutes []MailboxDecisionEventRoute `yaml:"decision_event_routes" json:"decision_event_routes,omitempty"`
	} `yaml:"mailbox" json:"mailbox,omitempty"`
}

type MailboxDecisionEventRoute struct {
	ItemType          string `yaml:"item_type" json:"item_type"`
	TerminalEventName string `yaml:"terminal_event_name" json:"terminal_event_name"`
	DeferredEventName string `yaml:"deferred_event_name" json:"deferred_event_name"`
}

type Method struct {
	Tier               string              `yaml:"tier" json:"tier,omitempty"`
	Deprecated         bool                `yaml:"deprecated,omitempty" json:"deprecated,omitempty"`
	Description        string              `yaml:"description" json:"description,omitempty"`
	Scope              Scope               `yaml:"scope" json:"scope"`
	Idempotency        any                 `yaml:"idempotency,omitempty" json:"idempotency,omitempty"`
	Params             []ContentDescriptor `yaml:"params" json:"params"`
	Result             *ContentDescriptor  `yaml:"result" json:"result,omitempty"`
	NotificationSchema any                 `yaml:"notification_schema,omitempty" json:"notification_schema,omitempty"`
	Errors             []string            `yaml:"errors" json:"errors"`
}

type Scope struct {
	Required []string `yaml:"required" json:"required"`
}

type ContentDescriptor struct {
	Name        string `yaml:"name" json:"name"`
	Required    bool   `yaml:"required" json:"required"`
	Description string `yaml:"description,omitempty" json:"description,omitempty"`
	Schema      any    `yaml:"schema" json:"schema"`
}

type ValidationReport struct {
	MethodCount           int
	SchemaCount           int
	ErrorCodeCount        int
	MutatingMethodCount   int
	SubscriptionMethodCnt int
}

type OpenRPCDocument struct {
	OpenRPC    string            `json:"openrpc"`
	Info       OpenRPCInfo       `json:"info"`
	Servers    []OpenRPCServer   `json:"servers"`
	Methods    []OpenRPCMethod   `json:"methods"`
	Components OpenRPCComponents `json:"components"`
}

type OpenRPCInfo struct {
	Title       string `json:"title"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

type OpenRPCServer struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

type OpenRPCMethod struct {
	Name               string              `json:"name"`
	Deprecated         bool                `json:"deprecated,omitempty"`
	Description        string              `json:"description,omitempty"`
	Params             []ContentDescriptor `json:"params"`
	Result             *ContentDescriptor  `json:"result,omitempty"`
	NotificationSchema any                 `json:"notification_schema,omitempty"`
	Errors             []OpenRPCError      `json:"errors,omitempty"`
}

type OpenRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type OpenRPCComponents struct {
	Schemas map[string]any          `json:"schemas"`
	Errors  map[string]OpenRPCError `json:"errors"`
}

func LoadPlatformSpec(path string) (*APISpecification, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var spec PlatformSpec
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		return nil, fmt.Errorf("parse platform spec: %w", err)
	}
	if len(spec.APISpecification.MethodCatalog) == 0 {
		return nil, fmt.Errorf("platform spec missing api_specification.method_catalog")
	}
	return &spec.APISpecification, nil
}

func Validate(api *APISpecification) (ValidationReport, error) {
	var problems []string
	if api == nil {
		return ValidationReport{}, fmt.Errorf("api specification is nil")
	}
	report := ValidationReport{
		MethodCount:           len(api.MethodCatalog),
		SchemaCount:           len(api.Components.Schemas),
		ErrorCodeCount:        len(api.Components.Errors),
		MutatingMethodCount:   len(api.Conventions.Idempotency.MutatingMethods),
		SubscriptionMethodCnt: countSubscriptionMethods(api.MethodCatalog),
	}
	if report.MethodCount == 0 {
		problems = append(problems, "method_catalog is empty")
	}
	if report.SchemaCount == 0 {
		problems = append(problems, "components.schemas is empty")
	}
	if report.ErrorCodeCount == 0 {
		problems = append(problems, "components.errors is empty")
	}
	if report.ErrorCodeCount > openRPCApplicationErrorCodeCapacity() {
		problems = append(problems, fmt.Sprintf("components.errors has %d entries; generated OpenRPC application error code range supports at most %d unique codes", report.ErrorCodeCount, openRPCApplicationErrorCodeCapacity()))
	}
	if _, ok := api.MethodCatalog["description"]; ok {
		problems = append(problems, "method_catalog.description must be metadata, not a method")
	}
	if _, ok := api.Components.Errors["description"]; ok {
		problems = append(problems, "components.errors.description must be metadata, not an error code")
	}
	problems = append(problems, validateExamplesPolicy(api.ExamplesPolicy)...)
	problems = append(problems, validateServiceDiscoveryPolicy(api.ServiceDiscoveryPolicy)...)
	if api.ServiceDiscoveryPolicy.Status == ServiceDiscoveryPolicyStatusNotPublished {
		if _, ok := api.MethodCatalog["rpc.discover"]; ok {
			problems = append(problems, "method_catalog must not include rpc.discover while api_specification.service_discovery_policy.status is not_published")
		}
	}

	scopeCatalog := stringSet(api.Conventions.Scopes.Catalog)
	mutatingCatalog := stringSet(api.Conventions.Idempotency.MutatingMethods)
	for _, entry := range sortedMethods(api.MethodCatalog) {
		methodName := entry.name
		method := entry.method
		if !strings.Contains(methodName, ".") {
			problems = append(problems, fmt.Sprintf("method %q must use namespace.method naming", methodName))
		}
		if strings.TrimSpace(method.Description) == "" {
			problems = append(problems, fmt.Sprintf("method %s missing description", methodName))
		}
		if strings.TrimSpace(method.Tier) == "" {
			problems = append(problems, fmt.Sprintf("method %s missing tier", methodName))
		}
		if len(method.Scope.Required) == 0 && !strings.HasPrefix(methodName, "rpc.") {
			problems = append(problems, fmt.Sprintf("method %s missing scope.required", methodName))
		}
		for _, scope := range method.Scope.Required {
			if _, ok := scopeCatalog[scope]; !ok {
				problems = append(problems, fmt.Sprintf("method %s references undeclared scope %q", methodName, scope))
			}
		}
		for i, param := range method.Params {
			validateDescriptor(fmt.Sprintf("method %s param[%d]", methodName, i), param, &problems)
		}
		if method.Result == nil {
			problems = append(problems, fmt.Sprintf("method %s missing result descriptor", methodName))
		} else {
			validateDescriptor(fmt.Sprintf("method %s result", methodName), *method.Result, &problems)
		}
		if strings.Contains(methodName, ".subscribe") && method.NotificationSchema == nil {
			problems = append(problems, fmt.Sprintf("subscription method %s missing notification_schema", methodName))
		}
		for _, code := range method.Errors {
			if _, ok := api.Components.Errors[code]; !ok {
				problems = append(problems, fmt.Sprintf("method %s references undeclared error %q", methodName, code))
			}
		}
		_, listedMutating := mutatingCatalog[methodName]
		hasIdempotency := method.Idempotency != nil
		if listedMutating && !hasIdempotency {
			problems = append(problems, fmt.Sprintf("mutating method %s missing idempotency convention", methodName))
		}
		if hasIdempotency && !listedMutating {
			problems = append(problems, fmt.Sprintf("method %s declares idempotency but is not listed in conventions.idempotency.mutating_methods", methodName))
		}
		if listedMutating {
			idempotencyKey, ok := methodParam(method, "idempotency_key")
			if !ok {
				problems = append(problems, fmt.Sprintf("mutating method %s missing idempotency_key param", methodName))
			} else if idempotencyKey.Required {
				problems = append(problems, fmt.Sprintf("mutating method %s idempotency_key param must be optional", methodName))
			}
		}
	}
	for mutating := range mutatingCatalog {
		if _, ok := api.MethodCatalog[mutating]; !ok {
			problems = append(problems, fmt.Sprintf("conventions.idempotency.mutating_methods references missing method %s", mutating))
		}
	}
	problems = append(problems, validateMailboxConventions(api.Conventions.Mailbox.DecisionEventRoutes)...)
	if _, ok := api.MethodCatalog["rpc.unsubscribe"]; !ok {
		problems = append(problems, "rpc.unsubscribe is described by subscription conventions but missing from method_catalog")
	}
	problems = append(problems, validateRefs(api)...)
	problems = append(problems, validateFilterParity(api)...)
	if len(problems) > 0 {
		sort.Strings(problems)
		return report, fmt.Errorf("api specification validation failed:\n- %s", strings.Join(problems, "\n- "))
	}
	return report, nil
}

func validateExamplesPolicy(policy ExamplesPolicy) []string {
	var problems []string
	if strings.TrimSpace(policy.Status) == "" {
		problems = append(problems, "api_specification.examples_policy missing status")
	} else if policy.Status != ExamplesPolicyStatusDeferred {
		problems = append(problems, fmt.Sprintf("api_specification.examples_policy.status = %q, want %q", policy.Status, ExamplesPolicyStatusDeferred))
	}
	if policy.Owner != ExamplesPolicyOwner {
		problems = append(problems, fmt.Sprintf("api_specification.examples_policy.owner = %q, want %q", policy.Owner, ExamplesPolicyOwner))
	}
	if policy.AppliesTo != ExamplesPolicyAppliesToAllGenerated {
		problems = append(problems, fmt.Sprintf("api_specification.examples_policy.applies_to = %q, want %q", policy.AppliesTo, ExamplesPolicyAppliesToAllGenerated))
	}
	if policy.OpenRPCMethodExamples != ExamplesPolicyOpenRPCExamplesOmitted {
		problems = append(problems, fmt.Sprintf("api_specification.examples_policy.openrpc_method_examples = %q, want %q", policy.OpenRPCMethodExamples, ExamplesPolicyOpenRPCExamplesOmitted))
	}
	if policy.RuntimeProbeFixtures != ExamplesPolicyRuntimeFixturesNotExamples {
		problems = append(problems, fmt.Sprintf("api_specification.examples_policy.runtime_probe_fixtures = %q, want %q", policy.RuntimeProbeFixtures, ExamplesPolicyRuntimeFixturesNotExamples))
	}
	if strings.TrimSpace(policy.Reason) == "" {
		problems = append(problems, "api_specification.examples_policy missing reason")
	}
	if len(policy.Requirements) == 0 {
		problems = append(problems, "api_specification.examples_policy must list enforcement requirements")
	}
	for i, requirement := range policy.Requirements {
		if strings.TrimSpace(requirement) == "" {
			problems = append(problems, fmt.Sprintf("api_specification.examples_policy.requirements[%d] is empty", i))
		}
	}
	if !policy.FutureSourceModelRequired {
		problems = append(problems, "api_specification.examples_policy.future_source_model_required must be true")
	}
	return problems
}

func validateServiceDiscoveryPolicy(policy ServiceDiscoveryPolicy) []string {
	var problems []string
	if strings.TrimSpace(policy.Status) == "" {
		problems = append(problems, "api_specification.service_discovery_policy missing status")
	} else if policy.Status != ServiceDiscoveryPolicyStatusNotPublished {
		problems = append(problems, fmt.Sprintf("api_specification.service_discovery_policy.status = %q, want %q", policy.Status, ServiceDiscoveryPolicyStatusNotPublished))
	}
	if policy.Owner != ServiceDiscoveryPolicyOwner {
		problems = append(problems, fmt.Sprintf("api_specification.service_discovery_policy.owner = %q, want %q", policy.Owner, ServiceDiscoveryPolicyOwner))
	}
	if policy.AppliesTo != ServiceDiscoveryPolicyAppliesToGeneratedCatalog {
		problems = append(problems, fmt.Sprintf("api_specification.service_discovery_policy.applies_to = %q, want %q", policy.AppliesTo, ServiceDiscoveryPolicyAppliesToGeneratedCatalog))
	}
	if policy.RPCDiscover != ServiceDiscoveryPolicyRPCDiscoverOmitted {
		problems = append(problems, fmt.Sprintf("api_specification.service_discovery_policy.rpc_discover = %q, want %q", policy.RPCDiscover, ServiceDiscoveryPolicyRPCDiscoverOmitted))
	}
	if policy.PublicationArtifact != ServiceDiscoveryPolicyPublicationArtifactOpenRPC {
		problems = append(problems, fmt.Sprintf("api_specification.service_discovery_policy.publication_artifact = %q, want %q", policy.PublicationArtifact, ServiceDiscoveryPolicyPublicationArtifactOpenRPC))
	}
	if policy.RuntimeBehavior != ServiceDiscoveryPolicyRuntimeBehaviorMethodNotFound {
		problems = append(problems, fmt.Sprintf("api_specification.service_discovery_policy.runtime_behavior = %q, want %q", policy.RuntimeBehavior, ServiceDiscoveryPolicyRuntimeBehaviorMethodNotFound))
	}
	if strings.TrimSpace(policy.Reason) == "" {
		problems = append(problems, "api_specification.service_discovery_policy missing reason")
	}
	if len(policy.Requirements) == 0 {
		problems = append(problems, "api_specification.service_discovery_policy must list enforcement requirements")
	}
	for i, requirement := range policy.Requirements {
		if strings.TrimSpace(requirement) == "" {
			problems = append(problems, fmt.Sprintf("api_specification.service_discovery_policy.requirements[%d] is empty", i))
		}
	}
	return problems
}

func validateMailboxConventions(routes []MailboxDecisionEventRoute) []string {
	var problems []string
	seen := map[string]struct{}{}
	for i, route := range routes {
		itemType := strings.TrimSpace(route.ItemType)
		terminalEventName := strings.TrimSpace(route.TerminalEventName)
		deferredEventName := strings.TrimSpace(route.DeferredEventName)
		if itemType == "" {
			problems = append(problems, fmt.Sprintf("conventions.mailbox.decision_event_routes[%d] missing item_type", i))
		}
		if terminalEventName == "" {
			problems = append(problems, fmt.Sprintf("conventions.mailbox.decision_event_routes[%d] missing terminal_event_name", i))
		}
		if deferredEventName == "" {
			problems = append(problems, fmt.Sprintf("conventions.mailbox.decision_event_routes[%d] missing deferred_event_name", i))
		}
		if itemType == "" {
			continue
		}
		if _, ok := seen[itemType]; ok {
			problems = append(problems, fmt.Sprintf("conventions.mailbox.decision_event_routes duplicate item_type %q", itemType))
			continue
		}
		seen[itemType] = struct{}{}
	}
	return problems
}

func GenerateOpenRPC(api *APISpecification) ([]byte, error) {
	if _, err := Validate(api); err != nil {
		return nil, err
	}
	errorCodes := openRPCApplicationErrorCodes(api.Components.Errors)
	methods := make([]OpenRPCMethod, 0, len(api.MethodCatalog))
	for _, entry := range sortedMethods(api.MethodCatalog) {
		methodName := entry.name
		method := entry.method
		errors := make([]OpenRPCError, 0, len(method.Errors))
		for _, code := range method.Errors {
			errors = append(errors, openRPCApplicationError(code, api.Components.Errors[code], errorCodes[code]))
		}
		methods = append(methods, OpenRPCMethod{
			Name:               methodName,
			Deprecated:         method.Deprecated,
			Description:        method.Description,
			Params:             normalizeDescriptors(method.Params),
			Result:             normalizeDescriptorPointer(method.Result),
			NotificationSchema: normalizeValue(method.NotificationSchema),
			Errors:             errors,
		})
	}
	componentErrors := make(map[string]OpenRPCError, len(api.Components.Errors))
	for code, schema := range api.Components.Errors {
		componentErrors[code] = openRPCApplicationError(code, schema, errorCodes[code])
	}
	doc := OpenRPCDocument{
		OpenRPC: OpenRPCVersion,
		Info: OpenRPCInfo{
			Title:       "Swarm User-Facing JSON-RPC API",
			Version:     "1.0.0",
			Description: strings.TrimSpace(api.Description),
		},
		Servers: []OpenRPCServer{{Name: "v1", URL: "/v1/rpc"}},
		Methods: methods,
		Components: OpenRPCComponents{
			Schemas: normalizeMap(api.Components.Schemas),
			Errors:  componentErrors,
		},
	}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}

func validateDescriptor(label string, descriptor ContentDescriptor, problems *[]string) {
	if strings.TrimSpace(descriptor.Name) == "" {
		*problems = append(*problems, fmt.Sprintf("%s missing name", label))
	}
	if descriptor.Schema == nil {
		*problems = append(*problems, fmt.Sprintf("%s missing schema", label))
	}
}

func validateRefs(api *APISpecification) []string {
	var problems []string
	schemas := stringSet(keys(api.Components.Schemas))
	walkRefs(api.Components.Schemas, func(ref string) {
		if !strings.HasPrefix(ref, "#/components/schemas/") {
			problems = append(problems, fmt.Sprintf("unsupported schema ref %q", ref))
			return
		}
		name := strings.TrimPrefix(ref, "#/components/schemas/")
		if _, ok := schemas[name]; !ok {
			problems = append(problems, fmt.Sprintf("schema ref %q targets missing component", ref))
		}
	})
	walkRefs(api.Components.Errors, func(ref string) {
		if !strings.HasPrefix(ref, "#/components/schemas/") {
			problems = append(problems, fmt.Sprintf("unsupported error schema ref %q", ref))
			return
		}
		name := strings.TrimPrefix(ref, "#/components/schemas/")
		if _, ok := schemas[name]; !ok {
			problems = append(problems, fmt.Sprintf("error schema ref %q targets missing component", ref))
		}
	})
	for methodName, method := range api.MethodCatalog {
		for _, param := range method.Params {
			walkRefs(param.Schema, func(ref string) {
				if !strings.HasPrefix(ref, "#/components/schemas/") {
					problems = append(problems, fmt.Sprintf("method %s unsupported param ref %q", methodName, ref))
					return
				}
				if _, ok := schemas[strings.TrimPrefix(ref, "#/components/schemas/")]; !ok {
					problems = append(problems, fmt.Sprintf("method %s param ref %q targets missing component", methodName, ref))
				}
			})
		}
		if method.Result != nil {
			walkRefs(method.Result.Schema, func(ref string) {
				if !strings.HasPrefix(ref, "#/components/schemas/") {
					problems = append(problems, fmt.Sprintf("method %s unsupported result ref %q", methodName, ref))
					return
				}
				if _, ok := schemas[strings.TrimPrefix(ref, "#/components/schemas/")]; !ok {
					problems = append(problems, fmt.Sprintf("method %s result ref %q targets missing component", methodName, ref))
				}
			})
		}
		walkRefs(method.NotificationSchema, func(ref string) {
			if !strings.HasPrefix(ref, "#/components/schemas/") {
				problems = append(problems, fmt.Sprintf("method %s unsupported notification ref %q", methodName, ref))
				return
			}
			if _, ok := schemas[strings.TrimPrefix(ref, "#/components/schemas/")]; !ok {
				problems = append(problems, fmt.Sprintf("method %s notification ref %q targets missing component", methodName, ref))
			}
		})
	}
	return problems
}

func validateFilterParity(api *APISpecification) []string {
	var problems []string
	for methodName, subscribe := range api.MethodCatalog {
		if !strings.HasSuffix(methodName, ".subscribe") {
			continue
		}
		listName := strings.TrimSuffix(methodName, ".subscribe") + ".list"
		list, ok := api.MethodCatalog[listName]
		if !ok {
			continue
		}
		listFilter, listOK := paramSchemaRef(list, "filter")
		subscribeFilter, subscribeOK := paramSchemaRef(subscribe, "filter")
		if listOK != subscribeOK || listFilter != subscribeFilter {
			problems = append(problems, fmt.Sprintf("%s filter ref %q must match %s filter ref %q", methodName, subscribeFilter, listName, listFilter))
		}
	}
	return problems
}

func methodParam(method Method, name string) (ContentDescriptor, bool) {
	for _, param := range method.Params {
		if param.Name == name {
			return param, true
		}
	}
	return ContentDescriptor{}, false
}

func paramSchemaRef(method Method, name string) (string, bool) {
	for _, param := range method.Params {
		if param.Name != name {
			continue
		}
		if schema, ok := normalizeValue(param.Schema).(map[string]any); ok {
			if ref, ok := schema["$ref"].(string); ok {
				return ref, true
			}
		}
		return "", true
	}
	return "", false
}

func walkRefs(value any, visit func(string)) {
	switch typed := normalizeValue(value).(type) {
	case map[string]any:
		for key, value := range typed {
			if key == "$ref" {
				if ref, ok := value.(string); ok {
					visit(ref)
				}
				continue
			}
			walkRefs(value, visit)
		}
	case []any:
		for _, item := range typed {
			walkRefs(item, visit)
		}
	}
}

func normalizeDescriptors(in []ContentDescriptor) []ContentDescriptor {
	out := make([]ContentDescriptor, 0, len(in))
	for _, descriptor := range in {
		normalized := descriptor
		normalized.Schema = normalizeValue(descriptor.Schema)
		out = append(out, normalized)
	}
	return out
}

func normalizeDescriptorPointer(in *ContentDescriptor) *ContentDescriptor {
	if in == nil {
		return nil
	}
	normalized := *in
	normalized.Schema = normalizeValue(in.Schema)
	return &normalized
}

func openRPCApplicationError(code string, detailsSchema any, numericCode int) OpenRPCError {
	return OpenRPCError{
		Code:    numericCode,
		Message: "Application error: " + code,
		Data: map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"required":             []any{"code", "details", "retryable", "correlation_id"},
			"properties": map[string]any{
				"code": map[string]any{
					"type":  "string",
					"const": code,
				},
				"details":        normalizeValue(detailsSchema),
				"retryable":      map[string]any{"type": "boolean"},
				"correlation_id": map[string]any{"type": "string"},
			},
		},
	}
}

func openRPCApplicationErrorCodes(errors map[string]any) map[string]int {
	names := keys(errors)
	sort.Strings(names)
	out := make(map[string]int, len(names))
	for i, name := range names {
		out[name] = OpenRPCApplicationErrorCodeStart - i
	}
	return out
}

func ApplicationErrorCodes(errors map[string]any) map[string]int {
	return openRPCApplicationErrorCodes(errors)
}

func openRPCApplicationErrorCodeCapacity() int {
	return OpenRPCApplicationErrorCodeStart - OpenRPCApplicationErrorCodeMinimum + 1
}

type methodEntry struct {
	name   string
	method Method
}

func sortedMethods(methods map[string]Method) []methodEntry {
	names := keys(methods)
	sort.Strings(names)
	out := make([]methodEntry, 0, len(names))
	for _, name := range names {
		out = append(out, methodEntry{name: name, method: methods[name]})
	}
	return out
}

func countSubscriptionMethods(methods map[string]Method) int {
	count := 0
	for name := range methods {
		if strings.Contains(name, "subscribe") && name != "rpc.unsubscribe" {
			count++
		}
	}
	return count
}

func keys[V any](in map[string]V) []string {
	out := make([]string, 0, len(in))
	for key := range in {
		out = append(out, key)
	}
	return out
}

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		clean := strings.TrimSpace(value)
		if clean == "" {
			continue
		}
		out[clean] = struct{}{}
	}
	return out
}

func normalizeMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = normalizeValue(value)
	}
	return out
}

func normalizeValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			out[key] = normalizeValue(value)
		}
		return out
	case map[interface{}]interface{}:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			out[fmt.Sprint(key)] = normalizeValue(value)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, value := range typed {
			out = append(out, normalizeValue(value))
		}
		return out
	default:
		return typed
	}
}

func EqualJSON(a, b []byte) bool {
	var left any
	var right any
	if err := json.Unmarshal(a, &left); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &right); err != nil {
		return false
	}
	leftRaw, _ := json.Marshal(left)
	rightRaw, _ := json.Marshal(right)
	return bytes.Equal(leftRaw, rightRaw)
}
