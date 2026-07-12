package failures

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

const EnvelopeSchemaVersion = "platform.failure/v1"

type Class string

const (
	ClassTimeout               Class = "platform.timeout"
	ClassConnectorFailure      Class = "platform.connector_failure"
	ClassComputeFailure        Class = "platform.compute_failure"
	ClassOutcomeUncertain      Class = "platform.outcome_uncertain"
	ClassRetryExhausted        Class = "platform.retry_exhausted"
	ClassBudgetExhausted       Class = "platform.budget_exhausted"
	ClassDataLimitExceeded     Class = "platform.data_limit_exceeded"
	ClassAuthenticationNeeded  Class = "platform.authentication_required"
	ClassAuthorizationDenied   Class = "platform.authorization_denied"
	ClassTargetUnreachable     Class = "platform.target_unreachable"
	ClassTargetAmbiguous       Class = "platform.target_ambiguous"
	ClassFanOutBoundExceeded   Class = "platform.fan_out_bound_exceeded"
	ClassChainDepthExceeded    Class = "platform.chain_depth_exceeded"
	ClassEarlyArrival          Class = "platform.early_arrival"
	ClassStaleArrival          Class = "platform.stale_arrival"
	ClassUnexpectedArrival     Class = "platform.unexpected_arrival"
	ClassConflictingDuplicate  Class = "platform.conflicting_duplicate"
	ClassReplyAlreadyTerminal  Class = "platform.reply_already_terminal"
	ClassSupersededGeneration  Class = "platform.superseded_generation"
	ClassLifecycleConflict     Class = "platform.lifecycle_conflict"
	ClassSchemaInvalid         Class = "platform.schema_invalid"
	ClassDependencyUnavailable Class = "platform.dependency_unavailable"
	ClassInternalFailure       Class = "platform.internal_failure"
)

const (
	SelectorAny            = "platform.any"
	SelectorAnyTaskFailure = "platform.any_task_failure"
)

type Detail struct {
	Code       string         `json:"code"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

type Envelope struct {
	SchemaVersion string `json:"schema_version"`
	Class         Class  `json:"class"`
	Detail        Detail `json:"detail"`
	Retryable     bool   `json:"retryable"`
	Deterministic bool   `json:"deterministic"`
	Message       string `json:"message"`
	Remediation   string `json:"remediation"`
	Component     string `json:"component,omitempty"`
	Operation     string `json:"operation,omitempty"`
}

type Definition struct {
	Class               Class
	TaskFailure         bool
	Retryable           bool
	Deterministic       bool
	MessageTemplate     string
	RemediationTemplate string
}

type Error struct {
	Failure Envelope
	cause   error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	parts := []string{string(e.Failure.Class), e.Failure.Detail.Code}
	if e.Failure.Component != "" {
		parts = append(parts, "component="+e.Failure.Component)
	}
	if e.Failure.Operation != "" {
		parts = append(parts, "operation="+e.Failure.Operation)
	}
	return strings.Join(parts, " ") + ": " + e.Failure.Message
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

var detailCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

var classOrder = []Class{
	ClassTimeout,
	ClassConnectorFailure,
	ClassComputeFailure,
	ClassOutcomeUncertain,
	ClassRetryExhausted,
	ClassBudgetExhausted,
	ClassDataLimitExceeded,
	ClassAuthenticationNeeded,
	ClassAuthorizationDenied,
	ClassTargetUnreachable,
	ClassTargetAmbiguous,
	ClassFanOutBoundExceeded,
	ClassChainDepthExceeded,
	ClassEarlyArrival,
	ClassStaleArrival,
	ClassUnexpectedArrival,
	ClassConflictingDuplicate,
	ClassReplyAlreadyTerminal,
	ClassSupersededGeneration,
	ClassLifecycleConflict,
	ClassSchemaInvalid,
	ClassDependencyUnavailable,
	ClassInternalFailure,
}

var definitions = map[Class]Definition{
	ClassTimeout:               definition(ClassTimeout, true, true, false, "Runtime deadline exceeded", "Retry after confirming the dependency can complete within the declared deadline"),
	ClassConnectorFailure:      definition(ClassConnectorFailure, true, true, false, "External connector failed", "Inspect the connector status and typed failure detail"),
	ClassComputeFailure:        definition(ClassComputeFailure, true, false, true, "Compute module failed", "Correct the module or its declared resource limits"),
	ClassOutcomeUncertain:      definition(ClassOutcomeUncertain, true, false, false, "Operation outcome is uncertain", "Reconcile the authoritative operation state before retrying"),
	ClassRetryExhausted:        definition(ClassRetryExhausted, true, false, true, "Retry policy was exhausted", "Resolve the underlying failure before starting a new execution"),
	ClassBudgetExhausted:       definition(ClassBudgetExhausted, true, false, true, "Execution budget was exhausted", "Increase or reset the declared budget before starting a new execution"),
	ClassDataLimitExceeded:     definition(ClassDataLimitExceeded, true, false, true, "Runtime data limit was exceeded", "Reduce the result size or raise the declared limit"),
	ClassAuthenticationNeeded:  definition(ClassAuthenticationNeeded, true, false, true, "Authentication is required", "Provide or refresh the required credential"),
	ClassAuthorizationDenied:   definition(ClassAuthorizationDenied, true, false, true, "Authorization was denied", "Grant the required authority or use an authorized principal"),
	ClassTargetUnreachable:     definition(ClassTargetUnreachable, true, false, false, "Runtime target is unreachable", "Restore or select a reachable target"),
	ClassTargetAmbiguous:       definition(ClassTargetAmbiguous, true, false, true, "Runtime target is ambiguous", "Make target selection resolve to exactly one target"),
	ClassFanOutBoundExceeded:   definition(ClassFanOutBoundExceeded, true, false, true, "Fan-out bound was exceeded", "Raise max_items or split the batch"),
	ClassChainDepthExceeded:    definition(ClassChainDepthExceeded, true, false, true, "Event chain depth was exceeded", "Break the causal event cycle or reduce its depth"),
	ClassEarlyArrival:          definition(ClassEarlyArrival, false, false, true, "Event arrived before its admitted window", "Retry only through the owning temporal policy"),
	ClassStaleArrival:          definition(ClassStaleArrival, false, false, true, "Event arrived after its owning lifecycle closed", "Start a new lifecycle rather than reusing the closed one"),
	ClassUnexpectedArrival:     definition(ClassUnexpectedArrival, false, false, true, "Event is not admitted by the active contract", "Send an event admitted by the active request or join"),
	ClassConflictingDuplicate:  definition(ClassConflictingDuplicate, false, false, true, "Duplicate identity carries conflicting content", "Reuse the original content or mint a new identity"),
	ClassReplyAlreadyTerminal:  definition(ClassReplyAlreadyTerminal, false, false, true, "Request already has a terminal reply", "Do not send a second distinct terminal reply"),
	ClassSupersededGeneration:  definition(ClassSupersededGeneration, true, false, true, "Agent generation was superseded", "Start new work under the current lifecycle generation"),
	ClassLifecycleConflict:     definition(ClassLifecycleConflict, true, false, true, "Agent lifecycle authority conflicts with the requested transition", "Reload the canonical lifecycle cell before retrying"),
	ClassSchemaInvalid:         definition(ClassSchemaInvalid, true, false, true, "Runtime input violates its schema", "Correct the runtime input to match the declared schema"),
	ClassDependencyUnavailable: definition(ClassDependencyUnavailable, true, true, false, "Required runtime dependency is unavailable", "Restore the dependency and retry the execution"),
	ClassInternalFailure:       definition(ClassInternalFailure, true, false, false, "The platform encountered an internal failure", "Inspect the typed detail and runtime diagnostics"),
}

var detailClasses = map[string]Class{
	"criteria_citation_validation_failed": ClassSchemaInvalid,
	"cross_flow_read_forbidden":           ClassAuthorizationDenied,
	"cross_flow_write_forbidden":          ClassAuthorizationDenied,
	"dependency_unavailable":              ClassDependencyUnavailable,
	"event_publish_failed":                ClassDependencyUnavailable,
	"external_dispatch_rate_limited":      ClassConnectorFailure,
	"invalid_emit_tool_name":              ClassSchemaInvalid,
	"invalid_tool_input":                  ClassSchemaInvalid,
	"llm_provider_rate_limited":           ClassConnectorFailure,
	"not_found":                           ClassTargetUnreachable,
	"parent_route_lookup_failed":          ClassTargetUnreachable,
	"query_failed":                        ClassDependencyUnavailable,
	"route_plan_preflight_failed":         ClassTargetUnreachable,
	"schema_validation_failed":            ClassSchemaInvalid,
	"typed_read_result_marshal_failed":    ClassInternalFailure,
	"typed_read_result_too_large":         ClassDataLimitExceeded,
	"write_failed":                        ClassDependencyUnavailable,
	"lifecycle_token_missing":             ClassLifecycleConflict,
	"lifecycle_transition_conflict":       ClassLifecycleConflict,
	"effect_recovery_prelaunch_abandoned": ClassLifecycleConflict,
	"effect_recovery_outcome_unconfirmed": ClassOutcomeUncertain,
	"superseded_generation":               ClassSupersededGeneration,
}

var targetDetailClasses = map[string]Class{
	"target_required_missing":                  ClassTargetUnreachable,
	"target_invalid_syntax":                    ClassTargetUnreachable,
	"target_unreachable_no_subscriber":         ClassTargetUnreachable,
	"target_not_subscribed":                    ClassTargetUnreachable,
	"target_unreachable_terminated":            ClassTargetUnreachable,
	"parent_route_incomplete":                  ClassTargetUnreachable,
	"target_ambiguous":                         ClassTargetAmbiguous,
	"target_unknown_flow":                      ClassTargetUnreachable,
	"target_sender_no_inbound_runtime":         ClassTargetUnreachable,
	"target_sender_empty_source_runtime":       ClassTargetUnreachable,
	"producer_target_common_path_forbidden":    ClassTargetUnreachable,
	"producer_broadcast_common_path_forbidden": ClassTargetUnreachable,
	"route_plan_address_value_missing":         ClassTargetUnreachable,
	"route_plan_target_unsupported":            ClassTargetUnreachable,
	"route_plan_target_unresolved":             ClassTargetUnreachable,
	"route_plan_target_ambiguous":              ClassTargetAmbiguous,
	"route_plan_instance_key_adapter_invalid":  ClassTargetUnreachable,
	"route_plan_instance_resolution_invalid":   ClassTargetUnreachable,
	"route_plan_instance_conflict":             ClassTargetAmbiguous,
	"route_plan_lifecycle_unavailable":         ClassTargetUnreachable,
}

func definition(class Class, taskFailure, retryable, deterministic bool, message, remediation string) Definition {
	return Definition{
		Class:               class,
		TaskFailure:         taskFailure,
		Retryable:           retryable,
		Deterministic:       deterministic,
		MessageTemplate:     message + " (%s).",
		RemediationTemplate: remediation + " (%s).",
	}
}

func Classes() []Class {
	return append([]Class(nil), classOrder...)
}

// EnvelopeJSONSchema returns the canonical event-schema projection of a failure
// envelope. It is built from the executable registry so class additions cannot
// drift from platform-event validation.
func EnvelopeJSONSchema() map[string]any {
	classes := make([]any, 0, len(classOrder))
	for _, class := range classOrder {
		classes = append(classes, string(class))
	}
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"schema_version": map[string]any{"type": "string", "enum": []any{EnvelopeSchemaVersion}},
			"class":          map[string]any{"type": "string", "enum": classes},
			"detail": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"code":       map[string]any{"type": "string"},
					"attributes": map[string]any{"type": "object", "additionalProperties": true},
				},
				"required":             []any{"code"},
				"additionalProperties": false,
			},
			"retryable":     map[string]any{"type": "boolean"},
			"deterministic": map[string]any{"type": "boolean"},
			"message":       map[string]any{"type": "string"},
			"remediation":   map[string]any{"type": "string"},
			"component":     map[string]any{"type": "string"},
			"operation":     map[string]any{"type": "string"},
		},
		"required": []any{
			"schema_version", "class", "detail", "retryable", "deterministic", "message", "remediation",
		},
		"additionalProperties": false,
	}
}

func Registry() []Definition {
	out := make([]Definition, 0, len(classOrder))
	for _, class := range classOrder {
		out = append(out, definitions[class])
	}
	return out
}

func DefinitionFor(class Class) (Definition, bool) {
	def, ok := definitions[class]
	return def, ok
}

func SelectorMembers(selector string) ([]Class, bool) {
	switch strings.TrimSpace(selector) {
	case SelectorAny:
		return Classes(), true
	case SelectorAnyTaskFailure:
		out := make([]Class, 0, len(classOrder))
		for _, class := range classOrder {
			if definitions[class].TaskFailure {
				out = append(out, class)
			}
		}
		return out, true
	default:
		return nil, false
	}
}

func Matches(selector string, class Class) bool {
	members, ok := SelectorMembers(selector)
	if !ok {
		return false
	}
	for _, member := range members {
		if member == class {
			return true
		}
	}
	return false
}

func New(class Class, detailCode, component, operation string, attributes map[string]any) error {
	return Wrap(class, detailCode, component, operation, attributes, nil)
}

func NewDetail(detailCode, component, operation string, attributes map[string]any) error {
	return WrapDetail(detailCode, component, operation, attributes, nil)
}

func WrapDetail(detailCode, component, operation string, attributes map[string]any, cause error) error {
	class, ok := detailClasses[strings.TrimSpace(detailCode)]
	if !ok {
		return Wrap(
			ClassInternalFailure,
			"invalid_failure_construction",
			component,
			operation,
			map[string]any{"requested_detail": strings.TrimSpace(detailCode)},
			cause,
		)
	}
	return Wrap(class, detailCode, component, operation, attributes, cause)
}

func NewTarget(detailCode, component, operation string, attributes map[string]any) error {
	return WrapTarget(detailCode, component, operation, attributes, nil)
}

func WrapTarget(detailCode, component, operation string, attributes map[string]any, cause error) error {
	class, ok := targetDetailClasses[strings.TrimSpace(detailCode)]
	if !ok {
		return Wrap(
			ClassInternalFailure,
			"invalid_failure_construction",
			component,
			operation,
			map[string]any{"requested_target_detail": strings.TrimSpace(detailCode)},
			cause,
		)
	}
	return Wrap(class, detailCode, component, operation, attributes, cause)
}

func Wrap(class Class, detailCode, component, operation string, attributes map[string]any, cause error) error {
	envelope, err := buildEnvelope(class, detailCode, component, operation, attributes)
	if err != nil {
		envelope = invalidConstructionEnvelope(class, detailCode, component, operation, err)
		cause = errors.Join(cause, err)
	}
	return &Error{Failure: envelope, cause: cause}
}

func FromError(err error, component, operation string) *Error {
	if err == nil {
		return nil
	}
	if existing, ok := asRaw(err); ok {
		return validatedError(existing, component, operation)
	}
	return Wrap(
		ClassInternalFailure,
		"unclassified_runtime_error",
		component,
		operation,
		nil,
		err,
	).(*Error)
}

func As(err error) (*Error, bool) {
	existing, ok := asRaw(err)
	if !ok {
		return nil, false
	}
	return validatedError(existing, existing.Failure.Component, existing.Failure.Operation), true
}

func asRaw(err error) (*Error, bool) {
	if err == nil {
		return nil, false
	}
	var out *Error
	if errors.As(err, &out) && out != nil {
		return out, true
	}
	return nil, false
}

func validatedError(existing *Error, component, operation string) *Error {
	if existing == nil {
		return nil
	}
	if err := ValidateEnvelope(existing.Failure); err != nil {
		return &Error{
			Failure: invalidConstructionEnvelope(
				existing.Failure.Class,
				existing.Failure.Detail.Code,
				component,
				operation,
				err,
			),
			cause: errors.Join(existing.cause, err),
		}
	}
	return existing
}

func EnvelopeFromError(err error) (Envelope, bool) {
	failure, ok := As(err)
	if !ok {
		return Envelope{}, false
	}
	return failure.Failure, true
}

func Normalize(err error, component, operation string) Envelope {
	failure := FromError(err, component, operation)
	if failure == nil {
		failure = FromError(
			New(ClassInternalFailure, "missing_failure", component, operation, nil),
			component,
			operation,
		)
	}
	return failure.Failure
}

func MarshalEnvelope(envelope Envelope) ([]byte, error) {
	if err := ValidateEnvelope(envelope); err != nil {
		return nil, err
	}
	return json.Marshal(envelope)
}

// EnvelopeValue returns the canonical generic-object representation used by
// event payloads and other structured runtime carriers.
func EnvelopeValue(envelope Envelope) (map[string]any, error) {
	raw, err := MarshalEnvelope(envelope)
	if err != nil {
		return nil, err
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("decode failure envelope value: %w", err)
	}
	return value, nil
}

func UnmarshalEnvelope(raw []byte) (Envelope, error) {
	var envelope Envelope
	if len(raw) == 0 || string(raw) == "null" {
		return envelope, fmt.Errorf("failure envelope is required")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(&envelope); err != nil {
		return envelope, fmt.Errorf("decode failure envelope: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return Envelope{}, fmt.Errorf("decode failure envelope: %w", err)
	}
	if err := ValidateEnvelope(envelope); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("trailing JSON value is not allowed")
		}
		return err
	}
	return nil
}

// CloneEnvelope copies evidence without establishing semantic authority.
// Malformed evidence remains observable so the receiving authority can reject it.
func CloneEnvelope(envelope *Envelope) *Envelope {
	if envelope == nil {
		return nil
	}
	raw, err := json.Marshal(envelope)
	if err == nil {
		var cloned Envelope
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&cloned); err == nil {
			if err := requireJSONEOF(decoder); err == nil {
				return &cloned
			}
		}
	}
	// Invalid, non-JSON evidence still remains non-null for authority rejection.
	cloned := *envelope
	cloned.Detail.Attributes = cloneAttributes(envelope.Detail.Attributes)
	return &cloned
}

// SemanticFingerprint identifies failure meaning without presentation text.
// Registry wording can evolve without changing persisted terminal-event identity.
func SemanticFingerprint(envelope Envelope) (string, error) {
	if envelope.SchemaVersion != EnvelopeSchemaVersion {
		return "", fmt.Errorf("failure envelope schema_version must be %q", EnvelopeSchemaVersion)
	}
	def, ok := definitions[envelope.Class]
	if !ok {
		return "", fmt.Errorf("unknown failure class %q", envelope.Class)
	}
	if err := validateDetail(envelope.Class, envelope.Detail); err != nil {
		return "", err
	}
	retryable, deterministic := decisions(def, envelope.Detail)
	if envelope.Retryable != retryable || envelope.Deterministic != deterministic {
		return "", fmt.Errorf("failure envelope decisions do not match registry for %s/%s", envelope.Class, envelope.Detail.Code)
	}
	semantic := struct {
		SchemaVersion string `json:"schema_version"`
		Class         Class  `json:"class"`
		Detail        Detail `json:"detail"`
		Retryable     bool   `json:"retryable"`
		Deterministic bool   `json:"deterministic"`
		Component     string `json:"component,omitempty"`
		Operation     string `json:"operation,omitempty"`
	}{
		SchemaVersion: envelope.SchemaVersion,
		Class:         envelope.Class,
		Detail:        envelope.Detail,
		Retryable:     envelope.Retryable,
		Deterministic: envelope.Deterministic,
		Component:     strings.TrimSpace(envelope.Component),
		Operation:     strings.TrimSpace(envelope.Operation),
	}
	raw, err := json.Marshal(semantic)
	if err != nil {
		return "", fmt.Errorf("marshal semantic failure identity: %w", err)
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256(raw)), nil
}

func FromEnvelope(envelope Envelope) error {
	if err := ValidateEnvelope(envelope); err != nil {
		return Wrap(
			ClassInternalFailure,
			"invalid_persisted_failure_envelope",
			"runtime",
			"decode_failure_envelope",
			nil,
			err,
		)
	}
	return &Error{Failure: envelope}
}

func Format(err error) string {
	failure := FromError(err, "runtime", "format_error")
	if failure == nil {
		return ""
	}
	raw, marshalErr := json.Marshal(failure.Failure)
	if marshalErr != nil {
		return failure.Error()
	}
	return string(raw)
}

func ValidateEnvelope(envelope Envelope) error {
	if envelope.SchemaVersion != EnvelopeSchemaVersion {
		return fmt.Errorf("failure envelope schema_version must be %q", EnvelopeSchemaVersion)
	}
	def, ok := definitions[envelope.Class]
	if !ok {
		return fmt.Errorf("unknown failure class %q", envelope.Class)
	}
	if err := validateDetail(envelope.Class, envelope.Detail); err != nil {
		return err
	}
	expectedRetryable, expectedDeterministic := decisions(def, envelope.Detail)
	if envelope.Retryable != expectedRetryable || envelope.Deterministic != expectedDeterministic {
		return fmt.Errorf("failure envelope decisions do not match registry for %s/%s", envelope.Class, envelope.Detail.Code)
	}
	if envelope.Message != fmt.Sprintf(def.MessageTemplate, envelope.Detail.Code) {
		return fmt.Errorf("failure envelope message does not match registry template")
	}
	if envelope.Remediation != fmt.Sprintf(def.RemediationTemplate, envelope.Detail.Code) {
		return fmt.Errorf("failure envelope remediation does not match registry template")
	}
	return nil
}

func buildEnvelope(class Class, detailCode, component, operation string, attributes map[string]any) (Envelope, error) {
	def, ok := definitions[class]
	if !ok {
		return Envelope{}, fmt.Errorf("unknown failure class %q", class)
	}
	detail := Detail{Code: strings.TrimSpace(detailCode), Attributes: cloneAttributes(attributes)}
	if err := validateDetail(class, detail); err != nil {
		return Envelope{}, err
	}
	retryable, deterministic := decisions(def, detail)
	return Envelope{
		SchemaVersion: EnvelopeSchemaVersion,
		Class:         class,
		Detail:        detail,
		Retryable:     retryable,
		Deterministic: deterministic,
		Message:       fmt.Sprintf(def.MessageTemplate, detail.Code),
		Remediation:   fmt.Sprintf(def.RemediationTemplate, detail.Code),
		Component:     strings.TrimSpace(component),
		Operation:     strings.TrimSpace(operation),
	}, nil
}

func invalidConstructionEnvelope(class Class, detailCode, component, operation string, constructionErr error) Envelope {
	def := definitions[ClassInternalFailure]
	detail := Detail{
		Code: "invalid_failure_construction",
		Attributes: map[string]any{
			"requested_class":    strings.TrimSpace(string(class)),
			"requested_detail":   strings.TrimSpace(detailCode),
			"construction_error": strings.TrimSpace(constructionErr.Error()),
		},
	}
	retryable, deterministic := decisions(def, detail)
	return Envelope{
		SchemaVersion: EnvelopeSchemaVersion,
		Class:         ClassInternalFailure,
		Detail:        detail,
		Retryable:     retryable,
		Deterministic: deterministic,
		Message:       fmt.Sprintf(def.MessageTemplate, detail.Code),
		Remediation:   fmt.Sprintf(def.RemediationTemplate, detail.Code),
		Component:     strings.TrimSpace(component),
		Operation:     strings.TrimSpace(operation),
	}
}

func validateDetail(class Class, detail Detail) error {
	if !detailCodePattern.MatchString(strings.TrimSpace(detail.Code)) {
		return fmt.Errorf("failure detail code %q must match %s", detail.Code, detailCodePattern.String())
	}
	if detail.Attributes != nil {
		if _, err := json.Marshal(detail.Attributes); err != nil {
			return fmt.Errorf("failure detail attributes must be JSON serializable: %w", err)
		}
	}
	switch class {
	case ClassOutcomeUncertain:
		switch detail.Code {
		case "run_terminal_persistence_unconfirmed":
			if len(detail.Attributes) != 1 {
				return fmt.Errorf("run_terminal_persistence_unconfirmed detail requires only attempted_status")
			}
			status, _ := detail.Attributes["attempted_status"].(string)
			switch strings.TrimSpace(status) {
			case "failed", "completed":
			default:
				return fmt.Errorf("run_terminal_persistence_unconfirmed attempted_status must be failed or completed")
			}
		case "directive_heartbeat_shutdown_unconfirmed", "directive_failure_persistence_unconfirmed", "directive_result_persistence_unconfirmed", "directive_execution_lease_expired":
			if len(detail.Attributes) != 0 {
				return fmt.Errorf("%s detail forbids attributes", detail.Code)
			}
		}
	case ClassBudgetExhausted:
		kind, _ := detail.Attributes["budget_kind"].(string)
		switch strings.TrimSpace(kind) {
		case "spend", "agent_turns", "tool_rounds":
		default:
			return fmt.Errorf("budget_exhausted detail budget_kind must be spend, agent_turns, or tool_rounds")
		}
	case ClassDataLimitExceeded:
		if value, _ := detail.Attributes["limit_kind"].(string); strings.TrimSpace(value) == "" {
			return fmt.Errorf("data_limit_exceeded detail limit_kind is required")
		}
		if !numberAttribute(detail.Attributes["limit"]) {
			return fmt.Errorf("data_limit_exceeded detail limit must be numeric")
		}
		if !numberAttribute(detail.Attributes["actual"]) {
			return fmt.Errorf("data_limit_exceeded detail actual must be numeric")
		}
	case ClassAuthenticationNeeded:
		if value, _ := detail.Attributes["auth_kind"].(string); strings.TrimSpace(value) == "" {
			return fmt.Errorf("authentication_required detail auth_kind is required")
		}
	case ClassAuthorizationDenied:
		if value, _ := detail.Attributes["action"].(string); strings.TrimSpace(value) == "" {
			return fmt.Errorf("authorization_denied detail action is required")
		}
	case ClassInternalFailure:
		switch detail.Code {
		case "directive_board_step_failed", "directive_execution_not_admitted":
			if len(detail.Attributes) != 0 {
				return fmt.Errorf("%s detail forbids attributes", detail.Code)
			}
		}
	}
	return nil
}

func decisions(def Definition, detail Detail) (bool, bool) {
	retryable := def.Retryable
	deterministic := def.Deterministic
	switch def.Class {
	case ClassConnectorFailure:
		if detail.Code == "provider_credit_exhausted" {
			retryable = false
			deterministic = true
		}
		if status, ok := intAttribute(detail.Attributes["status"]); ok {
			retryable = status == 408 || status == 429 || status >= 500
		}
	case ClassComputeFailure:
		deterministic = true
	case ClassInternalFailure:
		if detail.Code == "typed_read_result_marshal_failed" || detail.Code == "directive_execution_not_admitted" {
			deterministic = true
		}
	}
	return retryable, deterministic
}

func cloneAttributes(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	keys := make([]string, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]any, len(in))
	for _, key := range keys {
		out[key] = in[key]
	}
	return out
}

func numberAttribute(value any) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, json.Number:
		return true
	default:
		return false
	}
}

func intAttribute(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int32:
		return int(typed), true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), float64(int(typed)) == typed
	case json.Number:
		parsed, err := typed.Int64()
		return int(parsed), err == nil
	default:
		return 0, false
	}
}
