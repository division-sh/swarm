package decisioncard

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

type AnchorKind string

const (
	AnchorKindStageGate      AnchorKind = "stage_gate"
	AnchorKindHumanTask      AnchorKind = "human_task"
	AnchorKindProposedEffect AnchorKind = "proposed_effect"
)

type ScopeKind string

const (
	ScopeEntity ScopeKind = "entity"
	ScopeFlow   ScopeKind = "flow"
	ScopeGlobal ScopeKind = "global"
)

type Scope struct {
	Kind         ScopeKind `json:"kind"`
	FlowInstance string    `json:"flow_instance,omitempty"`
	EntityID     string    `json:"entity_id,omitempty"`
}

func (s Scope) Validate() error {
	s.FlowInstance = strings.Trim(strings.TrimSpace(s.FlowInstance), "/")
	s.EntityID = strings.TrimSpace(s.EntityID)
	switch s.Kind {
	case ScopeEntity:
		if s.FlowInstance == "" || s.EntityID == "" {
			return fmt.Errorf("entity decision-card scope requires flow_instance and entity_id")
		}
	case ScopeFlow:
		if s.FlowInstance == "" || s.EntityID != "" {
			return fmt.Errorf("flow decision-card scope requires flow_instance and forbids entity_id")
		}
	case ScopeGlobal:
		if s.FlowInstance != "" || s.EntityID != "" {
			return fmt.Errorf("global decision-card scope forbids flow_instance and entity_id")
		}
	default:
		return fmt.Errorf("decision-card scope %q is not registered", s.Kind)
	}
	return nil
}

type StageGateAnchor struct {
	Route             runtimeflowidentity.Route
	FlowID            string
	EntityID          string
	Source            events.RoutingSource
	Stage             string
	StageActivationID string
}

type HumanTaskAnchor struct {
	RequesterAgentID string
	OperationID      string
	Category         string
	Scope            Scope
	Source           events.RoutingSource
}

type ProposedEffectAnchor struct {
	RequestEventID string
	ActivityID     string
	Decision       string
	Scope          Scope
	Source         events.RoutingSource
}

func RegisteredAnchorKinds() []AnchorKind {
	return []AnchorKind{AnchorKindStageGate, AnchorKindHumanTask, AnchorKindProposedEffect}
}

func RegisteredAnchorKindNames() []string {
	kinds := RegisteredAnchorKinds()
	out := make([]string, len(kinds))
	for i, kind := range kinds {
		out[i] = string(kind)
	}
	return out
}

func RegisteredAnchorKindDescription() string {
	return strings.Join(RegisteredAnchorKindNames(), ", ")
}

func IsRegisteredAnchorKind(value string) bool {
	candidate := AnchorKind(strings.TrimSpace(value))
	for _, kind := range RegisteredAnchorKinds() {
		if candidate == kind {
			return true
		}
	}
	return false
}

// Anchor is the closed decision-card identity union. Its semantic payload is
// immutable and can only be constructed through a registered anchor kind.
type Anchor struct {
	kind AnchorKind
	data semanticvalue.Value
}

func NewStageGateAnchor(in StageGateAnchor) (Anchor, error) {
	in.Route = runtimeflowidentity.StoredRoute(in.Route.ScopeKey, in.Route.InstanceID, in.Route.InstancePath)
	in.FlowID = strings.TrimSpace(in.FlowID)
	in.EntityID = strings.TrimSpace(in.EntityID)
	in.Stage = strings.TrimSpace(in.Stage)
	in.StageActivationID = strings.TrimSpace(in.StageActivationID)
	if !in.Route.Valid() {
		return Anchor{}, fmt.Errorf("stage_gate anchor route is required")
	}
	for name, value := range map[string]string{
		"flow_id": in.FlowID, "entity_id": in.EntityID, "stage": in.Stage,
		"stage_activation_id": in.StageActivationID,
	} {
		if value == "" {
			return Anchor{}, fmt.Errorf("stage_gate anchor %s is required", name)
		}
	}
	if err := validateAnchorExecutionSource(in.Source); err != nil {
		return Anchor{}, fmt.Errorf("stage_gate anchor source: %w", err)
	}
	if err := validateStageGateSourceOwner(in); err != nil {
		return Anchor{}, err
	}
	values := map[string]any{
		"flow_scope_key":      in.Route.ScopeKey,
		"flow_instance_id":    in.Route.InstanceID,
		"flow_instance":       in.Route.InstancePath,
		"entity_id":           in.EntityID,
		"routing_source":      in.Source,
		"stage":               in.Stage,
		"stage_activation_id": in.StageActivationID,
	}
	values["flow_id"] = in.FlowID
	data, err := canonicaljson.FromGo(values)
	if err != nil {
		return Anchor{}, fmt.Errorf("admit stage_gate anchor: %w", err)
	}
	return Anchor{kind: AnchorKindStageGate, data: data}, nil
}

func NewHumanTaskAnchor(in HumanTaskAnchor) (Anchor, error) {
	in.RequesterAgentID = strings.TrimSpace(in.RequesterAgentID)
	in.OperationID = strings.TrimSpace(in.OperationID)
	in.Category = strings.TrimSpace(in.Category)
	if in.RequesterAgentID == "" {
		return Anchor{}, fmt.Errorf("human_task anchor requester_agent_id is required")
	}
	if in.OperationID == "" {
		return Anchor{}, fmt.Errorf("human_task anchor operation_id is required")
	}
	if in.Category == "" {
		return Anchor{}, fmt.Errorf("human_task anchor category is required")
	}
	if err := in.Scope.Validate(); err != nil {
		return Anchor{}, err
	}
	if err := validateAnchorExecutionSource(in.Source); err != nil {
		return Anchor{}, fmt.Errorf("human_task anchor source: %w", err)
	}
	data, err := canonicaljson.FromGo(map[string]any{
		"requester_agent_id": in.RequesterAgentID,
		"operation_id":       in.OperationID,
		"category":           in.Category,
		"scope":              in.Scope,
		"routing_source":     in.Source,
	})
	if err != nil {
		return Anchor{}, fmt.Errorf("admit human_task anchor: %w", err)
	}
	return Anchor{kind: AnchorKindHumanTask, data: data}, nil
}

func NewProposedEffectAnchor(in ProposedEffectAnchor) (Anchor, error) {
	in.RequestEventID = strings.TrimSpace(in.RequestEventID)
	in.ActivityID = strings.TrimSpace(in.ActivityID)
	in.Decision = strings.TrimSpace(in.Decision)
	if in.RequestEventID == "" || in.ActivityID == "" || in.Decision == "" {
		return Anchor{}, fmt.Errorf("proposed_effect anchor request_event_id, activity_id, and decision are required")
	}
	if err := in.Scope.Validate(); err != nil {
		return Anchor{}, err
	}
	if err := validateAnchorExecutionSource(in.Source); err != nil {
		return Anchor{}, fmt.Errorf("proposed_effect anchor source: %w", err)
	}
	data, err := canonicaljson.FromGo(map[string]any{
		"request_event_id": in.RequestEventID,
		"activity_id":      in.ActivityID,
		"decision":         in.Decision,
		"scope":            in.Scope,
		"routing_source":   in.Source,
	})
	if err != nil {
		return Anchor{}, fmt.Errorf("admit proposed_effect anchor: %w", err)
	}
	return Anchor{kind: AnchorKindProposedEffect, data: data}, nil
}

func validateAnchorExecutionSource(source events.RoutingSource) error {
	switch source.Kind() {
	case events.RoutingSourceRoot, events.RoutingSourceStaticFlow, events.RoutingSourceConcreteTemplateInstance:
		return nil
	default:
		return fmt.Errorf("requires root, static-flow, or concrete-template execution source")
	}
}

func validateStageGateSourceOwner(in StageGateAnchor) error {
	route := in.Source.Route()
	switch in.Source.Kind() {
	case events.RoutingSourceRoot:
		if in.FlowID != "." || route != (events.RouteIdentity{EntityID: in.EntityID}.Normalized()) {
			return fmt.Errorf("root stage_gate anchor source does not match its project owner")
		}
	case events.RoutingSourceStaticFlow:
		if in.FlowID == "" || route.FlowID != in.FlowID || route.EntityID != in.EntityID {
			return fmt.Errorf("flow stage_gate anchor source does not match its flow owner")
		}
	case events.RoutingSourceConcreteTemplateInstance:
		if in.FlowID == "" || route != (events.RouteIdentity{FlowID: in.FlowID, FlowInstance: in.Route.InstancePath, EntityID: in.EntityID}.Normalized()) {
			return fmt.Errorf("flow stage_gate anchor source does not match its flow owner")
		}
	default:
		return fmt.Errorf("stage_gate anchor source kind %q is not an execution owner", in.Source.Kind())
	}
	return nil
}

func validateProposedEffectSourceOwner(anchor ProposedEffectAnchor, continuation ProposedEffectContinuation) error {
	continuation = continuation.Canonical()
	scope := anchor.Scope
	scope.FlowInstance = strings.Trim(strings.TrimSpace(scope.FlowInstance), "/")
	scope.EntityID = strings.TrimSpace(scope.EntityID)
	if scope.Kind != ScopeEntity || scope.FlowInstance != continuation.FlowInstance || scope.EntityID != continuation.EntityID {
		return fmt.Errorf("proposed-effect anchor scope does not match its continuation owner")
	}

	route := anchor.Source.Route()
	switch anchor.Source.Kind() {
	case events.RoutingSourceRoot:
		if route != (events.RouteIdentity{EntityID: continuation.EntityID}.Normalized()) {
			return fmt.Errorf("root proposed-effect anchor source does not match its continuation owner")
		}
	case events.RoutingSourceStaticFlow, events.RoutingSourceConcreteTemplateInstance:
		want := (events.RouteIdentity{
			FlowID: continuation.FlowID, FlowInstance: continuation.FlowInstance, EntityID: continuation.EntityID,
		}).Normalized()
		if route != want {
			return fmt.Errorf("flow proposed-effect anchor source does not match its continuation owner")
		}
	default:
		return fmt.Errorf("proposed-effect anchor source kind %q is not an execution owner", anchor.Source.Kind())
	}
	return nil
}

func validateHumanTaskSourceOwner(anchor HumanTaskAnchor, continuation HumanTaskContinuation) error {
	want := continuation.RequesterRoute.Normalized()
	if want.Empty() {
		return fmt.Errorf("human-task continuation requester route is required")
	}
	if anchor.Source.Route() != want {
		return fmt.Errorf("human-task anchor source does not match its continuation requester owner")
	}
	switch anchor.Source.Kind() {
	case events.RoutingSourceRoot:
		if want.FlowID != "" || want.FlowInstance != "" {
			return fmt.Errorf("root human-task requester owner forbids flow identity")
		}
	case events.RoutingSourceStaticFlow, events.RoutingSourceConcreteTemplateInstance:
		if want.FlowID == "" || want.FlowInstance == "" {
			return fmt.Errorf("flow human-task requester owner requires flow identity")
		}
	default:
		return fmt.Errorf("human-task anchor source kind %q is not an execution owner", anchor.Source.Kind())
	}
	return nil
}

func DecodeAnchor(kind string, raw []byte) (Anchor, error) {
	value, err := canonicaljson.Decode(raw)
	if err != nil {
		return Anchor{}, fmt.Errorf("decode decision-card anchor: %w", err)
	}
	anchor := Anchor{kind: AnchorKind(strings.TrimSpace(kind)), data: value}
	if err := anchor.Validate(); err != nil {
		return Anchor{}, err
	}
	return anchor, nil
}

func (a Anchor) Kind() AnchorKind { return a.kind }

func (a Anchor) SemanticValue() semanticvalue.Value { return a.data }

func (a Anchor) Validate() error {
	switch a.kind {
	case AnchorKindStageGate:
		_, err := a.StageGate()
		return err
	case AnchorKindHumanTask:
		_, err := a.HumanTask()
		return err
	case AnchorKindProposedEffect:
		_, err := a.ProposedEffect()
		return err
	default:
		return fmt.Errorf("decision-card anchor kind %q is not registered", a.kind)
	}
}

func (a Anchor) Scope() (Scope, error) {
	switch a.kind {
	case AnchorKindStageGate:
		stage, err := a.StageGate()
		if err != nil {
			return Scope{}, err
		}
		return Scope{Kind: ScopeEntity, FlowInstance: stage.Route.InstancePath, EntityID: stage.EntityID}, nil
	case AnchorKindHumanTask:
		task, err := a.HumanTask()
		if err != nil {
			return Scope{}, err
		}
		return task.Scope, nil
	case AnchorKindProposedEffect:
		effect, err := a.ProposedEffect()
		if err != nil {
			return Scope{}, err
		}
		return effect.Scope, nil
	default:
		return Scope{}, fmt.Errorf("decision-card anchor kind %q is not registered", a.kind)
	}
}

func (a Anchor) ExecutionRoutingSource() (events.RoutingSource, error) {
	switch a.kind {
	case AnchorKindStageGate:
		anchor, err := a.StageGate()
		return anchor.Source, err
	case AnchorKindHumanTask:
		anchor, err := a.HumanTask()
		return anchor.Source, err
	case AnchorKindProposedEffect:
		anchor, err := a.ProposedEffect()
		return anchor.Source, err
	default:
		return events.RoutingSource{}, fmt.Errorf("decision-card anchor kind %q is not registered", a.kind)
	}
}

// ControlRoutingSource projects an immutable card owner into its non-authored
// runtime-control source. Root-owned cards are closed platform controls;
// flow-owned cards retain their exact typed route.
func (a Anchor) ControlRoutingSource() (events.RoutingSource, error) {
	source, err := a.ExecutionRoutingSource()
	if err != nil {
		return events.RoutingSource{}, err
	}
	if source.Kind() == events.RoutingSourceRoot {
		return events.NewPlatformControlRoutingSource(), nil
	}
	control, err := events.NewFlowOwnedControlRoutingSource(source.Route())
	if err != nil {
		return events.RoutingSource{}, fmt.Errorf("decision-card control source: %w", err)
	}
	return control, nil
}

func (a Anchor) ProposedEffect() (ProposedEffectAnchor, error) {
	if a.kind != AnchorKindProposedEffect {
		return ProposedEffectAnchor{}, fmt.Errorf("decision-card anchor %q is not proposed_effect", a.kind)
	}
	values, ok := a.data.ObjectMap()
	if !ok {
		return ProposedEffectAnchor{}, fmt.Errorf("proposed_effect anchor must be an object")
	}
	if err := exactAnchorFields(values, "proposed_effect", []string{"request_event_id", "activity_id", "decision", "scope", "routing_source"}, nil); err != nil {
		return ProposedEffectAnchor{}, err
	}
	scopeValue, ok := values["scope"]
	if !ok {
		return ProposedEffectAnchor{}, fmt.Errorf("proposed_effect anchor scope is required")
	}
	scopeMap, ok := scopeValue.ObjectMap()
	if !ok {
		return ProposedEffectAnchor{}, fmt.Errorf("proposed_effect anchor scope must be an object")
	}
	if err := exactAnchorFields(scopeMap, "proposed_effect scope", []string{"kind"}, []string{"flow_instance", "entity_id"}); err != nil {
		return ProposedEffectAnchor{}, err
	}
	out := ProposedEffectAnchor{
		RequestEventID: requiredAnchorString(values, "request_event_id"),
		ActivityID:     requiredAnchorString(values, "activity_id"),
		Decision:       requiredAnchorString(values, "decision"),
		Scope: Scope{
			Kind: ScopeKind(requiredAnchorString(scopeMap, "kind")), FlowInstance: optionalAnchorString(scopeMap, "flow_instance"), EntityID: optionalAnchorString(scopeMap, "entity_id"),
		},
	}
	if err := canonicaljson.ValueInto(values["routing_source"], &out.Source); err != nil {
		return ProposedEffectAnchor{}, fmt.Errorf("proposed_effect anchor routing_source: %w", err)
	}
	if out.RequestEventID == "" || out.ActivityID == "" || out.Decision == "" {
		return ProposedEffectAnchor{}, fmt.Errorf("proposed_effect anchor contains an empty required identity")
	}
	if err := out.Scope.Validate(); err != nil {
		return ProposedEffectAnchor{}, err
	}
	if err := validateAnchorExecutionSource(out.Source); err != nil {
		return ProposedEffectAnchor{}, fmt.Errorf("proposed_effect anchor source: %w", err)
	}
	return out, nil
}

func (a Anchor) StageGate() (StageGateAnchor, error) {
	if a.kind != AnchorKindStageGate {
		return StageGateAnchor{}, fmt.Errorf("decision-card anchor %q is not stage_gate", a.kind)
	}
	values, ok := a.data.ObjectMap()
	if !ok {
		return StageGateAnchor{}, fmt.Errorf("stage_gate anchor must be an object")
	}
	if err := exactAnchorFields(values, "stage_gate", []string{"flow_scope_key", "flow_instance_id", "flow_instance", "flow_id", "entity_id", "routing_source", "stage", "stage_activation_id"}, nil); err != nil {
		return StageGateAnchor{}, err
	}
	out := StageGateAnchor{
		Route: runtimeflowidentity.StoredRoute(
			requiredAnchorString(values, "flow_scope_key"),
			requiredAnchorString(values, "flow_instance_id"),
			requiredAnchorString(values, "flow_instance"),
		),
		FlowID:            requiredAnchorString(values, "flow_id"),
		EntityID:          requiredAnchorString(values, "entity_id"),
		Stage:             requiredAnchorString(values, "stage"),
		StageActivationID: requiredAnchorString(values, "stage_activation_id"),
	}
	if err := canonicaljson.ValueInto(values["routing_source"], &out.Source); err != nil {
		return StageGateAnchor{}, fmt.Errorf("stage_gate anchor routing_source: %w", err)
	}
	if !out.Route.Valid() || out.FlowID == "" || out.EntityID == "" || out.Stage == "" || out.StageActivationID == "" {
		return StageGateAnchor{}, fmt.Errorf("stage_gate anchor contains an empty required identity")
	}
	if err := validateAnchorExecutionSource(out.Source); err != nil {
		return StageGateAnchor{}, fmt.Errorf("stage_gate anchor source: %w", err)
	}
	if err := validateStageGateSourceOwner(out); err != nil {
		return StageGateAnchor{}, err
	}
	return out, nil
}

func (a Anchor) HumanTask() (HumanTaskAnchor, error) {
	if a.kind != AnchorKindHumanTask {
		return HumanTaskAnchor{}, fmt.Errorf("decision-card anchor %q is not human_task", a.kind)
	}
	values, ok := a.data.ObjectMap()
	if !ok {
		return HumanTaskAnchor{}, fmt.Errorf("human_task anchor must be an object")
	}
	if err := exactAnchorFields(values, "human_task", []string{"requester_agent_id", "operation_id", "category", "scope", "routing_source"}, nil); err != nil {
		return HumanTaskAnchor{}, err
	}
	scopeValue, ok := values["scope"]
	if !ok {
		return HumanTaskAnchor{}, fmt.Errorf("human_task anchor scope is required")
	}
	scopeMap, ok := scopeValue.ObjectMap()
	if !ok {
		return HumanTaskAnchor{}, fmt.Errorf("human_task anchor scope must be an object")
	}
	if err := exactAnchorFields(scopeMap, "human_task scope", []string{"kind"}, []string{"flow_instance", "entity_id"}); err != nil {
		return HumanTaskAnchor{}, err
	}
	out := HumanTaskAnchor{
		RequesterAgentID: requiredAnchorString(values, "requester_agent_id"),
		OperationID:      requiredAnchorString(values, "operation_id"),
		Category:         requiredAnchorString(values, "category"),
		Scope: Scope{
			Kind:         ScopeKind(requiredAnchorString(scopeMap, "kind")),
			FlowInstance: optionalAnchorString(scopeMap, "flow_instance"),
			EntityID:     optionalAnchorString(scopeMap, "entity_id"),
		},
	}
	if err := canonicaljson.ValueInto(values["routing_source"], &out.Source); err != nil {
		return HumanTaskAnchor{}, fmt.Errorf("human_task anchor routing_source: %w", err)
	}
	if out.RequesterAgentID == "" || out.OperationID == "" || out.Category == "" {
		return HumanTaskAnchor{}, fmt.Errorf("human_task anchor contains an empty required identity")
	}
	if err := out.Scope.Validate(); err != nil {
		return HumanTaskAnchor{}, err
	}
	if err := validateAnchorExecutionSource(out.Source); err != nil {
		return HumanTaskAnchor{}, fmt.Errorf("human_task anchor source: %w", err)
	}
	return out, nil
}

func exactAnchorFields(values map[string]semanticvalue.Value, label string, required, optional []string) error {
	allowed := make(map[string]struct{}, len(required)+len(optional))
	for _, field := range required {
		allowed[field] = struct{}{}
		if _, ok := values[field]; !ok {
			return fmt.Errorf("%s anchor %s is required", label, field)
		}
	}
	for _, field := range optional {
		allowed[field] = struct{}{}
	}
	for field := range values {
		if _, ok := allowed[field]; !ok {
			return fmt.Errorf("%s anchor field %s is not allowed", label, field)
		}
	}
	return nil
}

func requiredAnchorString(values map[string]semanticvalue.Value, field string) string {
	value, ok := values[field]
	if !ok {
		return ""
	}
	text, _ := value.String()
	return strings.TrimSpace(text)
}

func optionalAnchorString(values map[string]semanticvalue.Value, field string) string {
	return requiredAnchorString(values, field)
}
