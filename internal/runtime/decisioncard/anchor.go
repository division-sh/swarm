package decisioncard

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

type AnchorKind string

const (
	AnchorKindStageGate AnchorKind = "stage_gate"
	AnchorKindHumanTask AnchorKind = "human_task"
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
	FlowInstance      string
	FlowID            string
	EntityID          string
	Stage             string
	StageActivationID string
}

type HumanTaskAnchor struct {
	RequesterAgentID string
	OperationID      string
	Category         string
	Scope            Scope
}

// Anchor is the closed decision-card identity union. Its semantic payload is
// immutable and can only be constructed through a registered anchor kind.
type Anchor struct {
	kind AnchorKind
	data semanticvalue.Value
}

func NewStageGateAnchor(in StageGateAnchor) (Anchor, error) {
	in.FlowInstance = strings.Trim(strings.TrimSpace(in.FlowInstance), "/")
	in.FlowID = strings.TrimSpace(in.FlowID)
	in.EntityID = strings.TrimSpace(in.EntityID)
	in.Stage = strings.TrimSpace(in.Stage)
	in.StageActivationID = strings.TrimSpace(in.StageActivationID)
	for name, value := range map[string]string{
		"flow_instance": in.FlowInstance, "entity_id": in.EntityID, "stage": in.Stage,
		"stage_activation_id": in.StageActivationID,
	} {
		if value == "" {
			return Anchor{}, fmt.Errorf("stage_gate anchor %s is required", name)
		}
	}
	values := map[string]any{
		"flow_instance":       in.FlowInstance,
		"entity_id":           in.EntityID,
		"stage":               in.Stage,
		"stage_activation_id": in.StageActivationID,
	}
	if in.FlowID != "" {
		values["flow_id"] = in.FlowID
	}
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
	data, err := canonicaljson.FromGo(map[string]any{
		"requester_agent_id": in.RequesterAgentID,
		"operation_id":       in.OperationID,
		"category":           in.Category,
		"scope":              in.Scope,
	})
	if err != nil {
		return Anchor{}, fmt.Errorf("admit human_task anchor: %w", err)
	}
	return Anchor{kind: AnchorKindHumanTask, data: data}, nil
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
		return Scope{Kind: ScopeEntity, FlowInstance: stage.FlowInstance, EntityID: stage.EntityID}, nil
	case AnchorKindHumanTask:
		task, err := a.HumanTask()
		if err != nil {
			return Scope{}, err
		}
		return task.Scope, nil
	default:
		return Scope{}, fmt.Errorf("decision-card anchor kind %q is not registered", a.kind)
	}
}

func (a Anchor) StageGate() (StageGateAnchor, error) {
	if a.kind != AnchorKindStageGate {
		return StageGateAnchor{}, fmt.Errorf("decision-card anchor %q is not stage_gate", a.kind)
	}
	values, ok := a.data.ObjectMap()
	if !ok {
		return StageGateAnchor{}, fmt.Errorf("stage_gate anchor must be an object")
	}
	if err := exactAnchorFields(values, "stage_gate", []string{"flow_instance", "entity_id", "stage", "stage_activation_id"}, []string{"flow_id"}); err != nil {
		return StageGateAnchor{}, err
	}
	out := StageGateAnchor{
		FlowInstance:      requiredAnchorString(values, "flow_instance"),
		FlowID:            optionalAnchorString(values, "flow_id"),
		EntityID:          requiredAnchorString(values, "entity_id"),
		Stage:             requiredAnchorString(values, "stage"),
		StageActivationID: requiredAnchorString(values, "stage_activation_id"),
	}
	if out.FlowInstance == "" || out.EntityID == "" || out.Stage == "" || out.StageActivationID == "" {
		return StageGateAnchor{}, fmt.Errorf("stage_gate anchor contains an empty required identity")
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
	if err := exactAnchorFields(values, "human_task", []string{"requester_agent_id", "operation_id", "category", "scope"}, nil); err != nil {
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
	if out.RequesterAgentID == "" || out.OperationID == "" || out.Category == "" {
		return HumanTaskAnchor{}, fmt.Errorf("human_task anchor contains an empty required identity")
	}
	if err := out.Scope.Validate(); err != nil {
		return HumanTaskAnchor{}, err
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
