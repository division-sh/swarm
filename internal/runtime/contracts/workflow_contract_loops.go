package contracts

import (
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type FlowLoopDeclarations struct {
	Declared bool
	Entries  []FlowLoopDeclaration
}

type FlowLoopDeclaration struct {
	ID            string           `yaml:"-"`
	RevisionField string           `yaml:"revision_field"`
	MaxAttempts   LoopAttemptLimit `yaml:"max_attempts"`
	Escape        LoopEscapeSpec   `yaml:"escape"`
}

type LoopAttemptLimit struct {
	Literal   int
	PolicyRef string
}

type LoopEscapeSpec struct {
	AdvancesTo string   `yaml:"advances_to"`
	Emit       EmitSpec `yaml:"emit"`
}

type LoopOperationKind string

const (
	LoopOperationStart  LoopOperationKind = "start"
	LoopOperationAdmit  LoopOperationKind = "admit"
	LoopOperationRepeat LoopOperationKind = "repeat"
	LoopOperationClose  LoopOperationKind = "close"
)

type LoopOperationSpec struct {
	Start  string `yaml:"start"`
	Admit  string `yaml:"admit"`
	Repeat string `yaml:"repeat"`
	Close  string `yaml:"close"`
	From   string `yaml:"from"`
}

var loopDeclarationFieldOptions = map[string]struct{}{
	"revision_field": {}, "max_attempts": {}, "escape": {},
}
var loopEscapeFieldOptions = map[string]struct{}{
	"advances_to": {}, "emit": {},
}
var loopOperationFieldOptions = map[string]struct{}{
	"start": {}, "admit": {}, "repeat": {}, "close": {}, "from": {},
}

func (d *FlowLoopDeclarations) UnmarshalYAML(node *yaml.Node) error {
	if d == nil {
		return nil
	}
	*d = FlowLoopDeclarations{Declared: true}
	if node == nil || node.Kind != yaml.MappingNode {
		return fmt.Errorf("loops must be a keyed mapping")
	}
	seenIDs := map[string]struct{}{}
	seenFields := map[string]string{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		id := strings.TrimSpace(node.Content[i].Value)
		if id == "" {
			return fmt.Errorf("loops contains an empty loop id")
		}
		if _, ok := seenIDs[id]; ok {
			return fmt.Errorf("loops contains duplicate id %q", id)
		}
		seenIDs[id] = struct{}{}
		var decl FlowLoopDeclaration
		if err := node.Content[i+1].Decode(&decl); err != nil {
			return fmt.Errorf("loop %q: %w", id, err)
		}
		decl.ID = id
		if previous, ok := seenFields[decl.RevisionField]; ok {
			return fmt.Errorf("loops %q and %q use the same revision_field %q", previous, id, decl.RevisionField)
		}
		seenFields[decl.RevisionField] = id
		d.Entries = append(d.Entries, decl)
	}
	return nil
}

func (d *FlowLoopDeclaration) UnmarshalYAML(node *yaml.Node) error {
	if d == nil {
		return nil
	}
	if node == nil || node.Kind != yaml.MappingNode {
		return fmt.Errorf("loop declaration must be a mapping")
	}
	if err := validateClosedMapping("loop", node, loopDeclarationFieldOptions); err != nil {
		return err
	}
	type alias FlowLoopDeclaration
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*d = FlowLoopDeclaration(aux)
	d.RevisionField = strings.TrimSpace(d.RevisionField)
	if d.RevisionField == "" {
		return fmt.Errorf("revision_field is required")
	}
	if d.MaxAttempts.Empty() {
		return fmt.Errorf("max_attempts is required")
	}
	if strings.TrimSpace(d.Escape.AdvancesTo) == "" {
		return fmt.Errorf("escape.advances_to is required")
	}
	return nil
}

func (l *LoopAttemptLimit) UnmarshalYAML(node *yaml.Node) error {
	if l == nil {
		return nil
	}
	*l = LoopAttemptLimit{}
	if node == nil || node.Kind != yaml.ScalarNode {
		return fmt.Errorf("max_attempts must be a positive integer or {{policy_key}}")
	}
	if node.Tag == "!!int" {
		value, err := strconv.Atoi(strings.TrimSpace(node.Value))
		if err != nil || value <= 0 {
			return fmt.Errorf("max_attempts must be a positive integer")
		}
		l.Literal = value
		return nil
	}
	raw := strings.TrimSpace(node.Value)
	if !strings.HasPrefix(raw, "{{") || !strings.HasSuffix(raw, "}}") {
		return fmt.Errorf("max_attempts string must use {{policy_key}}")
	}
	key := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(raw, "{{"), "}}"))
	key = strings.TrimPrefix(key, "policy.")
	if key == "" || strings.ContainsAny(key, "{}[]() ") {
		return fmt.Errorf("max_attempts policy reference %q is invalid", raw)
	}
	l.PolicyRef = key
	return nil
}

func (l LoopAttemptLimit) Empty() bool {
	return l.Literal <= 0 && strings.TrimSpace(l.PolicyRef) == ""
}

func (l LoopAttemptLimit) String() string {
	if l.Literal > 0 {
		return strconv.Itoa(l.Literal)
	}
	if key := strings.TrimSpace(l.PolicyRef); key != "" {
		return "{{" + key + "}}"
	}
	return ""
}

func (e *LoopEscapeSpec) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	if node == nil || node.Kind != yaml.MappingNode {
		return fmt.Errorf("loop escape must be a mapping")
	}
	if err := validateClosedMapping("loop escape", node, loopEscapeFieldOptions); err != nil {
		return err
	}
	type alias LoopEscapeSpec
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*e = LoopEscapeSpec(aux)
	e.AdvancesTo = strings.TrimSpace(e.AdvancesTo)
	return nil
}

func (o *LoopOperationSpec) UnmarshalYAML(node *yaml.Node) error {
	if o == nil {
		return nil
	}
	if node == nil || node.Kind != yaml.MappingNode {
		return fmt.Errorf("handler loop operation must be a mapping")
	}
	if err := validateClosedMapping("handler loop operation", node, loopOperationFieldOptions); err != nil {
		return err
	}
	type alias LoopOperationSpec
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*o = LoopOperationSpec(aux)
	o.Start = strings.TrimSpace(o.Start)
	o.Admit = strings.TrimSpace(o.Admit)
	o.Repeat = strings.TrimSpace(o.Repeat)
	o.Close = strings.TrimSpace(o.Close)
	o.From = strings.TrimSpace(o.From)
	if _, _, err := o.Operation(); err != nil {
		return err
	}
	if o.From == "" {
		return fmt.Errorf("handler loop operation from is required")
	}
	return nil
}

func (o LoopOperationSpec) Operation() (LoopOperationKind, string, error) {
	candidates := []struct {
		kind LoopOperationKind
		id   string
	}{
		{LoopOperationStart, strings.TrimSpace(o.Start)},
		{LoopOperationAdmit, strings.TrimSpace(o.Admit)},
		{LoopOperationRepeat, strings.TrimSpace(o.Repeat)},
		{LoopOperationClose, strings.TrimSpace(o.Close)},
	}
	var kind LoopOperationKind
	var id string
	count := 0
	for _, item := range candidates {
		if item.id != "" {
			kind, id, count = item.kind, item.id, count+1
		}
	}
	if count != 1 {
		return "", "", fmt.Errorf("handler loop operation must declare exactly one of start, admit, repeat, or close")
	}
	return kind, id, nil
}

// ValidateLoopHandlerCombination is the single contract owner for the handler
// matrix accepted around a loop lifecycle operation.
func ValidateLoopHandlerCombination(handler SystemNodeEventHandler) error {
	if handler.Loop == nil {
		return nil
	}
	kind, _, err := handler.Loop.Operation()
	if err != nil {
		return err
	}
	if handler.CreateEntity || (handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty()) {
		return fmt.Errorf("loop operation requires an existing workflow instance and cannot create or select-or-create an entity")
	}
	if strings.TrimSpace(handler.Condition) != "" || strings.TrimSpace(handler.Logic) != "" {
		return fmt.Errorf("loop operation cannot use deprecated handler condition or logic")
	}
	if kind == LoopOperationAdmit {
		return nil
	}
	conflicts := make([]string, 0, 12)
	add := func(name string, present bool) {
		if present {
			conflicts = append(conflicts, name)
		}
	}
	add("evidence_target", strings.TrimSpace(handler.EvidenceTarget) != "")
	add("guard", handler.Guard != nil)
	add("rules", len(handler.Rules) > 0)
	add("on_complete", len(handler.OnComplete) > 0)
	add("accumulate", handler.Accumulate != nil)
	add("join", handler.Join != nil)
	add("fan_out", handler.FanOut != nil)
	add("action", strings.TrimSpace(handler.Action.ID) != "")
	add("activity", !handler.Activity.Empty())
	add("on_success", !handler.OnSuccess.Empty())
	add("clear", handler.Clear != nil)
	if len(conflicts) > 0 {
		return fmt.Errorf("loop %s operation cannot be combined with handler fields %s", kind, strings.Join(conflicts, ", "))
	}
	if strings.TrimSpace(handler.AdvancesTo) == "" {
		return fmt.Errorf("loop %s operation requires advances_to", kind)
	}
	return nil
}

func validateClosedMapping(context string, node *yaml.Node, allowed map[string]struct{}) error {
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if _, ok := allowed[key]; !ok {
			return NewUndefinedFieldDiagnostic(context, key, allowed)
		}
	}
	return nil
}
