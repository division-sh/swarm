package contracts

import (
	"fmt"
	"strconv"
	"strings"
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
	if handler.CreateEntity {
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
	add("guard", handler.Guard != nil)
	add("rules", len(handler.Rules) > 0)
	add("on_complete", len(handler.OnComplete) > 0)
	add("accumulate", handler.Accumulate != nil)
	add("join", handler.Join != nil)
	add("fan_out", handler.FanOut != nil)
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
