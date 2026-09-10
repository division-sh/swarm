package workflowlifecycle

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

func NewCompiledTransition(compiled contracts.CompiledTransition, selected handlerselection.HandlerRuleSelectionFact, guards []string) (Transition, error) {
	t := Transition{from: compiled.Edge().From, to: compiled.Edge().To, compiled: &compiled, selection: selected, guards: slices.Clone(guards)}
	return finishTransition(t)
}

// Guard termination is an execution outcome, not an authored topology edge.
func NewGuardTermination(graph contracts.WorkflowStageTopology, node identity.ExecutableNode, handler, guard, from, to string, guards []string) (Transition, error) {
	if !node.Valid() || node.FlowPath() != graph.FlowID || !slices.Contains(graph.Stages, from) || !slices.Contains(graph.Stages, to) {
		return Transition{}, fmt.Errorf("guard termination requires exact declared flow stages")
	}
	t := Transition{from: from, to: to, guardNode: node, guardHandler: handler, guardName: guard, guards: slices.Clone(guards), selection: handlerselection.NotApplicable()}
	return finishTransition(t)
}

func finishTransition(t Transition) (Transition, error) {
	if err := t.validateCause(); err != nil {
		return Transition{}, err
	}
	raw, err := json.Marshal(t.wire())
	if err != nil {
		return Transition{}, err
	}
	digest := sha256.Sum256(raw)
	t.id = "transition:" + hex.EncodeToString(digest[:])
	return t, nil
}

func (t Transition) FlowID() string {
	if t.compiled != nil {
		return t.compiled.FlowID()
	}
	return t.guardNode.FlowPath()
}

func (t Transition) GuardsEvaluated() []string                                { return slices.Clone(t.guards) }
func (t Transition) RuleSelection() handlerselection.HandlerRuleSelectionFact { return t.selection }

func (t Transition) Compiled() (contracts.CompiledTransition, bool) {
	if t.compiled == nil {
		return contracts.CompiledTransition{}, false
	}
	return *t.compiled, true
}

func (t Transition) HandlerOrigin() (identity.ExecutableNode, string, bool) {
	if t.compiled != nil {
		edge := t.compiled.Edge()
		return edge.Node, edge.HandlerEvent, edge.Node.Valid()
	}
	return t.guardNode, t.guardHandler, t.guardNode.Valid()
}

func (t Transition) ValidateAgainst(graph contracts.WorkflowStageTopology) error {
	if err := t.Validate(); err != nil {
		return err
	}
	if t.FlowID() != graph.FlowID {
		return fmt.Errorf("transition belongs to another flow")
	}
	if t.compiled != nil {
		return t.compiled.ValidateAgainst(graph)
	}
	if !slices.Contains(graph.Stages, t.from) || !slices.Contains(graph.Stages, t.to) {
		return fmt.Errorf("guard termination requires declared source and target")
	}
	if t.to != graph.GuardTerminationTarget() {
		return fmt.Errorf("guard termination target disagrees with the selected flow's kill target")
	}
	return nil
}

// ValidateHandlerEvidence preserves the executed rule or evaluated guard
// independently of the advance owner against the exact admitted handler.
func (t Transition) ValidateHandlerEvidence(handler contracts.SystemNodeEventHandler) error {
	if err := t.Validate(); err != nil {
		return err
	}
	if t.compiled == nil {
		failure, err := handler.Guard.FailureSpec()
		if err != nil || failure.Action != contracts.GuardFailureActionKill {
			return fmt.Errorf("guard termination requires the executed handler's kill disposition")
		}
		var labels []string
		for _, check := range handler.Guard.EffectiveChecks() {
			label := strings.TrimSpace(check.ID)
			if label == "" {
				label = strings.TrimSpace(check.Check)
			}
			if label != "" {
				labels = append(labels, label)
			}
		}
		if len(t.guards) == 0 || len(t.guards) > len(labels) || !slices.Equal(t.guards, labels[:len(t.guards)]) || t.guardName != t.guards[len(t.guards)-1] {
			return fmt.Errorf("guard termination evidence disagrees with the executed handler's guard checks")
		}
		return nil
	}
	if t.selection.Disposition() != handlerselection.DispositionSelected {
		return nil
	}
	var rules []contracts.HandlerRuleEntry
	switch t.selection.Context() {
	case handlerselection.ContextRules:
		rules = handler.Rules
	case handlerselection.ContextOnComplete:
		rules = handler.OnComplete
	case handlerselection.ContextJoinComplete:
		if handler.Join != nil {
			rules = []contracts.HandlerRuleEntry{handler.Join.OnComplete}
		}
	case handlerselection.ContextJoinTimeout:
		if handler.Join != nil {
			rules = []contracts.HandlerRuleEntry{handler.Join.Timeout.Outcome}
		}
	}
	for _, rule := range rules {
		ref, ok := rule.DeclarationIdentity()
		if ok && ref.Equal(t.selection.Ref()) {
			return nil
		}
	}
	return fmt.Errorf("selected rule is not owned by the executed handler/context")
}

func (t Transition) validateCause() error {
	if t.from == "" || t.to == "" || t.from == t.to || t.from != strings.TrimSpace(t.from) || t.to != strings.TrimSpace(t.to) {
		return fmt.Errorf("transition requires distinct canonical stages")
	}
	if err := t.selection.Validate(); err != nil {
		return err
	}
	if t.compiled != nil {
		if err := t.compiled.Validate(); err != nil {
			return err
		}
		e := t.compiled.Edge()
		if t.from != e.From || t.to != e.To || t.guardNode.Valid() || t.guardHandler != "" || t.guardName != "" {
			return fmt.Errorf("transition contradicts compiled carrier")
		}
		if e.RuleRef.Valid() && (!t.selection.Ref().Equal(e.RuleRef) || t.selection.Disposition() != handlerselection.DispositionSelected) {
			return fmt.Errorf("advance rule differs from executed rule")
		}
		if t.selection.Ref().Valid() && (!e.Node.Valid() || t.selection.Ref().Flow().String() != e.Node.FlowPath()) {
			return fmt.Errorf("selected rule is outside transition flow")
		}
	} else if !t.guardNode.Valid() || t.guardHandler == "" || t.guardName == "" || !slices.Contains(t.guards, t.guardName) || t.selection.Disposition() != handlerselection.DispositionNotApplicable {
		return fmt.Errorf("transition requires compiled carrier or actual guard termination")
	}
	return nil
}

func (t Transition) Validate() error {
	admitted, err := finishTransition(t)
	if err != nil {
		return err
	}
	if admitted.id != t.id {
		return fmt.Errorf("transition identity contradicts persisted evidence")
	}
	return nil
}

type transitionWire struct {
	Format               int                           `json:"format"`
	From                 string                        `json:"from"`
	To                   string                        `json:"to"`
	Compiled             *contracts.CompiledTransition `json:"compiled,omitempty"`
	SelectionContext     string                        `json:"selection_context"`
	SelectionDisposition string                        `json:"selection_disposition"`
	SelectedRule         string                        `json:"selected_rule,omitempty"`
	SelectedLabel        string                        `json:"selected_label,omitempty"`
	Guards               []string                      `json:"guards"`
	GuardNode            string                        `json:"guard_node,omitempty"`
	GuardHandler         string                        `json:"guard_handler,omitempty"`
	GuardName            string                        `json:"guard_name,omitempty"`
}

func (t Transition) wire() transitionWire {
	return transitionWire{Format: 1, From: t.from, To: t.to, Compiled: t.compiled, SelectionContext: string(t.selection.Context()), SelectionDisposition: string(t.selection.Disposition()), SelectedRule: t.selection.Ref().Key(), SelectedLabel: t.selection.DisplayLabel(), Guards: slices.Clone(t.guards), GuardNode: t.guardNode.Key(), GuardHandler: t.guardHandler, GuardName: t.guardName}
}

func (t Transition) MarshalJSON() ([]byte, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		ID       string         `json:"id"`
		Evidence transitionWire `json:"evidence"`
	}{t.id, t.wire()})
}

func (t *Transition) UnmarshalJSON(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var w struct {
		ID       string         `json:"id"`
		Evidence transitionWire `json:"evidence"`
	}
	if err := decoder.Decode(&w); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("transition has trailing JSON")
	}
	v := w.Evidence
	if v.Format != 1 {
		return fmt.Errorf("unsupported transition evidence format")
	}
	var ref identity.DeclarationIdentity
	var err error
	if v.SelectedRule != "" {
		ref, err = identity.ParseDeclarationIdentityKey(v.SelectedRule)
		if err != nil {
			return err
		}
	}
	flow, family, path := "", "", ""
	if ref.Valid() {
		flow, family, path = ref.Flow().String(), ref.Family(), ref.SemanticPath()
	}
	selected, err := handlerselection.Hydrate(v.SelectionContext, v.SelectionDisposition, flow, family, path, v.SelectedLabel)
	if err != nil {
		return err
	}
	value := Transition{from: v.From, to: v.To, id: w.ID, compiled: v.Compiled, selection: selected, guards: slices.Clone(v.Guards), guardHandler: v.GuardHandler, guardName: v.GuardName}
	if v.GuardNode != "" {
		value.guardNode, err = identity.ParseExecutableNodeKey(v.GuardNode)
		if err != nil {
			return err
		}
	}
	if err := value.Validate(); err != nil {
		return err
	}
	*t = value
	return nil
}
