package contracts

import (
	"fmt"
	"slices"
	"strings"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

// WorkflowTransitionSite identifies the selected compiled carrier, independently
// of its possible source stages and the rule that executed it. An inherited
// handler target remains handler-owned even when an execution rule was selected.
type WorkflowTransitionSite struct {
	Node           runtimeidentity.ExecutableNode
	HandlerEvent   string
	AdvanceCarrier HandlerAdvanceCarrierKind
	RuleRef        runtimeidentity.DeclarationIdentity
	LoopID         string
	LoopOperation  LoopOperationKind
	LoopEscape     bool
	TimerID        string
	DecisionID     string
	Verdict        string
}

func (e WorkflowStageTopologyEdge) Site() WorkflowTransitionSite {
	return WorkflowTransitionSite{
		Node: e.Node, HandlerEvent: e.HandlerEvent,
		AdvanceCarrier: e.AdvanceCarrier, RuleRef: e.RuleRef,
		LoopID: e.LoopID, LoopOperation: e.LoopOperation, LoopEscape: e.Source == "loop.escape",
		TimerID: e.TimerID, DecisionID: e.DecisionID, Verdict: e.Verdict,
	}
}

// CompiledTransition is the graph-owned result of exact carrier admission.
// Dynamic verdict, activation, generation and CAS authority remain with their
// execution owners; graph membership alone grants none of those permissions.
type CompiledTransition struct {
	flow string
	edge WorkflowStageTopologyEdge
}

// GuardTerminationTarget preserves the existing kill disposition's flow-local
// target selection. It is not an authored transition edge.
func (t WorkflowStageTopology) GuardTerminationTarget() string {
	for _, stages := range [][]string{t.TerminalStages, t.Stages} {
		for _, stage := range stages {
			if strings.EqualFold(stage, "killed") {
				return stage
			}
		}
	}
	return ""
}

func (t CompiledTransition) FlowID() string                  { return t.flow }
func (t CompiledTransition) Edge() WorkflowStageTopologyEdge { return t.edge }
func (t CompiledTransition) Valid() bool                     { return t.Validate() == nil }

// ValidateAgainst checks persisted or transported evidence against the selected
// source graph without selecting a replacement carrier.
func (t CompiledTransition) ValidateAgainst(graph WorkflowStageTopology) error {
	if err := t.Validate(); err != nil {
		return err
	}
	if t.flow != graph.FlowID {
		return fmt.Errorf("transition belongs to another flow")
	}
	expected, err := graph.AdmitTransition(t.edge.Site(), t.edge.From, t.edge.To)
	if err != nil {
		return err
	}
	if expected.edge != t.edge {
		return fmt.Errorf("transition evidence differs from its compiled carrier")
	}
	return nil
}

func (t WorkflowStageTopology) AdmitTransition(site WorkflowTransitionSite, from, to string) (CompiledTransition, error) {
	if !slices.Contains(t.Stages, from) || !slices.Contains(t.Stages, to) {
		return CompiledTransition{}, fmt.Errorf("flow %s transition requires declared source and target: %s -> %s", t.FlowID, from, to)
	}
	if from != to && slices.Contains(t.TerminalStages, from) {
		return CompiledTransition{}, fmt.Errorf("flow %s cannot exit terminal stage %s", t.FlowID, from)
	}
	var result CompiledTransition
	matched := false
	for _, edge := range t.Edges {
		if edge.From != from || edge.To != to || edge.Site() != site {
			continue
		}
		if matched {
			return CompiledTransition{}, fmt.Errorf("flow %s has ambiguous compiled carrier for %s -> %s", t.FlowID, from, to)
		}
		matched = true
		result = CompiledTransition{flow: t.FlowID, edge: edge}
		if err := result.Validate(); err != nil {
			return CompiledTransition{}, err
		}
	}
	if !matched {
		return CompiledTransition{}, fmt.Errorf("flow %s has no selected compiled carrier for %s -> %s", t.FlowID, from, to)
	}
	return result, nil
}
