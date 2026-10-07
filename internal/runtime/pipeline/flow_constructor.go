package pipeline

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

type FlowConstructor struct {
	source   semanticview.Source
	flowID   string
	input    string
	analysis *engine.EntityAssignmentAnalysis
	refusals []string
}

func (c FlowConstructor) Eligible() bool     { return c.analysis != nil && len(c.refusals) == 0 }
func (c FlowConstructor) Refusals() []string { return append([]string(nil), c.refusals...) }

func (c FlowConstructor) FlowID() string { return c.flowID }
func (c FlowConstructor) Input() string  { return c.input }
func (c FlowConstructor) KeyField() string {
	if c.analysis == nil {
		return ""
	}
	return c.analysis.ConstructorKey()
}
func (c FlowConstructor) SuppliedFields() []string {
	if c.analysis == nil {
		return nil
	}
	return c.analysis.ConstructorSuppliedFields()
}

// StandingConstructionIsKeyless classifies declaration ancestry, not executable
// readiness. A keyed edge requires an admitted creating input at request time.
func StandingConstructionIsKeyless(source semanticview.Source, flowID string) (bool, error) {
	bundle, found := semanticview.Bundle(source)
	if !found {
		return false, fmt.Errorf("standing construction requires the admitted flow tree")
	}
	view, found := bundle.FlowViewByID(flowID)
	if !found {
		return false, fmt.Errorf("standing constructor flow %s is absent", flowID)
	}
	for current := view; current != nil; current = current.Parent {
		if !current.Schema.Instance.Empty() {
			return false, nil
		}
	}
	return true, nil
}

// RequireStandingConstructionPath admits the whole no-argument ancestry before
// a service generation can be reconciled or its root tree constructed.
func RequireStandingConstructionPath(source semanticview.Source, flowID string) error {
	bundle, found := semanticview.Bundle(source)
	if !found {
		return fmt.Errorf("standing constructor requires the admitted flow tree")
	}
	view, found := bundle.FlowViewByID(flowID)
	if !found {
		return fmt.Errorf("standing constructor flow %s is absent", flowID)
	}
	rootID := flowID
	for current := view; current != nil; current = current.Parent {
		rootID = current.Paths.FlowPath
		constructor, err := CompileFlowConstructor(source, current.Paths.FlowPath, "")
		if err != nil {
			return err
		}
		if !constructor.Eligible() {
			return fmt.Errorf("standing constructor is ineligible for %s: %s", current.Paths.FlowPath, strings.Join(constructor.Refusals(), "; "))
		}
	}
	return requireKeylessConstructionTree(source, rootID)
}

func requireKeylessConstructionTree(source semanticview.Source, flowID string) error {
	constructor, err := CompileFlowConstructor(source, flowID, "")
	if err != nil {
		return err
	}
	if !constructor.Eligible() {
		return fmt.Errorf("standing constructor is ineligible for %s: %s", flowID, strings.Join(constructor.Refusals(), "; "))
	}
	bundle, _ := semanticview.Bundle(source)
	view, _ := bundle.FlowViewByID(flowID)
	for _, child := range view.Children {
		if !child.Schema.Instance.Empty() {
			continue
		}
		if err := requireKeylessConstructionTree(source, child.Paths.FlowPath); err != nil {
			return err
		}
	}
	return nil
}

// CompileFlowConstructors preserves every candidate's own analysis and refusal.
// Keyless flows have exactly one no-argument candidate, never payload seeds.
func CompileFlowConstructors(source semanticview.Source, flowID string) ([]FlowConstructor, error) {
	if source == nil {
		return nil, fmt.Errorf("flow constructor requires an admitted source")
	}
	flow, found := source.FlowSchemaByID(flowID)
	if !found {
		return nil, fmt.Errorf("flow %s has no admitted schema", flowID)
	}
	inputs := []string{""}
	if !flow.Instance.Empty() {
		inputs = append([]string(nil), source.FlowInputEvents(flowID)...)
		sort.Strings(inputs)
	}
	out := make([]FlowConstructor, 0, len(inputs))
	for _, input := range inputs {
		constructor, err := CompileFlowConstructor(source, flowID, input)
		if err != nil {
			constructor = FlowConstructor{source: source, flowID: flowID, input: input, refusals: []string{err.Error()}}
		}
		out = append(out, constructor)
	}
	return out, nil
}

// FlowConstructorDiagnosticAssignment keeps invalid candidates' read failures
// visible when no candidate is eligible. It cannot admit runtime construction.
func FlowConstructorDiagnosticAssignment(constructors []FlowConstructor) *engine.EntityAssignmentAnalysis {
	var eligible []*engine.EntityAssignmentAnalysis
	var invalid []*engine.EntityAssignmentAnalysis
	for _, constructor := range constructors {
		if constructor.Eligible() {
			eligible = append(eligible, constructor.analysis)
		} else if constructor.analysis != nil {
			invalid = append(invalid, constructor.analysis)
		}
	}
	if len(eligible) == 0 {
		return engine.IntersectConstructorAssignments(invalid)
	}
	return engine.IntersectConstructorAssignments(eligible)
}

func CompileFlowConstructor(source semanticview.Source, flowID, input string) (FlowConstructor, error) {
	if source == nil {
		return FlowConstructor{}, fmt.Errorf("flow constructor requires an admitted source")
	}
	flow, found := source.FlowSchemaByID(flowID)
	if !found || (!flow.Instance.Empty() && input == "") || (flow.Instance.Empty() && input != "") {
		return FlowConstructor{}, fmt.Errorf("flow %s constructor requires its exact keyed input or keyless no-argument signature", flowID)
	}
	analysis, err := engine.BuildConstructorAssignmentAnalysis(source, flowID, input)
	if err != nil {
		return FlowConstructor{}, err
	}
	constructor := FlowConstructor{source: source, flowID: flowID, input: input, analysis: analysis}
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			return FlowConstructor{}, err
		}
		if node.FlowPath() != flowID {
			continue
		}
		for event, handler := range record.Entry.EventHandlers {
			handler, err = c.QualifySystemNodeHandlerRuleRefsForEvent(node, event, handler)
			if err != nil {
				return FlowConstructor{}, err
			}
			readers := AnnotateWorkflowEntityAssignmentReaders(node, event, handler, analysis, WorkflowHandlerExecutableReaders(source, node, event, handler))
			for _, reader := range readers {
				if reader.AssignmentError != "" {
					constructor.refusals = append(constructor.refusals, fmt.Sprintf("%s/%s %s: %s", node.NodeID(), event, reader.Kind, reader.AssignmentError))
				}
			}
		}
	}
	for _, gate := range source.WorkflowGates() {
		if gate.FlowID != flowID {
			continue
		}
		known := engine.EntityAssignmentPresencePaths(analysis.StageFacts(gate.Stage))
		for field, expression := range gate.Context {
			if !expression.HasCELValue() {
				continue
			}
			if missing := workflowexpr.RequiredEntityReferences(expression.CEL, known); len(missing) != 0 {
				constructor.refusals = append(constructor.refusals, fmt.Sprintf("gate %s context %s: entity paths %s are not definitely assigned at stage %s entry", gate.Decision, field, strings.Join(missing, ", "), gate.Stage))
			}
		}
	}
	sort.Strings(constructor.refusals)
	return constructor, nil
}

// InitialFields projects one admitted constructor payload. It never executes a
// handler or borrows another input's facts, and has no persistence side effects.
func (c FlowConstructor) InitialFields(payload map[string]any, resolvedKey any) (map[string]any, error) {
	if !c.Eligible() {
		return nil, fmt.Errorf("input %s cannot create flow %s: %s", c.input, c.flowID, strings.Join(c.refusals, "; "))
	}
	if c.input == "" && (len(payload) != 0 || resolvedKey != nil) {
		return nil, fmt.Errorf("keyless flow %s has a no-argument constructor", c.flowID)
	}
	contract, found := entityruntime.ResolveForFlow(c.source, c.flowID)
	if !found {
		if !c.analysis.DeclaresFields() && resolvedKey == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("flow %s constructor lost its admitted state contract", c.flowID)
	}
	provided := map[string]any{}
	key := c.analysis.ConstructorKey()
	if key == "" {
		if resolvedKey != nil {
			return nil, fmt.Errorf("keyless flow %s cannot accept a resolved key", c.flowID)
		}
	} else {
		schema := semanticview.ResolveEventSchema(c.source, c.flowID, c.input)
		if !schema.HasSchema {
			return nil, fmt.Errorf("constructor %s has no admitted event schema", c.input)
		}
		value, err := entityruntime.NormalizeFieldValue(contract, key, resolvedKey)
		if err != nil || resolvedKey == nil {
			return nil, fmt.Errorf("constructor %s requires admitted resolved key %s: %v", c.input, key, err)
		}
		if supplied, present := payload[key]; present {
			candidate, err := entityruntime.NormalizeFieldValue(contract, key, supplied)
			if err != nil || !reflect.DeepEqual(value, candidate) {
				return nil, fmt.Errorf("constructor %s payload key %s contradicts the resolved typed key", c.input, key)
			}
		}
		validationPayload := payload
		properties, _ := schema.Schema.Schema["properties"].(map[string]any)
		if _, declared := properties[key]; declared {
			if _, supplied := payload[key]; !supplied {
				// Resolution satisfies only the declared key slot; publication
				// bytes and the supplied-field projection remain unchanged.
				validationPayload = cloneStringAnyMap(payload)
				if validationPayload == nil {
					validationPayload = map[string]any{}
				}
				validationPayload[key] = value
			}
		}
		if err := eventschema.ValidatePayloadAgainstSchema(schema.Schema.Schema, validationPayload); err != nil {
			return nil, fmt.Errorf("constructor %s payload: %w", c.input, err)
		}
		provided[key] = value
		for _, name := range c.analysis.ConstructorSuppliedFields() {
			if value, present := payload[name]; present {
				provided[name] = value
			}
		}
	}
	return entityruntime.Initialize(contract, provided)
}
