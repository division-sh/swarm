package contracts

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

type FlowStageDeclarations struct {
	Declared bool
	Entries  []FlowStageDeclaration
}

type FlowStageDeclaration struct {
	ID          string                      `yaml:"-"`
	Initial     bool                        `yaml:"initial"`
	Terminal    bool                        `yaml:"terminal"`
	Description string                      `yaml:"description"`
	Timers      []FlowStageTimerDeclaration `yaml:"timers"`
	Gate        *FlowStageGateDeclaration   `yaml:"gate"`
}

type FlowStageGateDeclaration struct {
	Decision string                                     `yaml:"decision"`
	Title    string                                     `yaml:"title"`
	Context  map[string]ExpressionValue                 `yaml:"context"`
	Outcomes map[string]FlowStageGateOutcomeDeclaration `yaml:"outcomes"`
}

type FlowStageGateOutcomeDeclaration struct {
	Label      string                            `yaml:"label"`
	Input      map[string]WorkflowGateInputField `yaml:"input"`
	AdvancesTo string                            `yaml:"advances_to"`
	Emit       EmitSpec                          `yaml:"emit"`
}

type FlowStageTimerDeclaration struct {
	ID         string `yaml:"id"`
	After      string `yaml:"after"`
	Emit       string `yaml:"emit"`
	AdvancesTo string `yaml:"advances_to"`
}

var stageDeclarationFieldOptions = map[string]struct{}{
	"initial":     {},
	"terminal":    {},
	"description": {},
	"timers":      {},
	"gate":        {},
}

var stageTimerFieldOptions = map[string]struct{}{
	"id":          {},
	"after":       {},
	"emit":        {},
	"advances_to": {},
}

var stageGateFieldOptions = map[string]struct{}{
	"decision": {},
	"title":    {},
	"context":  {},
	"outcomes": {},
}

var stageGateOutcomeFieldOptions = map[string]struct{}{
	"label":       {},
	"input":       {},
	"advances_to": {},
	"emit":        {},
}

var stageGateInputFieldOptions = map[string]struct{}{
	"type":     {},
	"required": {},
	"label":    {},
}

func (d *FlowStageDeclarations) UnmarshalYAML(node *yaml.Node) error {
	if d == nil {
		return nil
	}
	*d = FlowStageDeclarations{Declared: true}
	if node == nil || node.Kind == 0 {
		return nil
	}
	if node.Kind == yaml.ScalarNode && strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		return fmt.Errorf("stages must be a mapping or explicit empty sequence")
	}
	if node.Kind == yaml.SequenceNode {
		if len(node.Content) == 0 {
			return nil
		}
		return fmt.Errorf("stages must be a keyed mapping; only stages: [] is allowed as the explicit stateless form")
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("stages must be a keyed mapping or explicit empty sequence")
	}
	seen := map[string]struct{}{}
	out := make([]FlowStageDeclaration, 0, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			return fmt.Errorf("stages contains an empty stage id")
		}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("stages contains duplicate stage id %q", key)
		}
		seen[key] = struct{}{}
		var stage FlowStageDeclaration
		if err := node.Content[i+1].Decode(&stage); err != nil {
			return fmt.Errorf("stage %q: %w", key, err)
		}
		stage.ID = key
		if err := stage.normalizeTimerIDs(); err != nil {
			return fmt.Errorf("stage %q: %w", key, err)
		}
		out = append(out, stage)
	}
	if err := validateStageTimerIDNamespace(out); err != nil {
		return err
	}
	d.Entries = out
	return nil
}

func (s *FlowStageDeclaration) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*s = FlowStageDeclaration{}
		return nil
	}
	if node.Kind == yaml.ScalarNode && strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		*s = FlowStageDeclaration{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("stage declaration must be a mapping")
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if _, ok := stageDeclarationFieldOptions[key]; !ok {
			return NewUndefinedFieldDiagnostic("stage", key, stageDeclarationFieldOptions)
		}
	}
	type alias FlowStageDeclaration
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = FlowStageDeclaration(aux)
	return nil
}

func (t *FlowStageTimerDeclaration) UnmarshalYAML(node *yaml.Node) error {
	if t == nil {
		return nil
	}
	if node == nil || node.Kind == 0 {
		*t = FlowStageTimerDeclaration{}
		return nil
	}
	if node.Kind == yaml.ScalarNode && strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		*t = FlowStageTimerDeclaration{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("stage timer declaration must be a mapping")
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if _, ok := stageTimerFieldOptions[key]; !ok {
			return NewUndefinedFieldDiagnostic("stage timer", key, stageTimerFieldOptions)
		}
	}
	type alias FlowStageTimerDeclaration
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*t = FlowStageTimerDeclaration(aux)
	t.ID = strings.TrimSpace(t.ID)
	t.After = strings.TrimSpace(t.After)
	t.Emit = strings.TrimSpace(t.Emit)
	t.AdvancesTo = strings.TrimSpace(t.AdvancesTo)
	return nil
}

func (g *FlowStageGateDeclaration) UnmarshalYAML(node *yaml.Node) error {
	if g == nil {
		return nil
	}
	if node == nil || node.Kind == 0 || (node.Kind == yaml.ScalarNode && strings.EqualFold(strings.TrimSpace(node.Tag), "!!null")) {
		return fmt.Errorf("stage gate must be a mapping")
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("stage gate must be a mapping")
	}
	if err := validateUniqueNormalizedMappingKeys(node, "stage gate"); err != nil {
		return err
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if _, ok := stageGateFieldOptions[key]; !ok {
			return NewUndefinedFieldDiagnostic("stage gate", key, stageGateFieldOptions)
		}
	}
	var out FlowStageGateDeclaration
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "decision":
			if err := value.Decode(&out.Decision); err != nil {
				return err
			}
		case "title":
			if err := value.Decode(&out.Title); err != nil {
				return err
			}
		case "context":
			contextFields, err := decodeExpressionValueMapNode(value, "stage gate context")
			if err != nil {
				return err
			}
			for name, expression := range contextFields {
				if expression.Kind == ExpressionKindLiteral {
					if text, ok := expression.Literal.(string); ok {
						expression = CELExpression(text)
						contextFields[name] = expression
					}
				}
			}
			out.Context = contextFields
		case "outcomes":
			outcomes, err := decodeStageGateOutcomes(value)
			if err != nil {
				return err
			}
			out.Outcomes = outcomes
		}
	}
	out.Decision = strings.TrimSpace(out.Decision)
	out.Title = strings.TrimSpace(out.Title)
	if out.Decision == "" {
		return fmt.Errorf("stage gate decision is required")
	}
	if len(out.Outcomes) == 0 {
		return fmt.Errorf("stage gate %s requires at least one outcome", out.Decision)
	}
	for verdict, outcome := range out.Outcomes {
		verdict = strings.TrimSpace(verdict)
		if verdict == "" {
			return fmt.Errorf("stage gate %s contains an empty verdict", out.Decision)
		}
		outcome.AdvancesTo = strings.TrimSpace(outcome.AdvancesTo)
		if outcome.AdvancesTo == "" {
			return fmt.Errorf("stage gate %s outcome %s requires advances_to; use defer to keep waiting", out.Decision, verdict)
		}
		out.Outcomes[verdict] = outcome
	}
	*g = out
	return nil
}

func (o *FlowStageGateOutcomeDeclaration) UnmarshalYAML(node *yaml.Node) error {
	if o == nil {
		return nil
	}
	if node == nil || node.Kind != yaml.MappingNode {
		return fmt.Errorf("stage gate outcome must be a mapping")
	}
	if err := validateUniqueNormalizedMappingKeys(node, "stage gate outcome"); err != nil {
		return err
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if _, ok := stageGateOutcomeFieldOptions[key]; !ok {
			return NewUndefinedFieldDiagnostic("stage gate outcome", key, stageGateOutcomeFieldOptions)
		}
	}
	var out FlowStageGateOutcomeDeclaration
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "label":
			if err := value.Decode(&out.Label); err != nil {
				return err
			}
		case "input":
			fields, err := decodeStageGateInputFields(value)
			if err != nil {
				return err
			}
			out.Input = fields
		case "advances_to":
			if err := value.Decode(&out.AdvancesTo); err != nil {
				return err
			}
		case "emit":
			if err := value.Decode(&out.Emit); err != nil {
				return err
			}
		}
	}
	out.Label = strings.TrimSpace(out.Label)
	out.AdvancesTo = strings.TrimSpace(out.AdvancesTo)
	*o = out
	return nil
}

func (f *WorkflowGateInputField) UnmarshalYAML(node *yaml.Node) error {
	if f == nil {
		return nil
	}
	if node == nil || node.Kind != yaml.MappingNode {
		return fmt.Errorf("stage gate input field must be a mapping")
	}
	if err := validateUniqueNormalizedMappingKeys(node, "stage gate input field"); err != nil {
		return err
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if _, ok := stageGateInputFieldOptions[key]; !ok {
			return NewUndefinedFieldDiagnostic("stage gate input field", key, stageGateInputFieldOptions)
		}
	}
	type alias WorkflowGateInputField
	var out alias
	if err := node.Decode(&out); err != nil {
		return err
	}
	normalized, err := NormalizeNodeStateFieldType(out.Type)
	if err != nil {
		return fmt.Errorf("stage gate input type: %w", err)
	}
	out.Type = normalized
	out.Label = strings.TrimSpace(out.Label)
	*f = WorkflowGateInputField(out)
	return nil
}

func decodeStageGateOutcomes(node *yaml.Node) (map[string]FlowStageGateOutcomeDeclaration, error) {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("stage gate outcomes must be a mapping")
	}
	if err := validateUniqueNormalizedMappingKeys(node, "stage gate outcomes"); err != nil {
		return nil, err
	}
	out := make(map[string]FlowStageGateOutcomeDeclaration, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			return nil, fmt.Errorf("stage gate outcomes contains an empty verdict")
		}
		var outcome FlowStageGateOutcomeDeclaration
		if err := node.Content[i+1].Decode(&outcome); err != nil {
			return nil, fmt.Errorf("stage gate outcome %s: %w", key, err)
		}
		out[key] = outcome
	}
	return out, nil
}

func decodeStageGateInputFields(node *yaml.Node) (map[string]WorkflowGateInputField, error) {
	if node == nil || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		return nil, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("stage gate input must be a mapping")
	}
	if err := validateUniqueNormalizedMappingKeys(node, "stage gate input"); err != nil {
		return nil, err
	}
	out := make(map[string]WorkflowGateInputField, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			return nil, fmt.Errorf("stage gate input contains an empty field name")
		}
		var field WorkflowGateInputField
		if err := node.Content[i+1].Decode(&field); err != nil {
			return nil, fmt.Errorf("stage gate input %s: %w", key, err)
		}
		out[key] = field
	}
	return out, nil
}

func validateUniqueNormalizedMappingKeys(node *yaml.Node, label string) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	seen := map[string]string{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		raw := node.Content[i].Value
		normalized := strings.TrimSpace(raw)
		if previous, ok := seen[normalized]; ok {
			return fmt.Errorf("%s contains duplicate normalized key %q (from %q and %q)", label, normalized, previous, raw)
		}
		seen[normalized] = raw
	}
	return nil
}

func (d FlowStageDeclarations) GatePlans(flowID string) []WorkflowGatePlan {
	out := make([]WorkflowGatePlan, 0)
	for _, stage := range d.Entries {
		if stage.Gate == nil {
			continue
		}
		plan := WorkflowGatePlan{
			FlowID:   strings.TrimSpace(flowID),
			Stage:    strings.TrimSpace(stage.ID),
			Decision: strings.TrimSpace(stage.Gate.Decision),
			Title:    strings.TrimSpace(stage.Gate.Title),
			Context:  cloneExpressionValueMap(stage.Gate.Context),
			Outcomes: map[string]WorkflowGateOutcomePlan{},
		}
		for verdict, outcome := range stage.Gate.Outcomes {
			input := make(map[string]WorkflowGateInputField, len(outcome.Input))
			for name, field := range outcome.Input {
				input[strings.TrimSpace(name)] = field
			}
			plan.Outcomes[strings.TrimSpace(verdict)] = WorkflowGateOutcomePlan{
				Verdict: strings.TrimSpace(verdict), Label: strings.TrimSpace(outcome.Label), Input: input,
				AdvancesTo: strings.TrimSpace(outcome.AdvancesTo), Emit: cloneEmitSpec(outcome.Emit),
			}
		}
		out = append(out, plan)
	}
	return out
}

func (s *FlowStageDeclaration) normalizeTimerIDs() error {
	if s == nil || len(s.Timers) == 0 {
		return nil
	}
	stageID := strings.TrimSpace(s.ID)
	seen := map[string]struct{}{}
	for i := range s.Timers {
		timer := &s.Timers[i]
		if strings.TrimSpace(timer.After) == "" {
			return fmt.Errorf("timer row %d missing after", i)
		}
		if strings.TrimSpace(timer.Emit) == "" && strings.TrimSpace(timer.AdvancesTo) == "" {
			return fmt.Errorf("timer row %d must declare emit and/or advances_to", i)
		}
		if strings.TrimSpace(timer.ID) == "" {
			timer.ID = timer.defaultID(stageID)
		}
		timer.ID = strings.TrimSpace(timer.ID)
		if timer.ID == "" {
			return fmt.Errorf("timer row %d could not derive stable id", i)
		}
		if _, ok := seen[timer.ID]; ok {
			return fmt.Errorf("timers contains duplicate id %q; add explicit id values to disambiguate", timer.ID)
		}
		seen[timer.ID] = struct{}{}
	}
	return nil
}

func validateStageTimerIDNamespace(stages []FlowStageDeclaration) error {
	seen := map[string]string{}
	for _, stage := range stages {
		stageID := strings.TrimSpace(stage.ID)
		for _, timer := range stage.Timers {
			id := strings.TrimSpace(timer.ID)
			if id == "" {
				continue
			}
			if previousStage, ok := seen[id]; ok {
				return fmt.Errorf("stage timer id %q is declared in both stage %q and stage %q; timer ids must be unique within a flow", id, previousStage, stageID)
			}
			seen[id] = stageID
		}
	}
	return nil
}

func (t FlowStageTimerDeclaration) defaultID(stageID string) string {
	stageID = strings.TrimSpace(stageID)
	target := strings.TrimSpace(t.AdvancesTo)
	if target == "" {
		target = strings.TrimSpace(t.Emit)
	}
	if stageID == "" {
		return target
	}
	if target == "" {
		return stageID
	}
	return stageID + "." + target
}

func (d FlowStageDeclarations) StageIDs() []string {
	out := make([]string, 0, len(d.Entries))
	for _, stage := range d.Entries {
		id := strings.TrimSpace(stage.ID)
		if id != "" {
			out = append(out, id)
		}
	}
	return out
}

func (d FlowStageDeclarations) InitialStage() string {
	for _, stage := range d.Entries {
		if stage.Initial {
			return strings.TrimSpace(stage.ID)
		}
	}
	return ""
}

func (d FlowStageDeclarations) TerminalStages() []string {
	out := make([]string, 0, len(d.Entries))
	for _, stage := range d.Entries {
		if stage.Terminal {
			id := strings.TrimSpace(stage.ID)
			if id != "" {
				out = append(out, id)
			}
		}
	}
	return out
}

func (d FlowStageDeclarations) WorkflowStages(phase string) []WorkflowStageContract {
	out := make([]WorkflowStageContract, 0, len(d.Entries))
	for _, stage := range d.Entries {
		id := strings.TrimSpace(stage.ID)
		if id == "" {
			continue
		}
		out = append(out, WorkflowStageContract{
			ID:          id,
			Phase:       strings.TrimSpace(phase),
			Description: strings.TrimSpace(stage.Description),
		})
	}
	return out
}

func (d FlowStageDeclarations) InitialCount() int {
	count := 0
	for _, stage := range d.Entries {
		if stage.Initial {
			count++
		}
	}
	return count
}

func (d FlowStageDeclarations) TerminalCount() int {
	count := 0
	for _, stage := range d.Entries {
		if stage.Terminal {
			count++
		}
	}
	return count
}

func (d FlowStageDeclarations) IsExplicitStateless() bool {
	return d.Declared && len(d.Entries) == 0
}

func (s FlowSchemaDocument) UsesAuthoredStages() bool {
	return s.StageDeclarations.Declared
}

func (s FlowSchemaDocument) HasLegacyLifecycleFields() bool {
	return s.InitialStateDeclared || s.StatesDeclared || s.TerminalStatesDeclared
}

func (s FlowSchemaDocument) LoweredInitialState() string {
	if s.StageDeclarations.Declared {
		return s.StageDeclarations.InitialStage()
	}
	return strings.TrimSpace(s.InitialState)
}

func (s FlowSchemaDocument) LoweredStates() []string {
	if s.StageDeclarations.Declared {
		return s.StageDeclarations.StageIDs()
	}
	out := make([]string, 0, len(s.States))
	for _, state := range s.States {
		state = strings.TrimSpace(state)
		if state != "" {
			out = append(out, state)
		}
	}
	return out
}

func (s FlowSchemaDocument) LoweredTerminalStates() []string {
	if s.StageDeclarations.Declared {
		return s.StageDeclarations.TerminalStages()
	}
	out := make([]string, 0, len(s.TerminalStates))
	for _, state := range s.TerminalStates {
		state = strings.TrimSpace(state)
		if state != "" {
			out = append(out, state)
		}
	}
	return out
}

func (s FlowSchemaDocument) LoweredWorkflowStages(phase string) []WorkflowStageContract {
	if s.StageDeclarations.Declared {
		return s.StageDeclarations.WorkflowStages(phase)
	}
	out := make([]WorkflowStageContract, 0, len(s.States))
	for _, state := range s.States {
		state = strings.TrimSpace(state)
		if state == "" {
			continue
		}
		out = append(out, WorkflowStageContract{
			ID:    state,
			Phase: strings.TrimSpace(phase),
		})
	}
	return out
}
