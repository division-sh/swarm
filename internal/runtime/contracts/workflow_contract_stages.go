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
}

var stageTimerFieldOptions = map[string]struct{}{
	"id":          {},
	"after":       {},
	"emit":        {},
	"advances_to": {},
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
