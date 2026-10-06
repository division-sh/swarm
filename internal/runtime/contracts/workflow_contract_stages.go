package contracts

import (
	"fmt"
	"strings"
)

type FlowStageDeclarations struct {
	Declared bool
	Entries  []FlowStageDeclaration
}

type FlowStageDeclaration struct {
	ID          string                      `yaml:"-"`
	Final       bool                        `yaml:"final"`
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
	InputOrder []string                          `yaml:"-"`
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
	"final":       {},
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
				InputOrder: append([]string(nil), outcome.InputOrder...),
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
	if len(d.Entries) != 0 {
		return d.Entries[0].ID
	}
	return ""
}

func (d FlowStageDeclarations) FinalStages() []string {
	out := make([]string, 0, len(d.Entries))
	for _, stage := range d.Entries {
		if stage.Final {
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

func (d FlowStageDeclarations) IsExplicitStateless() bool {
	return d.Declared && len(d.Entries) == 0
}

func (s FlowSchemaDocument) UsesAuthoredStages() bool {
	return s.StageDeclarations.Declared
}

func (s FlowSchemaDocument) LoweredInitialState() string {
	return s.StageDeclarations.InitialStage()
}

func (s FlowSchemaDocument) LoweredStates() []string {
	return s.StageDeclarations.StageIDs()
}

func (s FlowSchemaDocument) LoweredFinalStates() []string {
	return s.StageDeclarations.FinalStages()
}

func (s FlowSchemaDocument) LoweredWorkflowStages(phase string) []WorkflowStageContract {
	return s.StageDeclarations.WorkflowStages(phase)
}
