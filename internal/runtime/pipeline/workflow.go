package pipeline

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"
)

type WorkflowStage struct {
	Name        PipelineStage `json:"name"`
	Phase       string        `json:"phase,omitempty"`
	Description string        `json:"description,omitempty"`
	Terminal    bool          `json:"terminal,omitempty"`
}

type WorkflowAction struct {
	Name            string `json:"name"`
	Category        string `json:"category,omitempty"`
	Description     string `json:"description,omitempty"`
	Effect          string `json:"effect,omitempty"`
	Emits           string `json:"emits,omitempty"`
	PlatformBuiltin string `json:"platform_builtin,omitempty"`
}

type WorkflowState struct {
	VerticalID string         `json:"vertical_id,omitempty"`
	Stage      PipelineStage  `json:"stage"`
	Status     string         `json:"status,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

type WorkflowGuard func(state WorkflowState, transition WorkflowTransition) bool

type WorkflowTransition struct {
	Name             string                                   `json:"name"`
	From             []PipelineStage                          `json:"from"`
	To               PipelineStage                            `json:"to"`
	Reason           string                                   `json:"reason,omitempty"`
	Trigger          string                                   `json:"trigger,omitempty"`
	Node             string                                   `json:"node,omitempty"`
	GuardIDs         []string                                 `json:"guard_ids,omitempty"`
	Guard            WorkflowGuard                            `json:"-"`
	Actions          []WorkflowAction                         `json:"actions,omitempty"`
	DataAccumulation runtimecontracts.WorkflowDataAccumulation `json:"data_accumulation,omitempty"`
}

type WorkflowDefinition struct {
	Name        string
	stages      map[PipelineStage]WorkflowStage
	transitions []WorkflowTransition
}

func NewWorkflowDefinition(name string, stages []WorkflowStage, transitions []WorkflowTransition) *WorkflowDefinition {
	stageMap := make(map[PipelineStage]WorkflowStage, len(stages))
	for _, stage := range stages {
		stage.Name = NormalizePipelineStage(string(stage.Name))
		stageMap[stage.Name] = stage
	}
	def := &WorkflowDefinition{
		Name:        strings.TrimSpace(name),
		stages:      stageMap,
		transitions: make([]WorkflowTransition, 0, len(transitions)),
	}
	for _, transition := range transitions {
		normFrom := make([]PipelineStage, 0, len(transition.From))
		for _, from := range transition.From {
			normFrom = append(normFrom, NormalizePipelineStage(string(from)))
		}
		transition.From = normFrom
		transition.To = NormalizePipelineStage(string(transition.To))
		if transition.Guard == nil {
			transition.Guard = alwaysWorkflowGuard
		}
		def.transitions = append(def.transitions, transition)
	}
	return def
}

func alwaysWorkflowGuard(_ WorkflowState, _ WorkflowTransition) bool { return true }

func (wd *WorkflowDefinition) Stage(stage PipelineStage) (WorkflowStage, bool) {
	if wd == nil {
		return WorkflowStage{}, false
	}
	stage = NormalizePipelineStage(string(stage))
	out, ok := wd.stages[stage]
	return out, ok
}

func (wd *WorkflowDefinition) NormalizeStage(raw string) PipelineStage {
	stage := NormalizePipelineStage(raw)
	if wd == nil {
		return stage
	}
	if _, ok := wd.stages[stage]; ok {
		return stage
	}
	return stage
}

func (wd *WorkflowDefinition) Transition(state WorkflowState, to PipelineStage) (WorkflowTransition, bool) {
	if wd == nil {
		return WorkflowTransition{}, false
	}
	state.Stage = wd.NormalizeStage(string(state.Stage))
	to = wd.NormalizeStage(string(to))
	if state.Stage == "" {
		if _, ok := wd.stages[to]; ok {
			return WorkflowTransition{
				Name:   "seed-" + string(to),
				From:   []PipelineStage{""},
				To:     to,
				Guard:  alwaysWorkflowGuard,
				Reason: "synthetic seed transition",
			}, true
		}
	}
	for _, transition := range wd.transitions {
		if transition.To != to {
			continue
		}
		if !containsPipelineStage(transition.From, state.Stage) {
			continue
		}
		if transition.Guard == nil || transition.Guard(state, transition) {
			return transition, true
		}
	}
	return WorkflowTransition{}, false
}

func (wd *WorkflowDefinition) TransitionByTrigger(
	state WorkflowState,
	trigger string,
	guardEvaluator func(WorkflowTransition) bool,
) (WorkflowTransition, bool) {
	if wd == nil {
		return WorkflowTransition{}, false
	}
	state.Stage = wd.NormalizeStage(string(state.Stage))
	trigger = strings.TrimSpace(trigger)
	if trigger == "" {
		return WorkflowTransition{}, false
	}
	for _, transition := range wd.transitions {
		if strings.TrimSpace(transition.Trigger) != trigger {
			continue
		}
		if !containsPipelineStage(transition.From, state.Stage) {
			continue
		}
		if guardEvaluator != nil && !guardEvaluator(transition) {
			continue
		}
		if transition.Guard == nil || transition.Guard(state, transition) {
			return transition, true
		}
	}
	return WorkflowTransition{}, false
}

func (wd *WorkflowDefinition) CanTransition(state WorkflowState, to PipelineStage) bool {
	_, ok := wd.Transition(state, to)
	return ok
}

func containsPipelineStage(stages []PipelineStage, want PipelineStage) bool {
	for _, stage := range stages {
		if strings.TrimSpace(string(stage)) == "*" {
			return true
		}
		if NormalizePipelineStage(string(stage)) == want {
			return true
		}
	}
	return false
}

func DefaultPipelineWorkflow() *WorkflowDefinition {
	return defaultWorkflowModule().WorkflowDefinition()
}

func LoadWorkflowDefinition(bundle *runtimecontracts.WorkflowContractBundle) (*WorkflowDefinition, error) {
	if bundle == nil {
		return nil, fmt.Errorf("workflow contract bundle is nil")
	}
	path := bundle.Paths.WorkflowSchemaFile
	doc := bundle.Workflow
	name := strings.TrimSpace(doc.Workflow.Name)
	if name == "" {
		return nil, fmt.Errorf("workflow.name missing in %s", path)
	}
	terminal := make(map[string]struct{}, len(doc.Workflow.TerminalStages))
	for _, stageID := range doc.Workflow.TerminalStages {
		stageID = strings.TrimSpace(stageID)
		if stageID != "" {
			terminal[stageID] = struct{}{}
		}
	}
	stages := make([]WorkflowStage, 0, len(doc.Workflow.Stages))
	for _, stage := range doc.Workflow.Stages {
		stageID := strings.TrimSpace(stage.ID)
		if stageID == "" {
			continue
		}
		_, isTerminal := terminal[stageID]
		stages = append(stages, WorkflowStage{
			Name:        PipelineStage(stageID),
			Phase:       strings.TrimSpace(stage.Phase),
			Description: strings.TrimSpace(stage.Description),
			Terminal:    isTerminal,
		})
	}
	actionDefs := make(map[string]runtimecontracts.GuardActionEntry, len(bundle.Hooks.Actions))
	for _, action := range bundle.Hooks.Actions {
		id := strings.TrimSpace(action.ID)
		if id == "" {
			continue
		}
		actionDefs[id] = action
	}
	transitions := make([]WorkflowTransition, 0, len(doc.Workflow.Transitions))
	for _, transition := range doc.Workflow.Transitions {
		id := strings.TrimSpace(transition.ID)
		to := strings.TrimSpace(transition.To)
		if id == "" || to == "" {
			continue
		}
		actions := make([]WorkflowAction, 0, len(transition.Actions))
		for _, action := range transition.Actions {
			action = strings.TrimSpace(action)
			if action == "" {
				continue
			}
			def := actionDefs[action]
			actions = append(actions, WorkflowAction{
				Name:            action,
				Category:        strings.TrimSpace(def.Category),
				Description:     strings.TrimSpace(def.Description),
				Effect:          strings.TrimSpace(def.Effect),
				Emits:           strings.TrimSpace(def.Emits),
				PlatformBuiltin: strings.TrimSpace(def.PlatformBuiltin),
			})
		}
		guardIDs := make([]string, 0, len(transition.Guards))
		for _, guard := range transition.Guards {
			guard = strings.TrimSpace(guard)
			if guard == "" {
				continue
			}
			guardIDs = append(guardIDs, guard)
		}
		transitions = append(transitions, WorkflowTransition{
			Name:             id,
			From:             workflowTransitionFromStages(transition.From),
			To:               PipelineStage(to),
			Reason:           strings.TrimSpace(transition.Trigger),
			Trigger:          strings.TrimSpace(transition.Trigger),
			Node:             strings.TrimSpace(transition.Node),
			GuardIDs:         guardIDs,
			Guard:            alwaysWorkflowGuard,
			Actions:          actions,
			DataAccumulation: transition.DataAccumulation,
		})
	}
	return NewWorkflowDefinition(name, stages, transitions), nil
}

func workflowTransitionFromStages(raw any) []PipelineStage {
	switch typed := raw.(type) {
	case string:
		return []PipelineStage{PipelineStage(strings.TrimSpace(typed))}
	case []any:
		out := make([]PipelineStage, 0, len(typed))
		for _, item := range typed {
			if s, ok := item.(string); ok {
				out = append(out, PipelineStage(strings.TrimSpace(s)))
			}
		}
		return out
	case []string:
		out := make([]PipelineStage, 0, len(typed))
		for _, item := range typed {
			out = append(out, PipelineStage(strings.TrimSpace(item)))
		}
		return out
	default:
		return nil
	}
}

func WorkflowRepoRoot() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
}

func workflowHasTransition(transitions []WorkflowTransition, from, to PipelineStage) bool {
	for _, transition := range transitions {
		if transition.To != to {
			continue
		}
		if containsPipelineStage(transition.From, from) {
			return true
		}
	}
	return false
}
