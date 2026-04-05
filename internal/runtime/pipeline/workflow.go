package pipeline

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeregistry "swarm/internal/runtime/core/registry"
	"swarm/internal/runtime/semanticview"
)

type WorkflowStage struct {
	Name        WorkflowStateID `json:"name"`
	Phase       string          `json:"phase,omitempty"`
	Description string          `json:"description,omitempty"`
	Terminal    bool            `json:"terminal,omitempty"`
}

type WorkflowStateID string

type WorkflowAction struct {
	Name            string `json:"name"`
	Category        string `json:"category,omitempty"`
	Description     string `json:"description,omitempty"`
	Effect          string `json:"effect,omitempty"`
	Emits           string `json:"emits,omitempty"`
	PlatformBuiltin string `json:"platform_builtin,omitempty"`
}

type WorkflowState struct {
	EntityID string          `json:"entity_id,omitempty"`
	Stage    WorkflowStateID `json:"stage"`
	Status   string          `json:"status,omitempty"`
	Metadata map[string]any  `json:"metadata,omitempty"`
}

const workflowStateBucketEntityProjection = "entity_projection"

func NormalizeWorkflowStateID(raw string) WorkflowStateID {
	return WorkflowStateID(strings.TrimSpace(raw))
}

func CanTransitionWorkflowState(workflow *WorkflowDefinition, from, to WorkflowStateID) bool {
	if workflow == nil {
		return from == to
	}
	return workflow.CanTransition(WorkflowState{Stage: NormalizeWorkflowStateID(string(from))}, NormalizeWorkflowStateID(string(to)))
}

func WorkflowStateTransition(workflow *WorkflowDefinition, from, to WorkflowStateID) (WorkflowTransition, bool) {
	if workflow == nil {
		return WorkflowTransition{}, false
	}
	return workflow.Transition(WorkflowState{Stage: NormalizeWorkflowStateID(string(from))}, NormalizeWorkflowStateID(string(to)))
}

type WorkflowGuard func(state WorkflowState, transition WorkflowTransition) bool

type WorkflowTransition struct {
	Name              string                                    `json:"name"`
	From              []WorkflowStateID                         `json:"from"`
	To                WorkflowStateID                           `json:"to"`
	Reason            string                                    `json:"reason,omitempty"`
	Trigger           string                                    `json:"trigger,omitempty"`
	Node              string                                    `json:"node,omitempty"`
	GuardIDs          []string                                  `json:"guard_ids,omitempty"`
	AllowTerminalExit bool                                      `json:"allow_terminal_exit,omitempty"`
	Guard             WorkflowGuard                             `json:"-"`
	Actions           []WorkflowAction                          `json:"actions,omitempty"`
	DataAccumulation  runtimecontracts.WorkflowDataAccumulation `json:"data_accumulation,omitempty"`
}

type WorkflowDefinition struct {
	Name        string
	stages      map[WorkflowStateID]WorkflowStage
	transitions []WorkflowTransition
}

func NewWorkflowDefinition(name string, stages []WorkflowStage, transitions []WorkflowTransition) *WorkflowDefinition {
	stageMap := make(map[WorkflowStateID]WorkflowStage, len(stages))
	for _, stage := range stages {
		stage.Name = NormalizeWorkflowStateID(string(stage.Name))
		stageMap[stage.Name] = stage
	}
	def := &WorkflowDefinition{
		Name:        strings.TrimSpace(name),
		stages:      stageMap,
		transitions: make([]WorkflowTransition, 0, len(transitions)),
	}
	for _, transition := range transitions {
		normFrom := make([]WorkflowStateID, 0, len(transition.From))
		for _, from := range transition.From {
			normFrom = append(normFrom, NormalizeWorkflowStateID(string(from)))
		}
		transition.From = normFrom
		transition.To = NormalizeWorkflowStateID(string(transition.To))
		if transition.Guard == nil {
			transition.Guard = alwaysWorkflowGuard
		}
		def.transitions = append(def.transitions, transition)
	}
	return def
}

func alwaysWorkflowGuard(_ WorkflowState, _ WorkflowTransition) bool { return true }

func (wd *WorkflowDefinition) Stage(stage WorkflowStateID) (WorkflowStage, bool) {
	if wd == nil {
		return WorkflowStage{}, false
	}
	stage = NormalizeWorkflowStateID(string(stage))
	out, ok := wd.stages[stage]
	return out, ok
}

func (wd *WorkflowDefinition) NormalizeStage(raw string) WorkflowStateID {
	stage := NormalizeWorkflowStateID(raw)
	if wd == nil {
		return stage
	}
	if _, ok := wd.stages[stage]; ok {
		return stage
	}
	return stage
}

func (wd *WorkflowDefinition) Transition(state WorkflowState, to WorkflowStateID) (WorkflowTransition, bool) {
	if wd == nil {
		return WorkflowTransition{}, false
	}
	state.Stage = wd.NormalizeStage(string(state.Stage))
	to = wd.NormalizeStage(string(to))
	if state.Stage == "" {
		if _, ok := wd.stages[to]; ok {
			return WorkflowTransition{
				Name:   "seed-" + string(to),
				From:   []WorkflowStateID{""},
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
		if !containsWorkflowStateID(transition.From, state.Stage) {
			continue
		}
		if state.Stage != "" && state.Stage != to {
			if stage, ok := wd.Stage(state.Stage); ok && stage.Terminal && !transition.AllowTerminalExit {
				continue
			}
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
		if !containsWorkflowStateID(transition.From, state.Stage) {
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

func (wd *WorkflowDefinition) CanTransition(state WorkflowState, to WorkflowStateID) bool {
	_, ok := wd.Transition(state, to)
	return ok
}

func containsWorkflowStateID(stages []WorkflowStateID, want WorkflowStateID) bool {
	for _, stage := range stages {
		if strings.TrimSpace(string(stage)) == "*" {
			return true
		}
		if NormalizeWorkflowStateID(string(stage)) == want {
			return true
		}
	}
	return false
}

func workflowStateBucketObject(instance WorkflowInstance, key string) (map[string]any, bool) {
	if instance.StateBuckets == nil {
		return nil, false
	}
	bucket, ok := instance.StateBuckets[strings.TrimSpace(key)]
	if !ok {
		return nil, false
	}
	out, ok := bucket.(map[string]any)
	return out, ok
}

func workflowMutableStateBucket(instance *WorkflowInstance, key string) map[string]any {
	if instance == nil {
		return map[string]any{}
	}
	if instance.StateBuckets == nil {
		instance.StateBuckets = map[string]any{}
	}
	key = strings.TrimSpace(key)
	bucket, _ := instance.StateBuckets[key].(map[string]any)
	if bucket == nil {
		bucket = map[string]any{}
		instance.StateBuckets[key] = bucket
	}
	return bucket
}

func workflowSetStateBucket(instance *WorkflowInstance, key string, value map[string]any) {
	if instance == nil {
		return
	}
	if instance.StateBuckets == nil {
		instance.StateBuckets = map[string]any{}
	}
	instance.StateBuckets[strings.TrimSpace(key)] = cloneMap(value)
}

func workflowDeleteStateBucket(instance *WorkflowInstance, key string) {
	if instance == nil || instance.StateBuckets == nil {
		return
	}
	delete(instance.StateBuckets, strings.TrimSpace(key))
}

func workflowMutableMetadata(instance *WorkflowInstance) map[string]any {
	if instance == nil {
		return map[string]any{}
	}
	if instance.Metadata == nil {
		instance.Metadata = map[string]any{}
	}
	return instance.Metadata
}

func truthyMetadataFlag(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "1", "true", "yes", "on":
			return true
		}
	}
	return asInt(v) > 0
}

func parseWorkflowTime(v any) time.Time {
	raw := strings.TrimSpace(asString(v))
	if raw == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}
	}
	return parsed.UTC()
}

func workflowMetadataSnapshot(instance WorkflowInstance) map[string]any {
	return cloneStringAnyMap(instance.Metadata)
}

func LoadWorkflowDefinition(source semanticview.Source) (*WorkflowDefinition, error) {
	if source == nil {
		return nil, ErrContractBundleNil
	}
	name := source.WorkflowName()
	if name == "" {
		return nil, fmt.Errorf("workflow.name missing from contract bundle semantics")
	}
	terminalStages := source.WorkflowTerminalStages()
	terminal := make(map[string]struct{}, len(terminalStages))
	for _, stageID := range terminalStages {
		stageID = strings.TrimSpace(stageID)
		if stageID != "" {
			terminal[stageID] = struct{}{}
		}
	}
	stageContracts := source.WorkflowStages()
	stages := make([]WorkflowStage, 0, len(stageContracts))
	for _, stage := range stageContracts {
		stageID := strings.TrimSpace(stage.ID)
		if stageID == "" {
			continue
		}
		_, isTerminal := terminal[stageID]
		stages = append(stages, WorkflowStage{
			Name:        WorkflowStateID(stageID),
			Phase:       strings.TrimSpace(stage.Phase),
			Description: strings.TrimSpace(stage.Description),
			Terminal:    isTerminal,
		})
	}
	actionInstructions := source.ActionInstructions()
	actionDefs := make(map[string]runtimeregistry.ActionInstruction, len(actionInstructions))
	for _, action := range actionInstructions {
		id := action.Key.String()
		if id == "" {
			continue
		}
		actionDefs[id] = action
	}
	transitionContracts := source.WorkflowTransitions()
	transitions := make([]WorkflowTransition, 0, len(transitionContracts))
	for _, transition := range transitionContracts {
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
				PlatformBuiltin: strings.TrimSpace(def.Builtin),
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
			Name:              id,
			From:              workflowTransitionFromStages(transition.From),
			To:                WorkflowStateID(to),
			Reason:            strings.TrimSpace(transition.Trigger),
			Trigger:           strings.TrimSpace(transition.Trigger),
			Node:              strings.TrimSpace(transition.Node),
			GuardIDs:          guardIDs,
			AllowTerminalExit: transition.AllowTerminalExit,
			Guard:             alwaysWorkflowGuard,
			Actions:           actions,
			DataAccumulation:  transition.DataAccumulation,
		})
	}
	return NewWorkflowDefinition(name, stages, transitions), nil
}

func workflowTransitionFromStages(raw any) []WorkflowStateID {
	switch typed := raw.(type) {
	case string:
		return []WorkflowStateID{WorkflowStateID(strings.TrimSpace(typed))}
	case []any:
		out := make([]WorkflowStateID, 0, len(typed))
		for _, item := range typed {
			if s, ok := item.(string); ok {
				out = append(out, WorkflowStateID(strings.TrimSpace(s)))
			}
		}
		return out
	case []string:
		out := make([]WorkflowStateID, 0, len(typed))
		for _, item := range typed {
			out = append(out, WorkflowStateID(strings.TrimSpace(item)))
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

func workflowHasTransition(transitions []WorkflowTransition, from, to WorkflowStateID) bool {
	for _, transition := range transitions {
		if transition.To != to {
			continue
		}
		if containsWorkflowStateID(transition.From, from) {
			return true
		}
	}
	return false
}
