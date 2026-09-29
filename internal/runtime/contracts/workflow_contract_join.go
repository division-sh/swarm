package contracts

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
)

const (
	JoinRemainingIgnore = "ignore"
)

var joinIDPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]*$`)

type JoinSpec struct {
	ID              string           `yaml:"id"`
	Stage           string           `yaml:"stage"`
	Members         JoinMembersSpec  `yaml:"members"`
	Window          *JoinWindowSpec  `yaml:"window"`
	Output          string           `yaml:"output"`
	OutputPath      paths.Path       `yaml:"-"`
	CompleteWhen    string           `yaml:"complete_when"`
	Remaining       string           `yaml:"remaining"`
	OnComplete      HandlerRuleEntry `yaml:"-"`
	Timeout         JoinTimeoutSpec  `yaml:"-"`
	OnCompleteFound bool             `yaml:"-"`
	TimeoutFound    bool             `yaml:"-"`
	stageFound      bool
	windowFound     bool
	outputFound     bool
	completeFound   bool
	remainingFound  bool
	timeoutFound    bool
}

type JoinMembersSpec struct {
	From            string     `yaml:"from"`
	FromPath        paths.Path `yaml:"-"`
	By              string     `yaml:"by"`
	ByPath          paths.Path `yaml:"-"`
	FromFanOut      bool       `yaml:"from_fan_out"`
	fromFound       bool
	BySet           bool `yaml:"-"`
	fromFanOutFound bool
}

type WorkflowJoinMode uint8

const (
	WorkflowJoinModeInvalid WorkflowJoinMode = iota
	WorkflowJoinModeArrival
	WorkflowJoinModeFanOutDelivery
)

type JoinWindowSpec struct {
	From     string     `yaml:"from"`
	FromPath paths.Path `yaml:"-"`
	By       string     `yaml:"by"`
	ByPath   paths.Path `yaml:"-"`
	BySet    bool       `yaml:"-"`
}

type JoinTimeoutSpec struct {
	After   string           `yaml:"after"`
	Outcome HandlerRuleEntry `yaml:"-"`
}

var joinFieldOptions = map[string]struct{}{
	"id":            {},
	"stage":         {},
	"members":       {},
	"window":        {},
	"output":        {},
	"complete_when": {},
	"remaining":     {},
	"on_complete":   {},
	"timeout":       {},
}

var joinMembersFieldOptions = map[string]struct{}{
	"from":         {},
	"by":           {},
	"from_fan_out": {},
}

var joinWindowFieldOptions = map[string]struct{}{
	"from": {},
	"by":   {},
}

var joinTimeoutFieldOptions = map[string]struct{}{
	"after":             {},
	"data_accumulation": {},
	"emit":              {},
	"advances_to":       {},
}

var joinOutcomeFieldOptions = map[string]struct{}{
	"data_accumulation": {},
	"emit":              {},
	"advances_to":       {},
}

func (s JoinSpec) HasCustomCompletion() bool {
	return strings.TrimSpace(s.CompleteWhen) != ""
}

func (s JoinSpec) EffectiveID() string {
	if id := strings.TrimSpace(s.ID); id != "" {
		return id
	}
	return strings.TrimSpace(s.Stage)
}

func (s JoinSpec) TimeoutOutcome() HandlerRuleEntry {
	return s.Timeout.Outcome
}

func (s JoinSpec) Mode() WorkflowJoinMode {
	if s.Members.FromFanOut || s.Members.fromFanOutFound {
		return WorkflowJoinModeFanOutDelivery
	}
	if strings.TrimSpace(s.Stage) != "" || strings.TrimSpace(s.Members.From) != "" || strings.TrimSpace(s.Members.By) != "" {
		return WorkflowJoinModeArrival
	}
	return WorkflowJoinModeInvalid
}

func (s JoinSpec) IsFanOutDeliveryBarrier() bool {
	return s.Mode() == WorkflowJoinModeFanOutDelivery
}

func (s JoinSpec) ValidateAuthoredShape() error {
	if s.Mode() != WorkflowJoinModeFanOutDelivery {
		return nil
	}
	if !s.Members.fromFanOutFound || !s.Members.FromFanOut {
		return fmt.Errorf("fan-out delivery join requires join.members.from_fan_out: true")
	}
	if strings.TrimSpace(s.ID) == "" {
		return fmt.Errorf("fan-out delivery join requires explicit join.id")
	}
	if !s.OnCompleteFound || joinOutcomeEmpty(s.OnComplete) {
		return fmt.Errorf("fan-out delivery join requires non-empty on_complete")
	}
	forbidden := make([]string, 0, 8)
	add := func(name string, present bool) {
		if present {
			forbidden = append(forbidden, name)
		}
	}
	add("stage", s.stageFound || strings.TrimSpace(s.Stage) != "")
	add("members.from", s.Members.fromFound || strings.TrimSpace(s.Members.From) != "")
	add("members.by", s.Members.BySet || strings.TrimSpace(s.Members.By) != "")
	add("window", s.windowFound || s.Window != nil)
	add("output", s.outputFound || strings.TrimSpace(s.Output) != "")
	add("complete_when", s.completeFound || strings.TrimSpace(s.CompleteWhen) != "")
	add("remaining", s.remainingFound || strings.TrimSpace(s.Remaining) != "")
	add("timeout", s.timeoutFound || s.TimeoutFound)
	if len(forbidden) != 0 {
		return fmt.Errorf("fan-out delivery join forbids arrival-only fields %s", strings.Join(forbidden, ", "))
	}
	return nil
}

func joinOutcomeEmpty(rule HandlerRuleEntry) bool {
	return strings.TrimSpace(rule.AdvancesTo) == "" && rule.Emit.Empty() && !rule.DataAccumulation.HasWrites()
}

func ValidateJoinHandlerIsolation(handler SystemNodeEventHandler) error {
	if handler.Join == nil {
		return nil
	}
	conflicts := make([]string, 0, 12)
	add := func(name string, present bool) {
		if present {
			conflicts = append(conflicts, name)
		}
	}
	add("create_entity", handler.CreateEntity)
	add("guard", handler.Guard != nil)
	add("condition", strings.TrimSpace(handler.Condition) != "")
	add("rules", len(handler.Rules) > 0)
	add("on_complete", len(handler.OnComplete) > 0)
	add("accumulate", handler.Accumulate != nil)
	add("advances_to", strings.TrimSpace(handler.AdvancesTo) != "")
	add("sets_gate", handler.SetsGate != nil)
	add("clear_gates", len(handler.ClearGates) > 0)
	add("data_accumulation", handler.DataAccumulation.HasWrites())
	add("emit", !handler.Emit.Empty())
	add("on_success", !handler.OnSuccess.Empty())
	add("activity", !handler.Activity.Empty())
	add("compute", handler.Compute != nil)
	add("query", handler.Query != nil)
	pairedFanOut := handler.Join.IsFanOutDeliveryBarrier() && handler.FanOut != nil
	add("fan_out", handler.FanOut != nil && !pairedFanOut)
	add("group_by", handler.GroupBy != nil)
	add("filter", handler.Filter != nil)
	add("reduce", handler.Reduce != nil)
	add("count", handler.Count != nil)
	add("clear", handler.Clear != nil)
	if handler.Join.IsFanOutDeliveryBarrier() && !pairedFanOut {
		return fmt.Errorf("fan-out delivery join requires one paired top-level handler.fan_out")
	}
	if len(conflicts) == 0 {
		return nil
	}
	return fmt.Errorf("handler.join is an outcome-owning finite barrier and cannot be combined with handler fields %s", strings.Join(conflicts, ", "))
}

func ValidateAccumulateHandlerIsolation(handler SystemNodeEventHandler) error {
	if handler.Accumulate == nil || len(handler.OnComplete) == 0 {
		return nil
	}
	return fmt.Errorf("handler.accumulate is open stream collection and cannot be combined with handler.on_complete; use same-arrival compute, filter, reduce, count, or rules, or use handler.join for finite completion")
}
