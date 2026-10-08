package contracts

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
)

const JoinDeadlineFromStageEntry = "stage_entry"

var joinIDPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]*$`)

type JoinSpec struct {
	ID              string            `yaml:"id"`
	Stage           string            `yaml:"stage"`
	Members         JoinMembersSpec   `yaml:"members"`
	Output          string            `yaml:"output"`
	OutputPath      paths.Path        `yaml:"-"`
	OnComplete      HandlerRuleEntry  `yaml:"-"`
	Deadline        *JoinDeadlineSpec `yaml:"deadline"`
	OnDeadline      HandlerRuleEntry  `yaml:"-"`
	Until           string            `yaml:"until"`
	OnCompleteFound bool              `yaml:"-"`
	OnDeadlineFound bool              `yaml:"-"`
	stageFound      bool
	outputFound     bool
	deadlineFound   bool
	onDeadlineFound bool
	untilFound      bool
}

type JoinMembersSpec struct {
	From            string     `yaml:"from"`
	FromPath        paths.Path `yaml:"-"`
	Count           *int       `yaml:"count"`
	By              string     `yaml:"by"`
	ByPath          paths.Path `yaml:"-"`
	FromFanOut      bool       `yaml:"from_fan_out"`
	fromFound       bool
	BySet           bool `yaml:"-" json:"-"`
	fromFanOutFound bool
}

type WorkflowJoinMode uint8

const (
	WorkflowJoinModeInvalid WorkflowJoinMode = iota
	WorkflowJoinModeArrival
	WorkflowJoinModeFanOutDelivery
)

type JoinDeadlineSpec struct {
	After string `yaml:"after"`
	From  string `yaml:"from"`
}

var joinFieldOptions = map[string]struct{}{
	"id":          {},
	"stage":       {},
	"members":     {},
	"output":      {},
	"on_complete": {},
	"deadline":    {},
	"on_deadline": {},
	"until":       {},
}

var joinMembersFieldOptions = map[string]struct{}{
	"from":         {},
	"count":        {},
	"by":           {},
	"from_fan_out": {},
}

var joinDeadlineFieldOptions = map[string]struct{}{
	"after": {},
	"from":  {},
}

var joinOutcomeFieldOptions = map[string]struct{}{
	"terminate":         {},
	"data_accumulation": {},
	"emit":              {},
	"advances_to":       {},
}

func (s JoinSpec) EffectiveID() string {
	if id := strings.TrimSpace(s.ID); id != "" {
		return id
	}
	return strings.TrimSpace(s.Stage)
}

func (s JoinSpec) Mode() WorkflowJoinMode {
	if s.Members.FromFanOut || s.Members.fromFanOutFound {
		return WorkflowJoinModeFanOutDelivery
	}
	if strings.TrimSpace(s.Stage) != "" || strings.TrimSpace(s.Members.From) != "" || s.Members.Count != nil || strings.TrimSpace(s.Members.By) != "" {
		return WorkflowJoinModeArrival
	}
	return WorkflowJoinModeInvalid
}

func (s JoinSpec) IsFanOutDeliveryBarrier() bool {
	return s.Mode() == WorkflowJoinModeFanOutDelivery
}

func (s JoinSpec) ValidateAuthoredShape() error {
	if id := s.EffectiveID(); id == "" || !joinIDPattern.MatchString(id) {
		return fmt.Errorf("join.id must be a simple stable identifier (defaults to stage for arrival joins)")
	}
	if s.Mode() != WorkflowJoinModeFanOutDelivery {
		return s.validateArrivalShape()
	}
	return s.validateFanOutShape()
}

func (s JoinSpec) validateArrivalShape() error {
	if strings.TrimSpace(s.Stage) == "" || strings.TrimSpace(s.Output) == "" {
		return fmt.Errorf("arrival join requires stage and output")
	}
	if !s.OnCompleteFound || joinOutcomeEmpty(s.OnComplete) {
		return fmt.Errorf("arrival join requires non-empty on_complete")
	}
	if (strings.TrimSpace(s.Members.From) != "") == (s.Members.Count != nil) {
		return fmt.Errorf("arrival join requires exactly one of members.from or members.count")
	}
	if s.Members.Count != nil && s.Members.fromFound {
		return fmt.Errorf("members.count forbids members.from, including empty values")
	}
	if strings.TrimSpace(s.Members.By) == "" {
		return fmt.Errorf("arrival join requires members.by")
	}
	if s.Members.Count != nil {
		if *s.Members.Count < 0 || *s.Members.Count > DefaultFanOutMaxItems {
			return fmt.Errorf("join.members.count must be a nonnegative integer literal at most %d", DefaultFanOutMaxItems)
		}
		if s.Deadline == nil && strings.TrimSpace(s.Until) == "" {
			return fmt.Errorf("count join requires deadline or until, including count zero")
		}
	}
	return s.validateArrivalClosure()
}

func (s JoinSpec) validateArrivalClosure() error {
	if s.Deadline != nil {
		if strings.TrimSpace(s.Deadline.After) == "" || s.Deadline.From != JoinDeadlineFromStageEntry {
			return fmt.Errorf("join.deadline requires after and from: stage_entry")
		}
		if !s.OnDeadlineFound || joinOutcomeEmpty(s.OnDeadline) {
			return fmt.Errorf("join.deadline requires non-empty on_deadline")
		}
	} else if s.deadlineFound || s.onDeadlineFound || s.OnDeadlineFound || !joinOutcomeEmpty(s.OnDeadline) {
		return fmt.Errorf("on_deadline is required if and only if deadline is declared")
	}
	if s.untilFound && strings.TrimSpace(s.Until) == "" {
		return fmt.Errorf("join.until requires a non-empty event")
	}
	if strings.ContainsAny(s.Until, "*?") {
		return fmt.Errorf("join.until requires one exact event, not a pattern")
	}
	return nil
}

func (s JoinSpec) validateFanOutShape() error {
	if !s.Members.FromFanOut {
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
	add("members.count", s.Members.Count != nil)
	add("output", s.outputFound || strings.TrimSpace(s.Output) != "")
	add("deadline", s.deadlineFound || s.Deadline != nil)
	add("on_deadline", s.onDeadlineFound || s.OnDeadlineFound || !joinOutcomeEmpty(s.OnDeadline))
	add("until", s.untilFound || s.Until != "")
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
