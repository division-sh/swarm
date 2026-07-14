package contracts

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"gopkg.in/yaml.v3"
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
}

type JoinMembersSpec struct {
	From     string     `yaml:"from"`
	FromPath paths.Path `yaml:"-"`
	By       string     `yaml:"by"`
	ByPath   paths.Path `yaml:"-"`
}

type JoinWindowSpec struct {
	From     string     `yaml:"from"`
	FromPath paths.Path `yaml:"-"`
	By       string     `yaml:"by"`
	ByPath   paths.Path `yaml:"-"`
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
	"from": {},
	"by":   {},
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

func (s *JoinSpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	if err := validateJoinMapping("join", node, joinFieldOptions); err != nil {
		return err
	}
	var aux struct {
		ID           string          `yaml:"id"`
		Stage        string          `yaml:"stage"`
		Members      JoinMembersSpec `yaml:"members"`
		Window       *JoinWindowSpec `yaml:"window"`
		Output       string          `yaml:"output"`
		CompleteWhen string          `yaml:"complete_when"`
		Remaining    string          `yaml:"remaining"`
		OnComplete   yaml.Node       `yaml:"on_complete"`
		Timeout      yaml.Node       `yaml:"timeout"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = JoinSpec{
		ID:           strings.TrimSpace(aux.ID),
		Stage:        strings.TrimSpace(aux.Stage),
		Members:      aux.Members,
		Window:       aux.Window,
		Output:       strings.TrimSpace(aux.Output),
		OutputPath:   paths.Parse(aux.Output),
		CompleteWhen: strings.TrimSpace(aux.CompleteWhen),
		Remaining:    strings.TrimSpace(strings.ToLower(aux.Remaining)),
	}
	if s.ID == "" {
		s.ID = s.Stage
	}
	if s.ID != "" && !joinIDPattern.MatchString(s.ID) {
		return fmt.Errorf("join.id %q must be a simple stable identifier", s.ID)
	}
	if err := validateJoinMapping("join.on_complete", &aux.OnComplete, joinOutcomeFieldOptions); err != nil {
		return err
	}
	if aux.OnComplete.Kind != 0 && !yamlNodeIsNull(&aux.OnComplete) {
		rule, err := decodeHandlerRuleEntryNode(&aux.OnComplete, handlerRuleDecodeContextJoinOnComplete)
		if err != nil {
			return err
		}
		if rule != nil {
			s.OnComplete = *rule
			s.OnCompleteFound = true
		}
	}
	if err := decodeJoinTimeout(&aux.Timeout, &s.Timeout); err != nil {
		return err
	}
	s.TimeoutFound = aux.Timeout.Kind != 0 && !yamlNodeIsNull(&aux.Timeout)
	return nil
}

func (s *JoinMembersSpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	if err := validateJoinMapping("join.members", node, joinMembersFieldOptions); err != nil {
		return err
	}
	type wire struct {
		From string `yaml:"from"`
		By   string `yaml:"by"`
	}
	var aux wire
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = JoinMembersSpec{
		From:     strings.TrimSpace(aux.From),
		FromPath: paths.Parse(aux.From),
		By:       strings.TrimSpace(aux.By),
		ByPath:   paths.Parse(aux.By),
	}
	return nil
}

func (s *JoinWindowSpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	if err := validateJoinMapping("join.window", node, joinWindowFieldOptions); err != nil {
		return err
	}
	type wire struct {
		From string `yaml:"from"`
		By   string `yaml:"by"`
	}
	var aux wire
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = JoinWindowSpec{
		From:     strings.TrimSpace(aux.From),
		FromPath: paths.Parse(aux.From),
		By:       strings.TrimSpace(aux.By),
		ByPath:   paths.Parse(aux.By),
	}
	return nil
}

func decodeJoinTimeout(node *yaml.Node, out *JoinTimeoutSpec) error {
	if out == nil || node == nil || node.Kind == 0 || yamlNodeIsNull(node) {
		return nil
	}
	if err := validateJoinMapping("join.timeout", node, joinTimeoutFieldOptions); err != nil {
		return err
	}
	var wire struct {
		After string `yaml:"after"`
	}
	if err := node.Decode(&wire); err != nil {
		return err
	}
	clone := *node
	clone.Content = make([]*yaml.Node, 0, len(node.Content))
	for i := 0; i+1 < len(node.Content); i += 2 {
		if strings.TrimSpace(node.Content[i].Value) == "after" {
			continue
		}
		clone.Content = append(clone.Content, node.Content[i], node.Content[i+1])
	}
	rule, err := decodeHandlerRuleEntryNode(&clone, handlerRuleDecodeContextJoinTimeout)
	if err != nil {
		return err
	}
	out.After = strings.TrimSpace(wire.After)
	if rule != nil {
		out.Outcome = *rule
	}
	return nil
}

func validateJoinMapping(context string, node *yaml.Node, allowed map[string]struct{}) error {
	if node == nil || node.Kind == 0 || yamlNodeIsNull(node) {
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("%s must be a mapping", context)
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if _, ok := allowed[key]; !ok {
			return NewUndefinedFieldDiagnostic(context, key, allowed)
		}
	}
	return nil
}

func yamlNodeIsNull(node *yaml.Node) bool {
	return node != nil && node.Kind == yaml.ScalarNode && strings.EqualFold(strings.TrimSpace(node.Tag), "!!null")
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
	add("select_or_create_entity", handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty())
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
	add("action", strings.TrimSpace(handler.Action.ID) != "")
	add("activity", !handler.Activity.Empty())
	add("compute", handler.Compute != nil)
	add("query", handler.Query != nil)
	add("fan_out", handler.FanOut != nil)
	add("group_by", handler.GroupBy != nil)
	add("filter", handler.Filter != nil)
	add("reduce", handler.Reduce != nil)
	add("count", handler.Count != nil)
	add("clear", handler.Clear != nil)
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
