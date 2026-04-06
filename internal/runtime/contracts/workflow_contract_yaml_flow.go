package contracts

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

func (r *HandlerRuleEntry) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		r.Description = strings.TrimSpace(node.Value)
		return nil
	}
	type alias HandlerRuleEntry
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*r = HandlerRuleEntry(aux)
	return nil
}

func (p *FlowInputPins) UnmarshalYAML(node *yaml.Node) error {
	if p == nil {
		return nil
	}
	var aux struct {
		Events yaml.Node `yaml:"events"`
		Reads  yaml.Node `yaml:"reads"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	events, err := decodeFlowPinEventsNode(&aux.Events)
	if err != nil {
		return err
	}
	reads, err := decodeFlowPinFieldNamesNode(&aux.Reads)
	if err != nil {
		return err
	}
	*p = FlowInputPins{
		Events: events,
		Reads:  reads,
	}
	return nil
}

func (p *FlowOutputPins) UnmarshalYAML(node *yaml.Node) error {
	if p == nil {
		return nil
	}
	var aux struct {
		Events yaml.Node `yaml:"events"`
		Writes yaml.Node `yaml:"writes"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	events, err := decodeFlowPinEventsNode(&aux.Events)
	if err != nil {
		return err
	}
	writes, err := decodeFlowPinFieldNamesNode(&aux.Writes)
	if err != nil {
		return err
	}
	*p = FlowOutputPins{
		Events: events,
		Writes: writes,
	}
	return nil
}

func decodeFlowPinEventsNode(node *yaml.Node) ([]string, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	var legacy []string
	if err := node.Decode(&legacy); err == nil {
		return append([]string{}, legacy...), nil
	}

	var structured []struct {
		Name string `yaml:"name"`
	}
	if err := node.Decode(&structured); err != nil {
		return nil, err
	}
	events := make([]string, 0, len(structured))
	for _, entry := range structured {
		name := strings.TrimSpace(entry.Name)
		if name == "" {
			continue
		}
		events = append(events, name)
	}
	return events, nil
}

func decodeFlowPinFieldNamesNode(node *yaml.Node) ([]string, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	var legacy []string
	if err := node.Decode(&legacy); err == nil {
		return append([]string{}, legacy...), nil
	}

	var structured []struct {
		Field string `yaml:"field"`
	}
	if err := node.Decode(&structured); err != nil {
		return nil, err
	}
	fields := make([]string, 0, len(structured))
	for _, entry := range structured {
		field := strings.TrimSpace(entry.Field)
		if field == "" {
			continue
		}
		fields = append(fields, field)
	}
	return fields, nil
}

func (s *ComputeSpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	if err := validateComputeFieldNodes(node); err != nil {
		return err
	}
	var aux struct {
		Operation   ComputeOperation `yaml:"operation"`
		Tiers       []ComputeTier    `yaml:"tiers"`
		Keys        ComputeKeyConfig `yaml:"keys"`
		Params      map[string]any   `yaml:"params"`
		StoreAs     string           `yaml:"store_as"`
		Description string           `yaml:"description"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = ComputeSpec{
		Operation:   aux.Operation,
		Tiers:       aux.Tiers,
		Keys:        aux.Keys,
		Params:      aux.Params,
		StoreAs:     strings.TrimSpace(aux.StoreAs),
		Description: strings.TrimSpace(aux.Description),
	}
	if err := validateTieredWeightedAverageSpec(*s); err != nil {
		return err
	}
	return nil
}

func validateComputeFieldNodes(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	allowed := map[string]struct{}{
		"operation":   {},
		"tiers":       {},
		"keys":        {},
		"params":      {},
		"store_as":    {},
		"description": {},
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := allowed[key]; ok {
			continue
		}
		return fmt.Errorf("UNDEFINED-FIELD: compute field %q not in platform spec", key)
	}
	return nil
}

func validateTieredWeightedAverageSpec(spec ComputeSpec) error {
	if spec.Operation != ComputeOpWeightedAverage || len(spec.Tiers) == 0 {
		return nil
	}
	if strings.TrimSpace(spec.Keys.DimensionKey) == "" {
		return fmt.Errorf("invalid compute spec: weighted_average with tiers requires keys.dimension_key")
	}
	if len(normalizeStrings(spec.Keys.ScoreKeys)) == 0 {
		return fmt.Errorf("invalid compute spec: weighted_average with tiers requires keys.score_keys")
	}
	return nil
}

func (v *FlowVariable) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		v.Description = strings.TrimSpace(node.Value)
		return nil
	}
	type alias FlowVariable
	var aux alias
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*v = FlowVariable(aux)
	return nil
}

func (t *WorkflowTransitionContract) UnmarshalYAML(node *yaml.Node) error {
	if t == nil {
		return nil
	}
	type shadow struct {
		ID                string                   `yaml:"id"`
		From              yaml.Node                `yaml:"from"`
		To                string                   `yaml:"to"`
		Trigger           string                   `yaml:"trigger"`
		Node              string                   `yaml:"node"`
		Guards            []string                 `yaml:"guards"`
		Actions           []string                 `yaml:"actions"`
		DataAccumulation  WorkflowDataAccumulation `yaml:"data_accumulation"`
		AllowTerminalExit bool                     `yaml:"allow_terminal_exit"`
	}
	var aux shadow
	if err := node.Decode(&aux); err != nil {
		return err
	}
	t.ID = aux.ID
	t.To = aux.To
	t.Trigger = aux.Trigger
	t.Node = aux.Node
	t.Guards = aux.Guards
	t.Actions = aux.Actions
	t.DataAccumulation = aux.DataAccumulation
	t.AllowTerminalExit = aux.AllowTerminalExit
	from, err := decodeStringListNode(&aux.From)
	if err != nil {
		return err
	}
	t.From = from
	return nil
}
