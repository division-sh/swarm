package contracts

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
	"swarm/internal/runtime/core/paths"
)

func (t *WorkflowTimerContract) UnmarshalYAML(node *yaml.Node) error {
	if t == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*t = WorkflowTimerContract{}
			return nil
		}
		t.ID = strings.TrimSpace(node.Value)
		return nil
	case yaml.MappingNode:
		type alias WorkflowTimerContract
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return err
		}
		*t = WorkflowTimerContract(aux)
		return nil
	default:
		return fmt.Errorf("unsupported workflow timer yaml node kind %d", node.Kind)
	}
}

func (e *EventEmission) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*e = EventEmission{}
			return nil
		}
		e.Single = strings.TrimSpace(node.Value)
		return nil
	case yaml.SequenceNode:
		var many []string
		if err := node.Decode(&many); err != nil {
			return err
		}
		e.Many = normalizeStrings(many)
		return nil
	default:
		return fmt.Errorf("unsupported event emission yaml node kind %d", node.Kind)
	}
}

func (h *SystemNodeEventHandler) UnmarshalYAML(node *yaml.Node) error {
	if h == nil {
		return nil
	}
	if err := validateHandlerFieldNodes(node); err != nil {
		return err
	}
	var aux struct {
		Action           yaml.Node                `yaml:"action"`
		CreateEntity     bool                     `yaml:"create_entity"`
		Template         string                   `yaml:"template"`
		InstanceIDFrom   string                   `yaml:"instance_id_from"`
		ConfigFrom       yaml.Node                `yaml:"config_from"`
		EvidenceTarget   string                   `yaml:"evidence_target"`
		Description      string                   `yaml:"description"`
		Emits            EventEmission            `yaml:"emits"`
		Guard            yaml.Node                `yaml:"guard"`
		AdvancesTo       yaml.Node                `yaml:"advances_to"`
		SetsGate         yaml.Node                `yaml:"sets_gate"`
		ClearGates       yaml.Node                `yaml:"clear_gates"`
		DataAccumulation WorkflowDataAccumulation `yaml:"data_accumulation"`
		Condition        string                   `yaml:"condition"`
		CompletionRule   string                   `yaml:"completion_rule"`
		Logic            string                   `yaml:"logic"`
		PolicyRef        string                   `yaml:"policy_ref"`
		OnComplete       yaml.Node                `yaml:"on_complete"`
		Rules            yaml.Node                `yaml:"rules"`
		Accumulate       *AccumulateSpec          `yaml:"accumulate"`
		Compute          *ComputeSpec             `yaml:"compute"`
		Query            yaml.Node                `yaml:"query"`
		FanOut           *FanOutSpec              `yaml:"fan_out"`
		GroupBy          *GroupBySpec             `yaml:"group_by"`
		Filter           *FilterSpec              `yaml:"filter"`
		Reduce           *ReduceSpec              `yaml:"reduce"`
		Count            *CountSpec               `yaml:"count"`
		Clear            yaml.Node                `yaml:"clear"`
		PayloadTransform *PayloadTransformSpec    `yaml:"payload_transform"`
		Branch           yaml.Node                `yaml:"branch"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*h = SystemNodeEventHandler{
		CreateEntity:     aux.CreateEntity,
		EvidenceTarget:   strings.TrimSpace(aux.EvidenceTarget),
		Description:      strings.TrimSpace(aux.Description),
		Emits:            aux.Emits,
		DataAccumulation: aux.DataAccumulation,
		Condition:        strings.TrimSpace(aux.Condition),
		CompletionRule:   strings.TrimSpace(aux.CompletionRule),
		Logic:            strings.TrimSpace(aux.Logic),
		PolicyRef:        strings.TrimSpace(aux.PolicyRef),
		Accumulate:       aux.Accumulate,
		Compute:          aux.Compute,
		FanOut:           aux.FanOut,
		GroupBy:          aux.GroupBy,
		Filter:           aux.Filter,
		Reduce:           aux.Reduce,
		Count:            aux.Count,
		PayloadTransform: aux.PayloadTransform,
	}
	var err error
	if h.Action, err = decodeActionSpecNode(&aux.Action); err != nil {
		return err
	}
	if strings.TrimSpace(h.Action.ID) != "" {
		if strings.TrimSpace(h.Action.Template) == "" {
			h.Action.Template = strings.TrimSpace(aux.Template)
		}
		if strings.TrimSpace(h.Action.InstanceIDFrom) == "" {
			h.Action.InstanceIDFrom = strings.TrimSpace(aux.InstanceIDFrom)
			h.Action.InstanceIDPath = paths.Parse(aux.InstanceIDFrom)
		}
		if h.Action.ConfigFrom == nil {
			if h.Action.ConfigFrom, err = decodeConfigFromSpecNode(&aux.ConfigFrom); err != nil {
				return err
			}
		}
	}
	if h.Guard, err = decodeGuardSpecNode(&aux.Guard); err != nil {
		return err
	}
	if h.AdvancesTo, err = decodeAdvancesToNode(&aux.AdvancesTo); err != nil {
		return err
	}
	if h.SetsGate, err = decodeGateSpecNode(&aux.SetsGate); err != nil {
		return err
	}
	if h.ClearGates, err = decodeClearGatesNode(&aux.ClearGates); err != nil {
		return err
	}
	if h.OnComplete, err = decodeHandlerRuleEntriesNode(&aux.OnComplete); err != nil {
		return err
	}
	if h.Rules, err = decodeHandlerRuleEntriesNode(&aux.Rules); err != nil {
		return err
	}
	if h.Query, err = decodeQuerySpecNode(&aux.Query); err != nil {
		return err
	}
	if h.Clear, err = decodeClearSpecNode(&aux.Clear); err != nil {
		return err
	}
	if h.Branch, err = decodeBranchSpecsNode(&aux.Branch); err != nil {
		return err
	}
	return nil
}

func (e *EventCatalogEntry) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	var aux struct {
		Emitter            yaml.Node `yaml:"emitter"`
		EmitterType        string    `yaml:"emitter_type"`
		Producer           yaml.Node `yaml:"producer"`
		ProducerLegacy     yaml.Node `yaml:"_producer"`
		AlternateEmitters  []string  `yaml:"alternate_emitters"`
		Consumer           yaml.Node `yaml:"consumer"`
		ConsumerLegacy     yaml.Node `yaml:"_consumer"`
		ConsumerType       yaml.Node `yaml:"consumer_type"`
		ConsumerTypeLegacy yaml.Node `yaml:"_consumer_type"`
		Source             string    `yaml:"_source"`
		Status             string    `yaml:"_status"`
		Intercepted        yaml.Node `yaml:"intercepted"`
		Passthrough        yaml.Node `yaml:"passthrough"`
		RuntimeHandling    string    `yaml:"runtime_handling"`
		OwningNode         string    `yaml:"owning_node"`
		DeliveryChannel    yaml.Node `yaml:"delivery_channel"`
		Payload            yaml.Node `yaml:"payload"`
		Required           []string  `yaml:"required"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	emitter, alternates, err := decodeEventEmitterNode(&aux.Emitter)
	if err != nil {
		return err
	}
	producer, err := decodeStringListNode(&aux.Producer)
	if err != nil {
		return err
	}
	legacyProducer, err := decodeStringListNode(&aux.ProducerLegacy)
	if err != nil {
		return err
	}
	consumer, err := decodeStringListNode(&aux.Consumer)
	if err != nil {
		return err
	}
	legacyConsumer, err := decodeStringListNode(&aux.ConsumerLegacy)
	if err != nil {
		return err
	}
	consumerType, err := decodeStringListNode(&aux.ConsumerType)
	if err != nil {
		return err
	}
	legacyConsumerType, err := decodeStringListNode(&aux.ConsumerTypeLegacy)
	if err != nil {
		return err
	}
	intercepted, err := decodeBoolNode(&aux.Intercepted)
	if err != nil {
		return err
	}
	passthrough, err := decodeBoolNode(&aux.Passthrough)
	if err != nil {
		return err
	}
	deliveryChannel, err := decodeScalarStringNode(&aux.DeliveryChannel)
	if err != nil {
		return err
	}
	payload, err := decodeEventPayloadSpecNode(&aux.Payload)
	if err != nil {
		return err
	}
	e.Emitter = emitter
	e.EmitterType = strings.TrimSpace(aux.EmitterType)
	e.Producer = mergeStringLists(producer, legacyProducer)
	e.AlternateEmitters = mergeStringLists(aux.AlternateEmitters, alternates)
	e.Consumer = mergeStringLists(consumer, legacyConsumer)
	e.ConsumerType = mergeStringLists(consumerType, legacyConsumerType)
	e.Source = strings.TrimSpace(aux.Source)
	e.Status = strings.TrimSpace(aux.Status)
	e.Intercepted = intercepted
	e.Passthrough = passthrough
	e.RuntimeHandling = strings.TrimSpace(aux.RuntimeHandling)
	e.OwningNode = strings.TrimSpace(aux.OwningNode)
	e.DeliveryChannel = deliveryChannel
	e.Payload = payload
	e.Required = normalizeStrings(aux.Required)
	return nil
}

func decodeGuardSpecNode(node *yaml.Node) (*GuardSpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	if node.Kind == yaml.ScalarNode && !strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") && strings.TrimSpace(node.Value) != "" {
		return nil, fmt.Errorf("DIALECT-GUARD: guard is string, must be {id, check}")
	}
	var spec GuardSpec
	if err := node.Decode(&spec); err != nil {
		return nil, err
	}
	if strings.TrimSpace(spec.ID) == "" && strings.TrimSpace(spec.Check) == "" && len(spec.Checks) == 0 && strings.TrimSpace(spec.OnFail) == "" && strings.TrimSpace(spec.PolicyRef) == "" {
		return nil, nil
	}
	return &spec, nil
}

func decodeAdvancesToNode(node *yaml.Node) (string, error) {
	if node == nil || node.Kind == 0 {
		return "", nil
	}
	if node.Kind == yaml.SequenceNode {
		return "", fmt.Errorf("DIALECT-ADV-LIST: advances_to is list, must be string")
	}
	return decodeScalarStringNode(node)
}

func validateHandlerFieldNodes(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	allowed := map[string]struct{}{
		"action":            {},
		"description":       {},
		"_note":             {},
		"evidence_target":   {},
		"create_entity":     {},
		"emits":             {},
		"guard":             {},
		"advances_to":       {},
		"sets_gate":         {},
		"clear_gates":       {},
		"data_accumulation": {},
		"condition":         {},
		"completion_rule":   {},
		"logic":             {},
		"policy_ref":        {},
		"on_complete":       {},
		"rules":             {},
		"accumulate":        {},
		"compute":           {},
		"query":             {},
		"fan_out":           {},
		"group_by":          {},
		"filter":            {},
		"reduce":            {},
		"count":             {},
		"clear":             {},
		"template":          {},
		"instance_id_from":  {},
		"config_from":       {},
		"from":              {},
		"payload_transform": {},
		"branch":            {},
		"dedup_by":          {},
	}
	deprecated := map[string]struct{}{
		"condition":          {},
		"logic":              {},
		"on_below_threshold": {},
		"on_dedup":           {},
		"on_pass":            {},
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := deprecated[key]; ok {
			return fmt.Errorf("DEPRECATED: handler uses deprecated field %q", key)
		}
		if key == "on_complete" && node.Content[i+1].Kind == yaml.MappingNode {
			return fmt.Errorf("DIALECT-OC-ORDER: on_complete is dict, must be ordered list")
		}
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("UNDEFINED-FIELD: handler field %q not in platform spec", key)
		}
	}
	return nil
}

func decodeGateSpecNode(node *yaml.Node) (*GateSpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	var spec GateSpec
	if err := node.Decode(&spec); err != nil {
		return nil, err
	}
	if strings.TrimSpace(spec.Name) == "" && spec.Value == nil {
		return nil, nil
	}
	return &spec, nil
}

func decodeClearGatesNode(node *yaml.Node) ([]string, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			return nil, nil
		}
		var all bool
		if err := node.Decode(&all); err == nil {
			if all {
				return []string{"*"}, nil
			}
			return nil, nil
		}
		return []string{strings.TrimSpace(node.Value)}, nil
	case yaml.SequenceNode:
		return decodeStringListNode(node)
	default:
		return nil, fmt.Errorf("unsupported clear_gates yaml node kind %d", node.Kind)
	}
}

func decodeHandlerRuleEntryNode(node *yaml.Node) (*HandlerRuleEntry, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	var rule HandlerRuleEntry
	if err := node.Decode(&rule); err != nil {
		return nil, err
	}
	if strings.TrimSpace(rule.ID) == "" && strings.TrimSpace(rule.Description) == "" && strings.TrimSpace(rule.Condition) == "" && strings.TrimSpace(rule.AdvancesTo) == "" && rule.Emits.Empty() && !rule.DataAccumulation.HasWrites() && rule.Compute == nil && rule.FanOut == nil {
		return nil, nil
	}
	return &rule, nil
}

func decodeHandlerRuleEntriesNode(node *yaml.Node) ([]HandlerRuleEntry, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.SequenceNode:
		var rules []HandlerRuleEntry
		if err := node.Decode(&rules); err != nil {
			return nil, err
		}
		return rules, nil
	case yaml.MappingNode:
		if hasAnyYAMLMappingKey(node, "condition", "advances_to", "emits", "data_accumulation", "compute", "fan_out") {
			rule, err := decodeHandlerRuleEntryNode(node)
			if err != nil || rule == nil {
				return nil, err
			}
			return []HandlerRuleEntry{*rule}, nil
		}
		rules := make([]HandlerRuleEntry, 0, len(node.Content)/2)
		for i := 0; i+1 < len(node.Content); i += 2 {
			id := strings.TrimSpace(node.Content[i].Value)
			if id == "" {
				continue
			}
			var rule HandlerRuleEntry
			if err := node.Content[i+1].Decode(&rule); err != nil {
				return nil, err
			}
			if strings.TrimSpace(rule.ID) == "" {
				rule.ID = id
			}
			rules = append(rules, rule)
		}
		return rules, nil
	default:
		return nil, fmt.Errorf("unsupported rules yaml node kind %d", node.Kind)
	}
}

func decodeQuerySpecNode(node *yaml.Node) (*QuerySpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	switch node.Kind {
	case yaml.MappingNode:
		var spec QuerySpec
		if err := node.Decode(&spec); err != nil {
			return nil, err
		}
		spec.hydratePaths()
		return &spec, nil
	case yaml.SequenceNode:
		var queries []QuerySpec
		if err := node.Decode(&queries); err != nil {
			return nil, err
		}
		for i := range queries {
			queries[i].hydratePaths()
		}
		return &QuerySpec{Queries: queries}, nil
	default:
		return nil, fmt.Errorf("unsupported query yaml node kind %d", node.Kind)
	}
}

func decodeActionSpecNode(node *yaml.Node) (ActionSpec, error) {
	if node == nil || node.Kind == 0 {
		return ActionSpec{}, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			return ActionSpec{}, nil
		}
		actionID, err := ParseHandlerActionID(node.Value)
		if err != nil {
			return ActionSpec{}, err
		}
		return ActionSpec{ID: actionID}, nil
	case yaml.MappingNode:
		var aux struct {
			ID             string    `yaml:"id"`
			Template       string    `yaml:"template"`
			InstanceIDFrom string    `yaml:"instance_id_from"`
			ConfigFrom     yaml.Node `yaml:"config_from"`
		}
		if err := node.Decode(&aux); err != nil {
			return ActionSpec{}, err
		}
		actionID, err := ParseHandlerActionID(aux.ID)
		if err != nil {
			return ActionSpec{}, err
		}
		configFrom, err := decodeConfigFromSpecNode(&aux.ConfigFrom)
		if err != nil {
			return ActionSpec{}, err
		}
		return ActionSpec{
			ID:             actionID,
			Template:       strings.TrimSpace(aux.Template),
			InstanceIDFrom: strings.TrimSpace(aux.InstanceIDFrom),
			InstanceIDPath: paths.Parse(aux.InstanceIDFrom),
			ConfigFrom:     configFrom,
		}, nil
	default:
		return ActionSpec{}, fmt.Errorf("unsupported action yaml node kind %d", node.Kind)
	}
}

func decodeClearSpecNode(node *yaml.Node) (*ClearSpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	var spec ClearSpec
	if err := node.Decode(&spec); err != nil {
		return nil, err
	}
	if strings.TrimSpace(spec.Target) == "" && len(spec.Targets) == 0 {
		return nil, nil
	}
	return &spec, nil
}

func decodeConfigFromSpecNode(node *yaml.Node) (*ConfigFromSpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("unsupported config_from yaml node kind %d", node.Kind)
	}
	spec := &ConfigFromSpec{Bindings: map[string]string{}}
	if hasYAMLMappingKey(node, "policy_keys") {
		type alias ConfigFromSpec
		var aux alias
		if err := node.Decode(&aux); err != nil {
			return nil, err
		}
		spec.PolicyKeys = normalizeStrings(aux.PolicyKeys)
		for key, value := range aux.Bindings {
			spec.Bindings[key] = value
		}
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" || key == "policy_keys" {
			continue
		}
		spec.Bindings[key] = strings.TrimSpace(node.Content[i+1].Value)
	}
	if len(spec.PolicyKeys) == 0 && len(spec.Bindings) == 0 {
		return nil, nil
	}
	spec.Entries = spec.ConfigEntries()
	return spec, nil
}

func decodeBranchSpecsNode(node *yaml.Node) ([]BranchSpec, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	var specs []BranchSpec
	if err := node.Decode(&specs); err != nil {
		return nil, err
	}
	return specs, nil
}

func decodeEventEmitterNode(node *yaml.Node) (EventEmitterRef, []string, error) {
	if node == nil || node.Kind == 0 {
		return EventEmitterRef{}, nil, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		value := strings.TrimSpace(node.Value)
		if value == "" {
			return EventEmitterRef{}, nil, nil
		}
		return EventEmitterRef{AgentID: value}, nil, nil
	case yaml.SequenceNode:
		values, err := decodeStringListNode(node)
		if err != nil || len(values) == 0 {
			return EventEmitterRef{}, nil, err
		}
		return EventEmitterRef{AgentID: values[0]}, values[1:], nil
	case yaml.MappingNode:
		var ref EventEmitterRef
		if err := node.Decode(&ref); err != nil {
			return EventEmitterRef{}, nil, err
		}
		return ref, nil, nil
	default:
		return EventEmitterRef{}, nil, fmt.Errorf("unsupported emitter yaml node kind %d", node.Kind)
	}
}

func decodeEventPayloadSpecNode(node *yaml.Node) (EventPayloadSpec, error) {
	if node == nil || node.Kind == 0 {
		return EventPayloadSpec{}, nil
	}
	var spec EventPayloadSpec
	if err := node.Decode(&spec); err != nil {
		return EventPayloadSpec{}, err
	}
	return spec, nil
}
