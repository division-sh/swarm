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

func (e *EmitSpec) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			*e = EmitSpec{}
			return nil
		}
		*e = EmitSpec{Event: strings.TrimSpace(node.Value)}
		return nil
	case yaml.MappingNode:
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := strings.TrimSpace(node.Content[i].Value)
			switch key {
			case "", "event", "fields", "target", "broadcast":
			default:
				return fmt.Errorf("UNDEFINED-FIELD: emit field %q not in platform spec", key)
			}
		}
		var event string
		fields := map[string]ExpressionValue{}
		var target EmitTargetSpec
		var broadcast bool
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := strings.TrimSpace(node.Content[i].Value)
			value := node.Content[i+1]
			switch key {
			case "event":
				if err := value.Decode(&event); err != nil {
					return err
				}
			case "fields":
				decoded, err := decodeEmitFieldsNode(value)
				if err != nil {
					return err
				}
				fields = decoded
			case "target":
				decoded, err := decodeEmitTargetNode(value)
				if err != nil {
					return err
				}
				target = decoded
			case "broadcast":
				if err := value.Decode(&broadcast); err != nil {
					return err
				}
			}
		}
		*e = EmitSpec{
			Event:     strings.TrimSpace(event),
			Fields:    fields,
			Target:    target.Normalized(),
			Broadcast: broadcast,
		}
		if e.EventType() == "" && len(e.Fields) > 0 {
			return fmt.Errorf("INVALID-EMIT: emit.event is required when emit.fields is present")
		}
		if e.Broadcast && e.HasTarget() {
			return fmt.Errorf("INVALID-EMIT: emit.target and emit.broadcast:true are mutually exclusive")
		}
		return nil
	default:
		return fmt.Errorf("unsupported emit yaml node kind %d", node.Kind)
	}
}

func decodeEmitTargetNode(node *yaml.Node) (EmitTargetSpec, error) {
	if node == nil || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		return EmitTargetSpec{}, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		value := strings.TrimSpace(node.Value)
		if value == "" {
			return EmitTargetSpec{}, nil
		}
		if value != string(EmitTargetKindSender) {
			return EmitTargetSpec{}, fmt.Errorf("INVALID-EMIT: emit.target scalar must be %q", EmitTargetKindSender)
		}
		return EmitTargetSpec{Kind: EmitTargetKindSender}, nil
	case yaml.MappingNode:
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := strings.TrimSpace(node.Content[i].Value)
			switch key {
			case "", "instance_id", "flow", "match", "allow_fanout":
			default:
				return EmitTargetSpec{}, fmt.Errorf("UNDEFINED-FIELD: emit.target field %q not in platform spec", key)
			}
		}
		var out EmitTargetSpec
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := strings.TrimSpace(node.Content[i].Value)
			value := node.Content[i+1]
			switch key {
			case "instance_id":
				if err := value.Decode(&out.InstanceID); err != nil {
					return EmitTargetSpec{}, err
				}
			case "flow":
				if err := value.Decode(&out.Flow); err != nil {
					return EmitTargetSpec{}, err
				}
			case "match":
				match, err := decodeEmitFieldsNode(value)
				if err != nil {
					return EmitTargetSpec{}, fmt.Errorf("INVALID-EMIT: emit.target.match must be a mapping: %w", err)
				}
				out.Match = match
			case "allow_fanout":
				if err := value.Decode(&out.AllowFanout); err != nil {
					return EmitTargetSpec{}, err
				}
			}
		}
		out = out.Normalized()
		switch {
		case out.InstanceID != "" && (out.Flow != "" || len(out.Match) > 0 || out.AllowFanout):
			return EmitTargetSpec{}, fmt.Errorf("INVALID-EMIT: emit.target.instance_id cannot be combined with flow/match/allow_fanout")
		case out.InstanceID != "":
			out.Kind = EmitTargetKindInstanceID
		case out.Flow != "" && len(out.Match) > 0:
			out.Kind = EmitTargetKindFlowMatch
		case out.AllowFanout:
			return EmitTargetSpec{}, fmt.Errorf("INVALID-EMIT: emit.target.allow_fanout requires flow and match")
		default:
			return EmitTargetSpec{}, fmt.Errorf("INVALID-EMIT: emit.target requires sender, instance_id, or flow+match")
		}
		return out, nil
	default:
		return EmitTargetSpec{}, fmt.Errorf("unsupported emit.target yaml node kind %d", node.Kind)
	}
}

func decodeEmitFieldsNode(node *yaml.Node) (map[string]ExpressionValue, error) {
	if node == nil {
		return nil, nil
	}
	if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		return nil, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("INVALID-EMIT: emit.fields must be a mapping")
	}
	fields := make(map[string]ExpressionValue, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		target := strings.TrimSpace(node.Content[i].Value)
		if target == "" {
			continue
		}
		value, err := decodeEmitFieldValueNode(node.Content[i+1])
		if err != nil {
			return nil, fmt.Errorf("INVALID-EMIT: emit.fields.%s: %w", target, err)
		}
		fields[target] = value
	}
	return fields, nil
}

func (m *MailboxWriteSpec) UnmarshalYAML(node *yaml.Node) error {
	if m == nil {
		return nil
	}
	if node == nil || node.Kind == 0 || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		*m = MailboxWriteSpec{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("INVALID-MAILBOX-WRITE: mailbox must be a mapping")
	}
	allowed := map[string]struct{}{
		"item_type":     {},
		"severity":      {},
		"summary":       {},
		"entity_id":     {},
		"flow_instance": {},
		"payload":       {},
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("UNDEFINED-FIELD: mailbox field %q not in platform spec", key)
		}
	}
	var out MailboxWriteSpec
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		var err error
		switch key {
		case "item_type":
			out.ItemType, err = decodeMailboxExpressionValueNode(value)
		case "severity":
			out.Severity, err = decodeMailboxExpressionValueNode(value)
		case "summary":
			out.Summary, err = decodeMailboxExpressionValueNode(value)
		case "entity_id":
			out.EntityID, err = decodeMailboxExpressionValueNode(value)
		case "flow_instance":
			out.FlowInstance, err = decodeMailboxExpressionValueNode(value)
		case "payload":
			out.Payload, err = decodeMailboxPayloadNode(value)
		}
		if err != nil {
			return err
		}
	}
	*m = out
	return nil
}

func decodeMailboxPayloadNode(node *yaml.Node) (map[string]ExpressionValue, error) {
	if node == nil || node.Kind == 0 || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		return nil, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("INVALID-MAILBOX-WRITE: mailbox.payload must be a mapping")
	}
	fields := make(map[string]ExpressionValue, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		target := strings.TrimSpace(node.Content[i].Value)
		if target == "" {
			continue
		}
		value, err := decodeMailboxExpressionValueNode(node.Content[i+1])
		if err != nil {
			return nil, fmt.Errorf("INVALID-MAILBOX-WRITE: mailbox.payload.%s: %w", target, err)
		}
		fields[target] = value
	}
	return fields, nil
}

func decodeMailboxExpressionValueNode(node *yaml.Node) (ExpressionValue, error) {
	if node == nil || node.Kind == 0 {
		return ExpressionValue{}, nil
	}
	if node.Kind == yaml.MappingNode {
		if err := validateEmitFieldExpressionMappingNode(node); err != nil {
			return ExpressionValue{}, fmt.Errorf("mailbox expression values must use explicit expression keys literal, ref, cel, or expression: %w", err)
		}
	}
	var value ExpressionValue
	if err := node.Decode(&value); err != nil {
		return ExpressionValue{}, err
	}
	return value, nil
}

func (s *ArtifactRepoSpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	if node == nil || node.Kind == 0 || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		*s = ArtifactRepoSpec{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("INVALID-ARTIFACT-REPO: artifact_repo must be a mapping")
	}
	allowed := map[string]struct{}{
		"provider":        {},
		"repo_id":         {},
		"namespace":       {},
		"partition_key":   {},
		"display_slug":    {},
		"request_id":      {},
		"author":          {},
		"provenance":      {},
		"allowed_paths":   {},
		"files":           {},
		"output":          {},
		"limits":          {},
		"success_event":   {},
		"success_payload": {},
		"failure_event":   {},
		"failure_payload": {},
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("UNDEFINED-FIELD: artifact_repo field %q not in platform spec", key)
		}
	}
	type alias ArtifactRepoSpec
	var out alias
	if err := node.Decode(&out); err != nil {
		return err
	}
	*s = ArtifactRepoSpec(out)
	return nil
}

func (f *ArtifactRepoFileSpec) UnmarshalYAML(node *yaml.Node) error {
	if f == nil {
		return nil
	}
	if node == nil || node.Kind == 0 || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		*f = ArtifactRepoFileSpec{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("INVALID-ARTIFACT-REPO: artifact_repo.files entries must be mappings")
	}
	allowed := map[string]struct{}{
		"path":         {},
		"content":      {},
		"content_type": {},
		"schema":       {},
		"max_bytes":    {},
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("UNDEFINED-FIELD: artifact_repo.files field %q not in platform spec", key)
		}
	}
	type alias ArtifactRepoFileSpec
	var out alias
	if err := node.Decode(&out); err != nil {
		return err
	}
	*f = ArtifactRepoFileSpec(out)
	return nil
}

func (s *ArtifactRepoSchemaSpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	if node == nil || node.Kind == 0 || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		*s = ArtifactRepoSchemaSpec{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("INVALID-ARTIFACT-REPO: artifact_repo.files.schema must be a mapping")
	}
	allowed := map[string]struct{}{
		"type":            {},
		"required_fields": {},
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("UNDEFINED-FIELD: artifact_repo.files.schema field %q not in platform spec", key)
		}
	}
	type alias ArtifactRepoSchemaSpec
	var out alias
	if err := node.Decode(&out); err != nil {
		return err
	}
	*s = ArtifactRepoSchemaSpec(out)
	return nil
}

func (o *ArtifactRepoOutputSpec) UnmarshalYAML(node *yaml.Node) error {
	if o == nil {
		return nil
	}
	if node == nil || node.Kind == 0 || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		*o = ArtifactRepoOutputSpec{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("INVALID-ARTIFACT-REPO: artifact_repo.output must be a mapping")
	}
	allowed := map[string]struct{}{
		"repo_url":             {},
		"current_ref":          {},
		"file_manifest":        {},
		"status":               {},
		"failure_reason":       {},
		"last_request_id":      {},
		"last_source_event_id": {},
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("UNDEFINED-FIELD: artifact_repo.output field %q not in platform spec", key)
		}
	}
	type alias ArtifactRepoOutputSpec
	var out alias
	if err := node.Decode(&out); err != nil {
		return err
	}
	*o = ArtifactRepoOutputSpec(out)
	return nil
}

func (l *ArtifactRepoLimitsSpec) UnmarshalYAML(node *yaml.Node) error {
	if l == nil {
		return nil
	}
	if node == nil || node.Kind == 0 || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		*l = ArtifactRepoLimitsSpec{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("INVALID-ARTIFACT-REPO: artifact_repo.limits must be a mapping")
	}
	allowed := map[string]struct{}{
		"max_yaml_bytes":     {},
		"max_markdown_bytes": {},
		"max_text_bytes":     {},
		"max_repo_bytes":     {},
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("UNDEFINED-FIELD: artifact_repo.limits field %q not in platform spec", key)
		}
	}
	type alias ArtifactRepoLimitsSpec
	var out alias
	if err := node.Decode(&out); err != nil {
		return err
	}
	*l = ArtifactRepoLimitsSpec(out)
	return nil
}

func decodeEmitFieldValueNode(node *yaml.Node) (ExpressionValue, error) {
	if node == nil {
		return ExpressionValue{}, nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") || strings.TrimSpace(node.Value) == "" {
			return ExpressionValue{}, nil
		}
		return CELExpression(node.Value), nil
	case yaml.MappingNode:
		if err := validateEmitFieldExpressionMappingNode(node); err != nil {
			return ExpressionValue{}, err
		}
		var expr ExpressionValue
		if err := node.Decode(&expr); err != nil {
			return ExpressionValue{}, err
		}
		return expr, nil
	default:
		return ExpressionValue{}, fmt.Errorf("field value must be a scalar CEL expression or explicit expression mapping")
	}
}

func validateEmitFieldExpressionMappingNode(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	semanticKeys := 0
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		switch key {
		case "literal", "ref", "cel", "expression":
			semanticKeys++
		case "kind":
		default:
			return fmt.Errorf("field value mapping must use explicit expression keys literal, ref, cel, or expression; found %q", key)
		}
	}
	if semanticKeys == 0 {
		return fmt.Errorf("field value mapping must declare literal, ref, cel, or expression")
	}
	return nil
}

func (h *SystemNodeEventHandler) UnmarshalYAML(node *yaml.Node) error {
	if h == nil {
		return nil
	}
	if err := validateHandlerFieldNodes(node); err != nil {
		return err
	}
	var aux struct {
		Action               yaml.Node                `yaml:"action"`
		CreateEntity         bool                     `yaml:"create_entity"`
		SelectEntity         yaml.Node                `yaml:"select_entity"`
		SelectOrCreateEntity yaml.Node                `yaml:"select_or_create_entity"`
		Template             string                   `yaml:"template"`
		InstanceIDFrom       string                   `yaml:"instance_id_from"`
		ConfigFrom           yaml.Node                `yaml:"config_from"`
		EvidenceTarget       string                   `yaml:"evidence_target"`
		Description          string                   `yaml:"description"`
		Emit                 EmitSpec                 `yaml:"emit"`
		Guard                yaml.Node                `yaml:"guard"`
		AdvancesTo           yaml.Node                `yaml:"advances_to"`
		SetsGate             yaml.Node                `yaml:"sets_gate"`
		ClearGates           yaml.Node                `yaml:"clear_gates"`
		DataAccumulation     WorkflowDataAccumulation `yaml:"data_accumulation"`
		Condition            string                   `yaml:"condition"`
		CompletionRule       string                   `yaml:"completion_rule"`
		Logic                string                   `yaml:"logic"`
		PolicyRef            string                   `yaml:"policy_ref"`
		OnComplete           yaml.Node                `yaml:"on_complete"`
		Rules                yaml.Node                `yaml:"rules"`
		Accumulate           *AccumulateSpec          `yaml:"accumulate"`
		Compute              *ComputeSpec             `yaml:"compute"`
		Query                yaml.Node                `yaml:"query"`
		FanOut               *FanOutSpec              `yaml:"fan_out"`
		GroupBy              *GroupBySpec             `yaml:"group_by"`
		Filter               *FilterSpec              `yaml:"filter"`
		Reduce               *ReduceSpec              `yaml:"reduce"`
		Count                *CountSpec               `yaml:"count"`
		Clear                yaml.Node                `yaml:"clear"`
		Branch               yaml.Node                `yaml:"branch"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*h = SystemNodeEventHandler{
		CreateEntity:     aux.CreateEntity,
		EvidenceTarget:   strings.TrimSpace(aux.EvidenceTarget),
		Description:      strings.TrimSpace(aux.Description),
		Emit:             aux.Emit,
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
	}
	var err error
	if h.SelectEntity, err = decodeSelectEntitySpecNode(&aux.SelectEntity); err != nil {
		return err
	}
	if h.SelectOrCreateEntity, err = decodeSelectOrCreateEntitySpecNode(&aux.SelectOrCreateEntity); err != nil {
		return err
	}
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
	if HandlerHasAmbiguousTopLevelEmit(*h) {
		return fmt.Errorf("AMBIGUOUS-EMIT: handler-top-level emit is only allowed on single-emit handlers; move emit ownership to the active branch, rule, timeout, or fan_out site")
	}
	return nil
}

func (e *EventCatalogEntry) UnmarshalYAML(node *yaml.Node) error {
	if e == nil {
		return nil
	}
	if hasYAMLMappingKey(node, "payload") {
		return fmt.Errorf("RETIRED: nested events.yaml payload blocks are no longer supported; move payload fields to the event top level")
	}
	flatPayload, err := buildFlatEventPayloadSpec(node)
	if err != nil {
		return err
	}
	var aux struct {
		Swarm              eventSwarmMetadataYAML `yaml:"swarm"`
		Note               string                 `yaml:"_note"`
		Emitter            yaml.Node              `yaml:"emitter"`
		EmitterType        string                 `yaml:"emitter_type"`
		Producer           yaml.Node              `yaml:"producer"`
		ProducerLegacy     yaml.Node              `yaml:"_producer"`
		AlternateEmitters  []string               `yaml:"alternate_emitters"`
		Consumer           yaml.Node              `yaml:"consumer"`
		ConsumerLegacy     yaml.Node              `yaml:"_consumer"`
		ConsumerType       yaml.Node              `yaml:"consumer_type"`
		ConsumerTypeLegacy yaml.Node              `yaml:"_consumer_type"`
		Source             string                 `yaml:"_source"`
		Status             string                 `yaml:"_status"`
		Intercepted        yaml.Node              `yaml:"intercepted"`
		Passthrough        yaml.Node              `yaml:"passthrough"`
		RuntimeHandling    string                 `yaml:"runtime_handling"`
		OwningNode         string                 `yaml:"owning_node"`
		DeliveryChannel    yaml.Node              `yaml:"delivery_channel"`
		Payload            yaml.Node              `yaml:"payload"`
		Required           []string               `yaml:"required"`
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
	swarmProducer, err := decodeStringListNode(&aux.Swarm.Producer)
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
	swarmConsumer, err := decodeStringListNode(&aux.Swarm.Consumer)
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
	payload := flatPayload
	if len(payload.Required) == 0 {
		payload.Required = normalizeStrings(aux.Required)
	}
	note, err := mergeCanonicalLegacyString(aux.Swarm.Note, aux.Note, "swarm.note", "_note")
	if err != nil {
		return err
	}
	source, err := mergeCanonicalLegacyString(aux.Swarm.Source, aux.Source, "swarm.source", "_source")
	if err != nil {
		return err
	}
	status, err := mergeCanonicalLegacyString(aux.Swarm.Status, aux.Status, "swarm.status", "_status")
	if err != nil {
		return err
	}
	producer, err = mergeCanonicalLegacyStringLists(swarmProducer, mergeStringLists(producer, legacyProducer), "swarm.producer", "producer/_producer")
	if err != nil {
		return err
	}
	consumer, err = mergeCanonicalLegacyStringLists(swarmConsumer, mergeStringLists(consumer, legacyConsumer), "swarm.consumer", "consumer/_consumer")
	if err != nil {
		return err
	}
	consumerType = mergeStringLists(consumerType, legacyConsumerType)
	e.Swarm = EventSwarmMetadata{
		Note:     note,
		Source:   source,
		Producer: producer,
		Consumer: consumer,
		Status:   status,
	}
	e.Note = e.SwarmNote()
	e.Emitter = emitter
	e.EmitterType = strings.TrimSpace(aux.EmitterType)
	e.Producer = e.SwarmProducer()
	e.AlternateEmitters = mergeStringLists(aux.AlternateEmitters, alternates)
	e.Consumer = e.SwarmConsumer()
	e.ConsumerType = consumerType
	e.Source = e.SwarmSource()
	e.Status = e.SwarmStatus()
	e.Intercepted = intercepted
	e.Passthrough = passthrough
	e.RuntimeHandling = strings.TrimSpace(aux.RuntimeHandling)
	e.OwningNode = strings.TrimSpace(aux.OwningNode)
	e.DeliveryChannel = deliveryChannel
	e.Payload = payload
	e.Required = normalizeStrings(aux.Required)
	return nil
}

type eventSwarmMetadataYAML struct {
	Note     string    `yaml:"note"`
	Source   string    `yaml:"source"`
	Producer yaml.Node `yaml:"producer"`
	Consumer yaml.Node `yaml:"consumer"`
	Status   string    `yaml:"status"`
}

func mergeCanonicalLegacyString(canonical, legacy, canonicalKey, legacyKey string) (string, error) {
	canonical = strings.TrimSpace(canonical)
	legacy = strings.TrimSpace(legacy)
	switch {
	case canonical == "":
		return legacy, nil
	case legacy == "":
		return canonical, nil
	case canonical == legacy:
		return canonical, nil
	default:
		return "", fmt.Errorf("event metadata fields %s and %s conflict: %q != %q", canonicalKey, legacyKey, canonical, legacy)
	}
}

func mergeCanonicalLegacyStringLists(canonical, legacy []string, canonicalKey, legacyKey string) ([]string, error) {
	canonical = normalizeStrings(canonical)
	legacy = normalizeStrings(legacy)
	switch {
	case len(canonical) == 0:
		return legacy, nil
	case len(legacy) == 0:
		return canonical, nil
	case sameStringSet(canonical, legacy):
		return canonical, nil
	default:
		return nil, fmt.Errorf("event metadata fields %s and %s conflict: %v != %v", canonicalKey, legacyKey, canonical, legacy)
	}
}

func sameStringSet(a, b []string) bool {
	a = normalizeStrings(a)
	b = normalizeStrings(b)
	if len(a) != len(b) {
		return false
	}
	seen := map[string]int{}
	for _, value := range a {
		seen[value]++
	}
	for _, value := range b {
		if seen[value] == 0 {
			return false
		}
		seen[value]--
	}
	return true
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
		"action":                  {},
		"description":             {},
		"_note":                   {},
		"evidence_target":         {},
		"create_entity":           {},
		"select_entity":           {},
		"select_or_create_entity": {},
		"emit":                    {},
		"guard":                   {},
		"advances_to":             {},
		"sets_gate":               {},
		"clear_gates":             {},
		"data_accumulation":       {},
		"condition":               {},
		"completion_rule":         {},
		"logic":                   {},
		"policy_ref":              {},
		"on_complete":             {},
		"rules":                   {},
		"accumulate":              {},
		"compute":                 {},
		"query":                   {},
		"fan_out":                 {},
		"group_by":                {},
		"filter":                  {},
		"reduce":                  {},
		"count":                   {},
		"clear":                   {},
		"template":                {},
		"instance_id_from":        {},
		"config_from":             {},
		"from":                    {},
		"branch":                  {},
		"dedup_by":                {},
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
		switch key {
		case "emits":
			return fmt.Errorf("RETIRED: handler field %q is retired; use emit: <event> or emit: {event, fields}", key)
		case "payload_transform":
			return fmt.Errorf("RETIRED: handler field %q is retired; move payload ownership into emit.fields at the active emit site", key)
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
	if strings.TrimSpace(rule.ID) == "" && strings.TrimSpace(rule.Description) == "" && strings.TrimSpace(rule.Condition) == "" && strings.TrimSpace(rule.AdvancesTo) == "" && rule.Emit.Empty() && !rule.DataAccumulation.HasWrites() && rule.Compute == nil && rule.FanOut == nil {
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
		if hasAnyYAMLMappingKey(node, "condition", "advances_to", "emit", "emits", "data_accumulation", "compute", "fan_out") {
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

func decodeSelectEntitySpecNode(node *yaml.Node) (*SelectEntitySpec, error) {
	spec, err := decodeEntitySelectionSpecNode(node, "select_entity")
	if err != nil || spec == nil {
		return nil, err
	}
	return &SelectEntitySpec{
		By:       spec.By,
		Bindings: spec.Bindings,
	}, nil
}

func decodeSelectOrCreateEntitySpecNode(node *yaml.Node) (*SelectOrCreateEntitySpec, error) {
	spec, err := decodeEntitySelectionSpecNode(node, "select_or_create_entity")
	if err != nil || spec == nil {
		return nil, err
	}
	return &SelectOrCreateEntitySpec{
		By:       spec.By,
		Bindings: spec.Bindings,
	}, nil
}

func decodeEntitySelectionSpecNode(node *yaml.Node, label string) (*SelectEntitySpec, error) {
	if node == nil || node.Kind == 0 || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		return nil, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("INVALID-SELECT-ENTITY: %s must be a mapping", label)
	}
	allowed := map[string]struct{}{
		"by": {},
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := allowed[key]; !ok {
			return nil, fmt.Errorf("UNDEFINED-FIELD: %s field %q not in platform spec", label, key)
		}
	}
	var aux struct {
		By map[string]string `yaml:"by"`
	}
	if err := node.Decode(&aux); err != nil {
		return nil, err
	}
	if len(aux.By) == 0 {
		return nil, fmt.Errorf("INVALID-SELECT-ENTITY: %s.by must declare at least one binding", label)
	}
	spec := &SelectEntitySpec{
		By:       cloneStringMap(aux.By),
		Bindings: make([]SelectEntityKeyBinding, 0, len(aux.By)),
	}
	for field, ref := range aux.By {
		field = strings.TrimSpace(field)
		ref = strings.TrimSpace(ref)
		if field == "" {
			return nil, fmt.Errorf("INVALID-SELECT-ENTITY: %s.by contains an empty entity field", label)
		}
		if ref == "" {
			return nil, fmt.Errorf("INVALID-SELECT-ENTITY: %s.by.%s requires a payload ref", label, field)
		}
		parsed := paths.Parse(ref)
		spec.Bindings = append(spec.Bindings, SelectEntityKeyBinding{
			Field:   field,
			Ref:     ref,
			RefPath: parsed,
		})
	}
	return spec, nil
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
		allowed := map[string]struct{}{
			"id":               {},
			"template":         {},
			"instance_id_from": {},
			"config_from":      {},
			"mailbox":          {},
			"artifact_repo":    {},
		}
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := strings.TrimSpace(node.Content[i].Value)
			if key == "" {
				continue
			}
			if _, ok := allowed[key]; ok {
				continue
			}
			switch key {
			case "type", "flow_template", "instance_id":
				return ActionSpec{}, fmt.Errorf("DEPRECATED: legacy action field %q is not supported; use action: create_flow_instance with template, instance_id_from, and config_from siblings", key)
			default:
				return ActionSpec{}, fmt.Errorf("UNDEFINED-FIELD: action field %q not in platform spec", key)
			}
		}
		var aux struct {
			ID             string            `yaml:"id"`
			Template       string            `yaml:"template"`
			InstanceIDFrom string            `yaml:"instance_id_from"`
			ConfigFrom     yaml.Node         `yaml:"config_from"`
			Mailbox        *MailboxWriteSpec `yaml:"mailbox"`
			ArtifactRepo   *ArtifactRepoSpec `yaml:"artifact_repo"`
		}
		if err := node.Decode(&aux); err != nil {
			return ActionSpec{}, err
		}
		if strings.TrimSpace(aux.ID) == "" {
			return ActionSpec{}, fmt.Errorf("action mapping missing id")
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
			Mailbox:        aux.Mailbox,
			ArtifactRepo:   aux.ArtifactRepo,
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
		return nil, fmt.Errorf("UNDEFINED-FIELD: config_from field %q not in platform spec", "policy_keys")
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
