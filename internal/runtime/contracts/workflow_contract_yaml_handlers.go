package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"gopkg.in/yaml.v3"
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
		if err := rejectWorkflowTimerRetiredDurationAliases(node); err != nil {
			return err
		}
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

func rejectWorkflowTimerRetiredDurationAliases(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	hasDelay, retired := workflowTimerDurationKeys(node, map[*yaml.Node]bool{})
	if len(retired) == 0 {
		return nil
	}
	if hasDelay {
		return fmt.Errorf("RETIRED timer duration fields %s cannot be combined with canonical delay; use delay only", strings.Join(retired, ", "))
	}
	if len(retired) == 1 {
		return fmt.Errorf("RETIRED timer duration field %s is not accepted; use delay", retired[0])
	}
	return fmt.Errorf("RETIRED timer duration fields %s are not accepted; use delay", strings.Join(retired, ", "))
}

func workflowTimerDurationKeys(node *yaml.Node, seen map[*yaml.Node]bool) (bool, []string) {
	if node == nil {
		return false, nil
	}
	if seen[node] {
		return false, nil
	}
	seen[node] = true
	switch node.Kind {
	case yaml.AliasNode:
		return workflowTimerDurationKeys(node.Alias, seen)
	case yaml.SequenceNode:
		hasDelay := false
		retired := []string{}
		for _, child := range node.Content {
			childHasDelay, childRetired := workflowTimerDurationKeys(child, seen)
			hasDelay = hasDelay || childHasDelay
			retired = appendRetiredTimerDurationFields(retired, childRetired...)
		}
		return hasDelay, retired
	case yaml.MappingNode:
		hasDelay := false
		retired := []string{}
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := strings.TrimSpace(node.Content[i].Value)
			if key == "<<" || strings.TrimSpace(node.Content[i].Tag) == "!!merge" {
				mergeHasDelay, mergeRetired := workflowTimerDurationKeys(node.Content[i+1], seen)
				hasDelay = hasDelay || mergeHasDelay
				retired = appendRetiredTimerDurationFields(retired, mergeRetired...)
				continue
			}
			switch key {
			case "delay":
				hasDelay = true
			case "delay_seconds", "delay_minutes", "delay_hours", "delay_days":
				retired = appendRetiredTimerDurationFields(retired, key)
			}
		}
		return hasDelay, retired
	default:
		return false, nil
	}
}

func appendRetiredTimerDurationFields(fields []string, candidates ...string) []string {
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		seen := false
		for _, existing := range fields {
			if existing == candidate {
				seen = true
				break
			}
		}
		if !seen {
			fields = append(fields, candidate)
		}
	}
	return fields
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
			if key == "" {
				continue
			}
			if _, ok := emitFieldOptions[key]; !ok {
				return NewUndefinedFieldDiagnostic("emit", key, emitFieldOptions)
			}
		}
		var event string
		var from string
		fields := map[string]ExpressionValue{}
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := strings.TrimSpace(node.Content[i].Value)
			value := node.Content[i+1]
			switch key {
			case "event":
				if err := value.Decode(&event); err != nil {
					return err
				}
			case "from":
				if err := value.Decode(&from); err != nil {
					return err
				}
				if err := validateEmitFromSource(from); err != nil {
					return err
				}
			case "fields":
				decoded, err := decodeEmitFieldsNode(value)
				if err != nil {
					return err
				}
				fields = decoded
			case "target":
				return fmt.Errorf("RETIRED-EMIT-ROUTING: emit.target is not accepted; use same-flow subscriptions, package connect with receiver select/reply, or an accepted external consumer")
			case "broadcast":
				return fmt.Errorf("RETIRED-EMIT-ROUTING: emit.broadcast is not accepted; use same-flow subscriptions, package connect with receiver select/reply, or a structured output pin with sink: harness for validation-only observation")
			}
		}
		*e = EmitSpec{
			Event:  strings.TrimSpace(event),
			From:   strings.TrimSpace(from),
			Fields: fields,
		}
		return nil
	default:
		return fmt.Errorf("unsupported emit yaml node kind %d", node.Kind)
	}
}

var emitFieldOptions = map[string]struct{}{
	"event":     {},
	"from":      {},
	"fields":    {},
	"target":    {},
	"broadcast": {},
}

var onSuccessFieldOptions = map[string]struct{}{
	"emit": {},
}

var activityFieldOptions = map[string]struct{}{
	"id":       {},
	"tool":     {},
	"input":    {},
	"approval": {},
}

var activityApprovalFieldOptions = map[string]struct{}{
	"decision": {},
}

var mailboxFieldOptions = map[string]struct{}{
	"item_type":     {},
	"severity":      {},
	"summary":       {},
	"entity_id":     {},
	"flow_instance": {},
	"payload":       {},
}

var artifactRepoFieldOptions = map[string]struct{}{
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

var artifactRepoFilesFieldOptions = map[string]struct{}{
	"path":         {},
	"content":      {},
	"content_type": {},
	"schema":       {},
	"max_bytes":    {},
}

var artifactRepoFilesSchemaFieldOptions = map[string]struct{}{
	"type":            {},
	"required_fields": {},
}

var artifactRepoOutputFieldOptions = map[string]struct{}{
	"repo_url":             {},
	"current_ref":          {},
	"file_manifest":        {},
	"status":               {},
	"failure":              {},
	"last_request_id":      {},
	"last_source_event_id": {},
}

var artifactRepoLimitsFieldOptions = map[string]struct{}{
	"max_yaml_bytes":     {},
	"max_markdown_bytes": {},
	"max_text_bytes":     {},
	"max_repo_bytes":     {},
}

var entitySelectionFieldOptions = map[string]struct{}{
	"by": {},
}

func (s *HandlerOnSuccessSpec) UnmarshalYAML(node *yaml.Node) error {
	if s == nil {
		return nil
	}
	if node == nil || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		*s = HandlerOnSuccessSpec{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("unsupported on_success yaml node kind %d", node.Kind)
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := onSuccessFieldOptions[key]; !ok {
			return NewUndefinedFieldDiagnostic("on_success", key, onSuccessFieldOptions)
		}
	}
	var aux struct {
		Emit EmitSpec `yaml:"emit"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*s = HandlerOnSuccessSpec{Emit: aux.Emit}
	if !s.Empty() && s.Emit.EventType() == "" {
		return fmt.Errorf("INVALID-EMIT: on_success.emit.event is required")
	}
	return nil
}

func (a *ActivitySpec) UnmarshalYAML(node *yaml.Node) error {
	if a == nil {
		return nil
	}
	if node == nil || node.Kind == 0 || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		*a = ActivitySpec{}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("INVALID-ACTIVITY: activity must be a mapping with tool and input")
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := activityFieldOptions[key]; !ok {
			return NewUndefinedFieldDiagnostic("activity", key, activityFieldOptions)
		}
	}
	var out ActivitySpec
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "id":
			if err := value.Decode(&out.ID); err != nil {
				return err
			}
		case "tool":
			if err := value.Decode(&out.Tool); err != nil {
				return err
			}
		case "input":
			input, err := decodeActivityInputNode(value)
			if err != nil {
				return err
			}
			out.Input = input
		case "approval":
			approval, err := decodeActivityApprovalNode(value)
			if err != nil {
				return err
			}
			out.Approval = approval
		}
	}
	out.ID = strings.TrimSpace(out.ID)
	out.Tool = strings.TrimSpace(out.Tool)
	*a = out
	return nil
}

func decodeActivityApprovalNode(node *yaml.Node) (*ActivityApprovalSpec, error) {
	if node == nil || node.Kind == 0 || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		return nil, fmt.Errorf("INVALID-ACTIVITY-APPROVAL: activity.approval must be a mapping with decision")
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("INVALID-ACTIVITY-APPROVAL: activity.approval must be a mapping with decision")
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if _, ok := activityApprovalFieldOptions[key]; !ok {
			return nil, NewUndefinedFieldDiagnostic("activity.approval", key, activityApprovalFieldOptions)
		}
	}
	var out ActivityApprovalSpec
	for i := 0; i+1 < len(node.Content); i += 2 {
		if strings.TrimSpace(node.Content[i].Value) == "decision" {
			if err := node.Content[i+1].Decode(&out.Decision); err != nil {
				return nil, err
			}
		}
	}
	rawDecision := out.Decision
	out.Decision = strings.TrimSpace(rawDecision)
	if out.Decision == "" {
		return nil, fmt.Errorf("INVALID-ACTIVITY-APPROVAL: activity.approval.decision is required; use the activity id when it is the intended stable approval class")
	}
	if rawDecision != out.Decision {
		return nil, fmt.Errorf("INVALID-ACTIVITY-APPROVAL: activity.approval.decision %q is not canonical; use %q", rawDecision, out.Decision)
	}
	return &out, nil
}

func decodeActivityInputNode(node *yaml.Node) (map[string]ExpressionValue, error) {
	if node == nil || strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		return nil, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("INVALID-ACTIVITY: activity.input must be a mapping")
	}
	fields := make(map[string]ExpressionValue, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		target := strings.TrimSpace(node.Content[i].Value)
		if target == "" {
			continue
		}
		value, err := decodeEmitFieldValueNode(node.Content[i+1])
		if err != nil {
			return nil, fmt.Errorf("INVALID-ACTIVITY: activity.input.%s: %w", target, err)
		}
		fields[target] = value
	}
	return fields, nil
}

func decodeEmitFieldsNode(node *yaml.Node) (map[string]ExpressionValue, error) {
	return decodeExpressionValueMapNode(node, "emit.fields")
}

func decodeExpressionValueMapNode(node *yaml.Node, label string) (map[string]ExpressionValue, error) {
	if node == nil {
		return nil, nil
	}
	if strings.EqualFold(strings.TrimSpace(node.Tag), "!!null") {
		return nil, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("INVALID-EMIT: %s must be a mapping", label)
	}
	if err := validateUniqueNormalizedMappingKeys(node, label); err != nil {
		return nil, fmt.Errorf("INVALID-EMIT: %w", err)
	}
	fields := make(map[string]ExpressionValue, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		target := strings.TrimSpace(node.Content[i].Value)
		if target == "" {
			continue
		}
		value, err := decodeEmitFieldValueNode(node.Content[i+1])
		if err != nil {
			return nil, fmt.Errorf("INVALID-EMIT: %s.%s: %w", label, target, err)
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
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := mailboxFieldOptions[key]; !ok {
			return NewUndefinedFieldDiagnostic("mailbox", key, mailboxFieldOptions)
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
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := artifactRepoFieldOptions[key]; !ok {
			return NewUndefinedFieldDiagnostic("artifact_repo", key, artifactRepoFieldOptions)
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
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := artifactRepoFilesFieldOptions[key]; !ok {
			return NewUndefinedFieldDiagnostic("artifact_repo.files", key, artifactRepoFilesFieldOptions)
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
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := artifactRepoFilesSchemaFieldOptions[key]; !ok {
			return NewUndefinedFieldDiagnostic("artifact_repo.files.schema", key, artifactRepoFilesSchemaFieldOptions)
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
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := artifactRepoOutputFieldOptions[key]; !ok {
			return NewUndefinedFieldDiagnostic("artifact_repo.output", key, artifactRepoOutputFieldOptions)
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
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := artifactRepoLimitsFieldOptions[key]; !ok {
			return NewUndefinedFieldDiagnostic("artifact_repo.limits", key, artifactRepoLimitsFieldOptions)
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
		Activity             ActivitySpec             `yaml:"activity"`
		CreateEntity         bool                     `yaml:"create_entity"`
		SelectEntity         yaml.Node                `yaml:"select_entity"`
		SelectOrCreateEntity yaml.Node                `yaml:"select_or_create_entity"`
		Template             string                   `yaml:"template"`
		InstanceIDFrom       string                   `yaml:"instance_id_from"`
		ConfigFrom           yaml.Node                `yaml:"config_from"`
		EvidenceTarget       string                   `yaml:"evidence_target"`
		Description          string                   `yaml:"description"`
		Emit                 EmitSpec                 `yaml:"emit"`
		OnSuccess            HandlerOnSuccessSpec     `yaml:"on_success"`
		Guard                yaml.Node                `yaml:"guard"`
		AdvancesTo           yaml.Node                `yaml:"advances_to"`
		SetsGate             yaml.Node                `yaml:"sets_gate"`
		ClearGates           yaml.Node                `yaml:"clear_gates"`
		DataAccumulation     WorkflowDataAccumulation `yaml:"data_accumulation"`
		Condition            string                   `yaml:"condition"`
		Logic                string                   `yaml:"logic"`
		Loop                 *LoopOperationSpec       `yaml:"loop"`
		OnComplete           yaml.Node                `yaml:"on_complete"`
		Rules                yaml.Node                `yaml:"rules"`
		Accumulate           *AccumulateSpec          `yaml:"accumulate"`
		Join                 *JoinSpec                `yaml:"join"`
		Compute              *ComputeSpec             `yaml:"compute"`
		Query                yaml.Node                `yaml:"query"`
		FanOut               *FanOutSpec              `yaml:"fan_out"`
		GroupBy              *GroupBySpec             `yaml:"group_by"`
		Filter               *FilterSpec              `yaml:"filter"`
		Reduce               *ReduceSpec              `yaml:"reduce"`
		Count                *CountSpec               `yaml:"count"`
		Clear                yaml.Node                `yaml:"clear"`
	}
	if err := node.Decode(&aux); err != nil {
		return err
	}
	*h = SystemNodeEventHandler{
		Activity:         aux.Activity,
		CreateEntity:     aux.CreateEntity,
		EvidenceTarget:   strings.TrimSpace(aux.EvidenceTarget),
		Description:      strings.TrimSpace(aux.Description),
		Emit:             aux.Emit,
		OnSuccess:        aux.OnSuccess,
		DataAccumulation: aux.DataAccumulation,
		Condition:        strings.TrimSpace(aux.Condition),
		Logic:            strings.TrimSpace(aux.Logic),
		Loop:             aux.Loop,
		Accumulate:       aux.Accumulate,
		Join:             aux.Join,
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
	if h.OnComplete, err = decodeHandlerRuleEntriesNode(&aux.OnComplete, handlerRuleDecodeContextOnComplete); err != nil {
		return err
	}
	if h.Rules, err = decodeHandlerRuleEntriesNode(&aux.Rules, handlerRuleDecodeContextRules); err != nil {
		return err
	}
	if h.Query, err = decodeQuerySpecNode(&aux.Query); err != nil {
		return err
	}
	if h.Clear, err = decodeClearSpecNode(&aux.Clear); err != nil {
		return err
	}
	if err := HandlerEmitSiteOwnershipError(*h); err != nil {
		return err
	}
	if HandlerHasAmbiguousTopLevelAction(*h) {
		return fmt.Errorf("AMBIGUOUS-ACTION: handler-top-level action is only allowed on handlers without rules; move action ownership to the active rule")
	}
	return nil
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

var handlerFieldOptions = map[string]struct{}{
	"action":                  {},
	"activity":                {},
	"description":             {},
	"_note":                   {},
	"evidence_target":         {},
	"create_entity":           {},
	"select_entity":           {},
	"select_or_create_entity": {},
	"emit":                    {},
	"on_success":              {},
	"guard":                   {},
	"advances_to":             {},
	"sets_gate":               {},
	"clear_gates":             {},
	"data_accumulation":       {},
	"condition":               {},
	"logic":                   {},
	"loop":                    {},
	"on_complete":             {},
	"rules":                   {},
	"accumulate":              {},
	"join":                    {},
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
	"dedup_by":                {},
}

func validateHandlerFieldNodes(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
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
		case "branch":
			return fmt.Errorf("RETIRED: handler field %q is retired; use rules for branch selection", key)
		case "emits":
			return fmt.Errorf("RETIRED: handler field %q is retired; use emit: <event> or emit: {event, fields}", key)
		case "payload_transform":
			return fmt.Errorf("RETIRED: handler field %q is retired; move payload ownership into emit.fields at the active emit site", key)
		}
		if key == "on_complete" {
			if _, err := resolveHandlerRuleCollectionNode(node.Content[i+1], handlerRuleDecodeContextOnComplete); err != nil {
				return err
			}
		}
		if _, ok := handlerFieldOptions[key]; !ok {
			return NewUndefinedFieldDiagnostic("handler", key, handlerFieldOptions)
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

type handlerRuleDecodeContext string

const (
	handlerRuleDecodeContextRules          handlerRuleDecodeContext = "rules"
	handlerRuleDecodeContextOnComplete     handlerRuleDecodeContext = "on_complete"
	handlerRuleDecodeContextJoinOnComplete handlerRuleDecodeContext = "join.on_complete"
	handlerRuleDecodeContextJoinTimeout    handlerRuleDecodeContext = "join.timeout"
)

func decodeHandlerRuleEntryNode(node *yaml.Node, context handlerRuleDecodeContext) (*HandlerRuleEntry, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	resolved, err := resolveHandlerRuleYAMLNode(node)
	if err != nil {
		return nil, err
	}
	var rule HandlerRuleEntry
	if err := resolved.Decode(&rule); err != nil {
		return nil, err
	}
	if err := rejectRuleActionOutsideRules(rule, context); err != nil {
		return nil, err
	}
	if !rule.ElementID.Valid() && strings.TrimSpace(rule.ID) == "" && strings.TrimSpace(rule.Description) == "" && strings.TrimSpace(rule.Condition) == "" && strings.TrimSpace(rule.AdvancesTo) == "" && rule.Emit.Empty() && strings.TrimSpace(rule.Action.ID) == "" && rule.Activity.Empty() && !rule.DataAccumulation.HasWrites() && rule.Compute == nil && rule.FanOut == nil {
		return nil, nil
	}
	return &rule, nil
}

func decodeHandlerRuleEntriesNode(node *yaml.Node, context handlerRuleDecodeContext) ([]HandlerRuleEntry, error) {
	if node == nil || node.Kind == 0 {
		return nil, nil
	}
	resolved, err := resolveHandlerRuleCollectionNode(node, context)
	if err != nil {
		return nil, err
	}
	node = resolved
	switch node.Kind {
	case yaml.SequenceNode:
		var rules []HandlerRuleEntry
		if err := node.Decode(&rules); err != nil {
			return nil, err
		}
		for _, rule := range rules {
			if err := rejectRuleActionOutsideRules(rule, context); err != nil {
				return nil, err
			}
		}
		if err := validatePolicySheetRows(rules, context); err != nil {
			return nil, err
		}
		return rules, nil
	case yaml.MappingNode:
		shape, err := classifyHandlerRuleMapping(node)
		if err != nil {
			return nil, err
		}
		if shape == handlerRuleMappingSingleton {
			rule, err := decodeHandlerRuleEntryNode(node, context)
			if err != nil || rule == nil {
				return nil, err
			}
			rules := []HandlerRuleEntry{*rule}
			if err := validatePolicySheetRows(rules, context); err != nil {
				return nil, err
			}
			return rules, nil
		}
		rules := make([]HandlerRuleEntry, 0, len(node.Content)/2)
		for i := 0; i+1 < len(node.Content); i += 2 {
			id := strings.TrimSpace(node.Content[i].Value)
			row, err := resolveHandlerRuleYAMLNode(node.Content[i+1])
			if err != nil {
				return nil, err
			}
			var rule HandlerRuleEntry
			if err := row.Decode(&rule); err != nil {
				return nil, err
			}
			if err := rejectRuleActionOutsideRules(rule, context); err != nil {
				return nil, err
			}
			if strings.TrimSpace(rule.ID) == "" {
				rule.ID = id
			}
			rules = append(rules, rule)
		}
		if err := validatePolicySheetRows(rules, context); err != nil {
			return nil, err
		}
		return rules, nil
	default:
		return nil, fmt.Errorf("unsupported rules yaml node kind %d", node.Kind)
	}
}

func resolveHandlerRuleCollectionNode(node *yaml.Node, context handlerRuleDecodeContext) (*yaml.Node, error) {
	resolved, err := resolveHandlerRuleYAMLNode(node)
	if err != nil {
		return nil, err
	}
	if resolved != nil && yamlNodeIsNull(resolved) {
		return nil, fmt.Errorf("%s handler rule collection must not be null", context)
	}
	if context == handlerRuleDecodeContextOnComplete && resolved != nil && resolved.Kind != yaml.SequenceNode {
		return nil, fmt.Errorf("DIALECT-OC-ORDER: on_complete is dict, must be ordered list")
	}
	return resolved, nil
}

type handlerRuleMappingShape uint8

const (
	handlerRuleMappingSingleton handlerRuleMappingShape = iota + 1
	handlerRuleMappingKeyed
)

// classifyHandlerRuleMapping resolves grammar from both the outer field names
// and child row shape. Keyed display labels are never reserved grammar tokens.
func classifyHandlerRuleMapping(node *yaml.Node) (handlerRuleMappingShape, error) {
	resolved, err := resolveHandlerRuleYAMLNode(node)
	if err != nil {
		return 0, err
	}
	if resolved == nil || resolved.Kind != yaml.MappingNode {
		return 0, fmt.Errorf("rule mapping must be a mapping")
	}
	node = resolved
	for i := 0; i+1 < len(node.Content); i += 2 {
		if strings.TrimSpace(node.Content[i].Value) == "" {
			return 0, fmt.Errorf("keyed handler rule label must not be empty")
		}
	}

	singletonErr := decodeSingletonHandlerRuleShape(node)
	keyedStructureErr := validateKeyedHandlerRuleStructure(node)
	keyedErr := keyedStructureErr
	if keyedStructureErr == nil {
		keyedErr = decodeKeyedHandlerRuleShape(node)
	}
	switch {
	case singletonErr == nil && keyedErr == nil:
		return 0, fmt.Errorf("AMBIGUOUS-RULE-GRAMMAR: mapping is valid as both one handler rule and keyed handler rules; use sequence form to state the row boundary explicitly")
	case singletonErr == nil:
		return handlerRuleMappingSingleton, nil
	case keyedStructureErr == nil:
		return handlerRuleMappingKeyed, nil
	default:
		return 0, fmt.Errorf("invalid handler rule mapping (singleton: %v; keyed: %v)", singletonErr, keyedErr)
	}
}

func decodeSingletonHandlerRuleShape(node *yaml.Node) error {
	resolved, err := resolveHandlerRuleYAMLNode(node)
	if err != nil {
		return err
	}
	var rule HandlerRuleEntry
	return resolved.Decode(&rule)
}

func decodeKeyedHandlerRuleShape(node *yaml.Node) error {
	if err := validateKeyedHandlerRuleStructure(node); err != nil {
		return err
	}
	resolved, err := resolveHandlerRuleYAMLNode(node)
	if err != nil {
		return err
	}
	for i := 0; i+1 < len(resolved.Content); i += 2 {
		label := strings.TrimSpace(resolved.Content[i].Value)
		row, err := resolveHandlerRuleYAMLNode(resolved.Content[i+1])
		if err != nil {
			return err
		}
		var rule HandlerRuleEntry
		if err := row.Decode(&rule); err != nil {
			return fmt.Errorf("keyed handler rule %q: %w", label, err)
		}
	}
	return nil
}

func validateKeyedHandlerRuleStructure(node *yaml.Node) error {
	resolved, err := resolveHandlerRuleYAMLNode(node)
	if err != nil {
		return err
	}
	if resolved == nil || resolved.Kind != yaml.MappingNode {
		return fmt.Errorf("keyed handler rules must be a mapping")
	}
	node = resolved
	if err := validateUniqueNormalizedMappingKeys(node, "keyed handler rules"); err != nil {
		return err
	}
	if len(node.Content) == 0 {
		return fmt.Errorf("keyed handler rules must contain at least one row")
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		label := strings.TrimSpace(node.Content[i].Value)
		if label == "" {
			return fmt.Errorf("keyed handler rule label must not be empty")
		}
		row, err := resolveHandlerRuleYAMLNode(node.Content[i+1])
		if err != nil {
			return fmt.Errorf("keyed handler rule %q: %w", label, err)
		}
		if row.Kind != yaml.MappingNode {
			return fmt.Errorf("keyed handler rule %q must be a mapping", label)
		}
	}
	return nil
}

func rejectRuleActionOutsideRules(rule HandlerRuleEntry, context handlerRuleDecodeContext) error {
	if context == handlerRuleDecodeContextRules || strings.TrimSpace(rule.Action.ID) == "" {
		return nil
	}
	return fmt.Errorf("UNSUPPORTED-ACTION: %s entries do not support action; rule-level action is only supported under handler.rules", context)
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
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" {
			continue
		}
		if _, ok := entitySelectionFieldOptions[key]; !ok {
			return nil, NewUndefinedFieldDiagnostic(label, key, entitySelectionFieldOptions)
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

func (a *ActionSpec) UnmarshalYAML(node *yaml.Node) error {
	if a == nil {
		return nil
	}
	spec, err := decodeActionSpecNode(node)
	if err != nil {
		return err
	}
	*a = spec
	return nil
}

var actionFieldOptions = map[string]struct{}{
	"id":               {},
	"template":         {},
	"instance_id_from": {},
	"config_from":      {},
	"mailbox":          {},
	"artifact_repo":    {},
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
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := strings.TrimSpace(node.Content[i].Value)
			if key == "" {
				continue
			}
			if _, ok := actionFieldOptions[key]; ok {
				continue
			}
			switch key {
			case "type", "flow_template", "instance_id":
				return ActionSpec{}, fmt.Errorf("DEPRECATED: legacy action field %q is not supported; use action: create_flow_instance with template, instance_id_from, and config_from siblings", key)
			default:
				return ActionSpec{}, NewUndefinedFieldDiagnostic("action", key, actionFieldOptions)
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
	if hasYAMLMappingKey(node, "target") {
		return nil, fmt.Errorf("RETIRED: clear field target is retired; use targets")
	}
	var spec ClearSpec
	if err := node.Decode(&spec); err != nil {
		return nil, err
	}
	if len(spec.Targets) == 0 {
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
		return nil, NewUndefinedFieldDiagnostic("config_from", "policy_keys", nil)
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		if key == "" || key == "policy_keys" {
			continue
		}
		if configFromKeyContainsSystemPrompt(key) {
			return nil, fmt.Errorf("RETIRED: config_from key %q cannot author system_prompt; declare intent: on the managed agent", key)
		}
		spec.Bindings[key] = strings.TrimSpace(node.Content[i+1].Value)
	}
	if len(spec.PolicyKeys) == 0 && len(spec.Bindings) == 0 {
		return nil, nil
	}
	spec.Entries = spec.ConfigEntries()
	return spec, nil
}

func configFromKeyContainsSystemPrompt(key string) bool {
	for _, segment := range strings.FieldsFunc(strings.TrimSpace(key), func(r rune) bool {
		return r == '.' || r == '[' || r == ']'
	}) {
		if strings.TrimSpace(segment) == "system_prompt" {
			return true
		}
	}
	return false
}
