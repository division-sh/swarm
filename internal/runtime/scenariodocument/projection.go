package scenariodocument

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

func projectDocument(value semanticvalue.Value) (CLI, error) {
	fields, err := mapping(value, "top-level", "name", "seed", "vars", "setup", "steps", "derive", "connector_responses", "expect", "invalid")
	if err != nil {
		return CLI{}, err
	}
	var out CLI
	out.Name, err = optionalText(fields, "name", "name", false)
	if err != nil {
		return CLI{}, err
	}
	out.Seed, err = optionalText(fields, "seed", "seed", false)
	if err != nil {
		return CLI{}, err
	}
	out.Vars = map[string]any{}
	if v, ok := fields["vars"]; ok {
		out.Vars, err = object(v, "vars")
		if err != nil {
			return CLI{}, err
		}
	}
	if v, ok := fields["setup"]; ok {
		out.Setup, err = projectSetup(v)
		if err != nil {
			return CLI{}, err
		}
	}
	if v, ok := fields["expect"]; ok {
		out.Expect, err = projectExpect(v)
		if err != nil {
			return CLI{}, err
		}
	}
	if v, ok := fields["invalid"]; ok {
		out.Invalid, err = projectInvalid(v)
		if err != nil {
			return CLI{}, err
		}
	}
	if v, ok := fields["derive"]; ok {
		if _, present := fields["steps"]; present {
			return CLI{}, fmt.Errorf("derive and steps are mutually exclusive")
		}
		out.Derive, err = projectDerive(v, fields, out.Name)
	} else {
		if _, present := fields["connector_responses"]; present {
			return CLI{}, fmt.Errorf("connector_responses requires derive")
		}
		v, present := fields["steps"]
		if !present {
			return CLI{}, fmt.Errorf("steps is required")
		}
		out.Steps, err = projectSteps(v)
	}
	return out, err
}

func projectDerive(value semanticvalue.Value, top map[string]semanticvalue.Value, name string) (*Derive, error) {
	if name == "" {
		return nil, fmt.Errorf("name is required for derive profile identity")
	}
	fields, err := mapping(value, "derive", "flow", "input", "payload")
	if err != nil {
		return nil, err
	}
	flow, err := requiredText(fields, "flow", "derive.flow")
	if err != nil {
		return nil, err
	}
	input, err := requiredText(fields, "input", "derive.input")
	if err != nil {
		return nil, err
	}
	flow = strings.Trim(flow, "/")
	if flow == "" {
		return nil, fmt.Errorf("derive.flow must name an exact flow")
	}
	payload, err := mapping(fields["payload"], "derive.payload", "generate", "set")
	if err != nil {
		return nil, err
	}
	generate, ok := payload["generate"].Bool()
	if !ok || !generate {
		return nil, fmt.Errorf("derive.payload.generate must be true")
	}
	out := &Derive{Name: name, FlowID: flow, Input: input, ConnectorResponses: map[string]any{}}
	if v, ok := payload["set"]; ok {
		out.Set, err = object(v, "derive.payload.set")
		if err != nil {
			return nil, err
		}
	}
	if v, ok := top["connector_responses"]; ok {
		out.ConnectorResponses, err = object(v, "connector_responses")
		if err != nil {
			return nil, err
		}
		for _, member := range v.Members() {
			if member.Name == "" || strings.TrimSpace(member.Name) != member.Name {
				return nil, fmt.Errorf("connector response tool id %q is not canonical", member.Name)
			}
		}
	}
	return out, nil
}

func projectSetup(value semanticvalue.Value) (Setup, error) {
	fields, err := mapping(value, "setup", "entities")
	if err != nil {
		return Setup{}, err
	}
	rows, err := sequence(fields["entities"], "setup.entities", true)
	if err != nil {
		return Setup{}, err
	}
	out := Setup{Entities: make([]SetupEntity, 0, len(rows))}
	seen := map[string]bool{}
	for i, row := range rows {
		item, err := projectSetupEntity(row, fmt.Sprintf("setup.entities[%d]", i))
		if err != nil {
			return Setup{}, err
		}
		if seen[item.Alias] {
			return Setup{}, fmt.Errorf("setup.entities[%d].as %q is duplicated", i, item.Alias)
		}
		seen[item.Alias] = true
		out.Entities = append(out.Entities, item)
	}
	return out, nil
}

func projectSetupEntity(value semanticvalue.Value, label string) (SetupEntity, error) {
	fields, err := mapping(value, label, "as", "type", "flow", "current_state", "fields", "gates")
	if err != nil {
		return SetupEntity{}, err
	}
	var out SetupEntity
	out.Alias, err = requiredText(fields, "as", label+".as")
	if err != nil {
		return out, err
	}
	out.EntityType, err = requiredText(fields, "type", label+".type")
	if err != nil {
		return out, err
	}
	if v, ok := fields["flow"]; ok {
		out.Flow, err = text(v, label+".flow", true)
		if err != nil {
			return out, err
		}
	}
	if v, ok := fields["current_state"]; ok {
		out.CurrentState, err = text(v, label+".current_state", true)
		out.StateSet = true
		if err != nil {
			return out, err
		}
	}
	if v, ok := fields["fields"]; ok {
		out.Fields, err = objectOrExpression(v, label+".fields")
		out.FieldsSet = true
		if err != nil {
			return out, err
		}
	}
	if v, ok := fields["gates"]; ok {
		out.Gates, err = objectOrExpression(v, label+".gates")
		out.GatesSet = true
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

func projectSteps(value semanticvalue.Value) ([]Step, error) {
	rows, err := sequence(value, "steps", true)
	if err != nil {
		return nil, err
	}
	out := make([]Step, 0, len(rows))
	for i, row := range rows {
		step, err := projectStep(row, fmt.Sprintf("steps[%d]", i))
		if err != nil {
			return nil, err
		}
		out = append(out, step)
	}
	return out, nil
}

func projectStep(value semanticvalue.Value, label string) (Step, error) {
	if _, found := value.Lookup("publish"); found {
		return projectPublish(value, label)
	}
	fields, err := mapping(value, label, "mailbox.decide", "mailbox.defer")
	if err != nil {
		return Step{}, err
	}
	if len(fields) != 1 {
		return Step{}, fmt.Errorf("scenario step must contain publish or one mailbox action")
	}
	for action, v := range fields {
		return projectMailbox(v, label, action)
	}
	return Step{}, fmt.Errorf("scenario step is empty")
}

func projectPublish(value semanticvalue.Value, label string) (Step, error) {
	fields, err := mapping(value, "publish step", "publish", "payload", "idempotency_key", "emitter", "source_event_id", "target", "target_flow_instance", "target_entity_id")
	if err != nil {
		return Step{}, err
	}
	name, err := requiredText(fields, "publish", label+".publish")
	if err != nil {
		return Step{}, err
	}
	out := Step{Action: "publish", PublishEvent: name}
	v, present := fields["payload"]
	if !present {
		return Step{}, fmt.Errorf("payload is required")
	}
	out.Payload, out.GeneratePayload, err = projectPayload(v)
	if err != nil {
		return Step{}, err
	}
	for _, key := range []string{"idempotency_key", "emitter", "source_event_id", "target", "target_flow_instance", "target_entity_id"} {
		if v, ok := fields[key]; ok {
			if _, err := text(v, label+"."+key, true); err != nil {
				return Step{}, err
			}
		}
	}
	_, target := fields["target"]
	_, flow := fields["target_flow_instance"]
	_, entity := fields["target_entity_id"]
	if target && (flow || entity) {
		return Step{}, fmt.Errorf("publish step target cannot be combined with target_flow_instance or target_entity_id")
	}
	if flow != entity {
		return Step{}, fmt.Errorf("target_flow_instance and target_entity_id must be supplied together")
	}
	out.IdempotencyKey = optionalLiteral(fields, "idempotency_key")
	out.Emitter = optionalLiteral(fields, "emitter")
	out.SourceEventID = optionalLiteral(fields, "source_event_id")
	out.Target = optionalLiteral(fields, "target")
	out.TargetFlowInstance = optionalLiteral(fields, "target_flow_instance")
	out.TargetEntityID = optionalLiteral(fields, "target_entity_id")
	return out, nil
}

func optionalLiteral(fields map[string]semanticvalue.Value, key string) any {
	if value, ok := fields[key]; ok {
		return literal(value)
	}
	return nil
}

func projectPayload(value semanticvalue.Value) (any, bool, error) {
	if name, ok := value.String(); ok && name == "generate" {
		return nil, true, nil
	}
	if expression(value) {
		return literal(value), false, nil
	}
	fields, err := object(value, "payload")
	if err != nil {
		return nil, false, fmt.Errorf("publish payload scalar must be exactly \"generate\" or a CEL expression that evaluates to an object: %w", err)
	}
	if value.Len() == 1 {
		if _, found := value.Lookup("generate"); found {
			return nil, false, fmt.Errorf("publish payload generation uses scalar payload: generate; mapping sentinels are unsupported")
		}
	}
	if v, found := value.Lookup("from"); found {
		if _, err := text(v, "payload.from", true); err != nil {
			return nil, false, err
		}
		for _, member := range value.Members() {
			if member.Name != "from" && member.Name != "set" {
				return nil, false, fmt.Errorf("fixture payload may contain only from and set, not %q", member.Name)
			}
		}
	}
	if v, found := value.Lookup("set"); found {
		set, err := object(v, "payload.set")
		if err != nil {
			return nil, false, err
		}
		if err := ValidateSetPaths(set); err != nil {
			return nil, false, err
		}
	}
	return fields, false, nil
}

func projectMailbox(value semanticvalue.Value, label, action string) (Step, error) {
	allowed := []string{"match", "idempotency_key", "until"}
	if action == "mailbox.decide" {
		allowed = []string{"match", "idempotency_key", "verdict", "fields"}
	}
	fields, err := mapping(value, action+" step", allowed...)
	if err != nil {
		return Step{}, err
	}
	match, err := mapping(fields["match"], action+".match", "anchor_kind", "activity_id", "card_id", "category", "decision", "entity_id", "flow_instance", "request_event_id", "requester_agent_id", "scope", "stage")
	if err != nil {
		return Step{}, err
	}
	if _, err := requiredText(match, "anchor_kind", action+".match.anchor_kind"); err != nil {
		return Step{}, err
	}
	for _, member := range fields["match"].Members() {
		if _, err := text(member.Value, action+".match."+member.Name, true); err != nil {
			return Step{}, err
		}
	}
	out := Step{Action: action, Match: literal(fields["match"]).(map[string]any)}
	key := "until"
	if action == "mailbox.decide" {
		key = "verdict"
	}
	if _, err := requiredText(fields, key, action+"."+key); err != nil {
		return Step{}, err
	}
	out.Verdict = optionalLiteral(fields, "verdict")
	out.Until = optionalLiteral(fields, "until")
	if v, found := fields["fields"]; found {
		out.Fields, err = objectOrExpression(v, label+".fields")
		if err != nil {
			return Step{}, err
		}
	}
	if v, found := fields["idempotency_key"]; found {
		out.IdempotencyKey, err = text(v, label+".idempotency_key", true)
		if err != nil {
			return Step{}, err
		}
	}
	return out, nil
}

func projectExpect(value semanticvalue.Value) (Expect, error) {
	fields, err := mapping(value, "expect", "events", "no_dead_letters", "entities")
	if err != nil {
		return Expect{}, err
	}
	var out Expect
	if v, ok := fields["events"]; ok {
		out.Events, err = projectEventExpect(v)
		if err != nil {
			return out, err
		}
	}
	if v, ok := fields["no_dead_letters"]; ok {
		b, valid := v.Bool()
		if !valid {
			return out, fmt.Errorf("expect.no_dead_letters must be boolean")
		}
		out.NoDeadLetters = &b
	}
	if v, ok := fields["entities"]; ok {
		out.Entities, err = projectEntityExpectations(v)
	}
	return out, err
}

func textList(value semanticvalue.Value, label string) ([]string, error) {
	items, err := sequence(value, label, false)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(items))
	for i, item := range items {
		v, err := text(item, fmt.Sprintf("%s[%d]", label, i), true)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func projectEventExpect(value semanticvalue.Value) (EventExpect, error) {
	if value.Kind() == semanticvalue.KindArray {
		include, err := textList(value, "expect.events")
		return EventExpect{Include: include}, err
	}
	fields, err := mapping(value, "expect.events", "include", "exact", "ordered")
	if err != nil {
		return EventExpect{}, err
	}
	var out EventExpect
	for _, key := range []string{"include", "exact", "ordered"} {
		if v, ok := fields[key]; ok {
			list, err := textList(v, "expect.events."+key)
			if err != nil {
				return out, err
			}
			switch key {
			case "include":
				out.Include = list
			case "exact":
				out.Exact = list
			case "ordered":
				out.Ordered = list
			}
		}
	}
	return out, nil
}

func projectEntityExpectations(value semanticvalue.Value) ([]EntityExpect, error) {
	items, err := sequence(value, "expect.entities", false)
	if err != nil {
		return nil, err
	}
	out := make([]EntityExpect, 0, len(items))
	for i, value := range items {
		item, err := projectEntityExpect(value, fmt.Sprintf("expect.entities[%d]", i))
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, nil
}

func projectEntityExpect(value semanticvalue.Value, label string) (EntityExpect, error) {
	fields, err := mapping(value, label, "ref", "type", "count", "current_state", "fields", "gates")
	if err != nil {
		return EntityExpect{}, err
	}
	var out EntityExpect
	out.Ref, err = optionalText(fields, "ref", label+".ref", true)
	if err != nil {
		return out, err
	}
	out.EntityType, err = optionalText(fields, "type", label+".type", true)
	if err != nil {
		return out, err
	}
	if out.EntityType == "" && out.Ref == "" {
		return out, fmt.Errorf("%s.type is required", label)
	}
	if v, ok := fields["count"]; ok {
		n, valid := literal(v).(int64)
		if !valid || n < 0 || int64(int(n)) != n {
			return out, fmt.Errorf("%s.count must be a non-negative integer", label)
		}
		count := int(n)
		out.Count = &count
	}
	if v, ok := fields["current_state"]; ok {
		out.CurrentState, err = text(v, label+".current_state", true)
		out.StateSet = true
		if err != nil {
			return out, err
		}
	}
	if v, ok := fields["fields"]; ok {
		out.Fields, err = objectOrExpression(v, label+".fields")
		out.FieldsSet = true
		if err != nil {
			return out, err
		}
	}
	if v, ok := fields["gates"]; ok {
		out.Gates, err = objectOrExpression(v, label+".gates")
		out.GatesSet = true
		if err != nil {
			return out, err
		}
	}
	if out.Count != nil && out.Ref != "" {
		return out, fmt.Errorf("%s.count cannot be combined with ref", label)
	}
	if out.Count != nil && out.HasDetailAssertion() {
		return out, fmt.Errorf("%s.count cannot be combined with current_state, fields, or gates", label)
	}
	if out.Count == nil && !out.HasDetailAssertion() {
		return out, fmt.Errorf("%s requires count, current_state, fields, or gates", label)
	}
	return out, nil
}

func projectInvalid(value semanticvalue.Value) (*Invalid, error) {
	fields, err := mapping(value, "invalid", "base", "cases")
	if err != nil {
		return nil, err
	}
	base, err := projectPublish(fields["base"], "invalid.base")
	if err != nil {
		return nil, err
	}
	baseFields, err := mapping(fields["base"], "invalid.base", "publish", "payload")
	if err != nil {
		return nil, err
	}
	if base.GeneratePayload {
		return nil, fmt.Errorf("generated invalid-case bases are not supported; author an explicit payload before applying invalid.set overrides")
	}
	items, err := sequence(fields["cases"], "invalid.cases", true)
	if err != nil {
		return nil, err
	}
	out := &Invalid{Base: literalObject(baseFields), Cases: make([]InvalidCase, 0, len(items))}
	for i, value := range items {
		label := fmt.Sprintf("invalid.cases[%d]", i)
		if _, present := value.Lookup("expect"); present {
			return nil, fmt.Errorf("%s: remove `expect`; invalid cases assert pre-mutation payload-schema rejection", label)
		}
		fields, err := mapping(value, label, "name", "set")
		if err != nil {
			return nil, err
		}
		name, err := optionalText(fields, "name", label+".name", false)
		if err != nil {
			return nil, err
		}
		if name == "" {
			name = fmt.Sprintf("case-%d", i+1)
		}
		var set map[string]any
		if v, ok := fields["set"]; ok {
			set, err = object(v, label+".set")
			if err != nil {
				return nil, err
			}
			if err := ValidateSetPaths(set); err != nil {
				return nil, fmt.Errorf("%s: %w", label, err)
			}
		}
		out.Cases = append(out.Cases, InvalidCase{Name: name, Set: set})
	}
	return out, nil
}

func literalObject(fields map[string]semanticvalue.Value) map[string]any {
	out := make(map[string]any, len(fields))
	for key, value := range fields {
		out[key] = literal(value)
	}
	return out
}
