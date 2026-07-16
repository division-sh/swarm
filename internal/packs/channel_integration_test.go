package packs_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

func TestChannelSchemaAdmissionRejectsRecursiveMalformedSchemasAtEveryTypedBoundary(t *testing.T) {
	tests := []struct {
		name   string
		schema runtimecontracts.ToolInputSchema
		want   string
	}{
		{name: "scalar regex", schema: runtimecontracts.ToolInputSchema{Type: "string", Pattern: "["}, want: "pattern is invalid"},
		{name: "object required", schema: runtimecontracts.ToolInputSchema{Type: "object", Required: []string{"missing"}}, want: "is not declared"},
		{name: "array items", schema: runtimecontracts.ToolInputSchema{Type: "array"}, want: "array requires items"},
		{name: "typed enum", schema: runtimecontracts.ToolInputSchema{Type: "integer", Enum: []runtimecontracts.SchemaLiteral{{Node: yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "one"}}}}, want: "must be integer"},
		{name: "additional properties schema", schema: runtimecontracts.ToolInputSchema{Type: "object", AdditionalProperties: runtimecontracts.ToolAdditionalProperties{Schema: &runtimecontracts.ToolInputSchema{Type: "array"}}}, want: "array requires items"},
	}
	boundaries := []struct {
		name  string
		admit func(*testing.T, runtimecontracts.ToolInputSchema) error
	}{
		{name: "yaml", admit: func(_ *testing.T, schema runtimecontracts.ToolInputSchema) error {
			raw, err := yaml.Marshal(schema)
			if err != nil {
				return err
			}
			var decoded runtimecontracts.ToolInputSchema
			return yaml.Unmarshal(raw, &decoded)
		}},
		{name: "interface", admit: func(t *testing.T, schema runtimecontracts.ToolInputSchema) error {
			spec := loadChannelPlatformSpec(t)
			versions := spec.Interfaces["swarm.hitl-channel"]
			definition := versions["v1"]
			definition.Schemas["presentation"] = schema
			versions["v1"] = definition
			spec.Interfaces["swarm.hitl-channel"] = versions
			_, err := packs.NewInterfaceRegistry(spec)
			return err
		}},
		{name: "trigger", admit: func(t *testing.T, schema runtimecontracts.ToolInputSchema) error {
			registry, channel, trigger, connector := loadTelegramChannelCompilerInputs(t)
			event := trigger.Events["inbound.telegram.text_message"]
			field := event.Fields["external_account_reference"]
			field.Schema = schema
			event.Fields["external_account_reference"] = field
			trigger.Events["inbound.telegram.text_message"] = event
			_, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
			return err
		}},
		{name: "channel", admit: func(t *testing.T, schema runtimecontracts.ToolInputSchema) error {
			registry, channel, trigger, connector := loadTelegramChannelCompilerInputs(t)
			channel.Manifest.OpaqueTypes["external_account_reference"] = schema
			_, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
			return err
		}},
		{name: "connector", admit: func(t *testing.T, schema runtimecontracts.ToolInputSchema) error {
			registry, channel, trigger, connector := loadTelegramChannelCompilerInputs(t)
			tool := connector.Tools["telegram.send_interactive"]
			tool.InputSchema = schema
			connector.Tools["telegram.send_interactive"] = tool
			_, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
			return err
		}},
	}
	for _, tc := range tests {
		for _, boundary := range boundaries {
			t.Run(tc.name+"/"+boundary.name, func(t *testing.T) {
				if err := boundary.admit(t, runtimecontracts.CloneToolInputSchema(tc.schema)); err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("admission error = %v, want %q", err, tc.want)
				}
			})
		}
	}
}

func TestChannelCompilerPreservesExactEnumAndPinsItInGeneration(t *testing.T) {
	var exact runtimecontracts.ToolInputSchema
	if err := yaml.Unmarshal([]byte(`
type: string
minLength: 1
pattern: ' approved $'
enum: [' approved ']
`), &exact); err != nil {
		t.Fatalf("decode exact schema: %v", err)
	}
	compile := func(t *testing.T, schema runtimecontracts.ToolInputSchema) packs.SatisfactionPlan {
		registry, channel, trigger, connector := loadTelegramChannelCompilerInputs(t)
		channel.Manifest.OpaqueTypes["external_account_reference"] = schema
		for _, eventName := range []string{"inbound.telegram.text_message", "inbound.telegram.callback_action"} {
			event := trigger.Events[eventName]
			field := event.Fields["external_account_reference"]
			field.Schema = schema
			event.Fields["external_account_reference"] = field
			trigger.Events[eventName] = event
		}
		plan, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
		if err != nil {
			t.Fatalf("CompileChannel: %v", err)
		}
		return plan
	}
	original := compile(t, exact)
	for _, eventName := range []string{"text", "action"} {
		eventSchema := original.Events[eventName].Descriptor.Fields["external_account_reference"].Schema
		if eventSchema.Pattern != " approved $" || channelSchemaEnumText(t, eventSchema) != " approved " {
			t.Fatalf("%s exact schema = %#v", eventName, eventSchema)
		}
	}
	originalGeneration, err := original.GenerationID()
	if err != nil {
		t.Fatalf("original GenerationID: %v", err)
	}
	changed := runtimecontracts.CloneToolInputSchema(exact)
	channelSchemaEnumScalar(t, &changed.Enum[0].Node).Value = " accepted "
	changed.Pattern = " accepted $"
	changedGeneration, err := compile(t, changed).GenerationID()
	if err != nil {
		t.Fatalf("changed GenerationID: %v", err)
	}
	if changedGeneration == originalGeneration {
		t.Fatal("exact enum/pattern change did not change compiled generation")
	}
}

func channelSchemaEnumText(t *testing.T, schema runtimecontracts.ToolInputSchema) string {
	t.Helper()
	var value string
	if err := schema.Enum[0].Node.Decode(&value); err != nil {
		t.Fatalf("decode schema enum: %v", err)
	}
	return value
}

func channelSchemaEnumScalar(t *testing.T, node *yaml.Node) *yaml.Node {
	t.Helper()
	if node == nil {
		t.Fatal("schema enum node is nil")
	}
	if node.Kind == yaml.ScalarNode {
		return node
	}
	for _, child := range node.Content {
		if child != nil {
			return channelSchemaEnumScalar(t, child)
		}
	}
	if node.Alias != nil {
		return channelSchemaEnumScalar(t, node.Alias)
	}
	t.Fatalf("schema enum node kind %d has no scalar", node.Kind)
	return nil
}

func TestTelegramChannelPackCompilesThroughAcceptedProductionInventories(t *testing.T) {
	plan := loadTelegramChannelPlan(t)

	wantMax := map[string]int{
		"presentation.text": 4096,
		"actions":           8,
		"actions[].label":   64,
		"actions[].token":   64,
	}
	for name, want := range wantMax {
		schema, ok := plan.Constraints[name]
		if !ok {
			t.Fatalf("constraint %q missing from %#v", name, plan.Constraints)
		}
		got := schema.MaxLength
		if name == "actions" {
			got = schema.MaxItems
		}
		if got == nil || *got != want {
			t.Fatalf("constraint %q max = %v, want %d", name, got, want)
		}
	}

	prepared, err := plan.PrepareOperationInput("deliver", map[string]any{
		"presentation": map[string]any{"text": "Review launch"},
		"actions": []any{
			map[string]any{"label": "Approve", "token": "approve_1"},
			map[string]any{"label": "Reject", "token": "reject_1"},
		},
	}, map[string]any{"destination": "-100123"})
	if err != nil {
		t.Fatalf("PrepareOperationInput(deliver): %v", err)
	}
	keyboard := prepared["reply_markup"].(map[string]any)["inline_keyboard"].([]any)
	if len(keyboard) != 2 {
		t.Fatalf("inline keyboard = %#v, want two ordered rows", keyboard)
	}

	cleared, err := plan.PrepareOperationInput("edit", map[string]any{
		"delivery_reference": map[string]any{"id": float64(42)},
		"presentation":       map[string]any{"text": "Already decided"},
		"actions":            []any{},
	}, map[string]any{"destination": "-100123"})
	if err != nil {
		t.Fatalf("PrepareOperationInput(edit clear): %v", err)
	}
	clearedKeyboard := cleared["reply_markup"].(map[string]any)["inline_keyboard"].([]any)
	if len(clearedKeyboard) != 0 {
		t.Fatalf("cleared inline keyboard = %#v, want empty", clearedKeyboard)
	}
	if got := cleared["message_id"]; got != float64(42) {
		t.Fatalf("projected message id = %#v, want bounded integer identity", got)
	}

	if _, err := packs.NewOutboundBindingPlan("telegram_ops", plan, "not-a-chat", nil); err == nil {
		t.Fatal("NewOutboundBindingPlan accepted invalid provider-owned destination")
	}
	binding, err := packs.NewOutboundBindingPlan("telegram_ops", plan, "-100123", nil)
	if err != nil {
		t.Fatalf("NewOutboundBindingPlan: %v", err)
	}
	if subject, err := plan.CapabilitySubject(); err != nil || subject.Kind != packs.SubjectChannelPack || subject.Status != packs.StatusAvailable {
		t.Fatalf("channel pack subject = %#v, err=%v", subject, err)
	}
	if subject, err := binding.CapabilitySubject(); err != nil || subject.Kind != packs.SubjectChannelOutbound || subject.Status != packs.StatusReady {
		t.Fatalf("channel outbound subject = %#v, err=%v", subject, err)
	}
}

func TestTelegramChannelRejectsOutOfRangeDeliveryReferenceBeforeConnectorProjection(t *testing.T) {
	plan := loadTelegramChannelPlan(t)
	if _, err := plan.PrepareOperationInput("edit", map[string]any{
		"delivery_reference": map[string]any{"id": float64(2147483648)},
		"presentation":       map[string]any{"text": "Outside Telegram message id range"},
		"actions":            []any{},
	}, map[string]any{"destination": "-100123"}); err == nil || !strings.Contains(err.Error(), "must be <=") {
		t.Fatalf("PrepareOperationInput accepted out-of-range delivery reference: %v", err)
	}
}

func TestTelegramChannelCompilerRejectsUnboundedMessageIDOutput(t *testing.T) {
	registry, channel, trigger, connector := loadTelegramChannelCompilerInputs(t)
	tool := connector.Tools["telegram.send_interactive"]
	allow := false
	minimum := float64(1)
	tool.OutputSchema = runtimecontracts.ToolInputSchema{
		Type: "object", Required: []string{"message_id"},
		AdditionalProperties: runtimecontracts.ToolAdditionalProperties{Allowed: &allow},
		Properties: map[string]runtimecontracts.ToolInputSchema{
			"message_id": {Type: "integer", Minimum: &minimum},
		},
	}
	connector.Tools["telegram.send_interactive"] = tool

	_, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err == nil || !strings.Contains(err.Error(), "source maximum is broader than target maximum") {
		t.Fatalf("CompileChannel error = %v, want unbounded provider result rejection", err)
	}
}

func TestTelegramChannelCompilerConsumesExactNormalizedIdentifierSchemas(t *testing.T) {
	tests := []struct {
		name  string
		event string
		field string
		widen func(runtimecontracts.ToolInputSchema) runtimecontracts.ToolInputSchema
		want  string
	}{
		{name: "text message", event: "inbound.telegram.text_message", field: "provider_message_reference", widen: withoutMaximum, want: "source maximum is broader"},
		{name: "text account", event: "inbound.telegram.text_message", field: "external_account_reference", widen: withoutPattern, want: "not provably assignable"},
		{name: "text conversation", event: "inbound.telegram.text_message", field: "conversation_reference", widen: withoutMaximumLength, want: "source maximum is broader"},
		{name: "callback message", event: "inbound.telegram.callback_action", field: "provider_message_reference", widen: withoutMaximum, want: "source maximum is broader"},
		{name: "callback account", event: "inbound.telegram.callback_action", field: "external_account_reference", widen: withoutPattern, want: "not provably assignable"},
		{name: "callback conversation", event: "inbound.telegram.callback_action", field: "conversation_reference", widen: withoutMaximumLength, want: "source maximum is broader"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			registry, channel, trigger, connector := loadTelegramChannelCompilerInputs(t)
			event := trigger.Events[tc.event]
			field := event.Fields[tc.field]
			field.Schema = tc.widen(field.Schema)
			event.Fields[tc.field] = field
			trigger.Events[tc.event] = event
			_, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CompileChannel error = %v, want %q", err, tc.want)
			}
		})
	}
}

func withoutMaximum(schema runtimecontracts.ToolInputSchema) runtimecontracts.ToolInputSchema {
	schema.Maximum = nil
	return schema
}

func withoutPattern(schema runtimecontracts.ToolInputSchema) runtimecontracts.ToolInputSchema {
	schema.Pattern = ""
	return schema
}

func withoutMaximumLength(schema runtimecontracts.ToolInputSchema) runtimecontracts.ToolInputSchema {
	schema.MaxLength = nil
	return schema
}

func TestProductionCompilerAcceptsStructurallyDifferentTighterSatisfier(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	channel, trigger, connector := mockChannelSatisfier()
	plan, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err != nil {
		t.Fatalf("CompileChannel(mock): %v", err)
	}
	wantMax := map[string]int{
		"presentation.text": 128,
		"actions":           2,
		"actions[].label":   24,
		"actions[].token":   20,
	}
	for name, want := range wantMax {
		schema := plan.Constraints[name]
		got := schema.MaxLength
		if name == "actions" {
			got = schema.MaxItems
		}
		if got == nil || *got != want {
			t.Fatalf("mock constraint %q max = %v, want %d", name, got, want)
		}
	}
	if _, err := packs.NewOutboundBindingPlan("mock_ops", plan, "queue-a", nil); err == nil {
		t.Fatal("mock binding accepted Telegram-shaped scalar destination")
	}
	binding, err := packs.NewOutboundBindingPlan("mock_ops", plan, map[string]any{"queue": "queue-a"}, nil)
	if err != nil {
		t.Fatalf("NewOutboundBindingPlan(mock): %v", err)
	}
	_, prepared, err := binding.PrepareOperation("acknowledge_interaction", map[string]any{
		"interaction_reference": map[string]any{"cursor": "cursor-a"},
	})
	if err != nil {
		t.Fatalf("PrepareOperation(mock acknowledge): %v", err)
	}
	if _, hasDestination := prepared["destination"]; hasDestination {
		t.Fatalf("acknowledgment gained ambient destination context: %#v", prepared)
	}
}

func TestProductionCompilerFailsClosedAcrossChannelContractPhases(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	tests := []struct {
		name   string
		mutate func(*packs.LoadedChannelPack, *packs.TriggerPackDescriptor, *packs.ConnectorPackDescriptor)
		want   string
	}{
		{
			name: "incomplete operation surface",
			mutate: func(channel *packs.LoadedChannelPack, _ *packs.TriggerPackDescriptor, _ *packs.ConnectorPackDescriptor) {
				delete(channel.Manifest.Operations, "edit")
			},
			want: "channel operations",
		},
		{
			name: "unknown operation",
			mutate: func(channel *packs.LoadedChannelPack, _ *packs.TriggerPackDescriptor, _ *packs.ConnectorPackDescriptor) {
				channel.Manifest.Operations["delete"] = packs.ChannelOperationBinding{Tool: "mock.edit"}
			},
			want: "channel operations",
		},
		{
			name: "missing opaque slot",
			mutate: func(channel *packs.LoadedChannelPack, _ *packs.TriggerPackDescriptor, _ *packs.ConnectorPackDescriptor) {
				delete(channel.Manifest.OpaqueTypes, "conversation_reference")
			},
			want: "channel opaque_types",
		},
		{
			name: "effect mismatch",
			mutate: func(_ *packs.LoadedChannelPack, _ *packs.TriggerPackDescriptor, connector *packs.ConnectorPackDescriptor) {
				tool := connector.Tools["mock.deliver"]
				tool.EffectClass = string(runtimecontracts.ActivityEffectClassReadOnly)
				connector.Tools["mock.deliver"] = tool
			},
			want: "effect class does not match",
		},
		{
			name: "unconsumed interface input",
			mutate: func(channel *packs.LoadedChannelPack, _ *packs.TriggerPackDescriptor, _ *packs.ConnectorPackDescriptor) {
				delete(channel.Manifest.Operations["deliver"].Input, "body")
			},
			want: `selected channel constraint "presentation.text" is not mapped`,
		},
		{
			name: "incompatible selected patterns",
			mutate: func(_ *packs.LoadedChannelPack, _ *packs.TriggerPackDescriptor, connector *packs.ConnectorPackDescriptor) {
				tool := connector.Tools["mock.edit"]
				controls := mockArraySchema(0, 2, mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
					"name": mockStringSchema(1, 24, ""), "value": mockStringSchema(1, 20, `^[A-Z]+$`),
				}, "name", "value"))
				tool.InputSchema.Properties["controls"] = controls
				connector.Tools["mock.edit"] = tool
			},
			want: "incompatible patterns",
		},
		{
			name: "missing finite text maximum",
			mutate: func(_ *packs.LoadedChannelPack, _ *packs.TriggerPackDescriptor, connector *packs.ConnectorPackDescriptor) {
				for _, name := range []string{"mock.deliver", "mock.edit"} {
					tool := connector.Tools[name]
					body := tool.InputSchema.Properties["body"]
					body.MaxLength = nil
					tool.InputSchema.Properties["body"] = body
					connector.Tools[name] = tool
				}
			},
			want: "finite maxLength",
		},
		{
			name: "event field type mismatch",
			mutate: func(_ *packs.LoadedChannelPack, trigger *packs.TriggerPackDescriptor, _ *packs.ConnectorPackDescriptor) {
				event := trigger.Events["mock.text"]
				event.Fields["text"] = packs.TriggerEventField{Schema: runtimecontracts.ToolInputSchema{Type: "integer"}, Required: true}
				trigger.Events["mock.text"] = event
			},
			want: "incompatible types",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			channel, trigger, connector := mockChannelSatisfier()
			tc.mutate(&channel, &trigger, &connector)
			_, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CompileChannel error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestProductionCompilerRejectsPartialRequiredConnectorObject(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	channel, trigger, connector := mockChannelSatisfier()
	tool := connector.Tools["mock.deliver"]
	destination := tool.InputSchema.Properties["destination"]
	destination.Properties = map[string]runtimecontracts.ToolInputSchema{"queue": destination.Properties["queue"]}
	destination.Required = append([]string(nil), destination.Required...)
	destination.Properties["region"] = mockStringSchema(1, 10, "")
	destination.Required = append(destination.Required, "region")
	tool.InputSchema.Properties["destination"] = destination
	connector.Tools["mock.deliver"] = tool

	_, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err == nil || !strings.Contains(err.Error(), `required path "destination.region" is covered 0 times`) {
		t.Fatalf("CompileChannel error = %v, want missing destination.region", err)
	}
}

func TestProductionCompilerRejectsDroppedRequiredInterfaceArrayLeaf(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	channel, trigger, connector := mockChannelSatisfier()
	tool := connector.Tools["mock.deliver"]
	controls := tool.InputSchema.Properties["controls"]
	controls.Items = pointerToChannelSchema(mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
		"name": mockStringSchema(1, 24, ""),
	}, "name"))
	tool.InputSchema.Properties["controls"] = controls
	connector.Tools["mock.deliver"] = tool
	binding := channel.Manifest.Operations["deliver"]
	binding.Input["controls"] = packs.ChannelMapping{
		Each: "input.actions",
		Item: []map[string]packs.ChannelMapping{{"name": {From: "item.label"}}},
	}
	channel.Manifest.Operations["deliver"] = binding

	_, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err == nil || !strings.Contains(err.Error(), `selected channel constraint "actions[].token" is not mapped`) {
		t.Fatalf("CompileChannel error = %v, want unconsumed actions token", err)
	}
}

func TestProductionCompilerRejectsRecursiveDirectionalAndCardinalityGaps(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	tests := []struct {
		name   string
		mutate func(*packs.LoadedChannelPack, *packs.ConnectorPackDescriptor)
		want   string
	}{
		{
			name: "input scalar source broader than target",
			mutate: func(channel *packs.LoadedChannelPack, connector *packs.ConnectorPackDescriptor) {
				destination := connector.Tools["mock.deliver"].InputSchema.Properties["destination"]
				queue := destination.Properties["queue"]
				max := 5
				queue.MaxLength = &max
				destination.Properties["queue"] = queue
				tool := connector.Tools["mock.deliver"]
				tool.InputSchema.Properties["destination"] = destination
				connector.Tools["mock.deliver"] = tool
				channel.Manifest.OpaqueTypes["destination"] = mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
					"queue": mockStringSchema(1, 10, `^[a-z0-9-]+$`),
				}, "queue")
			},
			want: "source maximum is broader than target maximum 5",
		},
		{
			name: "whole input object misses required target child",
			mutate: func(channel *packs.LoadedChannelPack, connector *packs.ConnectorPackDescriptor) {
				tool := connector.Tools["mock.deliver"]
				destination := tool.InputSchema.Properties["destination"]
				destination.Properties["region"] = mockStringSchema(1, 10, "")
				destination.Required = append(destination.Required, "region")
				tool.InputSchema.Properties["destination"] = destination
				connector.Tools["mock.deliver"] = tool
				channel.Manifest.OpaqueTypes["destination"] = mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
					"queue": mockStringSchema(1, 10, `^[a-z0-9-]+$`),
				}, "queue")
				binding := channel.Manifest.Operations["deliver"]
				delete(binding.Input, "destination.queue")
				binding.Input["destination"] = packs.ChannelMapping{From: "context.destination"}
				channel.Manifest.Operations["deliver"] = binding
			},
			want: `target requires property "region" that source does not require`,
		},
		{
			name: "output scalar source broader than target",
			mutate: func(_ *packs.LoadedChannelPack, connector *packs.ConnectorPackDescriptor) {
				tool := connector.Tools["mock.deliver"]
				ref := tool.OutputSchema.Properties["ref"]
				min, max := 1, 100
				ref.MinLength, ref.MaxLength = &min, &max
				tool.OutputSchema.Properties["ref"] = ref
				connector.Tools["mock.deliver"] = tool
			},
			want: "source minimum is broader than target minimum 22",
		},
		{
			name: "whole output object misses required target child",
			mutate: func(channel *packs.LoadedChannelPack, connector *packs.ConnectorPackDescriptor) {
				receipt := channel.Manifest.OpaqueTypes["delivery_receipt"]
				receipt.Properties["status"] = mockStringSchema(1, 12, "")
				receipt.Required = append(receipt.Required, "status")
				channel.Manifest.OpaqueTypes["delivery_receipt"] = receipt
				tool := connector.Tools["mock.edit"]
				tool.OutputSchema = mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
					"receipt": mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"revision": {Type: "integer"}}, "revision"),
				}, "receipt")
				connector.Tools["mock.edit"] = tool
				binding := channel.Manifest.Operations["edit"]
				delete(binding.Output, "delivery_receipt.revision")
				binding.Output["delivery_receipt"] = packs.ChannelMapping{From: "result.receipt"}
				channel.Manifest.Operations["edit"] = binding
			},
			want: `target requires property "status" that source does not require`,
		},
		{
			name: "ancestor and descendant target overlap",
			mutate: func(channel *packs.LoadedChannelPack, _ *packs.ConnectorPackDescriptor) {
				binding := channel.Manifest.Operations["deliver"]
				binding.Input["destination"] = packs.ChannelMapping{From: "context.destination"}
				channel.Manifest.Operations["deliver"] = binding
			},
			want: `target path "destination.queue" overlaps "destination"`,
		},
		{
			name: "duplicate recursive item source",
			mutate: func(channel *packs.LoadedChannelPack, connector *packs.ConnectorPackDescriptor) {
				tool := connector.Tools["mock.deliver"]
				controls := tool.InputSchema.Properties["controls"]
				controls.Items.Properties["alias"] = mockStringSchema(1, 24, "")
				controls.Items.Required = append(controls.Items.Required, "alias")
				tool.InputSchema.Properties["controls"] = controls
				connector.Tools["mock.deliver"] = tool
				binding := channel.Manifest.Operations["deliver"]
				mapping := binding.Input["controls"]
				mapping.Item[0]["alias"] = packs.ChannelMapping{From: "item.label"}
				binding.Input["controls"] = mapping
				channel.Manifest.Operations["deliver"] = binding
			},
			want: `item source path "label" overlaps "label"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			channel, trigger, connector := mockChannelSatisfier()
			tc.mutate(&channel, &connector)
			_, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CompileChannel error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestChannelRuntimeToolsExposeOnlyProviderNeutralContract(t *testing.T) {
	plan := loadTelegramChannelPlan(t)
	binding, err := packs.NewOutboundBindingPlan("ops", plan, "42", nil)
	if err != nil {
		t.Fatalf("NewOutboundBindingPlan: %v", err)
	}
	tools, err := binding.RuntimeTools()
	if err != nil {
		t.Fatalf("RuntimeTools: %v", err)
	}
	tool := tools["channel.ops.deliver"]
	if tool.Category != "channel_operation" || tool.HandlerType != "channel" || tool.HTTP != nil || len(tool.Credentials) != 0 || tool.ManagedCredential != nil {
		t.Fatalf("public channel tool leaked connector execution details: %#v", tool)
	}
	if _, ok := tool.InputSchema.Properties["presentation"]; !ok {
		t.Fatalf("public channel input = %#v, want presentation", tool.InputSchema)
	}
	if _, ok := tool.InputSchema.Properties["chat_id"]; ok {
		t.Fatalf("public channel input exposed connector destination: %#v", tool.InputSchema)
	}
	activityTool, err := binding.Structural.OperationTool("deliver")
	if err != nil {
		t.Fatalf("OperationTool: %v", err)
	}
	activityToolID, generation, err := binding.RuntimeActivityTarget("deliver")
	if err != nil {
		t.Fatalf("RuntimeActivityTarget: %v", err)
	}
	if activityToolID == binding.RuntimeToolID("deliver") || generation == "" || activityTool.HTTP == nil || activityTool.CompiledResult == nil {
		t.Fatalf("private channel activity target is not separated: id=%q generation=%q tool=%#v", activityToolID, generation, activityTool)
	}
}

func TestSatisfactionPlanCloneDeeplyIsolatesRuntimeOperation(t *testing.T) {
	original := loadTelegramChannelPlan(t)
	cloned := original.Clone()
	op := cloned.Operations["deliver"]
	text := op.ToolSchema.InputSchema.Properties["text"]
	if text.MaxLength == nil {
		t.Fatal("Telegram text maxLength missing")
	}
	*text.MaxLength = 7
	op.ToolSchema.InputSchema.Properties["text"] = text
	httpSpec := *op.ToolSchema.HTTP
	httpSpec.URL = "https://mutated.invalid"
	op.ToolSchema.HTTP = &httpSpec
	keyboard := op.Input["reply_markup.inline_keyboard"]
	keyboard.Item[0]["text"] = packs.ChannelMapping{From: "item.token"}
	op.Input["reply_markup.inline_keyboard"] = keyboard
	cloned.Operations["deliver"] = op

	originalOp := original.Operations["deliver"]
	if got := *originalOp.ToolSchema.InputSchema.Properties["text"].MaxLength; got == 7 {
		t.Fatalf("clone mutation changed original text maxLength to %d", got)
	}
	if originalOp.ToolSchema.HTTP.URL == "https://mutated.invalid" {
		t.Fatal("clone mutation changed original HTTP URL")
	}
	if got := originalOp.Input["reply_markup.inline_keyboard"].Item[0]["text"].From; got != "item.label" {
		t.Fatalf("clone mutation changed original item mapping to %q", got)
	}
}

func TestRuntimeActivityTargetPinsCompleteCompiledPlanGeneration(t *testing.T) {
	original := loadTelegramChannelPlan(t)
	cloned := original.Clone()
	originalGeneration, err := original.GenerationID()
	if err != nil {
		t.Fatalf("original GenerationID: %v", err)
	}
	cloneGeneration, err := cloned.GenerationID()
	if err != nil {
		t.Fatalf("clone GenerationID: %v", err)
	}
	if cloneGeneration != originalGeneration {
		t.Fatalf("clone generation = %q, want %q", cloneGeneration, originalGeneration)
	}

	operation := cloned.Operations["deliver"]
	text := operation.ToolSchema.InputSchema.Properties["text"]
	if text.MaxLength == nil || *text.MaxLength < 2 {
		t.Fatalf("fixture text schema = %#v", text)
	}
	changedMaximum := *text.MaxLength - 1
	text.MaxLength = &changedMaximum
	operation.ToolSchema.InputSchema.Properties["text"] = text
	cloned.Operations["deliver"] = operation
	changedGeneration, err := cloned.GenerationID()
	if err != nil {
		t.Fatalf("changed GenerationID: %v", err)
	}
	if changedGeneration == originalGeneration {
		t.Fatal("compiled schema change did not change the plan generation")
	}

	originalBinding, err := packs.NewOutboundBindingPlan("ops", original, "42", nil)
	if err != nil {
		t.Fatalf("original binding: %v", err)
	}
	changedBinding, err := packs.NewOutboundBindingPlan("ops", cloned, "42", nil)
	if err != nil {
		t.Fatalf("changed binding: %v", err)
	}
	originalTarget, originalPin, err := originalBinding.RuntimeActivityTarget("deliver")
	if err != nil {
		t.Fatalf("original target: %v", err)
	}
	changedTarget, changedPin, err := changedBinding.RuntimeActivityTarget("deliver")
	if err != nil {
		t.Fatalf("changed target: %v", err)
	}
	if originalTarget == changedTarget || originalPin == changedPin {
		t.Fatalf("replacement plan reused private target: original=(%q,%q) changed=(%q,%q)", originalTarget, originalPin, changedTarget, changedPin)
	}
}

func TestChannelCompilerZoneHasNoProviderSpecificRuntimeBranch(t *testing.T) {
	body, err := os.ReadFile("channel.go")
	if err != nil {
		t.Fatalf("read channel compiler: %v", err)
	}
	text := strings.ToLower(string(body))
	for _, forbidden := range []string{"internal/providertriggers", "internal/providerconnectors", "telegram", "slack", "discord"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("generic channel compiler contains provider-specific dependency %q", forbidden)
		}
	}
}

func loadTelegramChannelPlan(t *testing.T) packs.SatisfactionPlan {
	t.Helper()
	registry, channel, trigger, connector := loadTelegramChannelCompilerInputs(t)
	plan, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err != nil {
		t.Fatalf("CompileChannel(Telegram): %v", err)
	}
	return plan
}

func loadTelegramChannelCompilerInputs(t *testing.T) (*packs.InterfaceRegistry, packs.LoadedChannelPack, packs.TriggerPackDescriptor, packs.ConnectorPackDescriptor) {
	t.Helper()
	repo := filepath.Clean(filepath.Join("..", ".."))
	registry := loadChannelInterfaceRegistry(t)
	version, err := platform.PlatformVersion()
	if err != nil {
		t.Fatalf("PlatformVersion: %v", err)
	}
	triggerCatalog, _, err := providertriggers.NewCatalogSnapshotFromPackDirs(version, []string{filepath.Join(repo, "packs", "provider-triggers", "telegram")}, nil)
	if err != nil {
		t.Fatalf("load Telegram trigger: %v", err)
	}
	channels, err := packs.LoadChannelPackDirs(version, "platform", filepath.Join(repo, "packs", "channels", "telegram"))
	if err != nil {
		t.Fatalf("load Telegram channel: %v", err)
	}
	if len(channels) != 1 {
		t.Fatalf("Telegram channel packs = %#v, want one", channels)
	}
	triggerDescriptors := triggerCatalog.PackDescriptors()
	if len(triggerDescriptors) != 1 {
		t.Fatalf("Telegram trigger descriptors = %#v, want one", triggerDescriptors)
	}
	connectorID := channels[0].Envelope.Requires.Packs[packs.TypeConnector]
	var connector packs.ConnectorPackDescriptor
	for _, candidate := range providerconnectors.DefaultPackRegistry().PackDescriptors() {
		if candidate.Identity.ID == connectorID {
			connector = candidate
			break
		}
	}
	if connector.Identity.ID == "" {
		t.Fatalf("Telegram connector descriptor %q is missing", connectorID)
	}
	return registry, channels[0], triggerDescriptors[0], connector
}

func loadChannelInterfaceRegistry(t *testing.T) *packs.InterfaceRegistry {
	t.Helper()
	spec := loadChannelPlatformSpec(t)
	registry, err := packs.NewInterfaceRegistry(spec)
	if err != nil {
		t.Fatalf("NewInterfaceRegistry: %v", err)
	}
	return registry
}

func loadChannelPlatformSpec(t *testing.T) runtimecontracts.PlatformSpecDocument {
	t.Helper()
	repo := filepath.Clean(filepath.Join("..", ".."))
	snapshot, err := yamlsource.LoadFile(filepath.Join(repo, "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("load platform spec: %v", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := snapshot.Decode(&spec); err != nil {
		t.Fatalf("decode platform spec: %v", err)
	}
	return spec
}

func mockChannelSatisfier() (packs.LoadedChannelPack, packs.TriggerPackDescriptor, packs.ConnectorPackDescriptor) {
	text128 := mockStringSchema(1, 128, "")
	label24 := mockStringSchema(1, 24, "")
	token20 := mockStringSchema(1, 20, `^[a-z0-9-]+$`)
	actions := mockArraySchema(0, 2, mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
		"name": label24, "value": token20,
	}, "name", "value"))
	destination := mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"queue": mockStringSchema(1, 10, `^[a-z0-9-]+$`)}, "queue")
	deliveryReference := mockStringSchema(22, 22, `^mock-delivery:[0-9a-f]{8}$`)
	deliveryReceipt := mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"revision": {Type: "integer"}}, "revision")
	interaction := mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"cursor": mockStringSchema(1, 16, "")}, "cursor")
	externalAccount := mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"principal": mockStringSchema(1, 20, "")}, "principal")
	conversation := mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"room": mockStringSchema(1, 20, "")}, "room")
	connectorTools := map[string]runtimecontracts.ToolSchemaEntry{
		"mock.deliver": mockConnectorTool(mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
			"destination": destination, "body": text128, "controls": actions,
		}, "destination", "body", "controls"), mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"ref": deliveryReference}, "ref")),
		"mock.edit": mockConnectorTool(mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
			"destination": destination, "reference": deliveryReference, "body": text128, "controls": actions,
		}, "destination", "reference", "body", "controls"), mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{"revision": {Type: "integer"}}, "revision")),
		"mock.ack": mockConnectorTool(mockObjectSchema(map[string]runtimecontracts.ToolInputSchema{
			"cursor": mockStringSchema(1, 16, ""),
		}, "cursor"), runtimecontracts.ToolInputSchema{Type: "object"}),
	}
	manifest := packs.ChannelManifest{
		Provider: "mock",
		OpaqueTypes: map[string]runtimecontracts.ToolInputSchema{
			"destination": destination, "delivery_reference": deliveryReference, "delivery_receipt": deliveryReceipt,
			"interaction_reference": interaction, "external_account_reference": externalAccount, "conversation_reference": conversation,
		},
		Operations: map[string]packs.ChannelOperationBinding{
			"deliver": {
				Tool: "mock.deliver",
				Input: map[string]packs.ChannelMapping{
					"destination.queue": {From: "context.destination.queue"}, "body": {From: "input.presentation.text"},
					"controls": {Each: "input.actions", Item: []map[string]packs.ChannelMapping{{"name": {From: "item.label"}, "value": {From: "item.token"}}}},
				},
				Output: map[string]packs.ChannelMapping{"delivery_reference": {From: "result.ref"}},
			},
			"edit": {
				Tool: "mock.edit",
				Input: map[string]packs.ChannelMapping{
					"destination.queue": {From: "context.destination.queue"}, "reference": {From: "input.delivery_reference"}, "body": {From: "input.presentation.text"},
					"controls": {Each: "input.actions", Item: []map[string]packs.ChannelMapping{{"name": {From: "item.label"}, "value": {From: "item.token"}}}},
				},
				Output: map[string]packs.ChannelMapping{"delivery_receipt.revision": {From: "result.revision"}},
			},
			"acknowledge_interaction": {Tool: "mock.ack", Input: map[string]packs.ChannelMapping{"cursor": {From: "input.interaction_reference.cursor"}}},
		},
		Events: map[string]packs.ChannelEventBinding{
			"action": {Event: "mock.action", Fields: map[string]string{
				"token": "event.token", "interaction_reference.cursor": "event.cursor", "external_account_reference.principal": "event.principal",
				"conversation_reference.room": "event.room", "provider_message_reference": "event.message_ref",
			}},
			"text": {Event: "mock.text", Fields: map[string]string{
				"text": "event.text", "external_account_reference.principal": "event.principal", "conversation_reference.room": "event.room", "provider_message_reference": "event.message_ref",
			}},
		},
	}
	channel := packs.LoadedChannelPack{
		Envelope: packs.Envelope{
			ID: "provider.mock.hitl_channel", Type: packs.TypeChannel, Version: "0.1.0", ManifestHash: "sha256:mock-channel",
			Implements: []string{"swarm.hitl-channel/v1"}, Provenance: packs.Provenance{Source: "external"},
			Requires: packs.Requires{Packs: map[string]string{packs.TypeTrigger: "provider.mock", packs.TypeConnector: "provider.mock.connector"}},
		},
		Manifest: manifest, Source: "external:mock-channel",
	}
	triggerFields := func(names ...string) map[string]packs.TriggerEventField {
		fields := make(map[string]packs.TriggerEventField, len(names))
		for _, name := range names {
			var schema runtimecontracts.ToolInputSchema
			switch name {
			case "token":
				schema = mockStringSchema(1, 20, `^[a-z0-9-]+$`)
			case "cursor":
				schema = mockStringSchema(1, 16, "")
			case "principal", "room":
				schema = mockStringSchema(1, 20, "")
			case "message_ref":
				schema = deliveryReference
			case "text":
				schema = text128
			default:
				panic("missing mock trigger field schema for " + name)
			}
			fields[name] = packs.TriggerEventField{Schema: schema, Required: true}
		}
		return fields
	}
	trigger := packs.TriggerPackDescriptor{
		Identity: packs.PackIdentity{ID: "provider.mock", Type: packs.TypeTrigger, Version: "0.1.0", ManifestHash: "sha256:mock-trigger"}, Provider: "mock",
		Events: map[string]packs.TriggerEvent{
			"mock.action": {Name: "mock.action", Fields: triggerFields("token", "cursor", "principal", "room", "message_ref")},
			"mock.text":   {Name: "mock.text", Fields: triggerFields("text", "principal", "room", "message_ref")},
		},
	}
	connector := packs.ConnectorPackDescriptor{
		Identity: packs.PackIdentity{ID: "provider.mock.connector", Type: packs.TypeConnector, Version: "0.1.0", ManifestHash: "sha256:mock-connector"},
		Provider: "mock", Tools: connectorTools,
	}
	return channel, trigger, connector
}

func mockConnectorTool(input, output runtimecontracts.ToolInputSchema) runtimecontracts.ToolSchemaEntry {
	return runtimecontracts.ToolSchemaEntry{EffectClass: string(runtimecontracts.ActivityEffectClassNonIdempotentWrite), InputSchema: input, OutputSchema: output}
}

func mockStringSchema(min, max int, pattern string) runtimecontracts.ToolInputSchema {
	return runtimecontracts.ToolInputSchema{Type: "string", MinLength: &min, MaxLength: &max, Pattern: pattern}
}

func mockArraySchema(min, max int, items runtimecontracts.ToolInputSchema) runtimecontracts.ToolInputSchema {
	return runtimecontracts.ToolInputSchema{Type: "array", MinItems: &min, MaxItems: &max, Items: &items}
}

func pointerToChannelSchema(schema runtimecontracts.ToolInputSchema) *runtimecontracts.ToolInputSchema {
	return &schema
}

func mockObjectSchema(properties map[string]runtimecontracts.ToolInputSchema, required ...string) runtimecontracts.ToolInputSchema {
	allowed := false
	return runtimecontracts.ToolInputSchema{
		Type: "object", Properties: properties, Required: required,
		AdditionalProperties: runtimecontracts.ToolAdditionalProperties{Allowed: &allowed},
	}
}
