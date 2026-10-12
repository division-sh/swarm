package packs_test

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestChannelSchemaRelationFactoringConsumers(t *testing.T) {
	registry := loadChannelInterfaceRegistry(t)
	for _, tc := range []struct {
		name   string
		mutate func(*packs.LoadedChannelPack, *packs.TriggerPackDescriptor, *packs.ConnectorPackDescriptor)
		want   string
	}{
		{"input", func(_ *packs.LoadedChannelPack, _ *packs.TriggerPackDescriptor, c *packs.ConnectorPackDescriptor) {
			tool := c.Tools["mock.deliver"]
			input := tool.InputSchema()
			destination, _ := input.Property("destination")
			queue, _ := destination.Property("queue")
			destination = mustSchemaWithProperty(destination, "queue", mustSchemaWithMaxLength(queue, 5))
			c.Tools["mock.deliver"] = mustToolWithSchemas(tool, mustSchemaWithProperty(input, "destination", destination), tool.OutputSchema())
		}, "source maximum is broader than target maximum 5"},
		{"output", func(_ *packs.LoadedChannelPack, _ *packs.TriggerPackDescriptor, c *packs.ConnectorPackDescriptor) {
			tool := c.Tools["mock.deliver"]
			output := tool.OutputSchema()
			ref, _ := output.Property("ref")
			c.Tools["mock.deliver"] = mustToolWithSchemas(tool, tool.InputSchema(), mustSchemaWithProperty(output, "ref", mustSchemaWithLengthBounds(ref, 1, 100)))
		}, "source minimum is broader than target minimum 22"},
		{"each", func(_ *packs.LoadedChannelPack, _ *packs.TriggerPackDescriptor, c *packs.ConnectorPackDescriptor) {
			tool := c.Tools["mock.deliver"]
			input := tool.InputSchema()
			controls, _ := input.Property("controls")
			item, _ := controls.ItemsSchema()
			name := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaString, runtimecontracts.ToolSchemaMinLength(1), runtimecontracts.ToolSchemaMaxLength(24), runtimecontracts.ToolSchemaEnum("only"))
			item = mustSchemaWithProperty(item, "name", name)
			input = mustSchemaWithProperty(input, "controls", mustSchemaWithItems(controls, item))
			c.Tools["mock.deliver"] = mustToolWithSchemas(tool, input, tool.OutputSchema())
		}, "target enum is narrower than an unbounded source"},
		{"event", func(_ *packs.LoadedChannelPack, trigger *packs.TriggerPackDescriptor, _ *packs.ConnectorPackDescriptor) {
			event := trigger.Events["mock.text"]
			event.Fields["room"] = packs.TriggerEventField{Schema: mockStringSchema(1, 30, ""), Required: true}
			trigger.Events["mock.text"] = event
		}, "source maximum is broader than target maximum 20"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			channel, trigger, connector := mockChannelSatisfier()
			plan, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
			if err != nil {
				t.Fatal(err)
			}
			before, err := plan.Generation()
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(&channel, &trigger, &connector)
			refused, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("%s relation: %v, want %q", tc.name, err, tc.want)
			}
			if _, err := refused.Generation(); err == nil {
				t.Fatal("refused relation acquired a generation")
			}
			after, err := plan.Generation()
			if err != nil || !before.Equal(after) {
				t.Fatal("revalidation mutated the admitted plan generation")
			}
			// Reconstruct from fresh fixture ownership, not the mutated caller maps.
			channel, trigger, connector = mockChannelSatisfier()
			fresh, err := packs.CompileChannel(registry, channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
			if err != nil {
				t.Fatal(err)
			}
			generation, err := fresh.Generation()
			if err != nil || !before.Equal(generation) {
				t.Fatal("fresh reconstruction changed the plan generation")
			}
		})
	}
}
