package packs_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
)

func TestChannelCapabilityCompilerTruthTable(t *testing.T) {
	for mask := 0; mask < 8; mask++ {
		for _, native := range []bool{false, true} {
			t.Run(fmt.Sprintf("optional_%03b/native_%v", mask, native), func(t *testing.T) {
				channel, trigger, connector := mockChannelSatisfier()
				vector := packs.ChannelCapabilityVector{
					CardRender: true, ReplyToReference: true, ActionsAsText: true, InboxListing: true,
					ActionsAsButtons: mask&1 != 0, Edit: mask&2 != 0, Acknowledgment: mask&4 != 0,
				}
				var err error
				channel.Manifest.Capabilities, err = packs.CompileChannelCapabilities(vector)
				if err != nil {
					t.Fatal(err)
				}
				if !native {
					channel.Manifest.NativeInbox = nil
					for _, operation := range []string{"install_inbox_entry", "read_inbox_entry", "install_shared_inbox_entry", "read_shared_inbox_entry", "identify_inbox_address", "read_inbox_launcher", "read_default_inbox_launcher"} {
						delete(channel.Manifest.Operations, operation)
					}
				}
				if !vector.Edit {
					delete(channel.Manifest.Operations, "edit")
					delete(channel.Manifest.OpaqueTypes, "delivery_receipt")
				}
				if !vector.Acknowledgment {
					delete(channel.Manifest.Operations, "acknowledge_interaction")
				}
				if !vector.ActionsAsButtons && !vector.Acknowledgment {
					delete(channel.Manifest.Events, "action")
					delete(channel.Manifest.OpaqueTypes, "interaction_reference")
				}
				if !vector.ActionsAsButtons {
					for _, operation := range []string{"deliver", "edit"} {
						binding, found := channel.Manifest.Operations[operation]
						if !found {
							continue
						}
						delete(binding.Input, "controls")
						tool := connector.Tools[binding.Tool]
						properties := tool.InputSchema().Properties()
						delete(properties, "controls")
						required := []string{"destination", "body"}
						if operation == "edit" {
							required = append(required, "reference")
						}
						connector.Tools[binding.Tool], err = tool.WithSchemas(mockObjectSchema(properties, required...), tool.OutputSchema())
						if err != nil {
							t.Fatal(err)
						}
					}
				}
				plan, err := packs.CompileChannel(loadChannelInterfaceRegistry(t), channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
				if err != nil {
					t.Fatal(err)
				}
				if plan.Capabilities().Vector() != vector || plan.HasNativeInbox() != native {
					t.Fatal("compiler changed declared capabilities/native extension")
				}
				if generation, err := plan.Generation(); err != nil || !generation.Valid() {
					t.Fatalf("capability-selected generation missing: %v", err)
				}
				for operation, supported := range map[string]bool{"edit": vector.Edit, "acknowledge_interaction": vector.Acknowledgment} {
					_, _, err := plan.ConnectorOperation(operation)
					if (err == nil) != supported {
						t.Fatalf("%s capability/operation disagrees: %v", operation, err)
					}
					if !supported {
						var refusal *packs.UnsupportedChannelCapabilityError
						if !errors.As(err, &refusal) {
							t.Fatalf("%s missing typed capability refusal: %v", operation, err)
						}
					}
				}
				bounds, err := plan.PresentationBounds()
				if err != nil || (!vector.ActionsAsButtons && (bounds.Actions != 16 || bounds.LabelRunes != 64)) {
					t.Fatalf("text/native bounds are not admitted: %+v %v", bounds, err)
				}
				if !vector.ActionsAsButtons {
					input, err := plan.PrepareOperationInput("deliver", map[string]any{"presentation": map[string]any{"text": "Inbox"}}, map[string]any{"destination": map[string]any{"queue": "ops"}})
					if err != nil || input["controls"] != nil {
						t.Fatalf("text-only operation fabricated native controls: %+v %v", input, err)
					}
				}
			})
		}
	}
}
