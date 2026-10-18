package packs

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestPrivateToolNameAcceptedConnectorAndGenerationAdmission(t *testing.T) {
	tool := contracts.MustToolSchemaEntry(contracts.WithToolCategory("provider_connector"),
		contracts.WithToolHandler(contracts.ToolHandlerInProcess), contracts.WithToolEffect(contracts.ActivityEffectClassNonIdempotentWrite),
		contracts.WithToolSchemas(contracts.MustToolInputSchema(contracts.ToolSchemaObject), contracts.MustToolInputSchema(contracts.ToolSchemaObject)),
		contracts.WithToolInProcessTarget(contracts.ToolInProcessWhatsAppSendText))
	for _, name := range []string{"Read", "mcp__runtime-tools__whatsapp.send", "emit_whatsapp.send"} {
		t.Run(name, func(t *testing.T) {
			if err := validateAcceptedConnectorDescriptor(ConnectorPackDescriptor{Tools: map[string]contracts.ToolSchemaEntry{name: tool}}); err == nil || !strings.Contains(err.Error(), "private tool declaration") {
				t.Fatalf("accepted typed connector bypassed declaration admission: %v", err)
			}
			identity, err := admitChannelPlanIdentity("test connector tool", name)
			if err != nil {
				t.Fatal(err)
			}
			// The legal session transport/capabilities must pass before the name gate.
			capabilities, err := CompileChannelCapabilities(ChannelCapabilityVector{CardRender: true, ReplyToReference: true, ActionsAsText: true, InboxListing: true})
			if err != nil {
				t.Fatal(err)
			}
			plan := SatisfactionPlan{transport: ChannelTransportSession, capabilities: capabilities,
				operations: map[string]compiledChannelOperation{"deliver": {tool: identity, toolSchema: tool}}}
			if err := validateSatisfactionPlanGenerationInputs(plan); err == nil || !strings.Contains(err.Error(), "private tool declaration") {
				t.Fatalf("generation froze an invalid original tool ID: %v", err)
			}
			canonical, err := admitChannelPlanIdentity("test connector tool", "whatsapp.send")
			if err != nil {
				t.Fatal(err)
			}
			for _, slot := range []string{"identify", "apply", "readback"} {
				registration := &CompiledChannelRegistration{
					identify: compiledRegistrationOperation{name: "identify", toolID: canonical, tool: tool},
					apply:    compiledRegistrationOperation{name: "apply", toolID: canonical, tool: tool},
					readback: compiledRegistrationOperation{name: "readback", toolID: canonical, tool: tool},
				}
				plan.operations, plan.registration = nil, registration
				if err := validateSatisfactionPlanGenerationInputs(plan); err != nil {
					t.Fatalf("canonical registration fixture failed before name mutation: %v", err)
				}
				map[string]*compiledRegistrationOperation{"identify": &registration.identify, "apply": &registration.apply, "readback": &registration.readback}[slot].toolID = identity
				if err := validateSatisfactionPlanGenerationInputs(plan); err == nil || !strings.Contains(err.Error(), name) || !strings.Contains(err.Error(), "registration \""+slot+"\"") || !strings.Contains(err.Error(), "private tool declaration") {
					t.Fatalf("generation lost %s registration-name refusal: %v", slot, err)
				}
			}
		})
	}
}
