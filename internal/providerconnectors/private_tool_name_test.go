package providerconnectors

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestPrivateToolNameConnectorAdmission(t *testing.T) {
	tool := contracts.MustToolSchemaEntry(contracts.WithToolCategory("provider_connector"),
		contracts.WithToolHandler(contracts.ToolHandlerInProcess), contracts.WithToolEffect(contracts.ActivityEffectClassNonIdempotentWrite),
		contracts.WithToolSchemas(contracts.MustToolInputSchema(contracts.ToolSchemaObject), contracts.MustToolInputSchema(contracts.ToolSchemaObject)),
		contracts.WithToolInProcessTarget(contracts.ToolInProcessWhatsAppSendText))
	for _, name := range []string{"Read", "mcp__runtime-tools__whatsapp.send", "mcp__runtime-tools__mcp__runtime-tools__whatsapp.send", "emit_whatsapp.send", " whatsapp.send "} {
		t.Run(name, func(t *testing.T) {
			manifest := ConnectorManifest{Provider: "whatsapp", Tools: map[string]contracts.ToolSchemaEntry{name: tool}}
			if err := manifest.Validate(); err == nil || !strings.Contains(err.Error(), name) || !strings.Contains(err.Error(), "private tool declaration") {
				t.Fatalf("typed manifest lost exact private-name refusal: %v", err)
			}
			if registry, err := NewPackRegistry(LoadedPack{Manifest: manifest}); registry != nil || err == nil || !strings.Contains(err.Error(), "private tool declaration") {
				t.Fatalf("registry published invalid private tool: registry=%v err=%v", registry, err)
			}
			if errs := validateTool(name, tool); len(errs) != 1 || !strings.Contains(errs[0].Error(), "private tool declaration") {
				t.Fatalf("direct connector qualification has a different private-name owner: %v", errs)
			}
			if errs := validateRegistrationTool(name, tool); len(errs) != 1 || !strings.Contains(errs[0].Error(), "private tool declaration") {
				t.Fatalf("registration qualification lost the private-name gate: %v", errs)
			}
		})
	}
	for _, name := range []string{"Read", "mcp__runtime-tools__whatsapp.send", "emit_whatsapp.send"} {
		body := "provider: whatsapp\ntools:\n  " + name + ":\n    category: provider_connector\n    handler_type: in_process\n    effect_class: non_idempotent_write\n    in_process: whatsapp.send_text\n"
		if _, err := ParseConnectorManifest([]byte(body)); err == nil || !strings.Contains(err.Error(), "connector.yaml") || !strings.Contains(err.Error(), name) || !strings.Contains(err.Error(), "private tool declaration") {
			t.Fatalf("connector grammar lost declaration/source evidence: %v", err)
		}
	}
}

func TestPrivateToolNameConnectorSourcePreservesRawKeys(t *testing.T) {
	tool := contracts.MustToolSchemaEntry(contracts.WithToolCategory("provider_connector"),
		contracts.WithToolHandler(contracts.ToolHandlerInProcess), contracts.WithToolEffect(contracts.ActivityEffectClassNonIdempotentWrite),
		contracts.WithToolSchemas(contracts.MustToolInputSchema(contracts.ToolSchemaObject), contracts.MustToolInputSchema(contracts.ToolSchemaObject)),
		contracts.WithToolInProcessTarget(contracts.ToolInProcessWhatsAppSendText))
	for _, name := range []string{"", " Read ", " whatsapp.send "} {
		t.Run(name, func(t *testing.T) {
			source := semanticview.Wrap(&contracts.WorkflowContractBundle{Tools: map[string]contracts.ToolSchemaEntry{name: tool}})
			if errs := ValidateSource(source); len(errs) != 1 || !strings.Contains(errs[0].Error(), "private tool declaration") {
				t.Errorf("source qualification dropped or normalized the private ID: %v", errs)
			}
			if subjects, err := CapabilitySubjects(context.Background(), source, CapabilityOptions{Registry: &PackRegistry{}}); len(subjects) != 0 || err == nil || !strings.Contains(err.Error(), "private tool declaration") {
				t.Errorf("capability projection lost the private-name admission: subjects=%v err=%v", subjects, err)
			}
		})
	}
}
