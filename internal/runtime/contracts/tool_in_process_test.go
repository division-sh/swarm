package contracts

import (
	"fmt"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestInProcessToolTargetSurvivesCanonicalAndYAMLRoundTrip(t *testing.T) {
	for _, cell := range []struct{ target, category, effect string }{
		{"whatsapp.read_account", "provider_registration", "read_only"},
		{"whatsapp.send_text", "provider_connector", "non_idempotent_write"},
	} {
		t.Run(cell.target, func(t *testing.T) {
			body := fmt.Sprintf("operation:\n  category: %s\n  handler_type: in_process\n  effect_class: %s\n  in_process: %s\n", cell.category, cell.effect, cell.target)
			entries, err := admitW5Tools(t, body)
			if err != nil {
				t.Fatal(err)
			}
			entry := entries["operation"]
			value, err := entry.CanonicalValue()
			if err != nil || value["in_process"] != cell.target || entry.Handler().String() != "in_process" {
				t.Fatal("compiled tool lost its exact in-process target", value, err)
			}
			frozen, err := entry.CanonicalHash()
			if err != nil {
				t.Fatal(err)
			}
			value["in_process"] = "invented.operation"
			if after, err := entry.CanonicalHash(); err != nil || after != frozen {
				t.Fatal("canonical projection exposes mutable execution authority", err)
			}
			raw, err := yaml.Marshal(entries)
			if err != nil {
				t.Fatal(err)
			}
			reloaded, err := admitW5Tools(t, string(raw))
			if err != nil {
				t.Fatal(err)
			}
			if after, err := reloaded["operation"].CanonicalHash(); err != nil || after != frozen {
				t.Fatal("source round trip changed native execution identity", err)
			}
			copy, err := entry.WithSchemas(entry.InputSchema(), entry.OutputSchema())
			if err != nil {
				t.Fatal(err)
			}
			if after, err := copy.CanonicalHash(); err != nil || after != frozen {
				t.Fatal("compiled schema refinement dropped native target", err)
			}
			if _, err := entry.WithEffect(ActivityEffectClassIdempotentWrite); err == nil {
				t.Fatal("native operation adopted another effect class")
			}
		})
	}
}

func TestInProcessToolTargetRejectsUnsupportedOrMixedExecution(t *testing.T) {
	base := "operation:\n  category: provider_connector\n  handler_type: in_process\n  effect_class: non_idempotent_write\n"
	for _, extra := range []string{
		"", "  in_process: null\n", "  in_process: ''\n", "  in_process: {}\n", "  in_process: []\n", "  in_process: 7\n",
		"  in_process: unknown.send\n", "  in_process: /usr/bin/whatsapp\n", "  in_process: 'go://example/Send'\n",
		"  in_process: ' whatsapp.send_text'\n", "  in_process: 'whatsapp.send_text '\n",
		"  in_process: whatsapp.send_text\n  http: {method: POST, url: 'https://example.invalid'}\n",
		"  in_process: whatsapp.send_text\n  credentials: [invented_token]\n",
		"  in_process: whatsapp.send_text\n  response_mapping: {}\n",
		"  in_process: whatsapp.send_text\n  response_success: {kind: http_status_2xx}\n",
		"  in_process: whatsapp.read_account\n",
	} {
		t.Run(extra, func(t *testing.T) {
			if _, err := admitW5Tools(t, base+extra); err == nil {
				t.Fatal("unsupported native execution target admitted")
			}
		})
	}
	for _, handler := range []string{"", "http", "mcp", "channel", "platform_builtin", "wasm", "python"} {
		t.Run("foreign_handler/"+handler, func(t *testing.T) {
			body := fmt.Sprintf("operation:\n  handler_type: '%s'\n  in_process: whatsapp.send_text\n", handler)
			if _, err := admitW5Tools(t, body); err == nil {
				t.Fatal("native target escaped its closed handler")
			}
		})
	}
	module := fmt.Sprintf("operation:\n  handler_type: wasm\n  path: modules/op.wasm\n  abi: core-json-v1\n  entry: compute\n  digest: sha256:%s\n  input_schema: {type: object, properties: {value: {type: integer}}}\n  output_schema: {type: object, properties: {value: {type: integer}}}\n  limits: {gas: 100, memory_pages: 17, output_bytes: 1024}\n", strings.Repeat("0", 64))
	if _, err := admitW5Tools(t, module); err != nil {
		t.Fatal("module control is invalid", err)
	}
	if _, err := admitW5Tools(t, module+"  in_process: whatsapp.send_text\n"); err == nil || !strings.Contains(err.Error(), "module tools cannot declare in_process") {
		t.Fatal("module declaration silently discarded a native target", err)
	}
}

func TestInProcessToolTargetScopedResolutionRetainsExactDeclaration(t *testing.T) {
	entries, err := admitW5Tools(t, "send:\n  category: provider_connector\n  handler_type: in_process\n  effect_class: non_idempotent_write\n  in_process: whatsapp.send_text\n")
	if err != nil {
		t.Fatal(err)
	}
	root := &FlowContractView{Paths: FlowContractPaths{FlowPath: "."}, Tools: entries}
	child := FlowContractView{Paths: FlowContractPaths{FlowPath: "child"}}
	shadow := FlowContractView{Paths: FlowContractPaths{FlowPath: "shadow"}, Tools: map[string]ToolSchemaEntry{
		"send": MustToolSchemaEntry(WithToolSchemas(MustToolInputSchema(ToolSchemaObject), MustToolInputSchema(ToolSchemaObject))),
	}}
	root.Children = []FlowContractView{child, shadow}
	bundle := &WorkflowContractBundle{FlowTree: FlowTree{Root: root}}
	inherited, found := bundle.ToolEntryForFlow("child", "send")
	target, native := inherited.InProcess()
	if !found || !native || target != ToolInProcessWhatsAppSendText || target.Provider() != "whatsapp" {
		t.Fatal("scoped tool resolution dropped or inferred the provider target")
	}
	nearer, found := bundle.ToolEntryForFlow("shadow", "send")
	if !found {
		t.Fatal("nearer declaration disappeared")
	}
	if _, native := nearer.InProcess(); native {
		t.Fatal("wrong-kind shadow resurrected an ancestor native operation")
	}
	if _, err := NewToolSchemaEntry(WithToolHandler(ToolHandlerInProcess), WithToolInProcessTarget(ToolInProcessTarget(255))); err == nil {
		t.Fatal("numeric construction bypassed the closed native target vocabulary")
	}
}
