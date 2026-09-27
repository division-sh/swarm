package providerconnectors

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

func TestImportedActivitySchemaClassificationConsumesEffectiveSource(t *testing.T) {
	for _, flow := range []string{".", "worker", "nested/worker"} {
		t.Run(flow, func(t *testing.T) {
			handlers := map[string]runtimecontracts.SystemNodeEventHandler{
				"send.requested": {Activity: runtimecontracts.ActivitySpec{
					ID: "send", Tool: "telegram.send_message",
					Approval: &runtimecontracts.ActivityApprovalSpec{Decision: "send_review"},
				}},
			}
			nodes := map[string]runtimecontracts.SystemNodeContract{"sender": {ExecutionType: "system_node", EventHandlers: handlers}}
			bundle := &runtimecontracts.WorkflowContractBundle{Nodes: nodes}
			base := semanticviewtest.WrapRootAgents(bundle)
			if flow != "." {
				bundle.Nodes = nil
				bundle.FlowTree.Root.Nodes = nil
				child := &runtimecontracts.FlowContractView{Path: flow, Paths: runtimecontracts.FlowContractPaths{FlowPath: flow}, Nodes: nodes}
				bundle.FlowTree.ByID[flow] = child
				bundle.FlowTree.Root.Children = []runtimecontracts.FlowContractView{*child}
				base = semanticview.Wrap(bundle)
			}
			source, err := SourceWithConnectorPackImports(providerConnectorScopedSource{
				Source:       base,
				importScopes: []connectorPackTestImportScope{flowScopeWithConnectorPackImport(flow, "telegram", "telegram.send_message")},
			}, testPackRegistry(t))
			if err != nil {
				t.Fatal(err)
			}
			if len(bundle.GeneratedActivityEventEntries()) != 0 {
				t.Fatal("unimported base unexpectedly owns connector outcome schemas")
			}
			if _, err := semanticview.CompileActivityToolBindings(base); err == nil {
				t.Fatal("activity without an admitted tool compiled")
			}
			prefix := ""
			if flow != "." {
				prefix = flow + "/"
			}
			for _, suffix := range []string{"succeeded", "failed", "revision_requested", "rejected"} {
				name := prefix + "send." + suffix
				resolved := semanticview.ResolveEventSchema(source, flow, name)
				if !resolved.HasSchema || !resolved.HasClassification || resolved.Classification != runtimecontracts.CompiledEventSchemaGenerated || resolved.EventKey != name {
					t.Fatalf("effective generated event %s: %+v", name, resolved)
				}
				withoutTools := activitySourceWithoutTools{Source: source}
				if stable := semanticview.ResolveEventSchema(withoutTools, flow, name); !stable.HasCompiled || stable.Classification != runtimecontracts.CompiledEventSchemaGenerated || stable.CompiledSchema.AcceptanceSchemaDigest() != resolved.CompiledSchema.AcceptanceSchemaDigest() {
					t.Fatalf("compiled snapshot changed when a reader hid tool entries for %s: %+v", name, stable)
				}
			}
			for _, name := range []string{prefix + "other.succeeded", "sibling/send.succeeded"} {
				if resolved := semanticview.ResolveEventSchema(source, flow, name); resolved.HasClassification {
					t.Fatalf("unowned outcome acquired classification: %+v", resolved)
				}
			}
			delete(handlers, "send.requested")
			if retained := semanticview.ResolveEventSchema(source, flow, prefix+"send.succeeded"); !retained.HasCompiled || retained.Classification != runtimecontracts.CompiledEventSchemaGenerated {
				t.Fatalf("previous compiled snapshot lost its outcome: %+v", retained)
			}
			fresh, err := SourceWithConnectorPackImports(providerConnectorScopedSource{
				Source:       base,
				importScopes: []connectorPackTestImportScope{flowScopeWithConnectorPackImport(flow, "telegram", "telegram.send_message")},
			}, testPackRegistry(t))
			if err != nil {
				t.Fatal(err)
			}
			for _, suffix := range []string{"succeeded", "failed", "revision_requested", "rejected"} {
				if resolved := semanticview.ResolveEventSchema(fresh, flow, prefix+"send."+suffix); resolved.HasSchema || resolved.HasClassification {
					t.Fatalf("recompiled removed activity retained %s: %+v", suffix, resolved)
				}
			}
		})
	}
}

type activitySourceWithoutTools struct{ semanticview.Source }

func (activitySourceWithoutTools) ToolEntries() map[string]runtimecontracts.ToolSchemaEntry {
	return nil
}
