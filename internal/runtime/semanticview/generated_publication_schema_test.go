package semanticview

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestGeneratedPublicationStructuralSchemaPreservesExactOwner(t *testing.T) {
	for _, mode := range []string{"root", "static", "template", "nested_template"} {
		t.Run(mode, func(t *testing.T) {
			repo := canonicalrouting.RepoRoot(t)
			root := canonicalrouting.CopyPublicationActivity(t, mode, "http://127.0.0.1:1/send", true)
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			flow := "source"
			if mode == "root" {
				flow = "."
			} else if mode == "nested_template" {
				flow = "outer/source"
			}
			source := Wrap(bundle)
			for _, local := range []string{"send.succeeded", "send.failed", "send.revision_requested", "send.rejected"} {
				qualified := local
				if flow != "." {
					qualified = flow + "/" + local
				}
				t.Run("absent_flow/"+local, func(t *testing.T) {
					resolution := ResolveEventSchema(source, "absent-flow", qualified)
					if resolution.HasSchema || resolution.HasCompiled || resolution.HasStructural {
						t.Fatalf("absent flow borrowed generated schema: key=%q compiled_flow=%q class=%q", resolution.EventKey, resolution.CompiledSchema.FlowPath(), resolution.Classification)
					}
				})
				for _, receiver := range []string{flow, "sink"} {
					t.Run(receiver+"/"+local, func(t *testing.T) {
						resolution := ResolveEventSchema(source, receiver, local)
						field, ok := resolution.Field("activity_id")
						if !ok || field.Type.Kind != "string" || resolution.Classification != contracts.CompiledEventSchemaGenerated {
							t.Fatalf("structural schema borrowed another owner: found=%t kind=%q class=%q compiled_flow=%q compiled_event=%q", ok, field.Type.Kind, resolution.Classification, resolution.CompiledSchema.FlowPath(), resolution.CompiledSchema.EventName())
						}
					})
				}
			}
		})
	}
}
