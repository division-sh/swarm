package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestGeneratedPublicationSchemaOwnershipDoesNotBorrowSibling(t *testing.T) {
	for _, mode := range []string{"root", "static", "template", "nested_template"} {
		t.Run(mode, func(t *testing.T) {
			root := canonicalrouting.CopyPublicationActivity(t, mode, "http://127.0.0.1:1/send", true)
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			flow := "source"
			if mode == "root" {
				flow = "."
			} else if mode == "nested_template" {
				flow = "outer/source"
			}
			for _, local := range []string{"send.succeeded", "send.failed", "send.revision_requested", "send.rejected"} {
				t.Run(local, func(t *testing.T) {
					qualified := local
					if flow != "." {
						qualified = flow + "/" + local
					}
					generated, ok := bundle.GeneratedActivityEventEntries()[qualified]
					if !ok || generated.Payload.Properties["activity_id"].Type != "string" {
						t.Fatalf("activity producer missing: found=%t entry=%+v", ok, generated)
					}
					for _, receiver := range []string{flow, "sink"} {
						entry, key, ok := bundle.ResolveFlowEventCatalogEntry(receiver, local)
						if !ok || entry.Payload.Properties["activity_id"].Type != generated.Payload.Properties["activity_id"].Type {
							t.Errorf("%s catalog borrowed or lost generated owner: found=%t key=%q type=%q want=%q", receiver, ok, key, entry.Payload.Properties["activity_id"].Type, generated.Payload.Properties["activity_id"].Type)
						}
						compiled, ok, err := bundle.ResolveEffectiveCompiledFlowEventSchema(receiver, local)
						if err != nil || !ok || compiled.Classification() != CompiledEventSchemaGenerated || compiled.Importable() {
							t.Errorf("%s immutable generated owner: found=%t class=%s importable=%t err=%v", receiver, ok, compiled.Classification(), compiled.Importable(), err)
						}
					}
					sibling, _, ok := bundle.ResolveFlowEventCatalogEntry("sibling", local)
					if !ok || sibling.Payload.Properties["activity_id"].Type != "integer" {
						t.Errorf("sibling lost its independent integer declaration: found=%t entry=%+v", ok, sibling)
					}
				})
			}
			// Routing's generated schemas must not become resource declarations.
			importable, err := bundle.CompiledEventSchemas()
			if err != nil {
				t.Fatal(err)
			}
			for _, schema := range importable {
				if schema.FlowPath() == flow && strings.Contains(schema.EventName(), "send.") {
					t.Errorf("generated activity became importable: %+v", schema)
				}
			}
		})
	}
}
