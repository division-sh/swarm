package bootverify

import (
	"context"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestNodeRejectsRetiredJoinAndAccumulatorFieldsThroughSupportedSource(t *testing.T) {
	for _, owner := range []struct{ name, body string }{
		{"accumulate", "        into: observations\n        from: payload.window\n        key: payload.member_id\n"},
		{"join", "        stage: awaiting\n        members: {count: 1, by: payload.member_id}\n        output: payload.aggregation\n        on_complete: {advances_to: done}\n        deadline: {after: 1h, from: stage_entry}\n        on_deadline: {advances_to: failed}\n"},
	} {
		t.Run(owner.name, func(t *testing.T) {
			for _, field := range []string{"window", "dedup_by", "aggregation"} {
				t.Run(field, func(t *testing.T) {
					for _, shape := range []struct{ name, value string }{
						{"missing", ""}, {"null", "null"}, {"empty scalar", "''"}, {"scalar", "payload.member_id"},
						{"empty sequence", "[]"}, {"sequence", "[payload.member_id]"}, {"empty mapping", "{}"}, {"mapping", "{value: payload.member_id}"},
						{"alias empty", "*empty"}, {"anchored null", "&absent null"},
					} {
						t.Run(shape.name, func(t *testing.T) {
							body := owner.body
							if shape.name != "missing" {
								body += "        " + field + ": " + shape.value + "\n"
							}
							root := writeJoinRetirementSource(t, owner.name, body)
							repo := repoRootForBootverifyTest(t)
							bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
							if shape.name != "missing" {
								diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err)
								if !ok || diagnostic.Code != "contract_loader.undefined_field" || diagnostic.Problem != owner.name+" field \""+field+"\" is not supported." || filepath.Base(diagnostic.Location.File) != "nodes.yaml" || diagnostic.Location.Line <= 0 || diagnostic.Location.Column <= 0 {
									t.Fatalf("retired %s.%s (%s) escaped source rejection: %v", owner.name, field, shape.name, err)
								}
								return
							}
							if err != nil {
								t.Fatalf("canonical %s failed source admission: %v", owner.name, err)
							}
							source := semanticview.Wrap(bundle)
							if findings := Run(context.Background(), source, Options{}).HardInvalidities(); len(findings) != 0 {
								t.Fatalf("canonical %s failed strict verification: %#v", owner.name, findings)
							}
							if owner.name == "join" && len(source.WorkflowJoins()) != 1 {
								t.Fatalf("canonical intrinsic join disappeared: %#v", source.WorkflowJoins())
							}
							if owner.name == "accumulate" && len(source.WorkflowJoins()) != 0 {
								t.Fatalf("keyed accumulator invented a finite join: %#v", source.WorkflowJoins())
							}
						})
					}
				})
			}
		})
	}
}

func writeJoinRetirementSource(t *testing.T, owner, body string) string {
	t.Helper()
	root := t.TempDir()
	stages := ""
	if owner == "join" {
		stages = "stages:\n  awaiting: {initial: true}\n  done: {terminal: true}\n  failed: {terminal: true}\n"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: join-retirement\n"+stages+`pins:
  inputs:
    events:
      - {event: item.reported, source: harness}
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "entities.yaml"), "State: {}\n")
	// Business fields with these names remain legal; only the retired
	// execution-grammar fields are rejected.
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), `item.reported:
  member_id: text
  window: text
  dedup_by: text
  aggregation: text
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), `collector:
  description: &empty ''
  execution_type: system_node
  subscribes_to: [item.reported]
  event_handlers:
    item.reported:
      `+owner+":\n"+body)
	return root
}
