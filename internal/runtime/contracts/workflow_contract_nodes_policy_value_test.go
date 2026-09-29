package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodePolicyValueRows(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte(`worker:
  event_handlers:
    task.ready:
      rules:
        scaffold_paths:
          lookup:
            on: [payload.scaffold_type, payload.language]
            entries:
              - {key: [service, go], value: templates/service/go}
              - {key: [library, go], value: templates/library/go}
            into: computed.template_path
            default: fail
        validate_manifest:
          validate:
            set: deploy_manifest
            input: {source_ref: payload.source_ref}
            into: computed.validation.deploy_manifest
        render_bundle:
          compute_module:
            module: structured_renderer
            input: {component: payload.component}
            into: computed.rendered_bundle
        fallback: {else: true}
`))
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	rules := nodes["worker"].EventHandlers["task.ready"].Rules
	if len(rules) != 4 || rules[0].Compute == nil || rules[0].Compute.Lookup == nil ||
		rules[0].Compute.Lookup.RowID != "scaffold_paths" || len(rules[0].Compute.Lookup.Entries) != 2 ||
		rules[1].Compute.Validation.Set != "deploy_manifest" ||
		rules[2].Compute.Module.Module != "structured_renderer" {
		t.Fatalf("value row projection lost metadata: %#v", rules)
	}
}

func TestProjectNodePolicyLookupRejectsDuplicateKey(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("worker:\n  event_handlers:\n    task.ready:\n      rules:\n        - id: choose\n          lookup: {on: payload.kind, entries: [{key: a, value: first}, {key: a, value: second}], into: computed.choice}\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err == nil || !strings.Contains(err.Error(), "duplicate lookup key") {
		t.Fatalf("expected duplicate lookup key rejection, got %v", err)
	}
}
