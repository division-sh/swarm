package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodePolicyValueRows(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte(`worker:
  task.ready:
    rules:
      - lookup:
          on: [payload.scaffold_type, payload.language]
          entries:
            - {key: [service, go], value: templates/service/go}
            - {key: [library, go], value: templates/library/go}
          into: computed.template_path
          default: fail
      - validate:
          set: deploy_manifest
          input: {source_ref: payload.source_ref}
          into: computed.validation.deploy_manifest
      - compute_module:
          module: structured_renderer
          input: {component: payload.component}
          into: computed.rendered_bundle
`))
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	rules := nodes["worker"].EventHandlers["task.ready"].Rules
	if len(rules) != 3 || rules[0].Compute == nil || rules[0].Compute.Lookup == nil ||
		rules[0].Compute.Lookup.RowID != "rules[0]: lookup" || len(rules[0].Compute.Lookup.Entries) != 2 ||
		rules[1].Compute.Validation.RowID != "rules[1]: validate" ||
		rules[1].Compute.Validation.Set != "deploy_manifest" ||
		rules[2].Compute.Module.RowID != "rules[2]: compute_module" ||
		rules[2].Compute.Module.Module != "structured_renderer" {
		t.Fatalf("value row projection lost metadata: %#v", rules)
	}
	if rules[0].PolicyRow.Lookup != rules[0].Compute.Lookup ||
		rules[1].PolicyRow.Validation != rules[1].Compute.Validation ||
		rules[2].PolicyRow.Module != rules[2].Compute.Module {
		t.Fatal("value-row lowering introduced a competing compute plan")
	}
	handler, err := QualifySystemNodeHandlerRuleRefsForEvent(identitytest.RootNode(t, "worker"), "task.ready", nodes["worker"].EventHandlers["task.ready"])
	if err != nil {
		t.Fatal(err)
	}
	for index, row := range handler.Rules {
		ref, valid := row.DeclarationIdentity()
		if !valid || ref.SemanticPath() != []string{
			`nodes["worker"].handlers["task.ready"].rules[0]`,
			`nodes["worker"].handlers["task.ready"].rules[1]`,
			`nodes["worker"].handlers["task.ready"].rules[2]`,
		}[index] {
			t.Fatalf("row[%d] display displaced positional identity: %#v", index, ref)
		}
	}
	for _, tc := range []struct {
		name, input, secret string
		ok                  bool
	}{
		{"named_key", "{document: payload.document}", "{private_key: 'payload.named.key'}", true},
		{"multiple_parameters", "{document: payload.document}", "{one: stored_key, two: stored_key}", true},
		{"blank_key", "{document: payload.document}", "{private_key: ' '}", false},
		{"nontext_key", "{document: payload.document}", "{private_key: 12}", false},
		{"nonmapping", "{document: payload.document}", "[stored_key]", false},
		{"duplicate", "{document: payload.document}", "{private_key: one, private_key: two}", false},
		{"empty_ordinary", "{}", "{private_key: stored_key}", false},
	} {
		t.Run("secret_input/"+tc.name, func(t *testing.T) {
			body := "module: transform\ninput: " + tc.input + "\nsecret_input: " + tc.secret + "\ninto: computed.result\n"
			snapshot, err := yamlsource.Load([]byte(body))
			if err != nil {
				t.Fatal(err)
			}
			_, compute, err := projectNodePolicyValueRow(snapshot.Document("nodes.yaml").Root(), "compute_module")
			if (err == nil) != tc.ok {
				t.Fatalf("admission error=%v, want accepted=%t", err, tc.ok)
			}
			if tc.ok && compute.Module.SecretInput == nil {
				t.Fatal("secret bindings were not retained")
			}
		})
	}
	t.Run("secret_input/not_validate", func(t *testing.T) {
		snapshot, err := yamlsource.Load([]byte("set: check\ninput: {document: payload.document}\nsecret_input: {key: stored_key}\ninto: computed.validation.check\n"))
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := projectNodePolicyValueRow(snapshot.Document("nodes.yaml").Root(), "validate"); err == nil {
			t.Fatal("validate accepted a secret input")
		}
	})
}

func TestProjectNodePolicyLookupRejectsDuplicateKey(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("worker:\n  task.ready:\n    rules:\n      - lookup: {on: payload.kind, entries: [{key: a, value: first}, {key: a, value: second}], into: computed.choice}\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = projectNodeDeclarationsValue(snapshot.Document("nodes.yaml").Root())
	if err == nil || !strings.Contains(err.Error(), "duplicate lookup key") {
		t.Fatalf("expected duplicate lookup key rejection, got %v", err)
	}
}
