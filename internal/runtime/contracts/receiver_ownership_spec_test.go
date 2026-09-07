package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestReceiverCompositionAuthoritativeSpec(t *testing.T) {
	content, err := os.ReadFile(filepath.Join(handlerRuleIdentityGuardRepoRoot(t), "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := yaml.Unmarshal(content, &document); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		path              string
		required, retired []string
	}{
		{"contract_formats.event_schema.routing_derivation.route_plan_authority.compiled_connect_evaluation.target_owner_projection",
			[]string{"topology does not transfer state ownership", "optional work without a receiver entity is entityless", "receipt alone is not a delivery outcome", "release newer claim authority"},
			[]string{"proof minted by the compiled graph", "proves shared structural ownership"}},
		{"handler_specification.handler_fields.create_entity.default", []string{"admitted receiver ownership"}, []string{"(inherit)"}},
		{"flow_model.state_composition.ownership_semantics", []string{"Parent event identity is source/causal context only"}, nil},
		{"static_analyzer.slice_3a_pin_target_resolution.accepted_target_mechanisms.static_child_delivery_entity.rule",
			[]string{"receiver-owned", "Entityless child execution does not qualify", "complete persisted ParentRoute"}, nil},
	} {
		t.Run(check.path, func(t *testing.T) {
			var value any = document
			for _, key := range strings.Split(check.path, ".") {
				mapping, ok := value.(map[string]any)
				if !ok {
					t.Fatalf("missing authoritative spec owner %s at %s", check.path, key)
				}
				value, ok = mapping[key]
				if !ok {
					t.Fatalf("missing authoritative spec clause %s", check.path)
				}
			}
			encoded, err := yaml.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			text := strings.Join(strings.Fields(string(encoded)), " ")
			for _, phrase := range check.required {
				if !strings.Contains(text, phrase) {
					t.Errorf("required canonical rule missing: %q", phrase)
				}
			}
			for _, phrase := range check.retired {
				if strings.Contains(text, phrase) {
					t.Errorf("retired sharing interpretation survives: %q", phrase)
				}
			}
		})
	}
}
