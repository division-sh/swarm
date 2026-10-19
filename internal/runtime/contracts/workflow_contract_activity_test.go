package contracts

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestActivityApprovalYAMLIsStrictAndCanonical(t *testing.T) {
	var valid ActivitySpec
	if err := decodeNodeTestYAML([]byte("tool: provider.write\napproval: {decision: support_reply}\n"), &valid); err != nil {
		t.Fatal(err)
	}
	if valid.Approval == nil || valid.Approval.Decision != "support_reply" {
		t.Fatalf("approval = %#v", valid.Approval)
	}
	for _, tc := range []struct {
		name string
		yaml string
		want string
	}{
		{name: "scalar", yaml: "tool: provider.write\napproval: support_reply\n", want: "must be a mapping"},
		{name: "missing decision", yaml: "tool: provider.write\napproval: {}\n", want: "approval.decision is required"},
		{name: "unknown field", yaml: "tool: provider.write\napproval: {decision: support_reply, mode: once}\n", want: "mode"},
		{name: "noncanonical decision", yaml: "tool: provider.write\napproval: {decision: ' support_reply '}\n", want: "is not canonical"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got ActivitySpec
			err := decodeNodeTestYAML([]byte(tc.yaml), &got)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Unmarshal error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestActivityResultEventsMaterializeIntoCatalogSchemaAndProduces(t *testing.T) {
	bundle := &WorkflowContractBundle{
		Tools: map[string]ToolSchemaEntry{
			"source_scrape": MustToolSchemaEntry(
				WithToolHandler(ToolHandlerHTTP),

				WithToolSchemas(
					MustToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{
						"url": MustToolInputSchema(ToolSchemaString),
					}), ToolSchemaRequired("url")),
					MustToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{
						"title": MustToolInputSchema(ToolSchemaString),
					})),
				),
				WithToolHTTP(HTTPToolSpec{Method: "GET", URL: "https://example.test"}),
			),
		},
		Nodes: map[string]SystemNodeContract{
			"scanner": {
				EventHandlers: map[string]SystemNodeEventHandler{
					"source.requested": {
						Activity: ActivitySpec{
							Tool: "source_scrape",
							Input: map[string]ExpressionValue{
								"url": CELExpression("payload.url"),
							},
						},
					},
				},
			},
		},
	}
	compileRootContractTestFixture(bundle)
	success := "scanner_source_requested_source_scrape.succeeded"
	failure := "scanner_source_requested_source_scrape.failed"
	if _, ok := bundle.EventEntries()[success]; !ok {
		t.Fatalf("EventEntries missing generated success event %q", success)
	}
	if _, ok := bundle.ResolvedEventCatalog()[failure]; !ok {
		t.Fatalf("ResolvedEventCatalog missing generated failure event %q", failure)
	}
	produces := bundle.GeneratedActivityEventsForExecutableNode(identitytest.RootNode(t, "scanner"))
	if !stringSliceContains(produces, success) || !stringSliceContains(produces, failure) {
		t.Fatalf("NodeEffectiveProduces = %#v, want generated activity result events", produces)
	}
	registry := EventSchemaRegistryFromBundle(bundle)
	successSchema, ok := registry[success]
	if !ok {
		t.Fatalf("EventSchemaRegistry missing generated success schema")
	}
	props, ok := successSchema.Schema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("success schema properties = %#v", successSchema.Schema["properties"])
	}
	result, ok := props["result"].(map[string]any)
	if !ok {
		t.Fatalf("success result schema = %#v", props["result"])
	}
	resultProps, ok := result["properties"].(map[string]any)
	if !ok {
		t.Fatalf("result properties = %#v", result["properties"])
	}
	if _, ok := resultProps["title"]; !ok {
		t.Fatalf("result schema missing output field title: %#v", resultProps)
	}
}

func TestActivityPolicyRetiredFromCurrentSyntax(t *testing.T) {
	// USER6220888538 replaces class-driven defaults with one execution behavior.
	for _, tc := range []struct{ key, value string }{
		{"effect_class", "read_only"}, {"effect_class", "non_idempotent_write"},
		{"effect_class", "idempotent_write"}, {"effect_class", "long_running"},
		{"fork_policy", "reexecute_read"}, {"fork_policy", "reuse_recorded_result"},
		{"fork_policy", "require_manual_confirmation"}, {"fork_policy", "forbid_reexecution"},
		{"retry_max_attempts", "3"}, {"retry_backoff", "exponential"}, {"attempt", "2"},
	} {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			var spec ActivitySpec
			err := decodeNodeTestYAML([]byte("tool: provider.http\n"+tc.key+": "+tc.value+"\n"), &spec)
			if err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("retired activity policy did not refuse at source: %v", err)
			}
		})
	}
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
