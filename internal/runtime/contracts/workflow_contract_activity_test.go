package contracts

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestActivityApprovalYAMLIsStrictAndCanonical(t *testing.T) {
	var valid ActivitySpec
	if err := yaml.Unmarshal([]byte("tool: provider.write\napproval: {decision: support_reply}\n"), &valid); err != nil {
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
			err := yaml.Unmarshal([]byte(tc.yaml), &got)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Unmarshal error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestActivityResultEventsMaterializeIntoCatalogSchemaAndProduces(t *testing.T) {
	bundle := &WorkflowContractBundle{
		Tools: map[string]ToolSchemaEntry{
			"source_scrape": {
				HandlerType: "http",
				EffectClass: string(ActivityEffectClassReadOnly),
				InputSchema: ToolInputSchema{
					Type:     "object",
					Required: []string{"url"},
					Properties: map[string]ToolInputSchema{
						"url": {Type: "string"},
					},
				},
				OutputSchema: ToolInputSchema{
					Type: "object",
					Properties: map[string]ToolInputSchema{
						"title": {Type: "string"},
					},
				},
				HTTP: &HTTPToolSpec{Method: "GET", URL: "https://example.test"},
			},
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
	success := "scanner_source_requested_source_scrape.succeeded"
	failure := "scanner_source_requested_source_scrape.failed"
	if _, ok := bundle.EventEntries()[success]; !ok {
		t.Fatalf("EventEntries missing generated success event %q", success)
	}
	if _, ok := bundle.ResolvedEventCatalog()[failure]; !ok {
		t.Fatalf("ResolvedEventCatalog missing generated failure event %q", failure)
	}
	produces := bundle.NodeEffectiveProduces("scanner")
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

func TestActivityEffectClassDefaults(t *testing.T) {
	cases := []struct {
		name        string
		class       ActivityEffectClass
		maxAttempts int
		backoff     string
		forkPolicy  ActivityForkPolicy
		supported   bool
	}{
		{
			name:        "read only",
			class:       ActivityEffectClassReadOnly,
			maxAttempts: 3,
			backoff:     "exponential",
			forkPolicy:  ActivityForkReexecuteRead,
			supported:   true,
		},
		{
			name:        "idempotent write",
			class:       ActivityEffectClassIdempotentWrite,
			maxAttempts: 2,
			backoff:     "exponential",
			forkPolicy:  ActivityForkReuseRecordedResult,
			supported:   false,
		},
		{
			name:        "non idempotent write",
			class:       ActivityEffectClassNonIdempotentWrite,
			maxAttempts: 1,
			backoff:     "none",
			forkPolicy:  ActivityForkRequireConfirmation,
			supported:   true,
		},
		{
			name:        "long running split",
			class:       ActivityEffectClassLongRunning,
			maxAttempts: 1,
			backoff:     "none",
			forkPolicy:  ActivityForkForbidReexecution,
			supported:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defaults := ActivityRetryDefaultsForEffectClass(tc.class)
			if defaults.MaxAttempts != tc.maxAttempts || defaults.Backoff != tc.backoff {
				t.Fatalf("ActivityRetryDefaultsForEffectClass(%q) = %#v, want max=%d backoff=%q", tc.class, defaults, tc.maxAttempts, tc.backoff)
			}
			if got := ActivityForkPolicyForEffectClass(tc.class); got != tc.forkPolicy {
				t.Fatalf("ActivityForkPolicyForEffectClass(%q) = %q, want %q", tc.class, got, tc.forkPolicy)
			}
			if got := SupportedActivityEffectClass(tc.class); got != tc.supported {
				t.Fatalf("SupportedActivityEffectClass(%q) = %v, want %v", tc.class, got, tc.supported)
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
