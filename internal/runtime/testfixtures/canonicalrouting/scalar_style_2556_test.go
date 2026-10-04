package canonicalrouting

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestScalar2556GeneratedIdentifiersRemainData(t *testing.T) {
	root := CopyAgentSlugAdmission(t, "scalar-style", "worker", "api-worker")
	bundle := loadA2IndependentProducer(t, root)
	agent, found := bundle.Agents["worker"]
	if !found || agent.ID != "api-worker" || agent.Role != "api-worker" || agent.ResolvedIntent.Coordinate != "prompts/api-worker.md" {
		t.Fatalf("agent identifier was interpreted as an expression literal: %#v", bundle.Agents)
	}
	if _, err := os.Stat(filepath.Join(root, "prompts", "api-worker.md")); err != nil {
		t.Fatalf("intent filename lost its data identity: %v", err)
	}
}

func TestScalar2556GeneratedReplyValuesRemainText(t *testing.T) {
	for _, tc := range []struct{ request, account, wantRequest, wantAccount string }{
		{"", "", "human-request", "account-a"},
		{"request-9", "customer-2", "request-9", "customer-2"},
	} {
		t.Run(tc.wantRequest, func(t *testing.T) {
			root := CopyTemplateReplyVariant(t, TemplateReplyVariantOptions{
				Variant: TemplateReplyHumanContinuation, RequestKey: tc.request, AccountID: tc.account,
			})
			bundle := loadA2IndependentProducer(t, root)
			fields := bundle.FlowTree.ByPath["provider"].Nodes["provider-node"].EventHandlers["human_task.approved"].Emit.Fields
			for field, want := range map[string]string{"provider_request_id": tc.wantRequest, "account_id": tc.wantAccount, "result": "approved"} {
				got := fields[field]
				if got.Kind != contracts.ExpressionKindLiteral || got.Literal != want {
					t.Fatalf("%s = %#v, want literal text %q", field, got, want)
				}
			}
		})
	}
}
