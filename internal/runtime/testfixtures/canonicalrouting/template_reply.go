package canonicalrouting

import (
	"path/filepath"
	"strings"
	"testing"
)

// TemplateReplyVariant is the closed set of positive reply-routing variants
// proven by the reply conformance suite. Callers cannot supply routing YAML.
type TemplateReplyVariant uint8

const (
	TemplateReplyExplicitCorrelation TemplateReplyVariant = iota + 1
	TemplateReplyHumanContinuation
)

// TemplateReplyVariantOptions contains payload literals only. Routing
// authority is fixed by Variant and owned by this package.
type TemplateReplyVariantOptions struct {
	Variant    TemplateReplyVariant
	RequestKey string
	AccountID  string
}

// CopyTemplateReplyVariant derives one closed positive variant from the
// checked-in template-reply artifact.
func CopyTemplateReplyVariant(t testing.TB, opts TemplateReplyVariantOptions) string {
	t.Helper()
	root := CopyExample(t, TemplateReply)
	applyTemplateReplyExplicitCorrelation(t, root)
	switch opts.Variant {
	case TemplateReplyExplicitCorrelation:
	case TemplateReplyHumanContinuation:
		applyTemplateReplyHumanContinuation(t, root, opts.RequestKey, opts.AccountID)
	default:
		t.Fatalf("unsupported template reply variant %d", opts.Variant)
	}
	return root
}

func applyTemplateReplyExplicitCorrelation(t testing.TB, root string) {
	t.Helper()
	requesterSchema := filepath.Join(root, "flows", "requester", "schema.yaml")
	applyClosedReplacement(t, requesterSchema,
		"          replies_to: provider_requested\n",
		"          replies_to: provider_requested\n          correlation_key: provider_request_id\n")
	applyClosedReplacement(t, requesterSchema,
		"      - name: provider_requested\n        event: provider.requested\n",
		"      - name: provider_requested\n        event: provider.requested\n        key: provider_request_id\n        carries: [provider_request_id]\n")
	providerSchema := filepath.Join(root, "flows", "provider", "schema.yaml")
	applyClosedReplacement(t, providerSchema,
		"      - name: provider_replied\n        event: provider.replied\n",
		"      - name: provider_replied\n        event: provider.replied\n        key: provider_request_id\n        carries: [provider_request_id]\n")
	for _, flowID := range []string{"requester", "provider"} {
		eventsFile := filepath.Join(root, "flows", flowID, "events.yaml")
		applyClosedReplacement(t, eventsFile, "provider.requested:\n", "provider.requested:\n  provider_request_id: text\n")
		applyClosedReplacement(t, eventsFile, "provider.replied:\n", "provider.replied:\n  provider_request_id: text\n")
	}
	initiatorEvents := filepath.Join(root, "flows", "initiator", "events.yaml")
	applyClosedReplacement(t, initiatorEvents,
		"request.submitted:\n  account_id: text\n",
		"request.submitted:\n  account_id: text\n  provider_request_id: text\n")
	applyClosedReplacement(t, initiatorEvents,
		"requester.requested:\n  account_id: text\n",
		"requester.requested:\n  account_id: text\n  provider_request_id: text\n")
	requesterEvents := filepath.Join(root, "flows", "requester", "events.yaml")
	applyClosedReplacement(t, requesterEvents,
		"requester.requested:\n  account_id: text\n",
		"requester.requested:\n  account_id: text\n  provider_request_id: text\n")
	initiatorNodes := filepath.Join(root, "flows", "initiator", "nodes.yaml")
	applyClosedReplacement(t, initiatorNodes,
		"    request.submitted:\n      emit:\n        event: requester.requested\n        fields:\n          account_id: payload.account_id\n",
		"    request.submitted:\n      emit:\n        event: requester.requested\n        fields:\n          account_id: payload.account_id\n          provider_request_id: payload.provider_request_id\n")
	requesterNodes := filepath.Join(root, "flows", "requester", "nodes.yaml")
	applyClosedReplacement(t, requesterNodes,
		"          account_id: payload.account_id\n",
		"          provider_request_id: payload.provider_request_id\n          account_id: payload.account_id\n")
	providerNodes := filepath.Join(root, "flows", "provider", "nodes.yaml")
	applyClosedReplacement(t, providerNodes,
		"        fields:\n          account_id: payload.account_id\n",
		"        fields:\n          provider_request_id: payload.provider_request_id\n          account_id: payload.account_id\n")
}

func applyTemplateReplyHumanContinuation(t testing.TB, root, requestKey, accountID string) {
	t.Helper()
	requestKey = closedScalarLiteral(t, "request key", requestKey, "human-request")
	accountID = closedScalarLiteral(t, "account ID", accountID, "account-a")
	providerSchema := filepath.Join(root, "flows", "provider", "schema.yaml")
	applyClosedReplacement(t, providerSchema, "  outputs:\n", `      - name: human_task_deferred
        event: human_task.deferred
      - name: human_task_approved
        event: human_task.approved
  outputs:
`)
	writeClosedVariantFile(t, root, "flows/provider/nodes.yaml", `provider-node:
  id: provider-node
  execution_type: system_node
  subscribes_to: [provider.requested, human_task.deferred, human_task.approved]
  produces: [provider.replied]
  event_handlers:
    provider.requested: {}
    human_task.deferred: {}
    human_task.approved:
      emit:
        event: provider.replied
        fields:
          provider_request_id: {literal: `+requestKey+`}
          account_id: {literal: `+accountID+`}
          result: {literal: approved}
`)
}

func closedScalarLiteral(t testing.TB, label, value, fallback string) string {
	t.Helper()
	value = strings.TrimSpace(value)
	if value == "" {
		value = fallback
	}
	if strings.ContainsAny(value, "\r\n{}[],:#&*!|>'\"%@`") {
		t.Fatalf("template reply %s %q is not a plain YAML scalar", label, value)
	}
	return value
}

func writeClosedVariantFile(t testing.TB, root, relativePath, source string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relativePath))
	if err := writeFixtureFile(path, source); err != nil {
		t.Fatalf("write closed template reply variant %s: %v", path, err)
	}
}
