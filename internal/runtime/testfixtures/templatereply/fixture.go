package templatereply

import (
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

const (
	RequesterFlowID     = "requester"
	RequesterRequestPin = "provider_requested"
	RequesterReplyPin   = "provider_replied"
	RequesterNodeID     = "requester-node"
	ProviderFlowID      = "provider"
	ProviderRequestPin  = "provider_requested"
	ProviderReplyPin    = "provider_replied"
	ProviderNodeID      = "provider-node"
	RequestEvent        = "provider.requested"
	ReplyEvent          = "provider.replied"
	CorrelationKey      = "provider_request_id"
	ContinuationHuman   = "human_task"
)

type Options struct {
	MissingRepliesTo          bool
	MissingCorrelationCarry   bool
	AmbiguousRequestEdge      bool
	MismatchedProvider        bool
	DefaultEventIDCorrelation bool
	ExplicitCorrelation       bool
	OptionalReplyCorrelation  bool
	ProviderContinuation      string
	ContinuationRequestKey    string
	ContinuationAccountID     string
}

func LoadBundle(t testing.TB, opts Options) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle, err := LoadBundleResult(t, opts)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func LoadBundleResult(t testing.TB, opts Options) (*runtimecontracts.WorkflowContractBundle, error) {
	t.Helper()
	root := Write(t, opts)
	return runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		canonicalrouting.RepoRoot(t),
		root,
		runtimecontracts.DefaultPlatformSpecFile(canonicalrouting.RepoRoot(t)),
	)
}

func LoadSource(t testing.TB, opts Options) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(LoadBundle(t, opts))
}

func Write(t testing.TB, opts Options) string {
	t.Helper()
	if opts == (Options{}) || opts.DefaultEventIDCorrelation {
		return canonicalrouting.ExampleRoot(t, canonicalrouting.TemplateReply)
	}
	root := canonicalrouting.CopyExample(t, canonicalrouting.TemplateReply)
	if requiresExplicitCorrelation(opts) {
		addExplicitCorrelation(t, root)
	}
	requesterSchema := filepath.Join(root, "flows", RequesterFlowID, "schema.yaml")
	if opts.MissingRepliesTo {
		canonicalrouting.ReplaceFile(t, requesterSchema, "          replies_to: provider_requested\n", "")
	}
	if opts.MissingCorrelationCarry {
		canonicalrouting.ReplaceFile(t, requesterSchema, "        carries: [provider_request_id]\n", "")
	}
	packageFile := filepath.Join(root, "package.yaml")
	if opts.AmbiguousRequestEdge {
		canonicalrouting.ReplaceFile(t, packageFile,
			"  - from: requester.provider_requested\n    to: provider.provider_requested\n",
			"  - from: requester.provider_requested\n    to: provider.provider_requested\n  - from: requester.provider_requested\n    to: provider.provider_requested\n")
	}
	if opts.MismatchedProvider {
		addMismatchedProvider(t, root)
	}
	if opts.ProviderContinuation != "" {
		addProviderContinuation(t, root, opts)
	}
	return root
}

func requiresExplicitCorrelation(opts Options) bool {
	return opts.ExplicitCorrelation || opts.MissingRepliesTo || opts.MissingCorrelationCarry ||
		opts.AmbiguousRequestEdge || opts.MismatchedProvider || opts.OptionalReplyCorrelation ||
		opts.ProviderContinuation != ""
}

func addExplicitCorrelation(t testing.TB, root string) {
	t.Helper()
	requesterSchema := filepath.Join(root, "flows", RequesterFlowID, "schema.yaml")
	canonicalrouting.ReplaceFile(t, requesterSchema,
		"          replies_to: provider_requested\n",
		"          replies_to: provider_requested\n          correlation_key: provider_request_id\n")
	canonicalrouting.ReplaceFile(t, requesterSchema,
		"      - name: provider_requested\n        event: provider.requested\n",
		"      - name: provider_requested\n        event: provider.requested\n        key: provider_request_id\n        carries: [provider_request_id]\n")
	providerSchema := filepath.Join(root, "flows", ProviderFlowID, "schema.yaml")
	canonicalrouting.ReplaceFile(t, providerSchema,
		"      - name: provider_replied\n        event: provider.replied\n",
		"      - name: provider_replied\n        event: provider.replied\n        key: provider_request_id\n        carries: [provider_request_id]\n")
	for _, flowID := range []string{RequesterFlowID, ProviderFlowID} {
		eventsFile := filepath.Join(root, "flows", flowID, "events.yaml")
		canonicalrouting.ReplaceFile(t, eventsFile, "provider.requested:\n", "provider.requested:\n  provider_request_id: text\n")
		canonicalrouting.ReplaceFile(t, eventsFile, "provider.replied:\n", "provider.replied:\n  provider_request_id: text\n")
	}
	initiatorEvents := filepath.Join(root, "flows", "initiator", "events.yaml")
	canonicalrouting.ReplaceFile(t, initiatorEvents,
		"request.submitted:\n  account_id: text\n",
		"request.submitted:\n  account_id: text\n  provider_request_id: text\n")
	canonicalrouting.ReplaceFile(t, initiatorEvents,
		"requester.requested:\n  account_id: text\n",
		"requester.requested:\n  account_id: text\n  provider_request_id: text\n")
	requesterEvents := filepath.Join(root, "flows", RequesterFlowID, "events.yaml")
	canonicalrouting.ReplaceFile(t, requesterEvents,
		"requester.requested:\n  account_id: text\n",
		"requester.requested:\n  account_id: text\n  provider_request_id: text\n")
	initiatorNodes := filepath.Join(root, "flows", "initiator", "nodes.yaml")
	canonicalrouting.ReplaceFile(t, initiatorNodes,
		"    request.submitted:\n      emit:\n        event: requester.requested\n        fields:\n          account_id: payload.account_id\n",
		"    request.submitted:\n      emit:\n        event: requester.requested\n        fields:\n          account_id: payload.account_id\n          provider_request_id: payload.provider_request_id\n")
	requesterNodes := filepath.Join(root, "flows", RequesterFlowID, "nodes.yaml")
	canonicalrouting.ReplaceFile(t, requesterNodes,
		"          account_id: payload.account_id\n",
		"          provider_request_id: payload.provider_request_id\n          account_id: payload.account_id\n")
	providerNodes := filepath.Join(root, "flows", ProviderFlowID, "nodes.yaml")
	canonicalrouting.ReplaceFile(t, providerNodes,
		"        fields:\n          account_id: payload.account_id\n",
		"        fields:\n          provider_request_id: payload.provider_request_id\n          account_id: payload.account_id\n")
}

func addMismatchedProvider(t testing.TB, root string) {
	t.Helper()
	packageFile := filepath.Join(root, "package.yaml")
	canonicalrouting.ReplaceFile(t, packageFile,
		"  - id: provider\n    flow: provider\n    mode: static\n",
		"  - id: provider\n    flow: provider\n    mode: static\n  - id: other-provider\n    flow: other-provider\n    mode: static\n")
	canonicalrouting.ReplaceFile(t, packageFile,
		"  - from: provider.provider_replied\n    to: requester.provider_replied\n",
		"  - from: other-provider.provider_replied\n    to: requester.provider_replied\n")
	canonicalrouting.CopyTree(t, filepath.Join(root, "flows", ProviderFlowID), filepath.Join(root, "flows", "other-provider"))
	otherSchema := filepath.Join(root, "flows", "other-provider", "schema.yaml")
	canonicalrouting.ReplaceFile(t, otherSchema, "name: provider\n", "name: other-provider\n")
}

func addProviderContinuation(t testing.TB, root string, opts Options) {
	t.Helper()
	providerSchema := filepath.Join(root, "flows", ProviderFlowID, "schema.yaml")
	switch opts.ProviderContinuation {
	case ContinuationHuman:
		canonicalrouting.ReplaceFile(t, providerSchema, "  outputs:\n", `      - name: human_task_deferred
        event: human_task.deferred
      - name: human_task_approved
        event: human_task.approved
  outputs:
`)
		canonicalrouting.WriteFile(t, root, "flows/provider/nodes.yaml", humanProviderNodes(opts))
	default:
		t.Fatalf("unsupported provider continuation %q", opts.ProviderContinuation)
	}
}

func humanProviderNodes(opts Options) string {
	requestKey := opts.ContinuationRequestKey
	if requestKey == "" {
		requestKey = "human-request"
	}
	accountID := opts.ContinuationAccountID
	if accountID == "" {
		accountID = "account-a"
	}
	return `provider-node:
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
          provider_request_id: {literal: ` + requestKey + `}
          account_id: {literal: ` + accountID + `}
          result: {literal: approved}
`
}
