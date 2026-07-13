package templatereply

import (
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
	var root string
	if requiresExplicitCorrelation(opts) {
		variant := canonicalrouting.TemplateReplyExplicitCorrelation
		switch opts.ProviderContinuation {
		case ContinuationHuman:
			variant = canonicalrouting.TemplateReplyHumanContinuation
		case "":
		default:
			t.Fatalf("unsupported provider continuation %q", opts.ProviderContinuation)
		}
		root = canonicalrouting.CopyTemplateReplyVariant(t, canonicalrouting.TemplateReplyVariantOptions{
			Variant:    variant,
			RequestKey: opts.ContinuationRequestKey,
			AccountID:  opts.ContinuationAccountID,
		})
	} else {
		root = canonicalrouting.CopyExample(t, canonicalrouting.TemplateReply)
	}
	if opts.MissingRepliesTo {
		canonicalrouting.ApplyTemplateReplyNegativeMutation(t, root, canonicalrouting.TemplateReplyMissingRepliesTo)
	}
	if opts.MissingCorrelationCarry {
		canonicalrouting.ApplyTemplateReplyNegativeMutation(t, root, canonicalrouting.TemplateReplyMissingCorrelationCarry)
	}
	if opts.AmbiguousRequestEdge {
		canonicalrouting.ApplyTemplateReplyNegativeMutation(t, root, canonicalrouting.TemplateReplyAmbiguousRequestEdge)
	}
	if opts.MismatchedProvider {
		canonicalrouting.ApplyTemplateReplyNegativeMutation(t, root, canonicalrouting.TemplateReplyMismatchedProvider)
	}
	return root
}

func requiresExplicitCorrelation(opts Options) bool {
	return opts.ExplicitCorrelation || opts.MissingRepliesTo || opts.MissingCorrelationCarry ||
		opts.AmbiguousRequestEdge || opts.MismatchedProvider || opts.OptionalReplyCorrelation ||
		opts.ProviderContinuation != ""
}
