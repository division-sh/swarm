package templateflowpilot

import (
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

// Options applies one deliberate negative mutation to the canonical
// select-or-create artifact.
type Options struct {
	BadConnectMapping            bool
	UnsupportedReceiverSelection bool
	ProducerTarget               bool
	ProducerBroadcast            bool
}

func LoadBundle(t testing.TB, opts Options) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := Write(t, opts)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		canonicalrouting.RepoRoot(t),
		root,
		runtimecontracts.DefaultPlatformSpecFile(canonicalrouting.RepoRoot(t)),
	)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func LoadSource(t testing.TB, opts Options) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(LoadBundle(t, opts))
}

func Write(t testing.TB, opts Options) string {
	t.Helper()
	root := canonicalrouting.CopyExample(t, canonicalrouting.TemplateSelectOrCreate)
	addDataAccumulationOverlay(t, root)
	applyNegativeMutation(t, root, opts)
	return root
}

func addDataAccumulationOverlay(t testing.TB, root string) {
	t.Helper()
	producerEvents := filepath.Join(root, "flows", "producer", "events.yaml")
	canonicalrouting.ReplaceFile(t, producerEvents,
		"account.requested:\n  account_id: text\n",
		"account.requested:\n  account_id: text\n  score: text\n  decision: text\n")
	canonicalrouting.ReplaceFile(t, producerEvents,
		"account.ready:\n  account_id: text\n",
		"account.ready:\n  account_id: text\n  score: text\n  decision: text\n")
	canonicalrouting.ReplaceFile(t, filepath.Join(root, "flows", "account", "events.yaml"),
		"account_id: text\n", "account_id: text\n  score: text\n  decision: text\n")
	producerNodes := filepath.Join(root, "flows", "producer", "nodes.yaml")
	canonicalrouting.ReplaceFile(t, producerNodes,
		"          account_id: payload.account_id\n",
		"          account_id: payload.account_id\n          score: payload.score\n          decision: payload.decision\n")
	accountSchema := filepath.Join(root, "flows", "account", "schema.yaml")
	canonicalrouting.ReplaceFile(t, accountSchema,
		"mode: template\n",
		"mode: template\ninitial_state: pending\nstates: [pending, done]\nterminal_states: [done]\n")
	accountEntities := filepath.Join(root, "flows", "account", "entities.yaml")
	canonicalrouting.ReplaceFile(t, accountEntities,
		"    _unused_reason: receiver instance identity\n",
		"    _unused_reason: receiver instance identity\n  score:\n    type: text\n  decision:\n    type: text\n")
	accountNodes := filepath.Join(root, "flows", "account", "nodes.yaml")
	canonicalrouting.ReplaceFile(t, accountNodes,
		"    account.ready: {}\n",
		`    account.ready:
      data_accumulation:
        writes:
          - source_field: account_id
            target_field: account_id
          - source_field: score
            target_field: score
          - source_field: decision
            target_field: decision
      advances_to: done
`)
}

func applyNegativeMutation(t testing.TB, root string, opts Options) {
	// routing-example-census: negative-mutation issue=none owner=examples.routing.template_select_or_create proof=TestTemplateFlowPilotConformance_FailClosedMatrix
	t.Helper()
	packageFile := filepath.Join(root, "package.yaml")
	if opts.BadConnectMapping {
		canonicalrouting.ReplaceFile(t, packageFile,
			"  - from: producer.account_ready\n    to: account.account_ready\n",
			"  - from: producer.account_ready\n    to: account.account_ready\n    using:\n      instance:\n        source: missing_account_id\n        target: account_id\n")
	}
	if opts.UnsupportedReceiverSelection {
		accountNodes := filepath.Join(root, "flows", "account", "nodes.yaml")
		canonicalrouting.ReplaceFile(t, accountNodes,
			"    account.ready:\n      data_accumulation:\n",
			"    account.ready:\n      select_entity:\n        by:\n          account_id: payload.account_id\n      data_accumulation:\n")
	}
	producerNodes := filepath.Join(root, "flows", "producer", "nodes.yaml")
	if opts.ProducerTarget {
		canonicalrouting.ReplaceFile(t, producerNodes,
			"        event: account.ready\n        fields:\n",
			"        event: account.ready\n        target:\n          flow: account\n          match:\n            account_id: payload.account_id\n        fields:\n")
	}
	if opts.ProducerBroadcast {
		canonicalrouting.ReplaceFile(t, producerNodes,
			"        event: account.ready\n        fields:\n",
			"        event: account.ready\n        broadcast: true\n        fields:\n")
	}
}
