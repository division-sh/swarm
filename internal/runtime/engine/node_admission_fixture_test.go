package engine

import (
	"os"
	"path/filepath"
	"strings"
	"sync"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

// Fragment fixtures enter through the same source admission as complete bundles.
func loadNodeHandlerFixture(raw string) (runtimecontracts.SystemNodeEventHandler, error) {
	root, err := os.MkdirTemp("", "engine-node-admission-")
	if err != nil {
		return runtimecontracts.SystemNodeEventHandler{}, err
	}
	defer os.RemoveAll(root)
	for file, body := range map[string]string{
		"schema.yaml": "name: node-admission-fixture\nstages: []\n",
		"events.yaml": "proof.requested:\n  items: '[text]'\n",
		"nodes.yaml":  "worker:\n  execution_type: system_node\n  event_handlers:\n    proof.requested:\n      " + strings.ReplaceAll(strings.TrimSpace(raw), "\n", "\n      ") + "\n",
	} {
		if err := os.WriteFile(filepath.Join(root, file), []byte(body), 0600); err != nil {
			return runtimecontracts.SystemNodeEventHandler{}, err
		}
	}
	repo := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		return runtimecontracts.SystemNodeEventHandler{}, err
	}
	return bundle.Nodes["worker"].EventHandlers["proof.requested"], nil
}

// Struct-based executor fixtures borrow only the admitted authored marker. Their
// exact node/event/context identity is still qualified by the canonical owner.
var admittedFixtureRule = sync.OnceValues(func() (runtimecontracts.HandlerRuleEntry, error) {
	handler, err := loadNodeHandlerFixture("on_complete:\n  - {}\n")
	if err != nil {
		return runtimecontracts.HandlerRuleEntry{}, err
	}
	return handler.OnComplete[0], nil
})
