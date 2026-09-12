package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// CopySelectedInputAgentProbe keeps identical public names in different flows
// so admission must retain declaration ownership rather than infer it from ID.
func CopySelectedInputAgentProbe(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for _, flow := range []struct{ path, mode string }{{"", "static"}, {"child", "static"}, {"templ", "template"}} {
		for file, body := range map[string]string{
			"schema.yaml": "name: selected-input-agent\nmode: " + flow.mode + "\npins:\n  inputs:\n    events:\n      - {event: work.ready, source: external}\n",
			"events.yaml": "work.ready: {}\n",
			"agents.yaml": "worker:\n  id: worker\n  model: regular\n  intent:\n    inline: Complete the selected input.\n  subscriptions: [work.ready]\n",
		} {
			writeClosedVariantFile(t, root, filepath.Join(flow.path, file), body)
		}
	}
	return root
}

// CopySelectedRouteRecoveryInput keeps the original sibling fixture while
// giving the synthetic pending input a valid selected endpoint and recipient.
func CopySelectedRouteRecoveryInput(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	copyTree(t, filepath.Join(RepoRoot(t), "tests/tier11-flow-composition/test-sibling-both-instantiated-isolated"), root)
	ApplyOverlay(t, root, "events.yaml", "fork.cli.activate: {}\n")
	writeClosedVariantFile(t, root, "agents.yaml", "safe-agent:\n  id: safe-agent\n  model: regular\n  intent:\n    inline: Complete the selected input.\n  subscriptions: [fork.cli.activate]\n")
	return root
}
