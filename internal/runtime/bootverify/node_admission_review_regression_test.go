package bootverify

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestReviewer2492SupportedSource(t *testing.T) {
	for _, tc := range []struct{ name, before, after string }{
		{"control", "max_items: 100", "max_items: 100"},
		{"fractional_max", "max_items: 100", "max_items: 1.5"},
		{"float_integer_max", "max_items: 100", "max_items: 1.0"},
		{"empty_identity", "as: account_id", "as: account_id\n        identity: ''"},
		{"null_identity", "as: account_id", "as: account_id\n        identity: null"},
		{"forbidden_empty_op", "target_field: account_ids", "target_field: account_ids\n            op: ''"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := canonicalrouting.CopyNotifyAllChildren(t, canonicalrouting.NotifyAllChildrenOptions{})
			path := filepath.Join(root, "portfolio", "nodes.yaml")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(raw), tc.before) {
				t.Fatal("insertion not found")
			}
			if err := os.WriteFile(path, []byte(strings.ReplaceAll(string(raw), tc.before, tc.after)), 0600); err != nil {
				t.Fatal(err)
			}
			repo := canonicalrouting.RepoRoot(t)
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
			if tc.name != "control" {
				if err == nil || !strings.Contains(err.Error(), "nodes.yaml:") {
					t.Fatalf("invalid authored syntax must fail source admission, before boot: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if findings := Run(context.Background(), semanticview.Wrap(bundle), Options{}).HardInvalidities(); len(findings) > 0 {
				t.Fatalf("unchanged source control: %#v", findings)
			}
		})
	}
}
