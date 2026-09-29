package bootverify

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestNodeFanInDerivedFieldPresenceThroughSupportedSource(t *testing.T) {
	for _, tc := range []struct {
		name, anchor, field, diagnostic string
		example                         canonicalrouting.ArtifactID
	}{
		{"stream dedup", "        from: payload\n", "        dedup_by", "must not redeclare fan-in dedup_by", canonicalrouting.FanInStream},
		{"stream window", "        from: payload\n", "        window", "must not redeclare fan-in window", canonicalrouting.FanInStream},
		{"barrier members", "          from: entity.expected_operating_ids\n", "          by", "join.members.by derives from resolution.dedup_by", canonicalrouting.FanInBarrier},
		{"barrier window", "          from: entity.period_id\n", "          by", "join.window.by derives from resolution.window", canonicalrouting.FanInBarrier},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, shape := range []struct{ name, value string }{
				{"missing", ""}, {"null", "null"}, {"empty scalar", "''"}, {"scalar", "payload.operating_id"},
				{"empty sequence", "[]"}, {"sequence", "[payload.operating_id]"}, {"empty mapping", "{}"}, {"mapping", "{value: payload.operating_id}"},
				{"alias empty", "&empty ''"},
			} {
				t.Run(shape.name, func(t *testing.T) {
					root := canonicalrouting.CopyExample(t, tc.example)
					path := filepath.Join(root, "portfolio", "nodes.yaml")
					body, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					if strings.Count(string(body), tc.anchor) != 1 {
						t.Fatal("fixture must contain exactly one scoped insertion point")
					}
					if shape.name != "missing" {
						insertion := tc.field + ": " + shape.value + "\n"
						if shape.name == "alias empty" {
							body = []byte(strings.Replace(string(body), "  execution_type: system_node\n", "  execution_type: system_node\n  description: &empty ''\n", 1))
							insertion = tc.field + ": *empty\n"
						}
						body = []byte(strings.Replace(string(body), tc.anchor, tc.anchor+insertion, 1))
						if err := os.WriteFile(path, body, 0o600); err != nil {
							t.Fatal(err)
						}
					}
					repo := canonicalrouting.RepoRoot(t)
					bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
					if err != nil {
						if shape.name == "missing" || !strings.Contains(err.Error(), "nodes.yaml") {
							t.Fatalf("unexpected source admission error: %v", err)
						}
						return
					}
					findings := Run(context.Background(), semanticview.Wrap(bundle), Options{}).HardInvalidities()
					if shape.name == "missing" {
						if len(findings) != 0 {
							t.Fatalf("pin-owned derivation rejected: %#v", findings)
						}
					} else if !reportContains(findings, "composition_connect_validation", tc.diagnostic) {
						t.Fatalf("authored derived field escaped rejection: %#v", findings)
					}
				})
			}
		})
	}
}
