package authoringview

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestR3DescribeEffectiveLiteralPolicyAndRules(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := c.LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(repo, "examples/routing/policy-rules"), c.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	view, err := Build(context.Background(), semanticview.Wrap(bundle), BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(view.Root)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"a.b":null`, `"value":3`, `"description":"business"`, `"override":false`, `"matches":{"classes"`} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("missing %s: %s", want, raw)
		}
	}
	view.Root.Policy["business"].(map[string]any)["value"] = 99
	view.Root.Rules["matches"].Validation.Rules[0].ID = "changed"
	if bundle.Policy.Values["business"].Value.(map[string]any)["value"] != 3 || bundle.Rules["matches"].Validation.Rules[0].ID != "M1" {
		t.Fatal("describe aliases admitted source")
	}
}
