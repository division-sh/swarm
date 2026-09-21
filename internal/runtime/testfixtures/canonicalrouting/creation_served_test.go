package canonicalrouting

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestServedCreationFixturesVerificationConsumersAgree(t *testing.T) {
	for _, tc := range []struct {
		name string
		copy func(testing.TB) string
	}{
		{"target_route", CopyRootIngressLegacyTemplateTargetRoute},
		{"auto_emit", CopyRootIngressLegacyTemplateAutoEmit},
		{"empire_outbox", CopyTemplateInstanceEmpireOutbox},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := RepoRoot(t)
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, tc.copy(t), contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			for _, finding := range bootverify.Run(context.Background(), semanticview.Wrap(bundle), bootverify.Options{}).HardInvalidities() {
				t.Errorf("%s: %s", finding.CheckID, finding.Message)
			}
		})
	}
}
