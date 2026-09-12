package canonicalrouting

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestArtifactPublicationVerificationConsumersAgree(t *testing.T) {
	for _, mode := range []string{"root", "static"} {
		t.Run(mode, func(t *testing.T) {
			repo := RepoRoot(t)
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, CopyPublicationArtifact(t, mode), contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			for _, finding := range bootverify.Run(context.Background(), semanticview.Wrap(bundle), bootverify.Options{}).HardInvalidities() {
				t.Errorf("%s: %s", finding.CheckID, finding.Message)
			}
		})
	}
}
