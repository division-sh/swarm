package bootverify

import (
	"errors"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
)

func TestRunRejectsAncestorEventNameWithoutReceiverLocalDeclarationOrConnect(t *testing.T) {
	root := canonicalrouting.CopyAncestorEventWithoutReceiverMembership(t)
	repo := repoRootForBootverifyTest(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if bundle != nil || !errors.Is(err, runtimecontracts.ErrInvalidField) {
		t.Fatalf("ancestor event supplied receiver membership: bundle=%v err=%v", bundle, err)
	}
	for _, want := range []string{`handler "root.started"`, "undeclared event", "declare the event in this flow or bind its producer-owned input"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("receiver admission lost %q: %v", want, err)
		}
	}
}
