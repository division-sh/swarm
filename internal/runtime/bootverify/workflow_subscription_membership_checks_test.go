package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestRunRejectsAncestorEventNameWithoutReceiverLocalDeclarationOrConnect(t *testing.T) {
	root := canonicalrouting.CopyAncestorEventWithoutReceiverMembership(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.HardInvalidities(), "legacy_qualified_subscription", "root.started") {
		t.Fatalf("hard invalidities = %#v, want receiver-local subscription rejection", report.HardInvalidities())
	}
	for _, finding := range report.HardInvalidities() {
		if finding.CheckID != "legacy_qualified_subscription" || !strings.Contains(finding.Message, "root.started") {
			continue
		}
		for _, want := range []string{"receiver-local event", "input pin", "nearest common ancestor schema.yaml"} {
			if !strings.Contains(finding.Message+" "+finding.Remediation, want) {
				t.Fatalf("finding = %#v, want teaching text %q", finding, want)
			}
		}
		return
	}
	t.Fatal("receiver-local subscription finding not found")
}
