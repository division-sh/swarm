package bootverify

import (
	"context"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestRunAcceptsNormalizerReadingNestedPathBelowPayloadNamedField(t *testing.T) {
	root := canonicalrouting.CopyPayloadNamedField(t)
	repoRoot := repoRootForBootverifyTest(t)
	bundle := loadFixtureBundleAt(t, repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if reportContains(report.Errors(), "payload_field_coverage", "payload.payload.message.chat.id") || reportContains(report.Errors(), "payload_field_coverage", "payload.message.chat.id") {
		t.Fatalf("normalizer path rejected: %#v", report.Errors())
	}
}
