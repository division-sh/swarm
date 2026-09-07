package serveapp

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestReceiverCompositionUnroutedSourceFailsVerification(t *testing.T) {
	root := canonicalrouting.CopyReceiverEntitylessUnrouted(t)
	bundle := loadWorkflowValidationBundleAt(t, root)
	report := bootverify.Run(context.Background(), semanticview.Wrap(bundle), bootverify.Options{})
	findings := fmt.Sprint(report.Errors())
	if !strings.Contains(findings, "pin_target_resolution") || !strings.Contains(findings, "target_required_missing") {
		t.Fatalf("unrouted child output was not rejected at the earliest gate: %s", findings)
	}
}
