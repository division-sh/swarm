package swarmflowtest

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/requiredagentsparentconnect"
)

func TestCatalogCollectBootIssues_DoesNotReintroduceFlowLocalEventWarningDrift(t *testing.T) {
	repoRoot := repoRootForTest(t)
	bundle := catalogLoadBootBundle(t, filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-child-flow-local-events"))

	issues := catalogCollectBootIssues(bundle)
	for _, issue := range issues {
		if issue.Category != "EVENT-NO-SCHEMA" {
			continue
		}
		if strings.Contains(issue.Message, "child/child.internal") || strings.Contains(issue.Message, "child/child.done") {
			t.Fatalf("unexpected flow-local event warning drift: %#v", issues)
		}
	}
}

func TestCatalogCollectBootIssues_DoesNotReintroduceFlowOutputConsumerWarningDrift(t *testing.T) {
	bundle := catalogLoadBootBundle(t, requiredagentsparentconnect.Write(t))

	issues := catalogCollectBootIssues(bundle)
	for _, issue := range issues {
		if issue.Category == "EVENT-NO-CONSUMER" && strings.Contains(issue.Message, "work.ready") {
			t.Fatalf("unexpected flow-owned output consumer warning drift: %#v", issues)
		}
	}
}
